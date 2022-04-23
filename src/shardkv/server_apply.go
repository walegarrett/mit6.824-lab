package shardkv

import (
	"fmt"
	"time"
	"6.824/raft"
	"6.824/shardmaster"
)

// 开启循环，处理raft底层系统返回的apply信息，进行实际数据的本地apply和改变
func (kv *ShardKV) waitApplyCh() {
	defer func ()  {
		kv.log(fmt.Sprintf("kvkilled: %v", kv.killed()))
		time.Sleep(time.Millisecond * 1000)
		kv.log(fmt.Sprintf("kv apply killed, applych len: %d", len(kv.applyCh)))
	}()

	for {
		select {
		case <-kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				kv.log(fmt.Sprintf("get install sn, idx: %d", msg.CommandIndex))
				kv.applySnapshot()
				continue
			}
			kv.log(fmt.Sprintf("get applymsg: idx: %d, msg: %+v", msg.CommandIndex, msg))

			// 判断返回的是什么类型的命令，根据分类分别执行相应的操作
			if op, ok := msg.Command.(Op); ok {
				kv.applyOp(msg, op)
			} else if config, ok := msg.Command.(shardmaster.Config); ok {
				kv.applyConfig(msg, config)
			} else if mergeData, ok := msg.Command.(MergeShardData); ok {
				kv.applyMergeShardData(msg, mergeData)
			} else if cleanUp, ok := msg.Command.(CleanShardDataArgs); ok {
				kv.applyCleanUp(msg, cleanUp)
			} else {
				panic("applyerr")
			}
		}
	}
}

func (kv *ShardKV) applySnapshot() {
	kv.lock("waitApplyCh_sn")
	kv.readSnapShotData(kv.persister.ReadSnapshot())
	kv.unlock("waitApplyCh_sn")
}

// 将get,put等相应的操作应用到本地服务器中
func (kv *ShardKV) applyOp(msg raft.ApplyMsg, op Op) {
	msgIdx := msg.CommandIndex
	kv.lock("waitApplyCh")
	kv.log(fmt.Sprintf("in applyOp: %+v", op))

	// 根据key得到这个key所在的分片
	shardId := key2shard(op.Key)
	isRepeated := kv.isRepeated(shardId, op.ClientId, op.MsgId)

	if kv.configReady(op.ConfigNum, op.Key) == OK {
		switch op.Op {
		case "Put": // 进行真正的数据添加
			if !isRepeated {
				kv.data[shardId][op.Key] = op.Value
				kv.lastMsgIdx[shardId][op.ClientId] = op.MsgId
			}
		case "Append": // 进行真正的数据追加
			if !isRepeated {
				_, v := kv.dataGet(op.Key)
				kv.data[shardId][op.Key] = v + op.Value
				kv.lastMsgIdx[shardId][op.ClientId] = op.MsgId
			}
		case "Get":
		default:
			panic(fmt.Sprintf("unknown method: %s", op.Op))
		}

		kv.log(fmt.Sprintf("apply op: msgIdx: %d, op: %+v, data: %v", msgIdx, op, kv.data[shardId][op.Key]))

		// 保存一次快照
		kv.saveSnapshot(msgIdx)

		if ch, ok := kv.notifyCh[op.ReqId]; ok {
			nm := NotifyMsg {
				Err: OK,
			}
			if op.Op == "Get" {
				nm.Err, nm.Value = kv.dataGet(op.Key)
			}
			ch <- nm
		}
		kv.unlock("waitApplyCh")
	} else {
		// config not ready
		if ch, ok := kv.notifyCh[op.ReqId]; ok {
			ch <- NotifyMsg{Err:ErrWrongGroup}
		}
		kv.unlock("waitApplyCh")
		return
	}
}

func (kv *ShardKV) dataGet(key string) (err Err, val string) {
	if v, ok := kv.data[key2shard(key)][key]; ok {
		err = OK
		val = v
		return
	} else {
		err = ErrNoKey
		return err, ""
	}
}

// 将参数中的新配置应用到本地服务器
func (kv *ShardKV) applyConfig(msg raft.ApplyMsg, config shardmaster.Config) {
	kv.lock("applyConfig")
	defer kv.unlock("applyConfig")
	kv.log(fmt.Sprintf("in applyConfig: %+v", config))

	// 本地保留的配置是新的配置，旧的操作传递的配置过期了
	if config.Num <= kv.config.Num {
		kv.saveSnapshot(msg.CommandIndex)
		return
	}

	// 配置不是最新的
	if config.Num != kv.config.Num + 1 {
		panic("applyConfig err")
	}

	// 本服务器维护的旧配置
	oldConfig := kv.config.Copy()
	// 本服务器现在负责的所有分片
	ownshardIds := make([]int, 0, shardmaster.NShards)
	// 相较于旧配置，新配置中(本组中)新增加的分片
	newShardIds := make([]int, 0, shardmaster.NShards)
	// 相较于旧配置，新配置中(本组)删除了的分片
	deleteShardIds := make([]int, 0, shardmaster.NShards)

	// 遍历所有的分片
	for i := 0; i < shardmaster.NShards; i++ {
		if config.Shards[i] == kv.gid {
			ownshardIds = append(ownshardIds, i)
			if oldConfig.Shards[i] != kv.gid {
				// 本组中新增加的分片，需要注意的是，虽然分片被分配到了本组servers，但是分片数据没有更新到本地服务器
				newShardIds = append(newShardIds, i)
			}
		} else {
			if oldConfig.Shards[i] == kv.gid {
				deleteShardIds = append(deleteShardIds, i)
			}
		}
	}

	d := make(map[int]MergeShardData)
	// 遍历已经删除了的分片
	for _, shardId := range deleteShardIds {
		mergeShardData := MergeShardData {
			ConfigNum: oldConfig.Num, // 旧配置的ID
			ShardNum: shardId,
			Data: kv.data[shardId],
			MsgIndexes: kv.lastMsgIdx[shardId],
		}
		d[shardId] = mergeShardData
		// 从本服务器中维护的数据中删除已经删除了的分片
		kv.data[shardId] = make(map[string]string)
		kv.lastMsgIdx[shardId] = make(map[int64]int64)
	}

	// 将本组中已删除的分片记录到历史分片中
	kv.historyShards[oldConfig.Num] = d

	kv.ownShards = make(map[int]bool)
	for _, shardId := range ownshardIds {
		kv.ownShards[shardId] = true
	}

	// 将本组中新增加的分片标记为等待重新分片
	kv.waitShardIds = make(map[int]bool)
	if oldConfig.Num != 0 {
		for _, shardId := range newShardIds {
			kv.waitShardIds[shardId] = true
		}
	}

	// 更新本服务器维护的最新配置
	kv.config = config.Copy()
	kv.oldConfig = oldConfig

	kv.saveSnapshot(msg.CommandIndex)
}

// 增加本服务器中新增分片的所有key/value数据
func (kv *ShardKV) applyMergeShardData(msg raft.ApplyMsg, data MergeShardData) {
	kv.lock("applyMergeShardData")
	defer kv.unlock("applyMergeShardData")

	// 保存快照
	defer kv.saveSnapshot(msg.CommandIndex)

	kv.log(fmt.Sprintf("in applyMerge: %+v, msgidx: %d", data, msg.CommandIndex))

	if kv.config.Num != data.ConfigNum + 1 {
		return
	}
	// 分片不是需要进行更新的分片
	if _, ok := kv.waitShardIds[data.ShardNum]; !ok {
		return
	}

	kv.data[data.ShardNum] = make(map[string]string)
	kv.lastMsgIdx[data.ShardNum] = make(map[int64]int64)
	
	for k, v := range data.Data {
		kv.data[data.ShardNum][k] = v
	}
	for k, v := range data.MsgIndexes {
		kv.lastMsgIdx[data.ShardNum][k] = v
	}

	// 等待更新数据的新分片已经被填充新数据了，将其从map中删除，表示不能重复查询和填充数据
	delete(kv.waitShardIds, data.ShardNum)

	go kv.reqCleanShardData(kv.oldConfig, data.ShardNum)
}

// 从历史记录中删除指定的分片数据
func (kv *ShardKV) applyCleanUp(msg raft.ApplyMsg, data CleanShardDataArgs) {
	kv.lock("ApplyCleanUp")
	kv.log(fmt.Sprintf("applyCleanUp: msg: %+v, data: %+v", msg, data))
	if kv.historyDataExist(data.ConfigNum, data.ShardNum) {
		delete(kv.historyShards[data.ConfigNum], data.ShardNum)
	}
	kv.saveSnapshot(msg.CommandIndex)
	kv.unlock("ApplyCleanUp")
}