package shardkv

import (
	"fmt"
	"time"
	"6.824/shardmaster"
)

// 更新分片的key/value数据
func (kv *ShardKV) pullShards() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullShardsTimer.C:
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.lock("pullShards")
				// 遍历所有等待更新的分片
				for shardId, _ := range kv.waitShardIds {
					go kv.pullShard(shardId, kv.oldConfig)
				}
				kv.unlock("pullShards")
			}
			kv.pullShardsTimer.Reset(PullShardsInterval)
		}
	}
}

func (kv *ShardKV) pullShard(shardId int, config shardmaster.Config) {
	args := FetchShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardId,
	}

	// 遍历分片所属组中的所有副本服务器
	for _, s := range config.Groups[config.Shards[shardId]] {
		srv := kv.make_end(s) // 根据服务器建立终端
		reply := FetchShardDataReply{}
		if ok := srv.Call("ShardKV.FetchShardData", &args, &reply); ok {
			if reply.Success {
				kv.lock("pullShard")
				if _, ok = kv.waitShardIds[shardId]; ok && kv.config.Num == config.Num+1 {
					replyCopy := reply.Copy()
					mergeArgs := MergeShardData{
						ConfigNum:  args.ConfigNum,
						ShardNum:   args.ShardNum,
						Data:       replyCopy.Data,
						MsgIndexes: replyCopy.MsgIndexes,
					}
					kv.log(fmt.Sprintf("pullShard get data: %+v", mergeArgs))
					kv.unlock("pullShard")
					// 向raft底层发送命令请求
					_, _, isLeader := kv.rf.Start(mergeArgs)
					if !isLeader {
						break
					}
				} else {
					kv.unlock("pullShard")
				}
			}
		}
	}
}

func (kv *ShardKV) FetchShardData(args *FetchShardDataArgs, reply *FetchShardDataReply){
	kv.lock("fetchShardData")
	defer kv.unlock("fetchShardData")
	defer kv.log(fmt.Sprintf("resp fetchsharddata: args: %+v, reply: %+v", args, reply))

	if args.ConfigNum >= kv.config.Num {
		return
	}

	if configData, ok := kv.historyShards[args.ConfigNum]; ok {
		if shardData, ok := configData[args.ShardNum]; ok {
			reply.Success = true
			reply.Data = make(map[string]string)
			reply.MsgIndexes = make(map[int64]int64)
			for k, v := range shardData.Data {
				reply.Data[k] = v
			}
			for k, v := range shardData.MsgIndexes {
				reply.MsgIndexes[k] = v
			}
		}
	}

	return
}

// 请求清除指定配置中的分片数据
func (kv *ShardKV) reqCleanShardData(config shardmaster.Config, shardId int) {
	configNum := config.Num
	args := &CleanShardDataArgs{
		ConfigNum: configNum,
		ShardNum: shardId,
	}

	t := time.NewTimer(ReqCleanShardDataTimeOut)
	defer t.Stop()

	for {
		// 遍历该分片所在组中的所有副本服务器
		for _, s := range config.Groups[config.Shards[shardId]] {
			reply := &CleanShardDataReply{}
			srv := kv.make_end(s)
			done := make(chan bool, 1)
			r := false

			go func(args *CleanShardDataArgs, reply *CleanShardDataReply) {
				done <- srv.Call("ShardKV.CleanShardData", args, reply)
			}(args, reply)

			t.Reset(ReqCleanShardDataTimeOut)

			select {
			case <-kv.stopCh:
				return
			case r = <-done:
			case <-t.C:
			}
			if r && reply.Success {
				return
			}
		}

		kv.lock("reqCleanShardData")
		// 没有需要进行更新的分片了
		if kv.config.Num != configNum + 1 || len(kv.waitShardIds) == 0 {
			kv.unlock("reqCleanShardData")
			break
		}
		kv.unlock("reqCleanShardData")
	}
}

// 处理客户端的清除分片数据请求，需要先向raft系统发送日志使集群达成共识
func (kv *ShardKV) CleanShardData(args *CleanShardDataArgs, reply *CleanShardDataReply) {
	kv.lock("cleanShardData")

	if args.ConfigNum >= kv.config.Num {
		// 此时没有数据，无法清除相应的分片数据
		kv.unlock("cleanShardData")
		return
	}
	kv.unlock("cleanShardData")

	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		return
	}

	for i := 0; i < 10; i++ {
		kv.lock("cleanShardData")
		// 分片是否存在历史数据中
		exist := kv.historyDataExist(args.ConfigNum, args.ShardNum)
		kv.unlock("cleanShardData")
		if !exist {
			reply.Success = true
			return
		}
		time.Sleep(time.Millisecond * 20)
	}
	return
}

// 判断一个分片是否存在指定配置的历史分片数据中
func (kv *ShardKV) historyDataExist(configNum int, shardId int) bool {
	if _, ok := kv.historyShards[configNum]; ok {
		if _, ok = kv.historyShards[configNum][shardId]; ok {
			return true
		}
	}
	return false
}