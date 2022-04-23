package shardkv

// import "../shardmaster"
import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardmaster"
)

const (
	PullConfigInterval = time.Millisecond * 100
	PullShardsInterval = time.Millisecond * 200
	WaitCmdTimeOut = time.Millisecond * 500
	ReqCleanShardDataTimeOut = time.Millisecond * 500
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd // 根据serverId创建一个终端的函数

	gid          int // 当前server服务器所属的组

	// 用于维护配置的master的集群服务器
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	
	// Your definitions here.

	config shardmaster.Config // 最新的配置
	oldConfig shardmaster.Config // 旧配置
	notifyCh map[int64]chan NotifyMsg

	// 每个分片的：clientId -> msgId map，记录客户端的消息，避免执行相同的命令
	lastMsgIdx [shardmaster.NShards]map[int64]int64

	ownShards map[int]bool // 分片是否属于当前组，属于当前组的所有分片

	// 每个分片的key/value数据
	data [shardmaster.NShards]map[string]string 

	waitShardIds map[int]bool // 某个分片是否需要更新

	// configNum -> shard -> data，历史配置中的分片实际数据
	historyShards map[int]map[int]MergeShardData

	mck *shardmaster.Clerk // master客户端

	dead int32
	stopCh chan struct{}
	persister *raft.Persister

	lastApplyIndex int
	lastApplyTerm int

	pullConfigTimer *time.Timer
	pullShardsTimer *time.Timer

	// debug
	DebugLog bool
	lockStart time.Time
	lockEnd time.Time
	lockName string
}

func (kv *ShardKV) lock(m string) {
	kv.mu.Lock()
	kv.lockStart = time.Now()
	kv.lockName = m
}

func (kv *ShardKV) unlock(m string) {
	kv.lockEnd = time.Now()
	duration := kv.lockEnd.Sub(kv.lockStart)
	kv.lockName = ""
	kv.mu.Unlock()
	if duration > time.Millisecond * 2 {
		kv.log(fmt.Sprintf("lock too long:%s:%s\n", m, duration))
	}
}

func (kv *ShardKV) log(m string) {
	if kv.DebugLog {
		log.Printf("server me: %d, gid: %d, config: %+v, waitid: %+v, log: %s", 
					kv.me, kv.gid, kv.config, kv.waitShardIds, m)
	}
}
// 判断一个客户端对同一个分片的请求命令是否重复了
func (kv *ShardKV) isRepeated(shardId int, clientId int64, id int64) bool {
	if val, ok := kv.lastMsgIdx[shardId][clientId]; ok {
		return val == id
	}
	return false
}
//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	close(kv.stopCh)
	kv.log("kill kv get")
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persister = persister
	kv.mck = shardmaster.MakeClerk(kv.masters)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	// 一组中的所有server组成了raft集群系统，也就是说，
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, kv.gid)

	kv.stopCh = make(chan struct{})

	kv.DebugLog = false

	kv.data = [shardmaster.NShards]map[string]string{}
	for i, _ := range kv.data {
		kv.data[i] = make(map[string]string)
	}

	kv.lastMsgIdx = [shardmaster.NShards]map[int64]int64{}
	for i, _ := range kv.lastMsgIdx {
		kv.lastMsgIdx[i] = make(map[int64]int64)
	}

	kv.waitShardIds = make(map[int]bool)
	kv.historyShards = make(map[int]map[int]MergeShardData)
	config := shardmaster.Config {
		Num: 0,
		Shards: [shardmaster.NShards]int{},
		Groups: map[int][]string{},
	}
	kv.config = config
	kv.oldConfig = config
	
	kv.readSnapShotData(kv.persister.ReadSnapshot())

	kv.notifyCh = make(map[int64]chan NotifyMsg)

	kv.pullConfigTimer = time.NewTimer(PullConfigInterval)
	kv.pullShardsTimer = time.NewTimer(PullShardsInterval)

	go kv.waitApplyCh()
	go kv.pullConfig()
	go kv.pullShards()

	return kv
}

// 开启循环，拉取最新配置
func (kv *ShardKV) pullConfig() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullConfigTimer.C:
			// 只有leader才会更新配置
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.pullConfigTimer.Reset(PullConfigInterval)
				break
			}

			kv.lock("pullConfig")
			lastNum := kv.config.Num
			kv.log(fmt.Sprintf("pull config get last: %d", lastNum))
			kv.unlock("pullConfig")

			// 查询master客户端，查找最新的配置
			config := kv.mck.Query(lastNum + 1)

			if config.Num == lastNum + 1 {
				// 找到新的config
				kv.log(fmt.Sprintf("pull config found config: %+v, lastNum: %d", config, lastNum))
				kv.lock("pullConfig")
				if len(kv.waitShardIds) == 0 && kv.config.Num + 1 == config.Num {
					kv.log(fmt.Sprintf("pull config start config: %+v, lastNum: %d", config, lastNum))
					kv.unlock("pullConfig")
					// 向底层raft系统发送配置命令日志，使整个group的server达成一致并更新自己知道的最新配置
					kv.rf.Start(config.Copy())
				} else {
					kv.unlock("pullConfig")
				}
			}
			kv.pullConfigTimer.Reset(PullConfigInterval)
		}
	}
}

// 判断配置是否是有效的，根据key计算出的分片来判断
func (kv *ShardKV) configReady(configNum int, key string) Err {
	if configNum == 0 || configNum != kv.config.Num {
		kv.log("configReadyerr1")
		return ErrWrongGroup
	}

	shardId := key2shard(key)

	// 这个分片不是属于当前group管理的
	if _, ok := kv.ownShards[shardId]; !ok {
		kv.log("configReadyerr2")
		return ErrWrongGroup
	}

	if _, ok := kv.waitShardIds[shardId]; ok {
		kv.log("configReadyerr3")
		return ErrWrongGroup
	}

	return OK
}