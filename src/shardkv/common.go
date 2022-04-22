package shardkv

import "6.824/labgob"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// 每个分片由一个副本服务器组成，在这个副本服务器中运行了raft系统
//
// You will have to modify these definitions.
//

func init() {
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(FetchShardDataArgs{})
	labgob.Register(FetchShardDataReply{})
	labgob.Register(CleanShardDataArgs{})
	labgob.Register(CleanShardDataReply{})
	labgob.Register(MergeShardData{})
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut = "ErrTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	MsgId int64
	ConfigNum int
}

func (c *PutAppendArgs) copy() PutAppendArgs {
	r := PutAppendArgs {
		Key: c.Key,
		Value: c.Value,
		Op: c.Op,
		ClientId: c.ClientId,
		MsgId: c.MsgId,
		ConfigNum: c.ConfigNum,
	}
	return r
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	MsgId int64
	ConfigNum int
}

func (c *GetArgs) copy() GetArgs {
	r := GetArgs {
		Key: c.Key,
		ClientId: c.ClientId,
		MsgId: c.MsgId,
		ConfigNum: c.ConfigNum,
	}
	return r
}

type GetReply struct {
	Err   Err
	Value string
}

type FetchShardDataArgs struct {
	ConfigNum int
	ShardNum int
}

type FetchShardDataReply struct {
	Success bool
	MsgIndexes map[int64]int64
	Data map[string]string // 实际的key/value数据
}

func (reply *FetchShardDataReply) Copy() FetchShardDataReply {
	res := FetchShardDataReply {
		Success: reply.Success,
		Data: make(map[string]string),
		MsgIndexes: make(map[int64]int64),
	}
	for k, v := range reply.Data {
		res.Data[k] = v
	}
	for k, v := range reply.MsgIndexes {
		res.MsgIndexes[k] = v
	}
	return res
}

type CleanShardDataArgs struct {
	ConfigNum int
	ShardNum int
}

 type CleanShardDataReply struct {
	 Success bool
 }

 type MergeShardData struct {
	 ConfigNum int
	 ShardNum int
	 MsgIndexes map[int64]int64 // 客户端发起的命令请求，用于判断请求重复
	 Data map[string]string
 }