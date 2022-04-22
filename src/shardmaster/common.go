package shardmaster

import "6.824/labgob"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//
// master管理一系列编号的配置。每个配置都描述了一组副本组和向副本组分配碎片。
// 每当此分配需要更改时，master就会使用新分配创建一个新配置。
// 当密钥/值客户端和服务器想要了解当前（或过去）配置时，他们会联系master。
//

// The number of shards. 分片的数量
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid，分片所对应的组
	Groups map[int][]string // gid -> servers[]，每个组维护一个分片的所有副本服务器
}

const (
	OK = "OK"
	ErrWrongLeader = "wrongLeader"
	ErrTimeout = "timeout"
)

func init() {
	labgob.Register(Config{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
}

type Err string
type msgId int64

type CommonArgs struct {
	MsgId msgId
	ClientId int64
}

type JoinArgs struct {
	CommonArgs
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	CommonArgs
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	CommonArgs
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	CommonArgs
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

// 复制一个配置
func (c *Config) Copy() Config {
	config := Config {
		Num: c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for gid, s := range c.Groups {
		config.Groups[gid] = append([]string{}, s...)
	}
	return config
}