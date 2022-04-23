package shardkv

import (
	"fmt"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string // "Put" or "Append" Get
	ClientId  int64
	MsgId     int64
	ReqId     int64
	ConfigNum int
}

type NotifyMsg struct {
	Err   Err
	Value string
}

// 处理客户端发来的请求，先向底层raft系统发送日志，使得集群达成一致
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer func() {
		kv.log(fmt.Sprintf("in rpc get, args: %+v, reply: %+v", args, reply))
	}()

	op := Op {
		MsgId: args.MsgId,
		ClientId: args.ClientId,
		ReqId: nrand(),
		Key: args.Key,
		Op: "Get",
		ConfigNum: args.ConfigNum,
	}

	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

// 处理客户端发来的请求，先向底层raft系统发送日志，使得集群达成一致
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.log(fmt.Sprintf("in rpc putappend, args: %+v", args))
	op := Op {
		MsgId: args.MsgId,
		ClientId: args.ClientId,
		ReqId: nrand(),
		Key: args.Key,
		Value: args.Value,
		Op: args.Op,
		ConfigNum: args.ConfigNum,
	}
	reply.Err = kv.waitCmd(op).Err
	kv.log(fmt.Sprintf("in rpc putappend, args: %+v, reply: %+v", args, reply))
}

// 向底层raft系统发送操作命令，等待apply成功的通知
func (kv *ShardKV) waitCmd(op Op) (res NotifyMsg) {
	kv.log("waitcmd func enter")
	ch := make(chan NotifyMsg, 1)

	kv.lock("waitCmd")
	// 这里不检查 wait shard id
	// 若是新 leader，需要想办法产生本 term 的日志
	if op.ConfigNum == 0 || op.ConfigNum < kv.config.Num {
		kv.log("configReadyerr1")
		res.Err = ErrWrongGroup
		kv.unlock("waitCmd")
		return
	}
	kv.unlock("waitCmd")

	// 向底层raft系统发送响应的操作命令，使集群对指定的操作达成一致
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	kv.lock("waitCmd")
	// 当本地apply操作后，会向这个通道发送数据，在这里需要接收数据并返回结果
	kv.notifyCh[op.ReqId] = ch
	kv.unlock("waitCmd")

	kv.log(fmt.Sprintf("start cmd: index: %d, term: %d, op: %+v", index, term, op))

	t:= time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()

	select {
	case res = <-ch:
		kv.removeCh(op.ReqId)
		return
	case <-t.C:
		kv.removeCh(op.ReqId)
		res.Err = ErrTimeOut
		return
	}
}

func (kv *ShardKV) removeCh(id int64) {
	kv.lock("removeCh")
	delete(kv.notifyCh, id)
	kv.unlock("removeCh")
}