package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"
	"6.824/labrpc"
)

const ChangeLeaderInterval = time.Millisecond * 20

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	clientId int64
	leaderId int	// 客户端知道的leader

	DebugLog bool 
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) genMsgId() msgId {
	return msgId(nrand())
}

func (ck *Clerk) log(v ...interface{}) {
	if ck.DebugLog {
		log.Printf("client:%d leaderId: %d, log:%v", ck.clientId, ck.leaderId, v)
	}
}

// 创建客户端
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.log("in getting: ", key)
	args := GetArgs{
		Key: key,
		MsgId: ck.genMsgId(),
		ClientId: ck.clientId,
	}

	leaderId := ck.leaderId

	for {
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.log("req server err not ok", leaderId)
		} else if reply.Err != OK {
			ck.log("req server err", leaderId, reply.Err)
		}

		if !ok {
			// 等待更换leader
			time.Sleep(ChangeLeaderInterval)
			// 尝试下一个server作为Leader，再次进入for循环，发起请求
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			ck.log("get kv", key, reply.Value)
			ck.leaderId = leaderId // 更新clert维护的leaderId
			return reply.Value
		case ErrNoKey:
			ck.log("get err no key", key)
			ck.leaderId = leaderId // 更新clert维护的leaderId
			return ""
		case ErrTimeOut:
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs {
		Key: key,
		Value: value,
		Op: op,
		MsgId: ck.genMsgId(),
		ClientId: ck.clientId,
	}
	leaderId := ck.leaderId

	for {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.log("req server err not ok", leaderId)
		} else if reply.Err != OK {
			ck.log("req server err", leaderId, ok, reply.Err)
		}

		if !ok {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			ck.log("put append key ok:", key, value)
			return
		case ErrNoKey: // 这种错误按理来说是不应该出现的
			log.Fatal("client putappend get err notkey")
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		case ErrTimeOut:
			continue
		default:
			log.Fatal("client unknown err", reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
