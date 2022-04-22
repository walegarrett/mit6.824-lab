package shardmaster

//
// Shardmaster clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) genMsgId() msgId {
	return msgId(nrand())
}

// 获取最新的配置
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.

	args.ClientId = ck.clientId
	args.MsgId = ck.genMsgId()

	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 添加新的副本组
func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.

	args.ClientId = ck.clientId
	args.MsgId = ck.genMsgId()

	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 删除一个副本组
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.

	args.ClientId = ck.clientId
	args.MsgId = ck.genMsgId()

	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 将当前所有者的一个分片移交给另一个组
func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.

	args.ClientId = ck.clientId
	args.MsgId = ck.genMsgId()

	args.Shard = shard // 分片Id
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
