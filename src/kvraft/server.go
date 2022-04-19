package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)
const WaitCmdTimeout = time.Millisecond * 500
const MaxLockTime = time.Millisecond * 10

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// Clerk通过（ClientId，MsgId）唯一标识了一个客户端的请求
	ClientId int64
	MsgId msgId

	// 唯一标识一个操作请求，相当于id
	ReqId int64

	Key string
	Value string
	Method string
}

// 用于通知
type NotifyMsg struct {
	Err Err
	Value string
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	stopCh chan struct{}

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// 真正的key/value数据
	data map[string]string

	// key->clientId, value->msgId，记录每一个client请求的msgId，防止重复apply
	lastApplied map[int64]msgId // last apply put/append msg

	// kev->ReqId, value->NotifyMsg
	msgNotify map[int64]chan NotifyMsg

	// 用于持久化
	persister *raft.Persister

	lastApplyIndex int
	lastApplyTerm int

	// debug
	DebugLog bool
	lockStart time.Time
	lockEnd time.Time
	lockName string
}

func (kv *KVServer) lock(m string) {
	kv.mu.Lock()
	kv.lockStart = time.Now()
	kv.lockName = m
}

func (kv *KVServer) unlock(m string) {
	kv.lockEnd = time.Now()
	duration := kv.lockEnd.Sub(kv.lockStart)
	kv.lockName = ""
	kv.mu.Unlock()
	if duration > MaxLockTime {
		kv.log(fmt.Sprintf("lock too long:%s:%s\n", m, duration))
	}
}

func (kv *KVServer) log(m string) {
	if kv.DebugLog {
		log.Printf("server me: %d, log:%s", kv.me, m)
	}
}

// 获取key对应的value
func (kv *KVServer) dataGet(key string) (err Err, val string) {
	if v, ok := kv.data[key]; ok {
		err = OK
		val = v
		return
	} else {
		err = ErrNoKey
		return
	}
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.log(fmt.Sprintf("in rpc getting, args:%+v", args))
	defer func ()  {
		kv.log(fmt.Sprintf("in rpc getting, args:%+v, reply:%+v", args, reply))
	}()

	_, isLeader := kv.rf.GetState()
	// 所有的操作均需要转向leader
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		MsgId: args.MsgId,
		ReqId: nrand(),
		Key: args.Key,
		Method: "Get",
		ClientId: args.ClientId,
	}

	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
	kv.log(fmt.Sprintf("get key:%s, err:%s, v:%s", op.Key, reply.Err, reply.Value))
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer func ()  {
		kv.log(fmt.Sprintf("in rpc putappend, args:%+v, reply:%+v", args, reply))
	}()

	op := Op {
		MsgId: args.MsgId,
		ReqId: nrand(),
		Key: args.Key,
		Value: args.Value,
		Method: args.Op,
		ClientId: args.ClientId,
	}
	reply.Err = kv.waitCmd(op).Err
}

// 删除通知通道
func (kv *KVServer) removeCh(id int64) {
	kv.lock("removeCh")
	delete(kv.msgNotify, id)
	kv.unlock("removeCh")
}

// 在这里等待操作被真正地应用apply到data数据库中
func (kv *KVServer) waitCmd(op Op) (res NotifyMsg) {
	kv.log("waitcmd func enter")

	// 向底层raft系统发送操作指令
	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	kv.lock("waitCmd")
	ch := make(chan NotifyMsg, 1)
	kv.msgNotify[op.ReqId] = ch
	kv.unlock("waitCmd")

	kv.log(fmt.Sprintf("start cmd: index:%d, term: %d, op:%+v", index, term, op))

	t := time.NewTimer(WaitCmdTimeout)
	defer t.Stop()

	select{
	case res = <-ch:
		kv.removeCh(op.ReqId)
		return
	case <-t.C:
		kv.removeCh(op.ReqId)
		res.Err = ErrTimeOut
		return
	}
}
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister

	kv.data = make(map[string]string)

	kv.lastApplied = make(map[int64]msgId)

	kv.stopCh = make(chan struct{})
	kv.applyCh = make(chan raft.ApplyMsg)

	// 这里把applyCh通道传给底层raft系统，这样可以在应用层接收到raft发出的应用通知（修改实际的数据）
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 读取持久化的数据
	kv.readPersist(kv.persister.ReadSnapshot())

	kv.DebugLog = false

	kv.msgNotify = make(map[int64]chan NotifyMsg)

	// You may need initialization code here.

	go kv.waitApplyCh()

	return kv
}

// 开启循环接收应用通道的信息，真正修改存储的数据
func (kv *KVServer) waitApplyCh() {
	for {
		select {
		case <- kv.stopCh:
			kv.log("stop ch get")
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				kv.log(fmt.Sprintf("get install snapshot, idx: %d", msg.CommandIndex))
				kv.lock("waitApplyCh_snapshot")
				kv.readPersist(kv.persister.ReadSnapshot())
				kv.unlock("waitApplyCh_snapshot")
				continue
			}
			msgIdx := msg.CommandIndex
			op := msg.Command.(Op)

			kv.lock("waitApplyCh")

			isRepeated := kv.isRepeated(op.ClientId, op.MsgId)

			switch op.Method {
			case "Put":
				if !isRepeated {
					kv.data[op.Key] = op.Value
					kv.lastApplied[op.ClientId] = op.MsgId
				}
			case "Append":
				if !isRepeated {
					_, v := kv.dataGet(op.Key)
					kv.data[op.Key] = v + op.Value
					kv.lastApplied[op.ClientId] = op.MsgId
				}
			case "Get":
			default:
				panic(fmt.Sprintf("unknown method:%s", op.Method))
			}

			kv.log(fmt.Sprintf("apply op: msgIdx:%d, op:%+v, data:%v", msgIdx, op, kv.data[op.Key]))

			// 保存快照
			kv.saveSnapshot(msgIdx)

			// 通知操作成功了
			if ch, ok := kv.msgNotify[op.ReqId]; ok {
				_, v := kv.dataGet(op.Key)
				ch <- NotifyMsg{
					Err: OK,
					Value: v,
				}
			}

			kv.unlock("waitApplyCh")

		}
	}
}

// 判断一个客户端发送的是否是同一个请求
func (kv *KVServer) isRepeated(clientId int64, id msgId) bool {
	if val, ok := kv.lastApplied[clientId]; ok {
		return val == id
	}
	return false
}
// 读取持久化的数据
func (kv *KVServer) readPersist (data []byte){
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData map[string]string
	var lastApplied map[int64]msgId

	if d.Decode(&kvData) != nil || d.Decode(&lastApplied) != nil {
		log.Fatal("kv read persist err")
	} else {
		kv.data = kvData
		kv.lastApplied = lastApplied
	}
}

// 保存快照，这里需要保存raft底层节点的快照
func (kv *KVServer) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 {
		return
	}

	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	data := kv.genSnapshotData()

	kv.rf.SavePersistAndSnapshot(logIndex, data)
}

// 生成需要快照的数据，data字典和lastApplied字典
func (kv *KVServer) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(kv.data); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.lastApplied); err != nil {
		panic(err)
	}

	data := w.Bytes()

	return data
}