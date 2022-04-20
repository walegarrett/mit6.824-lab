package shardmaster

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const WaitCmdTimeOut = time.Millisecond * 500
const MaxLockTime = time.Millisecond * 10

type NotifyMsg struct {
	Err Err
	WrongLeader bool
	Config Config
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	stopCh chan struct{}

	msgNotify map[int64]chan NotifyMsg
	lastApplied map[int64]msgId

	configs []Config // indexed by config num，配置数组，保存了配置历史

	// debug
	DebugLog bool
	lockStart time.Time
	lockEnd time.Time
	lockName string
}

type Op struct {
	// Your data here.
	MsgId msgId
	ClientId int64
	ReqId int64
	Args interface{}
	Method string
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	res := sm.runCmd("Join", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	res := sm.runCmd("Leave", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	res := sm.runCmd("Move", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	defer func ()  {
		sm.log(fmt.Sprintf("sm query: args:%+v, reply:%+v\n", args, reply))
	}()

	sm.lock("query")
	if args.Num > 0 && args.Num < len(sm.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sm.getConfigByIndex(args.Num)
		sm.unlock("query")
		return
	}
	sm.unlock("query")
	
	res := sm.runCmd("Query", args.MsgId, args.ClientId, *args)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.Config
}

func (sm *ShardMaster) runCmd(method string, id msgId, clientId int64, args interface{}) (res NotifyMsg) {
	op := Op {
		MsgId: id,
		ClientId: clientId,
		ReqId: nrand(),
		Args: args,
		Method: method,
	}
	res = sm.waitCmd(op)
	return
}

// 向raft底层系统发送命令，用于分布式系统的一致性保证
func (sm *ShardMaster) waitCmd(op Op) (res NotifyMsg) {
	sm.log(fmt.Sprintf("%+v", op))
	// 向raft系统发送命令
	index, term, isLeader := sm.rf.Start(op)

	if !isLeader {
		res.Err = ErrWrongLeader
		res.WrongLeader = true
		return
	}
	sm.lock("waitCmd")
	ch := make(chan NotifyMsg, 1)
	sm.msgNotify[op.ReqId] = ch
	sm.unlock("waitCmd")
	sm.log(fmt.Sprintf("start cmd: index:%d, term:%d, op:%+v", index, term, op))

	t:= time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()

	select {
	case res = <-ch:
		sm.removeCh(op.ReqId)
		return
	case <-t.C:
		sm.removeCh(op.ReqId)
		res.WrongLeader = true
		res.Err = ErrTimeout
		return
	}
}
// 调整配置组，新的配置应该尽可能均匀地在整个组中分配碎片，并且应该移动尽可能少的碎片来实现这一目标
func (sm *ShardMaster) adjustConfig(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		// 只有一个组，将分片都放在这个组
		for k, _ := range config.Groups {
			for i, _ := range config.Shards {
				config.Shards[i] = k // 所有分片都放在这一个组
			}
		}
	} else if len(config.Groups) <= NShards {
		// 组的数量小于分片的数量，需要将多余的分片平均分到每个组中
		avg := NShards / len(config.Groups) // 每组负责的分片数量
		// 每个 gid 分 avg 个 shard，这里计算取余的结果，因为/运算是不精确的，会丢少值
		otherShardsCount := NShards - avg * len(config.Groups)
		
		needLoop := false
		lastGid := 0

		// 这里的循环始终将0号组作为中介，每次增加和减少都改变0号组，直至分配平衡
	LOOP:
		sm.log(fmt.Sprintf("config: %+v", config))
		// 所有的group
		var keys []int
		for k := range config.Groups {
			keys = append(keys, k)
		}
		sort.Ints(keys) // Ints 按升序对 int 切片进行排序。

		for _, gid := range keys {
			lastGid = gid
			count := 0 // 计算有多少分片是存储在这个group中
			for _, val := range config.Shards {
				if val == gid {
					count += 1
				}
			}
			// 判断是否需要改变
			if count == avg {
				continue // 这个分组负责的分片数量刚好等于平均值，不需要改变
			} else if count > avg && otherShardsCount == 0 {
				// 这个分组负责的分片大于平均值，需要分散一些分片
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg {
							// 将这些多余分片都托管给0号分组
							config.Shards[i] = 0
						} else {
							c += 1
						}
					}
				}
			} else if count > avg && otherShardsCount > 0 {
				// 减到 othersShardsCount 为 0
				// 若还 count > avg, set to 0
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg + otherShardsCount {
							config.Shards[i] = 0
						} else {
							if c == avg {
								otherShardsCount -= 1
							} else {
								c += 1
							}
						}
					}
				}
			} else {
				// 这个分组负责的分片小于平均值,count < avg, 此时有可能没有位置
				for i, val := range config.Shards {
					if count == avg {
						break
					}
					// 负担一些0号分组的任务到当前组group
					if val == 0 && count < avg {
						config.Shards[i] = gid
					}
				}

				if count < avg {
					sm.log(fmt.Sprintf("needLoop: %+v, %+v, %d", config, otherShardsCount, gid))
					needLoop = true
				}
			}
		}

		if needLoop {
			needLoop = false
			goto LOOP
		}

		if lastGid != 0 {
			for i, val := range config.Shards {
				if val == 0 {
					config.Shards[i] = lastGid
				}
			}
		}

	} else {
		// len(config.Groups) > NShards
		// 组数大于分片数，也就是说会有多余的组，每个组最多负责一个分片
		gids := make(map[int]int)
		// 存储没有分配组的分片
		emptyShards := make([]int, 0, NShards)
		for i, gid := range config.Shards {
			// 是否有分片没有组负责
			if gid == 0 {
				emptyShards = append(emptyShards, i)
				continue
			} 
			// 一个组是否负责了多个分片
			if _, ok := gids[gid]; ok {
				emptyShards = append(emptyShards, i)
				config.Shards[i] = 0
			} else {
				gids[gid] = 1
			}
		}
		n := 0
		if len(emptyShards) > 0 {
			var keys []int
			for k := range config.Groups {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, gid := range keys {
				// 为没有分配分片的组分配一些分片
				if _, ok := gids[gid]; !ok {
					config.Shards[emptyShards[n]] = gid
					n += 1
				}
				if n >= len(emptyShards) {
					break
				}
			}
		}
	}
}

// 添加新的副本组
func (sm *ShardMaster) join(args JoinArgs) {
	// 首先创建一个新的config
	config := sm.getConfigByIndex(-1)
	config.Num += 1

	for k, v := range args.Servers {
		config.Groups[k] = v
	}
	// 新的配置应该尽可能均匀地在整个组中分配碎片，并且应该移动尽可能少的碎片来实现这一目标。
	// 如果GID不是当前配置的一部分，master应该允许重复使用GID（即，应该允许GID加入，然后离开，然后再次加入）。
	sm.adjustConfig(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) leave(args LeaveArgs) {
	// 首先创建一个新的config
	config := sm.getConfigByIndex(-1)
	config.Num += 1

	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
		// 将已经分配给将要删除的组的分片重新分配给其他组，这里全分为0组，后续会进行调整
		for i, v := range config.Shards {
			if v == gid {
				config.Shards[i] = 0
			}
		}
	}

	sm.adjustConfig(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) move(args MoveArgs) {
	config := sm.getConfigByIndex(-1)
	config.Num += 1
	// 重新修改负责该分片的组，将该分片移动到指定的新组
	config.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, config)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.stopCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) removeCh(id int64) {
	sm.lock("removeCh")
	delete(sm.msgNotify, id)
	sm.unlock("removeCh")
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Op{})
	
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.stopCh = make(chan struct{})
	sm.DebugLog = false

	sm.lastApplied = make(map[int64]msgId)
	sm.msgNotify = make(map[int64]chan NotifyMsg)

	go sm.apply()

	return sm
}

// 开启循环，等待apply通道发送的apply请求，真正将命令应用于本地保存的配置数据中
func (sm *ShardMaster) apply() {
	for {
		select {
		case <-sm.stopCh:
			return
		case msg := <-sm.applyCh:
			if !msg.CommandValid {
				continue
			}
			sm.log(fmt.Sprintf("get msg: %+v", msg))

			op := msg.Command.(Op)

			sm.lock("apply")
			isRepeated := sm.isRepeated(op.ClientId, op.MsgId)
			if !isRepeated {
				switch op.Method {
				case "Join":
					sm.join(op.Args.(JoinArgs))
				case "Leave":
					sm.leave(op.Args.(LeaveArgs))
				case "Move":
					sm.move(op.Args.(MoveArgs))
				case "Query":
				default:
					panic("unknown method")
				}
			}
			res := NotifyMsg{
				Err: OK,
				WrongLeader: false,
			}

			if op.Method != "Query" {
				sm.lastApplied[op.ClientId] = op.MsgId
			} else {
				res.Config = sm.getConfigByIndex(op.Args.(QueryArgs).Num)
			}
			if ch, ok := sm.msgNotify[op.ReqId]; ok {
				sm.log(fmt.Sprintf("sm apply: op:%+v, res.config:%+v\n", op, res.Config))
				ch <- res
			}
			sm.unlock("apply")
		}
	}
}

// 根据id返回最新的配置
func (sm *ShardMaster) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sm.configs) {
		return sm.configs[len(sm.configs) - 1].Copy()
	} else {
		return sm.configs[idx].Copy()
	}
}
// 判断一个客户端发送的命令是否是重复的，不能处理重复的相同命令
func (sm *ShardMaster) isRepeated(clientId int64, id msgId) bool {
	if val, ok := sm.lastApplied[clientId]; ok {
		return val == id
	}
	return false
}
func (sm *ShardMaster) lock(m string) {
	sm.mu.Lock()
	sm.lockStart = time.Now()
	sm.lockName = m
}

func (sm *ShardMaster) unlock(m string) {
	sm.lockEnd = time.Now()
	duration := sm.lockEnd.Sub(sm.lockStart)
	sm.lockName = ""
	sm.mu.Unlock()
	if duration > MaxLockTime {
		sm.log(fmt.Sprintf("lock too long:%s:%s\n", m, duration))
	}
}

func (sm *ShardMaster) log(m string) {
	if sm.DebugLog {
		log.Printf("shardMaster me: %d, config: %+v, log: %s", sm.me, sm.configs, m)
	}
}