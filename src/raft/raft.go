package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// import "bytes"
// import "../labgob"

// 节点的角色
type Role int
const (
	Follower Role = 0
	Candidate Role = 1
	Leader Role = 2
)

// 相关时间间隔和超时设定
const (
	ElectionTimeout = time.Millisecond * 300 // 选举超时，必须大于心跳间隔
	HeartBeatTimeout = time.Millisecond * 150 // leader心跳超时
	ApplyInterval = time.Millisecond * 100 // apply log间隔
	RPCTimeout = time.Millisecond * 100
	MaxLockTime = time.Millisecond * 10 // debug
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// 日志条目的定义
type LogEntry struct {
	Term int // 当前log的任期
	Idx int // 当前日志条目在日志条目集中的索引，注意，这个日志条目不一定在logEntries数组中，因为有可能系统进行了快照
	Command interface{} // 当前log中存储的应用层命令
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role Role // 当前Raft节点的角色

	// 需要进行持久化的几个属性
	term int // 当前任期
	voteFor int // 向哪个server候选人投票
	logEntries []LogEntry // 日志条目集

	// server的两个易失属性
	commitIndex int // 已知的最大的已经被提交的日志条目索引号
	lastApplied int // 当前server被应用到状态机的日志条目索引号

	// 当前server作为领导人有的两个易失属性
	nextIndex []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值

	// 用于snapshot的属性
	lastSnapshotIndex int // 快照中的index
	lastSnapshotTerm int // 快照中的term

	// 相关定时器的定义
	electionTimer *time.Timer
	appendEntriesTimers []*time.Timer
	applyTimer *time.Timer

	// 通道相关定义
	notifyApplyCh chan struct{} // 通知应用层进行apply
	stopCh chan struct{}
	applyCh chan ApplyMsg

	// debug相关属性
	DebugLog bool
	lockStart time.Time
	lockEnd time.Time
	lockName string
	gid int
}

// 自定义lock与unlock
func (rf *Raft) lock(m string){
	rf.mu.Lock()
	rf.lockStart = time.Now()
	rf.lockName = m
}

func (rf *Raft) unlock(m string) {
	rf.lockEnd = time.Now()
	rf.lockName = ""
	duration := rf.lockEnd.Sub(rf.lockStart)
	if rf.lockName != "" && duration > MaxLockTime {
		rf.log("lock too long:%s:%s:iskill:%v", m, duration, rf.killed())
	}
	rf.mu.Unlock()
}

// 输出日志
func (rf *Raft) log(format string, a ...interface{}){
	if !rf.DebugLog {
		return
	}
	// 获取当前server的最新日志的任期和索引
	term, idx := rf.lastLogTermIndex()

	r := fmt.Sprintf(format, a...)
	s := fmt.Sprintf("gid:%d, me:%d, role:%v, term:%d, commitIdx:%v, snidx:%d, apply:%v, matchidx:%v, nextidx:%+v, lastlogterm:%d, idx:%d",
					  rf.gid, rf.me, rf.role, rf.term, rf.commitIndex, rf.lastSnapshotIndex, rf.lastApplied, rf.matchIndex, rf.nextIndex, term, idx)
	log.Printf("%s:log:%s\n", s, r)
}


// 获取当前server的最新日志条目所属任期和日志条目索引
func (rf *Raft) lastLogTermIndex() (int, int) {
	term := rf.logEntries[len(rf.logEntries) - 1].Term
	// 日志条目索引是快照的索引加上日志条目集的长度减一
	index := rf.lastSnapshotIndex + len(rf.logEntries) - 1;
	return term, index
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.lock("get state")
	defer rf.unlock("get state")
	return rf.term, rf.role == Leader
}

// 获取持久化的数据
func (rf *Raft) GetPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// 持久化相关的数据
	e.Encode(rf.term)
	e.Encode(rf.voteFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.logEntries)

	data := w.Bytes()
	return data
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.GetPersistData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var voteFor int
	var logs []LogEntry
	var commitIndex, lastSnapshotIndex, lastSnapshotTerm int

	if d.Decode(&term) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&commitIndex) != nil || d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil || d.Decode(&logs) != nil {
		log.Fatal("rf read persisit err")
	}else {
		rf.term = term
		rf.voteFor = voteFor
		rf.commitIndex = commitIndex
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.logEntries = logs
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lock("start")
	term := rf.term
	isLeader := rf.role == Leader
	// 获取最新的日志条目索引，此时考虑做了快照的情况
	_, lastIndex := rf.lastLogTermIndex()
	// 日志条目索引需要自增一表示将要填充新日志的位置
	index := lastIndex + 1
	// Your code here (2B).
	if isLeader {
		rf.logEntries = append(rf.logEntries, LogEntry{
			Term: rf.term,
			Command: command,
			Idx: index,
		})
		rf.matchIndex[rf.me] = index
		// 持久化
		rf.persist()
	}
	// 重置appendEntries的定时器为0表示立即运行appendEntries的定时任务
	rf.resetHeartBeatTimers()
	rf.unlock("start")
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.stopCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 随机一个选举超时时间
func randElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}

// 开始apply log
func (rf *Raft) startApplyLogs() {
	defer rf.applyTimer.Reset(ApplyInterval)

	rf.lock("applyLogsPhase1")
	var msgs []ApplyMsg
	if rf.lastApplied < rf.lastSnapshotIndex {
		msgs = make([]ApplyMsg, 0, 1)
		msgs = append(msgs, ApplyMsg{
			CommandValid: false,
			Command: "installSnapShot",
			CommandIndex: rf.lastSnapshotIndex,
		})
	} else if rf.commitIndex <= rf.lastApplied {
		msgs = make([]ApplyMsg, 0)
	} else {
		rf.log("rfapply")
		msgs = make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command: rf.logEntries[rf.getRealIdxByLogIndex(i)].Command,
				CommandIndex: i,
			})
		}
	}
	rf.unlock("applyLogsPhase1")

	for _, msg := range msgs {
		rf.applyCh <- msg
		rf.lock("applyLogsPhase2")
		rf.log("send applych idx:%d", msg.CommandIndex)
		rf.lastApplied = msg.CommandIndex
		rf.unlock("applyLogsPhase2")
	}
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, gid ...int) *Raft {
		
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh

	rf.DebugLog = false
	if len(gid) != 0 {
		rf.gid = gid[0]
	} else {
		rf.gid = -1
	}

	rf.stopCh = make(chan struct{})
	rf.term = 0
	rf.voteFor = -1 // 没给任何人投票
	rf.role = Follower // 初始为跟随者
	rf.logEntries = make([]LogEntry, 1) // 索引0用于存放lastSnapshot

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.electionTimer = time.NewTimer(randElectionTimeout())
	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	for peerIdx, _ := range rf.peers {
		rf.appendEntriesTimers[peerIdx] = time.NewTimer(HeartBeatTimeout)
	}
	rf.applyTimer = time.NewTimer(ApplyInterval)

	rf.notifyApplyCh = make(chan struct{}, 100)

	// apply log
	go func() {
		for {
			select {
			case <- rf.stopCh:
				return
			case <-rf.applyTimer.C:
				rf.notifyApplyCh <- struct{}{}
			case <-rf.notifyApplyCh:
				rf.startApplyLogs()
			}
		}
	}()

	// 异步发起投票
	go func ()  {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C: // 投票定时时间到，发起投票
				rf.startElection()
			}
		}
	}()

	// leader发送appendEntries rpc请求，发送日志
	for peerIdx, _ := range peers {
		if peerIdx == rf.me {
			continue
		}
		go func (index int)  {
			for {
				select{
				case <-rf.stopCh:
					return
				case <-rf.appendEntriesTimers[index].C:
					rf.appendEntriesToPeer(index)
				}
			}
		}(peerIdx)
	}
	return rf
}
