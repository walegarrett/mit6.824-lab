package raft

import (
	"log"
	"time"
)

/*
	尽管通常服务器都是独立的创建快照，但是领导人必须偶尔的发送快照给一些落后的跟随者。
	这通常发生在当领导人已经丢弃了下一条需要发送给跟随者的日志条目的时候。
	幸运的是这种情况不是常规操作：一个与领导人保持同步的跟随者通常都会有这个条目。
	然而一个运行非常缓慢的跟随者或者新加入集群的服务器（第 6 节）将不会有这个条目。
	这时让这个跟随者更新到最新的状态的方式就是通过网络把快照发送给他们。
*/
type InstallSnapshoArgs struct {
	Term int // 领导人的任期号
	LeaderId int // 领导人的 Id，以便于跟随者重定向请求
	LastIncludeIndex int // 快照中包含的最后日志条目的索引值
	LastIncludeTerm int // 快照中包含的最后日志条目的任期号
	Data []byte // 原始数据
}

type InstallSnapshotReply struct {
	Term int // 当前任期号，便于领导人更新自己
}

// server处理leader发送的install snapshot请求
func (rf *Raft) InstallSnapshot(args *InstallSnapshoArgs, reply *InstallSnapshotReply) {
	rf.lock("install snapshot")
	defer rf.unlock("install snapshot")

	reply.Term = rf.term
	// 如果LeaderTerm < currentTerm就立即回复
	if args.Term < rf.term {
		return
	}

	if args.Term > rf.term || rf.role != Follower {
		rf.term = args.Term
		rf.changeRole(Follower)
		rf.resetElectionTimer()
		defer rf.persist()
	}

	if rf.lastSnapshotIndex >= args.LastIncludeIndex {
		return
	}

	start := args.LastIncludeIndex - rf.lastSnapshotIndex
	if start < 0 {
		// 一般不可能出现这种情况
		log.Fatal("install snapshot")
	} else if start >= len(rf.logEntries) {
		rf.logEntries = make([]LogEntry, 1)
		rf.logEntries[0].Term = args.LastIncludeTerm
		rf.logEntries[0].Idx = args.LastIncludeIndex
	} else {
		// 在指定偏移量写入数据
		rf.logEntries = rf.logEntries[start:]
	}

	rf.lastSnapshotIndex = args.LastIncludeIndex
	rf.lastSnapshotTerm = args.LastIncludeTerm
	rf.persister.SaveStateAndSnapshot(rf.GetPersistData(), args.Data)
}

// 发送快照给指定的server
func (rf *Raft) sendInstallSnapshot(peerIdx int){
	rf.lock("send install snapshot")
	args := InstallSnapshoArgs{
		Term: rf.term,
		LeaderId: rf.me,
		LastIncludeIndex: rf.lastSnapshotIndex,
		LastIncludeTerm: rf.lastSnapshotTerm,
		Data: rf.persister.ReadSnapshot(),
	}
	rf.unlock("send install snapshot")

	timer := time.NewTimer(RPCTimeout)
	defer timer.Stop()

	for {
		timer.Stop()
		timer.Reset(RPCTimeout)
		okCh := make(chan bool, 1)
		reply := InstallSnapshotReply{}
		go func ()  {
			ok := rf.peers[peerIdx].Call("Raft.InstallSnapshot", &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			okCh <- ok
		}()

		ok := false
		select {
		case <-rf.stopCh:
			return
		case <-timer.C:
			continue
		case ok = <-okCh:
			if !ok{
				continue
			}
		}

		rf.lock("send install snapshot")
		defer rf.unlock("send install snapshot")

		// 角色发生了变化
		if rf.term != args.Term || rf.role != Leader {
			return
		}

		if reply.Term > rf.term {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.term = reply.Term
			rf.persist()
			return
		}

		if args.LastIncludeIndex > rf.matchIndex[peerIdx] {
			rf.matchIndex[peerIdx] = args.LastIncludeIndex
		}
		if args.LastIncludeIndex + 1 > rf.nextIndex[peerIdx] {
			rf.nextIndex[peerIdx] = args.LastIncludeIndex + 1
		}
		return
	}
}