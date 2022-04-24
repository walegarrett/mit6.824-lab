package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term int
	LeaderId int // 领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int // 新的日志条目紧随之前的索引值
	PrevLogTerm int // prevLogIndex 条目的任期号
	Entries []LogEntry // 准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int // 领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term int // 当前的任期号，用于领导人去更新自己
	Success bool // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
	NextIndex int
}

// 重置appendEntries的定时器
func (rf *Raft) resetHeartBeatTimers() {
	for peerIdx, _ := range rf.appendEntriesTimers {
		rf.appendEntriesTimers[peerIdx].Stop()
		rf.appendEntriesTimers[peerIdx].Reset(0)
	}
}

func (rf *Raft) resetHeartBeatTimer(peerIdx int) {
	rf.appendEntriesTimers[peerIdx].Stop()
	rf.appendEntriesTimers[peerIdx].Reset(HeartBeatTimeout)
}

// leader向其他server发送日志
func (rf *Raft) appendEntriesToPeer(peerIdx int) {
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for !rf.killed() {
		rf.lock("appendToPeerPhase1")
		// 只有leader才能向其他server发送日志
		if rf.role != Leader {
			rf.resetHeartBeatTimer(peerIdx)
			rf.unlock("appendToPeerPhase1")
			return
		}
		
		args := rf.getAppendEntriesArgs(peerIdx)
		rf.resetHeartBeatTimer(peerIdx)
		rf.unlock("appendToPeerPhase1")

		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)

		reply := AppendEntriesReply{}
		resCh := make(chan bool, 1)

		// 向指定的peer发送append Entries请求
		go func (args *AppendEntriesArgs, reply *AppendEntriesReply)  {
			ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			resCh <- ok
		}(&args, &reply)

		select {
		case <-rf.stopCh:
			return
		case <-RPCTimer.C:
			rf.log("appendToPeer, rpctimeout: peer:%d, args:%+v", peerIdx, args)
			continue
		case ok := <-resCh:
			if !ok {
				rf.log("appendToPeer not ok")
				continue
			}
		}

		rf.log("appendToPeer, peer:%d, args:%+v, reply:%+v", peerIdx, args, reply)

		// 处理append Entries的响应
		rf.lock("appendToPeerPhase2")

		// 收到的term任期大于当前term则表示系统中有新的leader，需要将自己变更为跟随者
		if reply.Term > rf.term {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.term = reply.Term
			rf.persist()
			rf.unlock("appendToPeerPhase2")
			return
		}

		// 发送append entries期间，自己的角色发生了变化
		if rf.role != Leader || rf.term != args.Term {
			rf.unlock("appendToPeerPhase2")
			return
		}

		// append 成功
		if reply.Success {
			if reply.NextIndex > rf.nextIndex[peerIdx] {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.matchIndex[peerIdx] = reply.NextIndex - 1
			}
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.term {
				// commit 自己 term的index
				rf.updateCommitIndex()
			}
			rf.persist()
			rf.unlock("appendToPeerPhase2")
			return
		}
		// append 失败

		if reply.NextIndex != 0 {
			if reply.NextIndex > rf.lastSnapshotIndex {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.unlock("appendToPeerPhase2")
				continue
			} else {
				// 此时其他follower的日志太落后于当前leader了
				// peer的log快照短于leader通过appendEntries rpc发给它的内容，leader会将自己的快照发给follower
				go rf.sendInstallSnapshot(peerIdx)
				rf.unlock("appendToPeerPhase2")
				return
			}
		} else {
			// server的日志条目是乱序的
			rf.unlock("appendToPeerPhase2")
		}
	}
}

// server处理leader发来的appendEntries请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("appendEntries")
	rf.log("get appendentries:%+v", *args)

	reply.Term = rf.term
	// 如果 leaderTerm < currentTerm 就返回 false
	if rf.term > args.Term {
		rf.unlock("appendEntries")
		return
	}
	rf.term = args.Term
	rf.changeRole(Follower)
	rf.resetElectionTimer()

	_, lastLogIndex := rf.lastLogTermIndex()

	if args.PrevLogIndex < rf.lastSnapshotIndex {
		// 按理来说lastsanaptshotindex已经被apply，正常情况下不该发生
		reply.Success = false
		reply.NextIndex = rf.lastSnapshotIndex + 1
	} else if args.PrevLogIndex > lastLogIndex {
		// 当前server缺少中间的log，需要告诉leader从哪里缺少的
		reply.Success = false
		reply.NextIndex = rf.getNextIndex()
	} else if args.PrevLogIndex == rf.lastSnapshotIndex {
		// 上一个index刚好是快照
		// 产生冲突，乱序，需要删除
		if rf.outOfOrderAppendEntries(args) {
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			// 保留 logs[0]，因为logs[0]存放的是快照index
			rf.logEntries = append(rf.logEntries[:1], args.Entries...)
			reply.NextIndex = rf.getNextIndex()
		}
	} else if rf.logEntries[rf.getRealIdxByLogIndex(args.PrevLogIndex)].Term == args.PrevLogTerm {
		// 包括刚好是后续的 log 和需要删除部分 两种情况
		// 产生冲突，乱序，需要删除
		if rf.outOfOrderAppendEntries(args) {
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[0:rf.getRealIdxByLogIndex(args.PrevLogIndex)+1], args.Entries...)
			reply.NextIndex = rf.getNextIndex()
		}

	} else {
		// 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false 
		rf.log("prev log not match")
		reply.Success = false
		// 尝试跳过一个term
		term := rf.logEntries[rf.getRealIdxByLogIndex(args.PrevLogIndex)].Term
		idx := args.PrevLogIndex
		for idx > rf.commitIndex && idx > rf.lastSnapshotIndex && rf.logEntries[rf.getRealIdxByLogIndex(idx)].Term == term {
			idx -= 1
		}
		reply.NextIndex = idx + 1
	}

	if reply.Success {
		// 如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

	rf.persist()
	rf.log("get appendentries:%+v, reply:%+v", *args, *reply)
	rf.unlock("append Entries")
}

func (rf *Raft) getAppendEntriesArgs(peerIdx int) AppendEntriesArgs {
	prevLogIndex, prevLogTerm, logs := rf.getAppendLogs(peerIdx)
	args := AppendEntriesArgs {
		Term: rf.term,
		LeaderId: rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: logs,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

// 获取要发送给指定server的log相关信息
func (rf *Raft) getAppendLogs(peerIdx int) (prevLogIndex, prevLogTerm int, res []LogEntry) {
	nextIdx := rf.nextIndex[peerIdx]
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	if nextIdx <= rf.lastSnapshotIndex || nextIdx > lastLogIndex {
		// 不需要发送log
		prevLogIndex = lastLogIndex
		prevLogTerm = lastLogTerm
		return
	}

	// 封装要发送给server的所有日志条目
	res = append([]LogEntry{}, rf.logEntries[rf.getRealIdxByLogIndex(nextIdx):]...)
	
	prevLogIndex = nextIdx - 1
	if prevLogIndex == rf.lastSnapshotIndex {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.getLogByIndex(prevLogIndex).Term
	}
	return
}

// 获取去除快照索引之后真的日志条目的索引
func (rf *Raft) getRealIdxByLogIndex(logIndex int) int {
	idx := logIndex - rf.lastSnapshotIndex
	if idx < 0 {
		return -1
	} else {
		return idx
	}
}

func (rf *Raft) getLogByIndex(logIndex int) LogEntry {
	idx := logIndex - rf.lastSnapshotIndex
	return rf.logEntries[idx]
}

// 提交日志
func (rf *Raft) updateCommitIndex() {
	rf.log("in updating commitindex")
	hasCommit := false
	for i := rf.commitIndex + 1; i <= rf.lastSnapshotIndex + len(rf.logEntries); i++ {
		count := 0
		// 判断所有其他peer中确定被append的日志条目索引
		for _, m := range rf.matchIndex {
			if m >= i {
				count += 1
				// 确保raft系统中一半以上的server都确认收到了这个日志条目，则更新本服务器中的commitIndex值
				if count > len(rf.peers) / 2 {
					rf.commitIndex = i
					hasCommit = true
					rf.log("update commit index:%d", i)
					break
				}
			}
		}
		// 一轮下来发现没有可以更新的commitIndex则退出，没有必要继续增大尝试的索引了，因为raft系统的大部分服务器还没有收到一致的日志
		if rf.commitIndex != i {
			break
		}
	}

	// 确定leader已经commit了，此时可以通知应用层进行apply了
	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}

// 获取需要发给leader的nextIndex
func (rf *Raft) getNextIndex() int {
	_, idx := rf.lastLogTermIndex()
	return idx + 1
}

// 已经存在的日志条目和新的日志条目是否产生冲突
func (rf *Raft) outOfOrderAppendEntries(args *AppendEntriesArgs) bool {
	argsLastIndex := args.PrevLogIndex + len(args.Entries)
	lastTerm, lastIndex := rf.lastLogTermIndex()
	// 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的
	if argsLastIndex < lastIndex && lastTerm == args.Term {
		return true
	}
	return false
}