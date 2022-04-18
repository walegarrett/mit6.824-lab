package raft

import "time"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // 候选人的任期号
	CandidateId int // 请求选票的候选人的 Id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm int // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

//
// example RequestVote RPC handler.
//
// 处理别的server发送给自己的投票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("request vote")
	defer rf.unlock("request vote")

	defer func ()  {
		rf.log("get request vote, args:%+v, reply:%+v", args, reply)
	}()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	reply.Term = rf.term
	reply.VoteGranted = false

	// 如果term < currentTerm返回 false
	if args.Term < rf.term {
		return
	} else if args.Term == rf.term {
		// 如果 votedFor 为空或者就是 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
		if rf.role == Leader {
			return
		}
		if rf.voteFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}
		if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
			// 已经投给别的server了
			return
		}
		// 还有一种可能，没有投票
	}

	defer rf.persist()

	// 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者
	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
		rf.changeRole(Follower)
	}

	// 选举限制：候选人的日志至少和自己一样新，那么就投票给他
	if lastLogTerm > args.LastLogTerm || 
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			return
	}

	rf.term = args.Term
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	rf.log("vote for :%d", args.CandidateId)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
// 发送投票选举请求给指定的server
func (rf *Raft) sendRequestVoteToPeer(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	t := time.NewTimer(RPCTimeout)
	defer t.Stop()

	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	// 不断给指定的server发送投票请求
	for {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch := make(chan bool, 1)
		r := RequestVoteReply{}

		go func ()  {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <-t.C:
			return
		case <-rpcTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			} else {
				// 获取请求的结果
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return
			}
		}
	}
}

// 某个server成为候选人，发起投票
func (rf * Raft) startElection() {
	rf.lock("start election")
	rf.electionTimer.Reset(randElectionTimeout())

	// 如果当前server的角色本身就是leader，则没有必要进行投票选举
	if rf.role == Leader {
		rf.unlock("start election")
		return
	}
	rf.log("start election")
	rf.changeRole(Candidate)

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs {
		Term: rf.term,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}

	rf.persist()
	rf.unlock("start election")

	grantedCount := 1 // 自己肯定投给自己，初始化为1
	chResCount := 1 // 总共收到的回复，包括true和false的
	votesCh := make(chan bool, len(rf.peers))

	// 给每个server发送投票信息
	for peerIdx, _ := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		go func(ch chan bool, peerIdx int) {
			reply := RequestVoteReply{}
			rf.sendRequestVoteToPeer(peerIdx, &args, &reply)

			ch <- reply.VoteGranted

			// 如果返回的server的任期号大于当前server的任期号，则选举失败，转换自己为Follower
			if reply.Term > args.Term {
				rf.lock("start election change term")
				if rf.term < reply.Term {
					rf.term = reply.Term
					rf.changeRole(Follower)
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.unlock("start election change term")
			}
		}(votesCh, peerIdx)
	}

	// 统计票数
	for {
		r := <-votesCh
		chResCount += 1
		if r {
			grantedCount += 1
		}
		if chResCount == len(rf.peers) || grantedCount > len(rf.peers) / 2 ||
			chResCount - grantedCount > len(rf.peers) / 2 {
			break
			
		}
	}

	// 赞成票数小于一半，选举失败
	if grantedCount <= len(rf.peers) / 2 {
		rf.log("grantedCount <= len/2:count:%d", grantedCount)
		return
	}

	rf.lock("start election 2")
	rf.log("before try change to leader, count:%d, args:%+v", grantedCount, args)
	// 确保在选举期间没有发生角色变化，即可能别的候选者成为了leader等意外情况
	if rf.term == args.Term && rf.role == Candidate {
		rf.changeRole(Leader)
		rf.persist()
	}

	// 如果当选领导者，马上向其他server发送心跳
	if rf.role == Leader {
		rf.resetHeartBeatTimers()
	}

	rf.unlock("start election 2")

}

// 修改当前server的角色
func (rf *Raft) changeRole(role Role) {
	rf.role = role
	switch role {
	case Follower:
	case Candidate: // 自己成为候选者
		rf.term += 1
		rf.voteFor = rf.me
		rf.resetElectionTimer()
	case Leader: // 成为了Leader
		_, lastLogIndex := rf.lastLogTermIndex()
		rf.nextIndex = make([]int, len(rf.peers))
		for peerIdx := 0; peerIdx < len(rf.peers); peerIdx++ {
			// 将所有server的nextIndex初始化为领导人最后索引值加一
			rf.nextIndex[peerIdx] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastLogIndex
		rf.resetElectionTimer()
	default:
		panic("unknown role")
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
}