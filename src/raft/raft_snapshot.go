package raft

func (rf *Raft) sendInstallSnapshot(peerIdx int){
	rf.lock("send install snapshot")

	rf.unlock("send install snapshot")
}