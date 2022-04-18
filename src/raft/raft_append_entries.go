package raft

// 重置appendEntries的定时器
func (rf *Raft) resetHeartBeatTimers() {
	for peerIdx, _ := range rf.appendEntriesTimers {
		rf.appendEntriesTimers[peerIdx].Stop()
		rf.appendEntriesTimers[peerIdx].Reset(0)
	}
}