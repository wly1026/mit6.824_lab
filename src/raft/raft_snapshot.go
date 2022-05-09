package raft

import "time"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < rf.log.Base {
		Debug(dSnap, "S%d|T%d calls Snapshot with older index than rf", rf.me, rf.currentTerm)
		return
	}

	tempLogEntries := make([]LogEntry, 0)
	tempLogEntries = append(tempLogEntries, LogEntry{Term: rf.log.get(index).Term})
	for i := index + 1; i < rf.log.size(); i++ {
		tempLogEntries = append(tempLogEntries, rf.log.get(i))
	}

	rf.log.LogEntries = tempLogEntries
	rf.log.Base = index

	rf.persister.SaveStateAndSnapshot(rf.convertByte(), snapshot)
	Debug(dSnap, "S%d|T%d Snapshot until index %d| logLen: %d| commitId: %d| appliedId: %d", rf.me, rf.currentTerm, index, rf.log.size(), rf.commitIndex, rf.lastApplied)
}

// 1) update rf 2) apply to state machine 3) persist
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	Debug(dSnap, "S%d|T%d receive rpc from S%d", rf.me, rf.currentTerm, args.LeaderId)

	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.log.Base {
		Debug(dSnap, "S%d|T%d rejects to installsnap from S%d | out of date term", rf.me, rf.currentTerm, args.LeaderId)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.startElectionEtime = time.Now()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.state = FOLLOWER
	} else if rf.state != FOLLOWER {
		rf.state = FOLLOWER
	}

	reply.Term = rf.currentTerm

	// rule 6 ??
	tempLogEntries := make([]LogEntry, 0)
	tempLogEntries = append(tempLogEntries, LogEntry{Term: args.LastIncludedTerm})
	for i := args.LastIncludedIndex + 1; i < rf.log.size(); i++ {
		tempLogEntries = append(tempLogEntries, rf.log.get(i))
	}

	rf.log.LogEntries = tempLogEntries
	rf.log.Base = args.LastIncludedIndex
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	Debug(dSnap, "S%d|T%d update rfstate| Base:%d| LogLen:%d", rf.me, rf.currentTerm, rf.log.Base, rf.log.size())
	// 3) persist
	rf.persister.SaveStateAndSnapshot(rf.convertByte(), args.Data)

	rf.mu.Unlock()

	// 2) apply snapshot
	go func() {
		applyMsg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.applyCh <- applyMsg

		rf.mu.Lock()
		Debug(dSnap, "S%d|T%d applys snapshot from S%d", rf.me, rf.currentTerm, args.LeaderId)
		rf.mu.Unlock()
	}()
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapShotAndDealWithReply(peerId int) {
	rf.mu.Lock()

	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.Base,
		LastIncludedTerm:  rf.log.LogEntries[0].Term,
		Data:              rf.persister.snapshot,
	}
	rf.mu.Unlock()
	reply := &InstallSnapshotReply{}
	if rf.sendSnapshot(peerId, args, reply) {
		rf.mu.Lock()

		if rf.state != LEADER {
			Debug(dSnap, "S%d|T%d lost leadership, state: %d[install]", rf.me, rf.currentTerm, rf.state)
			rf.mu.Unlock()
			return
		}

		if rf.currentTerm != args.Term {
			Debug(dSnap, "S%d|T%d term inconsistency[install]", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.voteFor = -1
			rf.persist()
		} else {
			rf.nextIndex[peerId] = rf.log.Base + 1
			rf.matchIndex[peerId] = rf.log.Base // logs before Base must be applied, no need to checkCommit
			Debug(dSnap, "S%d|T%d installs snapshot to S%d| nextId:%d| matchId:%d", rf.me, rf.currentTerm, peerId, rf.nextIndex[peerId], rf.matchIndex[peerId])
		}
		rf.mu.Unlock()
		return
	} else {
		rf.mu.Lock()
		Debug(dSnap, "S%d|T%d fails to install snapshot to S%d", rf.me, rf.currentTerm, peerId)
		rf.mu.Unlock()
	}
}
