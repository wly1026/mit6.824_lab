package raft

import (
	"time"
)

type AppendEntryArgs struct {
	Term     int // 2A
	LeaderId int // 2A

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rule1: Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		Debug(dLog, "S%d|T%d rejects to append entries from S%d | out of date term[AppendEntry]", rf.me, rf.currentTerm, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.startElectionEtime = time.Now()

	// must: args.Term >= rf.currentTerm
	// >: means currentTerm is out of date
	// == meaning that other leader wins in the current term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term

		rf.persist()

		rf.state = FOLLOWER
	} else if rf.state != FOLLOWER {
		rf.state = FOLLOWER
	}

	// rule2: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// case1: rf.logs is short. case2: not match
	reply.Term = rf.currentTerm
	// case1
	if args.PrevLogIndex >= rf.log.size() {
		Debug(dLog, "S%d|T%d rejects to append entries from S%d| short log| args.prev:%d| logLen:%d", rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, rf.log.size())
		reply.Success = false
		reply.ConflictIndex = rf.log.size()
		reply.ConflictTerm = -1
		return
	}

	// case2
	if rf.log.get(args.PrevLogIndex).Term != args.PrevLogTerm {
		Debug(dLog, "S%d|T%d rejects to append entries from S%d | inconsistency log[AppendEntry]", rf.me, rf.currentTerm, args.LeaderId)
		reply.Success = false
		reply.ConflictTerm = rf.log.get(args.PrevLogIndex).Term
		for i := args.PrevLogIndex; i >= rf.log.Base; i-- {
			if rf.log.get(i).Term != reply.ConflictTerm {
				return
			}
			reply.ConflictIndex = i
		}
	}

	// rule3:If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// rule4:Append any new entries not already in the log
	// In this case, rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm
	i, j := 0, args.PrevLogIndex+1 // i, j are the first index that are not match or out of bound
	for ; j < rf.log.size() && i < len(args.Entries); i, j = i+1, j+1 {
		if args.Entries[i].Term != rf.log.get(j).Term {
			break
		}
	}

	rf.log.LogEntries = append(rf.log.LogEntries[:j-rf.log.Base], args.Entries[i:]...)
	rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = true
	Debug(dLog, "S%d|T%d appends logs from S%d| logLen: %d[AppendEntry]", rf.me, rf.currentTerm, args.LeaderId, rf.log.size())
	// rule5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// ?? why args.LeaderCommit is smaller than len(rf.logs) - 1
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.size()-1)
		// applyEntries
		go rf.applyEntries()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// don't hold lock while sending sth to channel
func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	Debug(dCommit, "S%d|T%d starts to apply entries [%d:%d][ApplyEntry]", rf.me, rf.currentTerm, rf.lastApplied+1, rf.commitIndex)

	if rf.lastApplied >= rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
	copy(entries, rf.log.LogEntries[rf.lastApplied-rf.log.Base+1:rf.commitIndex-rf.log.Base+1])
	index := rf.lastApplied + 1
	rf.mu.Unlock()

	for _, entry := range entries {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: index,
		}
		rf.applyCh <- applyMsg

		rf.mu.Lock()
		rf.lastApplied = index // ?? add rf.lastApplied step by step or at the end
		rf.mu.Unlock()
		index = index + 1
	}

	rf.mu.Lock()
	Debug(dCommit, "S%d|T%d finishes applying entries [%d:%d][ApplyEntry]", rf.me, rf.currentTerm, rf.lastApplied+1, rf.commitIndex)
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if rf.state != LEADER {
		Debug(dLog, "S%d|T%d is not leader[broadcast]", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(peerId int) {
		retry:

			rf.mu.Lock()
			if rf.state != LEADER {
				Debug(dLog, "S%d|T%d lost leadership, state: %d[broadcast]", rf.me, rf.currentTerm, rf.state)
				rf.mu.Unlock()
				return
			}

			// 2D, send snapshot to servers
			if rf.log.Base >= rf.nextIndex[peerId] {
				Debug(dSnap, "S%d|T%d send rpc to S%d|base: %d| next: %d", rf.me, rf.currentTerm, peerId, rf.log.Base, rf.nextIndex[peerId])
				go rf.sendSnapShotAndDealWithReply(peerId)
				rf.mu.Unlock()
				return
			}

			// 2B, send entry
			args := &AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[peerId] - 1,
				PrevLogTerm:  rf.log.get(rf.nextIndex[peerId] - 1).Term,
				Entries:      rf.log.LogEntries[rf.nextIndex[peerId]-rf.log.Base:],
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := &AppendEntryReply{}
			if rf.sendAppendEntry(peerId, args, reply) {
				rf.mu.Lock()

				if rf.state != LEADER {
					Debug(dLog, "S%d|T%d lost leadership, state: %d[broadcast]", rf.me, rf.currentTerm, rf.state)
					rf.mu.Unlock()
					return
				}

				if rf.currentTerm != args.Term {
					Debug(dLog, "S%d|T%d term inconsistency[broadcast]", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
					Debug(dLog, "S%d|T%d replica entries to S%d| entrylen:%d| matchidx:%d| nextidx:%d [broadcast]", rf.me, rf.currentTerm, peerId, len(args.Entries), rf.matchIndex[peerId], rf.nextIndex[peerId])

					// check if should commit
					rf.checkCommit()
				} else {
					if reply.Term > args.Term {
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.voteFor = -1
						rf.persist()
						rf.mu.Unlock()
						return
					}

					// log inconsistency
					if reply.ConflictTerm < 0 {
						// servers'log is short
						rf.nextIndex[peerId] = reply.ConflictIndex
					} else {
						i := reply.ConflictIndex
						for ; i > rf.log.Base; i-- {
							if rf.log.get(i).Term == reply.ConflictTerm {
								break
							}
						}

						if i == rf.log.Base {
							// not found the term
							rf.nextIndex[peerId] = reply.ConflictIndex
						} else {
							rf.nextIndex[peerId] = i
						}
					}
					rf.mu.Unlock()
					goto retry
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) checkCommit() {
	lastLogIndex := rf.log.size() - 1
	for i := lastLogIndex; i > rf.commitIndex && rf.log.get(i).Term == rf.currentTerm; i-- {
		cnts := 1 // count leader self
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i {
				cnts++
				if cnts > len(rf.peers)/2 {
					Debug(dCommit, "S%d|T%d update commitIndex as %d[checkCommit]", rf.me, rf.currentTerm, i)
					rf.commitIndex = i
					go rf.applyEntries()
					return // ??? break
				}
			}
		}
	}
}
