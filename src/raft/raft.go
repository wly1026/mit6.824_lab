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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	HEARTBEATINTERVAL            = 100
	ELECTIONTIMEOUT_MIN          = 500
	ELECTIONTIMEOUT_RANDOM_EXTRA = 150
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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

	// persist state
	currentTerm int // 2A
	voteFor     int // 2A
	logs        []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaderes
	nextIndex  []int
	matchIndex []int

	// extra
	state              int           // 2A: 0 follower, 1 candidate, 2 leader
	heartbeatCh        chan bool     // 2A
	electionCh         chan bool     // 2A
	startElectionEtime time.Time     // 2A
	applyCh            chan ApplyMsg // 2B
}

// why capitalize
type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // 2A
	CandidateId int // 2A

	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

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
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rule1. this request has been out of date
	if args.Term < rf.currentTerm {
		Debug(dElect, "S%d|T%d rejects to vote for S%d | out of date term[RequestVote]", rf.me, rf.currentTerm, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// current term is old, reset it. make args.Term == rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = FOLLOWER
	}

	reply.Term = rf.currentTerm

	// rule 2:  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	lastLogIndex := len(rf.logs) - 1
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		(args.LastLogTerm > rf.logs[lastLogIndex].Term ||
			(args.LastLogTerm == rf.logs[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		Debug(dElect, "S%d|T%d votes for S%d[RequestVote]", rf.me, rf.currentTerm, args.CandidateId)
		// only reset when vote for the request
		rf.startElectionEtime = time.Now()
	} else {
		Debug(dElect, "S%d rejects to vote for S%d because it has voted or it is more up-dated[RequestVote]", rf.me, args.CandidateId)
		reply.VoteGranted = false
	}

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
		rf.state = FOLLOWER
		// rf.voteFor = -1
	} else if rf.state != FOLLOWER {
		rf.state = FOLLOWER
		// rf.voteFor = -1
	}

	// rule2: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		Debug(dLog, "S%d|T%d rejects to append entries from S%d | inconsistency log[AppendEntry]", rf.me, rf.currentTerm, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// rule3:If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// rule4:Append any new entries not already in the log
	// In this case, rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm
	i, j := 0, args.PrevLogIndex+1 // i, j are the first index that are not match or out of bound
	for ; j < len(rf.logs) && i < len(args.Entries); i, j = i+1, j+1 {
		if args.Entries[i].Term != rf.logs[j].Term {
			break
		}
	}

	rf.logs = append(rf.logs[:j], args.Entries[i:]...)
	reply.Term = rf.currentTerm
	reply.Success = true
	Debug(dLog, "S%d|T%d appends logs from S%d| logLen: %d[AppendEntry]", rf.me, rf.currentTerm, args.LeaderId, len(rf.logs))
	// rule5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// ?? why args.LeaderCommit is smaller than len(rf.logs) - 1
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
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

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
	}
	Debug(dCommit, "S%d|T%d apply entries [%d:%d][ApplyEntry]", rf.me, rf.currentTerm, rf.lastApplied+1, rf.commitIndex)
	rf.lastApplied = rf.commitIndex // ?? add rf.lastApplied step by step or at the end
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if term, isLeader = rf.GetState(); isLeader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.logs = append(rf.logs, LogEntry{command, term})
		index = len(rf.logs) - 1

		Debug(dLog, "S%d|T%d receive commands | logLen: %d[Start]",
			rf.me, rf.currentTerm, len(rf.logs))
	}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartbeatTimer() {
	for !rf.killed() {
		time.Sleep((time.Duration(rand.Intn(300))) * time.Millisecond)
		rf.heartbeatCh <- true
	}
}

func (rf *Raft) electionRunoutTimer() {
	for !rf.killed() {
		currStartElectionTime := time.Now()

		rf.mu.Lock()
		rf.startElectionEtime = currStartElectionTime
		rf.mu.Unlock()

		electionRunoutTime := ELECTIONTIMEOUT_MIN + rand.Intn(ELECTIONTIMEOUT_RANDOM_EXTRA)
		time.Sleep(time.Duration(electionRunoutTime) * time.Millisecond)

		rf.mu.Lock()
		// leader will not restart election
		if !currStartElectionTime.Before(rf.startElectionEtime) && rf.state != LEADER {
			rf.electionCh <- true
		}
		rf.mu.Unlock()
	}
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
			args := &AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[peerId] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[peerId]-1].Term,
				Entries:      rf.logs[rf.nextIndex[peerId]:],
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
					go rf.checkCommit()
				} else {
					if reply.Term > args.Term {
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.voteFor = -1
						rf.mu.Unlock()
						return
					}
					// log inconsistency
					rf.nextIndex[peerId]--
					rf.mu.Unlock()
					goto retry
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) checkCommit() {
	lastLogIndex := len(rf.logs) - 1
	for i := lastLogIndex; i > rf.commitIndex && rf.logs[i].Term == rf.currentTerm; i-- {
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

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.startElectionEtime = time.Now()
	rf.currentTerm = rf.currentTerm + 1
	Debug(dElect, "S%d|T%d starts to join election", rf.me, rf.currentTerm)
	rf.state = CANDIDATE
	rf.voteFor = rf.me
	rf.mu.Unlock()

	votes := 1
	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.logs) - 1,
			LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		}
		rf.mu.Unlock()

		reply := &RequestVoteReply{}
		go func(peerId int) {
			if rf.sendRequestVote(peerId, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != CANDIDATE || args.Term != rf.currentTerm {
					Debug(dElect, "S%d|T%d ends this round", rf.me, rf.currentTerm)
					return
				}

				if reply.VoteGranted {
					votes++
					if votes > len(rf.peers)/2 {
						rf.state = LEADER
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.logs)
							rf.matchIndex[i] = 0 // ???
						}
						Debug(dElect, "S%d|T%d becomes the leader, and starts heatbeats", rf.me, rf.currentTerm)

						rf.mu.Unlock()
						rf.broadcastHeartbeat()
						rf.mu.Lock()
						return
					}
				} else {
					// two reasons when not grant the vote. 1. out-date current 2. has voted for other candidates
					if reply.Term > rf.currentTerm {
						rf.state = FOLLOWER
						rf.currentTerm = reply.Term
					}
				}
			}
		}(i)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.heartbeatCh:
			rf.broadcastHeartbeat()
		case <-rf.electionCh:
			rf.startElection()
		}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.heartbeatCh = make(chan bool)
	rf.electionCh = make(chan bool)
	// 2B
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	Debug(dLog, "S%d|T%d is made", rf.me, rf.currentTerm)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTimer()
	go rf.electionRunoutTimer()

	return rf
}
