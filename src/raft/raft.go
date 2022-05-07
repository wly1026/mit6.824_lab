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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
	log         Log // 2D

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

type Log struct {
	Base       int
	LogEntries []LogEntry
}

func (log *Log) size() int {
	return log.Base + len(log.LogEntries)
}

func (log *Log) get(i int) LogEntry {
	return log.LogEntries[i-log.Base]
}

func (log *Log) set(i int, entry LogEntry) {
	log.LogEntries[i-log.Base] = entry
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
	Debug(dSnap, "S%d|T%d Snapshot until index %d| logLen: %d", rf.me, rf.currentTerm, index, rf.log.size())
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
		rf.log.LogEntries = append(rf.log.LogEntries, LogEntry{command, term})
		rf.persist()
		index = rf.log.size() - 1

		Debug(dLog, "S%d|T%d receive commands | logLen: %d[Start]",
			rf.me, rf.currentTerm, rf.log.size())
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
		time.Sleep((time.Duration(HEARTBEATINTERVAL)) * time.Millisecond)
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
	//rf.logs = append(rf.logs, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// 2D
	rf.log = Log{
		Base:       0,
		LogEntries: []LogEntry{{Term: 0}},
	}

	Debug(dLog, "S%d|T%d is made", rf.me, rf.currentTerm)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTimer()
	go rf.electionRunoutTimer()

	return rf
}
