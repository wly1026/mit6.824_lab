package raft

import "time"

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
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// rule 2:  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	lastLogIndex := rf.log.size() - 1
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		(args.LastLogTerm > rf.log.get(lastLogIndex).Term ||
			(args.LastLogTerm == rf.log.get(lastLogIndex).Term && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.persist()
		Debug(dElect, "S%d|T%d votes for S%d[RequestVote]", rf.me, rf.currentTerm, args.CandidateId)
		// only reset when vote for the request
		rf.startElectionEtime = time.Now()
	} else {
		Debug(dElect, "S%d rejects to vote for S%d because it has voted or it is more up-dated[RequestVote]", rf.me, args.CandidateId)
		reply.VoteGranted = false
	}
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

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.startElectionEtime = time.Now()
	rf.currentTerm = rf.currentTerm + 1
	Debug(dElect, "S%d|T%d starts to join election", rf.me, rf.currentTerm)
	rf.state = CANDIDATE
	rf.voteFor = rf.me

	rf.persist()

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
			LastLogIndex: rf.log.size() - 1,
			LastLogTerm:  rf.log.get(rf.log.size() - 1).Term,
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
							rf.nextIndex[i] = rf.log.size()
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

						rf.persist()
					}
				}
			}
		}(i)
	}
}
