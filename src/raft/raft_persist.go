package raft

import (
	"bytes"

	"6.824/labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.convertByte())
	Debug(dPersist, "S%d|T%d save persist| votedFor: %d| logsLen: %d", rf.me, rf.currentTerm, rf.voteFor, rf.log.size())
}

func (rf *Raft) convertByte() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		Debug(dPersist, "S%d error[readPersisit]", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = votedFor
		rf.log = log
		Debug(dPersist, "S%d|T%d read persist| votedFor: %d| logsLen: %d| base: %d", rf.me, rf.currentTerm, rf.voteFor, rf.log.size(), rf.log.Base)
	}
}
