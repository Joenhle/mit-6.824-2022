package raft

import (
	"fmt"
	"log"
)

func (r *Raft) debug(format string, a ...interface{}) {
	if Debug {
		raftInfo := fmt.Sprintf("Raft[dead:%v me:%v state:%v currentTerm:%v votedFor:%v logs:%v commitIndex:%v lastApplied:%v nextIndex:%v matchIndex:%v] ",
			r.killed(), r.me, r.state, r.currentTerm, r.votedFor, r.logs, r.commitIndex, r.lastApplied, r.nextIndex, r.matchIndex)
		log.Printf(raftInfo+format, a...)
	}
}

func (r *Raft) getLastLog(atomic bool) Log {
	if atomic {
		r.RLock()
		defer r.RUnlock()
	}
	lastLog := Log{Index: 0, Term: 0}
	if len(r.logs) > 0 {
		lastLog = r.logs[len(r.logs)-1]
	}
	return lastLog
}

func (r *Raft) getState() RaftState {
	r.RLock()
	defer r.RUnlock()
	return r.state
}

func (r *Raft) setState(newState RaftState) {
	r.Lock()
	defer r.Unlock()
	r.state = newState
}

func (r *Raft) changeState(expectState RaftState, resetElectionTimeout bool) {
	if resetElectionTimeout {
		r.electionChannel <- struct{}{}
	}
	switch expectState {
	case FLOWER:
		r.debug("is changing state")
		r.state = FLOWER
		r.votedFor = -1
		r.nextIndex = make([]int, len(r.peers))
		r.matchIndex = make([]int, len(r.peers))
		r.debug("changed over")
		r.electionChannel <- struct{}{}
		break
	case CANDIDATE:
		r.debug("is changing state")
		r.state = CANDIDATE
		r.currentTerm += 1
		r.votedFor = r.me
		r.debug("changed over")
		r.requestVote()
		break
	case LEADER:
		r.debug("is changing state")
		r.state = LEADER
		lastLog := r.getLastLog(false)
		for i := 0; i < len(r.nextIndex); i++ {
			r.nextIndex[i] = lastLog.Index + 1
			r.matchIndex[i] = 0
		}
		r.debug("changed over")
		r.logReplicateChannel <- struct{}{}
		break
	default:
		log.Fatalf("no such state[%v]", expectState)
	}
}

func (r *Raft) getPrevLogInfo(server int) (int, int) {
	if r.nextIndex[server]-1 >= 1 {
		term := r.logs[r.nextIndex[server]-2].Term
		return r.nextIndex[server] - 1, term
	}
	return 0, 0
}
