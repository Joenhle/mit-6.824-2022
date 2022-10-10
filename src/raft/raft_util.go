package raft

import (
	"fmt"
	"log"
	"time"
)

const Debug = false

func (r *Raft) debug(format string, a ...interface{}) {
	if Debug {
		raftInfo := fmt.Sprintf("%v Raft[dead:%v me:%v state:%v CurrentTerm:%v VotedFor:%v, logs:%v, commitIndex:%v lastApplied:%v nextIndex:%v matchIndex:%v] ",
			time.Since(Begin).Milliseconds(), r.killed(), r.me, r.state, r.CurrentTerm, r.VotedFor, r.Logs, r.commitIndex, r.lastApplied, r.nextIndex, r.matchIndex)
		log.Printf(raftInfo+format, a...)
	}
}

func (r *Raft) getLastLog() Log {
	return r.Logs[len(r.Logs)-1]
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
		r.VotedFor = -1
		r.nextIndex = make([]int, len(r.peers))
		r.matchIndex = make([]int, len(r.peers))
		r.debug("changed over")
		r.electionChannel <- struct{}{}
		break
	case CANDIDATE:
		r.debug("is changing state")
		r.state = CANDIDATE
		r.CurrentTerm += 1
		r.VotedFor = r.me
		r.debug("changed over")
		r.requestVote()
		break
	case LEADER:
		r.debug("is changing state")
		r.state = LEADER
		r.Logs = append(r.Logs, Log{
			Index:   len(r.Logs),
			Term:    r.CurrentTerm,
			Commend: -1,
		})
		lastLog := r.getLastLog()
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
	r.persist()
}

func (r *Raft) getPrevLogInfo(server int) (int, int) {
	return r.nextIndex[server] - 1, r.Logs[r.nextIndex[server]-1].Term
}
