package raft

import (
	"math"
	"math/rand"
	"time"
)

const (
	MaxSleepTime = 50
	MinSleepTime = 25
)

func getRand(seed int) int {
	rand.Seed(time.Now().Unix() + int64(seed))
	return MinSleepTime + rand.Intn(MaxSleepTime-MinSleepTime)
}

func ElectionTimeOut() time.Duration {
	//选举超时时间为150毫秒~200毫秒
	return time.Duration(150+rand.Intn(50)) * time.Millisecond
}

func (r *Raft) watchElectionTimeout() {
	for r.killed() == false {
		time.Sleep(time.Duration(getRand(r.me)) * time.Millisecond)
		if r.getState() != LEADER {
			ticker := time.NewTicker(ElectionTimeOut())
			select {
			case <-ticker.C:
				r.Lock()
				r.changeState(CANDIDATE, true)
				r.Unlock()
			case <-r.electionChannel:
				continue
			}
		}
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// todo 每个candidate上任后，只进行一次选举就够了，因为第一次如果都没有成功拿到选票，大概率第二次也不会拿到选票，所以只做一次或许优化程度更高
func (r *Raft) requestVote() {
	if r.killed() == false && r.state == CANDIDATE {
		r.debug("[requestVote] 准备开始投选举票")
		voteNum := 1
		for peerIndex, _ := range r.peers {
			if peerIndex == r.me {
				continue
			}
			go func(server int) {
				r.Lock()
				lastLog := r.getLastLog()
				req := &RequestVoteArgs{
					Term:         r.CurrentTerm,
					CandidateId:  r.me,
					LastLogIndex: lastLog.Index,
					LastLogTerm:  lastLog.Term,
				}
				resp := &RequestVoteReply{}
				r.Unlock()
				if ok := r.sendRequestVote(server, req, resp); ok {
					r.Lock()
					if r.state != CANDIDATE {
						r.Unlock()
						return
					}
					if resp.VoteGranted {
						voteNum += 1
						r.debug("[requestVote] 获得server-%v的选票", server)
						if voteNum >= int(math.Ceil(float64(len(r.peers))/2)) {
							r.debug("[reqyestVote] 获得了大部分选票，开始变为LEADER")
							r.changeState(LEADER, true)
							r.Unlock()
							return
						}
					}
					if resp.Term > r.CurrentTerm {
						r.changeState(FLOWER, false)
						r.Unlock()
						return
					}
					r.Unlock()
				} else {
					r.debug("requestVote RPC 调用失败, target is %v", server)
				}
			}(peerIndex)
		}
	}
}

func (r *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := r.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}

func (r *Raft) HandleRequestVote(req *RequestVoteArgs, reply *RequestVoteReply) {
	r.Lock()
	reply.Term = r.CurrentTerm
	if req.Term < r.CurrentTerm {
		reply.VoteGranted = false
		r.debug("[HandleRequestVote] voted = %v", false)
		r.Unlock()
		return
	}
	grant := false
	if req.Term > r.CurrentTerm {
		r.CurrentTerm = req.Term
		r.persist()
		r.changeState(FLOWER, true)
	}
	hasVoteRight := r.VotedFor == -1 || r.VotedFor == req.CandidateId
	lastLog := r.getLastLog()
	upToDate := req.LastLogTerm > lastLog.Term || (req.LastLogTerm == lastLog.Term && req.LastLogIndex >= lastLog.Index)
	if hasVoteRight && upToDate {
		grant = true
		r.VotedFor = req.CandidateId
		r.persist()
		r.electionChannel <- struct{}{}
	}

	r.debug("[HandleRequestVote] voted = %v", grant)
	reply.VoteGranted = grant
	r.Unlock()
	return
}
