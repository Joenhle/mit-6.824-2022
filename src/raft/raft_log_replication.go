package raft

import (
	"math"
	"time"
)

const (
	HEART_BEAT_DURATION = time.Duration(200 * time.Millisecond) //心跳时间为最小超时时间的一半
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (r *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := r.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (r *Raft) HandleAppendEntries(req *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.Lock()
	if req.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		r.Unlock()
		return
	}
	if req.Term > r.currentTerm {
		reply.Term = r.currentTerm
		r.currentTerm = req.Term
		reply.Success = false
		r.changeState(FLOWER, true)
		r.Unlock()
		return
	}

	reply.Term = r.currentTerm
	reply.Success = false

	if r.state != FLOWER {
		r.changeState(FLOWER, true)
	} else {
		r.electionChannel <- struct{}{}
	}

	if req.PrevLogIndex == 0 {
		reply.Success = true
		r.logs = req.Entries
		if req.LeaderCommit > r.commitIndex {
			r.commitIndex = int(math.Min(float64(req.LeaderCommit), float64(r.getLastLog(false).Index)))
		}
		r.Unlock()
		return
	}

	if len(r.logs) < req.PrevLogIndex {
		r.Unlock()
		return
	}
	if r.logs[req.PrevLogIndex-1].Term != req.PrevLogTerm {
		r.logs = r.logs[:req.PrevLogIndex-1]
		r.Unlock()
		return
	}
	newLogs := append(r.logs[:req.PrevLogIndex], req.Entries...)
	r.logs = newLogs
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = int(math.Min(float64(req.LeaderCommit), float64(r.getLastLog(false).Index)))
	}
	reply.Success = true
	r.Unlock()
	return
}

func (r *Raft) healthyCheck() {
	for r.killed() == false {
		time.Sleep(time.Duration(getRand(r.me)) * time.Millisecond)
		if r.getState() == LEADER {
			ticker := time.NewTicker(HEART_BEAT_DURATION)
			select {
			case <-ticker.C:
			case <-r.logReplicateChannel:
				r.appendEntries()
			}
		}
	}
}

func (r *Raft) appendEntries() {
	r.debug("[appendEntries] 开始进行日志同步检测")
	replicateNum := 1
	for peerIndex, _ := range r.peers {
		if peerIndex == r.me {
			continue
		}
		go func(server int) {
			r.Lock()
			lastLog := r.getLastLog(false)
			prevIndex, prevTerm := r.getPrevLogInfo(server)
			req := &AppendEntriesArgs{
				Term:         r.currentTerm,
				LeaderId:     r.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				LeaderCommit: r.commitIndex,
			}
			if lastLog.Index >= r.nextIndex[server] && r.nextIndex[server]-1 >= 0 {
				req.Entries = r.logs[r.nextIndex[server]-1:]
			} else {
				req.Entries = []Log{}
			}
			resp := &AppendEntriesReply{}
			r.Unlock()
			// 注意这里的RPC可能会延迟达到7000毫秒才返回，注意别长时间占锁
			if ok := r.sendAppendEntries(server, req, resp); ok {
				r.Lock()
				if r.state != LEADER {
					r.Unlock()
					return
				}
				if resp.Success {
					r.debug("[appendEntries] server-%d日志同步成功", server)
					r.matchIndex[server] = r.getLastLog(false).Index
					r.nextIndex[server] = r.matchIndex[server] + 1
					replicateNum += 1
					if replicateNum >= int(math.Ceil(float64(len(r.peers))/2)) {
						r.debug("[appendEntries] 超过半数server同步成功，更新commitIndex")
						r.commitIndex = r.getLastLog(false).Index
					}
				} else if resp.Term > r.currentTerm {
					r.debug("[appendEntries] 检测到server-%d的任期大于当前任期，准备变为FLOWER", server)
					r.changeState(FLOWER, false)
					r.Unlock()
					return
				} else {
					r.debug("[appendEntries] server-%d日志同步失败", server)
					if r.nextIndex[server] > 1 {
						r.nextIndex[server] -= 1
					}
				}
				r.Unlock()
			} else {
				r.debug("[appendEntries] server-%d RPC调用失败", server)
			}
		}(peerIndex)
	}
}
