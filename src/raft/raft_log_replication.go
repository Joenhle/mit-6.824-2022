package raft

import (
	"math"
	"time"
)

const (
	HEART_BEAT_DURATION = 100 * time.Millisecond //心跳时间为最小超时时间的一半
)

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

type Log struct {
	Index   int
	Term    int
	Commend interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (r *Raft) Start(command interface{}) (int, int, bool) {
	now := time.Now()
	r.Lock()
	defer r.Unlock()
	if r.killed() || r.state != LEADER {
		return -1, -1, false
	}
	r.Logs = append(r.Logs, Log{
		Commend: command,
		Index:   len(r.Logs),
		Term:    r.CurrentTerm,
	})
	r.persist()
	r.debug("[Start] Leader接受到命令，送入log。costTime = %d", time.Since(now).Milliseconds())
	return len(r.Logs) - 1, r.CurrentTerm, true
}

func (r *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := r.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (r *Raft) HandleAppendEntries(req *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.Lock()
	if req.Term < r.CurrentTerm {
		reply.Term = r.CurrentTerm
		reply.Success = false
		r.Unlock()
		return
	}
	if req.Term > r.CurrentTerm {
		reply.Term = r.CurrentTerm
		r.CurrentTerm = req.Term
		r.persist()
		reply.Success = false
		r.changeState(FLOWER, true)
		r.Unlock()
		return
	}

	reply.Term = r.CurrentTerm
	reply.Success = false

	if r.state != FLOWER {
		r.changeState(FLOWER, true)
	} else {
		r.electionChannel <- struct{}{}
	}
	if len(r.Logs) < req.PrevLogIndex+1 {
		reply.NextIndex = len(r.Logs)
		r.Unlock()
		return
	}
	if r.Logs[req.PrevLogIndex].Term != req.PrevLogTerm {
		nextIndex := 1
		for i := req.PrevLogIndex - 1; i >= 0; i-- {
			if r.Logs[i].Term != r.Logs[req.PrevLogIndex].Term {
				nextIndex = i + 1
				break
			}
		}
		reply.NextIndex = nextIndex
		r.Logs = r.Logs[:nextIndex]
		r.persist()
		r.Unlock()
		return
	}
	newLogs := append(r.Logs[:req.PrevLogIndex+1], req.Entries...)
	r.Logs = newLogs
	r.persist()
	if req.LeaderCommit > r.commitIndex {
		to := int(math.Min(float64(req.LeaderCommit), float64(r.getLastLog().Index)))
		if r.Logs[to].Term == r.CurrentTerm {
			r.commitIndex = to
			r.applyMsg()
		}
	}
	reply.Success = true
	r.Unlock()
	return
}

func (r *Raft) healthyCheck() {
	for r.killed() == false {
		time.Sleep(time.Duration(getRand(r.me)) * time.Millisecond)
		if r.getState() == LEADER && r.killed() == false {
			ticker := time.NewTicker(HEART_BEAT_DURATION)
			select {
			case <-ticker.C:
				if r.getState() == LEADER && r.killed() == false {
					r.appendEntries()
				}
			case <-r.logReplicateChannel:
				if r.getState() == LEADER && r.killed() == false {
					r.appendEntries()
				}
			}
		}
	}
}

func (r *Raft) applyMsg() {
	for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
		r.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      r.Logs[i].Commend,
			CommandIndex: r.Logs[i].Index,
		}
	}
	r.lastApplied = r.commitIndex
}

func (r *Raft) appendEntries() {
	r.debug("[appendEntries] 开始进行日志同步检测")
	replicateNum := 1
	disconnectNum := 0
	lastLog := r.getLastLog()
	for peerIndex, _ := range r.peers {
		if peerIndex == r.me {
			continue
		}
		go func(server int) {
			r.Lock()
			if r.state != LEADER || r.killed() == true {
				r.Unlock()
				return
			}
			prevIndex, prevTerm := r.getPrevLogInfo(server)
			req := &AppendEntriesArgs{
				Term:         r.CurrentTerm,
				LeaderId:     r.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				LeaderCommit: r.commitIndex,
			}
			if lastLog.Index >= r.nextIndex[server] && r.nextIndex[server] >= 0 {
				req.Entries = r.Logs[r.nextIndex[server] : lastLog.Index+1]
			} else {
				req.Entries = []Log{}
			}
			resp := &AppendEntriesReply{}
			r.Unlock()
			// 注意这里的RPC可能会延迟达到7000毫秒才返回，注意别长时间占锁
			if ok := r.sendAppendEntries(server, req, resp); ok {
				r.Lock()
				if r.state != LEADER || r.killed() == true {
					r.Unlock()
					return
				}
				if resp.Success {
					r.debug("[appendEntries] server-%d日志同步成功", server)
					r.matchIndex[server] = lastLog.Index
					r.nextIndex[server] = r.matchIndex[server] + 1
					replicateNum += 1
					if replicateNum >= int(math.Ceil(float64(len(r.peers))/2)) {
						if lastLog.Term == r.CurrentTerm {
							r.debug("[appendEntries] 超过半数server同步成功，开始更新commitIndex，lastApplied")
							r.commitIndex = lastLog.Index
							r.applyMsg()
							r.debug("[appendEntries] 超过半数server同步成功，更新commitIndex，lastApplied完成")
						} else {
							r.debug("[appendEntries] 超过半数server同步成功，但不能提交非当前任期的log")
						}
					}
				} else if resp.Term > r.CurrentTerm {
					r.debug("[appendEntries] 检测到server-%d的任期大于当前任期，准备变为FLOWER", server)
					r.changeState(FLOWER, false)
					r.Unlock()
					return
				} else {
					r.debug("[appendEntries] server-%d日志同步失败", server)
					if resp.NextIndex >= 1 {
						r.nextIndex[server] = resp.NextIndex
					} else if r.nextIndex[server]-1 >= 1 {
						r.nextIndex[server] -= 1
					}
				}
				r.Unlock()
			} else {
				r.Lock()
				if r.state != LEADER || r.killed() == true {
					r.Unlock()
					return
				}
				disconnectNum += 1
				if disconnectNum >= int(math.Ceil(float64(len(r.peers)/2))) {
					r.debug("[appendEntries] 超过半数server失去响应, 重新变为flower")
					r.changeState(FLOWER, false)
				}
				r.debug("[appendEntries] server-%d RPC调用失败", server)
				r.Unlock()
			}
		}(peerIndex)
	}
}
