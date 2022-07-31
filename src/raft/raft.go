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
	"log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

type Log struct {
	index int
	term int
}

type RaftState int

const (
	flower RaftState = iota
	candidate
	leader
	heartBeatTime = 175 * time.Millisecond //心跳时间为最小超时时间的一半
)

func generateElectionTimeOut() time.Duration {
	//选举超时时间为350毫秒~500毫秒
	return time.Duration(350+rand.Intn(150)) * time.Millisecond
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

	state       RaftState
	currentTerm int
	votedFor    int
	logs  []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	heartBeat   chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == leader
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
	term         int
	candidateId  int64
	lastLogIndex int64
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term        int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//向candidate回复该server的当前时期，以更新candidate自身的当前时期
	reply.term = rf.currentTerm
	reply.voteGranted = true

	//当该server已经用了投票权，或者已经将票投给了request的candidate直接返回true
	if rf.votedFor != -1 || rf.votedFor == args.candidateId {
		return
	}
	//当candidate日志日期落后于该server时，拒绝给他投票，返回false
	if args.lastLogTerm < rf.currentTerm || (args.lastLogTerm == rf.currentTerm && args.lastLogIndex < int64(len(rf.logs)-1)) {
		reply.voteGranted = false
	}
	return
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
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	term         int //leader term
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []Log
	leaderCommit int
}

type AppendEntriesReply struct {
	term    int  //current term,for leader to update itself
	success bool //true if flower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

/*
	问题1:当leader进行一致性检测的时候如果发现server的term比自己更大，是否意味着自己不是最新的leader?
	回答：我觉得应该是的，raft确保了同一个term只有一个leader存在，如果当前leader发现了比自己还要大的term，肯定自己就不是最新的leader，更改自己的状态为flower，并有可能准备在下次收到最新
         leader的心跳时进行回滚日志，重新跟上步伐。
*/
func (rf *Raft) AppendEntries(heartbeat bool) {
	lastLog := rf.logs[len(rf.logs)-1]
	for peerIndex, _ := range rf.peers {
		if peerIndex == rf.me {
			continue
		}
		/* 这里为什么是用nextIndex数组来进行一致性检验，因为对于每个server他们的日志可能和leader的日志步伐都不是一致的，有的慢一些，有的快一些。所以用nextIndex来跟踪每一个server
		的日志步伐，且能一次更新到最新的日志。 */
		if lastLog.index > rf.nextIndex[peerIndex] || heartbeat {
			nextIndex := rf.nextIndex[peerIndex]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if lastLog.index+1 < nextIndex {
				nextIndex = lastLog.index
			}
			prevLog := rf.logs[nextIndex-1]
			req := &AppendEntriesArgs{
				term: rf.currentTerm,
				leaderId: rf.me,
				prevLogIndex: prevLog.index,
				prevLogTerm: prevLog.term,
				entries: make([]Log, lastLog.index-nextIndex+1),
				leaderCommit: rf.commitIndex,
			}
			copy(req.entries, rf.logs[nextIndex:])
			resp := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peerIndex, req, resp)
			if !ok {
				return
			}
			rf.mu.Lock()

			rf.mu.Unlock()
		}
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		if rf.state == leader {
			//如果是leader就周期性的发送心跳
			time.Sleep(heartBeatTime)
			//todo 发送心跳
			for peerIndex, _ := range rf.peers {
				if peerIndex == rf.me {
					continue
				}
				req := AppendEntriesArgs{
					term: rf.currentTerm,
					leaderId: rf.me,
					prevLogIndex:

				}
				go rf.sendAppendEntries(peerIndex)
			}
		} else {
			//如果不是leader并且在选举超时时间内没有收到信息就发起下一轮选举
			select {
			case <-time.After(generateElectionTimeOut()):
				log.Printf("server [%d] 选举时间超时，开始发起新一轮选举", rf.me)
				//todo 开始下一轮选举
			case <-rf.heartBeat:
				//收到心跳，继续稳住，不会谋反
			}
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
