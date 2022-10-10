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
	"bytes"
	"log"
	"mit6.824-lab/labgob"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"mit6.824-lab/labrpc"
)

type RaftState int

const (
	FLOWER RaftState = iota
	CANDIDATE
	LEADER
)

var Begin = time.Now()

type Raft struct {
	sync.RWMutex                     // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent variables
	CurrentTerm int
	VotedFor    int
	Logs        []Log

	state             RaftState
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int
	lastHeartBeatTime time.Time
	applyCh           chan ApplyMsg

	electionChannel     chan struct{}
	logReplicateChannel chan struct{}
}

func (r *Raft) GetState() (int, bool) {
	// Your code here (2A).
	r.RLock()
	defer r.RUnlock()
	return r.CurrentTerm, r.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm int
		votedFor    int
		logs        = make([]Log, 0)
	)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatalf("raft读取持久化数据失败")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Logs = logs
		rf.debug("读取持久化数据 currentTerm:%v, votedFor:%v, logs:%v", currentTerm, votedFor, logs)
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

func (rf *Raft) Kill() {
	rf.debug("server-%d was killed", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	r := &Raft{
		state:               FLOWER,
		VotedFor:            -1,
		commitIndex:         0,
		lastApplied:         0,
		electionChannel:     make(chan struct{}, 10),
		logReplicateChannel: make(chan struct{}, 10),
		applyCh:             applyCh,
		Logs:                []Log{{Index: 0, Term: 0}},
		peers:               peers,
		nextIndex:           make([]int, len(peers)),
		matchIndex:          make([]int, len(peers)),
		persister:           persister,
		me:                  me,
	}
	r.readPersist(persister.ReadRaftState())
	r.debug("raft initial over")
	go r.watchElectionTimeout()
	go r.healthyCheck()
	return r
}
