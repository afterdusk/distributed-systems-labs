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

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// Program constants.
//
const (
	tickFrequencyMS      = 5
	heartbeatFrequencyMS = 20
	minElectionTimeoutMS = 100
	maxElectionTimeoutMS = 2000
)

//
// Raft server states.
//
const (
	follower  = iota
	candidate = iota
	leader    = iota
)

//
// Log entry object.
//
type LogEntry struct {
	Term    int
	Command interface{}
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

	// persistent state
	currentTerm int
	votedFor    *int
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state (leaders)
	nextIndex  []int
	matchIndex []int

	// election state
	state           int
	lastHeard       time.Time
	electionTimeout time.Duration
	votes           int
}

func (rf *Raft) updateLastHeard(now time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeard = now
}

func (rf *Raft) updateCommit(leaderCommit int, lastNewIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if leaderCommit > rf.commitIndex {
		smaller := leaderCommit
		if lastNewIndex+1 < smaller {
			smaller = lastNewIndex + 1
		}
		rf.commitIndex = smaller
	}
}

func (rf *Raft) maybeVoteFor(server int, term int, logIndex int) (bool, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	validTerm := term >= rf.currentTerm
	canVote := rf.votedFor == nil || *rf.votedFor == server
	upToDate := len(rf.log)-1 <= logIndex

	if validTerm && canVote && upToDate {
		DPrintf("%v (term %v) granted vote to %v (term %v)\n", rf.me, rf.currentTerm, server, term)
		rf.votedFor = &server
		return true, rf.currentTerm
	}
	return false, rf.currentTerm
}

func (rf *Raft) maybeAppend(term int, logIndex int, logTerm int, entries *[]LogEntry) (bool, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	validTerm := term >= rf.currentTerm
	samePrevLog := logIndex < 0 || rf.log[logIndex].Term != logTerm

	if validTerm && samePrevLog {
		i := logIndex + 1
		for _, entry := range *entries {
			// TODO: Is it safe to always overwrite?
			if len(rf.log) > i {
				rf.log[i] = entry
			} else {
				rf.log = append(rf.log, entry)
			}
			// TODO: Correct place to increment term seen?
			rf.currentTerm = entry.Term
			i++
		}
		return true, i
	}
	return false, -1
}

// returns true if majority is achieved
func (rf *Raft) addVoteAndTestMajority() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votes++
	return rf.votes > len(rf.peers)/2
}

// TODO: Consider using a function that returns a read-only copy of state instead
func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state == leader
}

// TODO: Consider using a function that returns a read-only copy of state instead
func (rf *Raft) hasTimedOut(now time.Time) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.lastHeard.Add(rf.electionTimeout).Before(now)
}

// TODO: Consider using a function that returns a read-only copy of state instead
func (rf *Raft) getTermAndLog() (term int, logIndex int, logTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	logIndex = len(rf.log) - 1
	logTerm = 0
	if logIndex >= 0 {
		logTerm = rf.log[logIndex].Term
	}
	return term, logIndex, logTerm
}

func (rf *Raft) initState() {
	rf.mu.Lock()
	rf.mu.Unlock()

	rf.state = follower
	rf.lastHeard = time.Now()
}

func (rf *Raft) maybeBecomeFollower(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == follower || term <= rf.currentTerm {
		return false
	}

	DPrintf("%v (term %v) becomes follower\n", rf.me, rf.currentTerm)
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = nil
	return true
}

func (rf *Raft) maybeBecomeCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == leader {
		return false
	}

	DPrintf("%v (term %v) becomes candidate\n", rf.me, rf.currentTerm)
	rf.state = candidate
	rf.votedFor = &rf.me
	rf.votes = 1
	rf.currentTerm++
	rf.electionTimeout = time.Duration(rand.Intn(maxElectionTimeoutMS-minElectionTimeoutMS)+minElectionTimeoutMS) * time.Millisecond
	return true
}

func (rf *Raft) maybeBecomeLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != candidate {
		return false
	}

	DPrintf("%v (term %v) becomes leader\n", rf.me, rf.currentTerm)
	rf.state = leader
	return true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	term, _, _ := rf.getTermAndLog()
	isleader := rf.isLeader()
	DPrintf("%v (term %v) isLeader: %v\n", rf.me, term, isleader)
	return term, isleader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.maybeBecomeFollower(args.Term)

	reply.VoteGranted, reply.Term = rf.maybeVoteFor(args.CandidateID, args.Term, args.LastLogIndex)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.maybeBecomeFollower(args.Term)
	rf.updateLastHeard(time.Now())

	var lastNewIndex int
	reply.Success, lastNewIndex = rf.maybeAppend(args.Term, args.PrevLogIndex, args.PrevLogTerm, &args.Entries)

	// TODO: Is it safe to split the critical section? rf.commitIndex might change
	rf.updateCommit(args.LeaderCommit, lastNewIndex)
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
func (rf *Raft) sendRequestVote(server int, term int, logIndex int, logTerm int) {
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: logIndex,
		LastLogTerm:  logTerm,
	}
	reply := &RequestVoteReply{}
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); ok {
		rf.maybeBecomeFollower(reply.Term)

		if reply.VoteGranted {
			if hasMajority := rf.addVoteAndTestMajority(); hasMajority {
				if becameLeader := rf.maybeBecomeLeader(); becameLeader {
					go rf.heartbeat()
				}
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, term int, logIndex int, logTerm int) {
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: logIndex,
		PrevLogTerm:  logTerm,
	}
	reply := &AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		rf.maybeBecomeFollower(reply.Term)
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
	rf.initState()
	go rf.tick()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//
// starts leader election when it hasn't heard from another
// peer for a while.
//
func (rf *Raft) tick() {
	defer time.AfterFunc(time.Duration(tickFrequencyMS)*time.Millisecond, rf.tick)

	if rf.isLeader() || !rf.hasTimedOut(time.Now()) {
		return
	}

	rf.maybeBecomeCandidate()

	// TODO: Confirm if LastLogIndex is the length - 1, and not the lastCommitted
	currentTerm, lastLogIndex, lastLogTerm := rf.getTermAndLog()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, currentTerm, lastLogIndex, lastLogTerm)
	}
}

//
// sends periodic append entries to peers if leader.
//
func (rf *Raft) heartbeat() {
	if !rf.isLeader() {
		return
	}

	defer time.AfterFunc(time.Duration(heartbeatFrequencyMS)*time.Millisecond, rf.heartbeat)

	// TODO: Confirm if PrevLogIndex is the length - 1
	currentTerm, prevLogIndex, prevLogTerm := rf.getTermAndLog()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, currentTerm, prevLogIndex, prevLogTerm)
	}
}
