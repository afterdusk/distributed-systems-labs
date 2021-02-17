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
	"sort"
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
	tickFrequencyMS      = 10
	heartbeatFrequencyMS = 50
	minElectionTimeoutMS = 300
	maxElectionTimeoutMS = 800
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
// Helper method that generates a random duration (in milliseconds) given
// the provided lower and upper bounds.
//
func genRandomDuration(lowerMS int, upperMS int) time.Duration {
	duration := time.Duration(rand.Intn(upperMS-lowerMS)+lowerMS) * time.Millisecond
	return duration
}

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
	commitIndex  int
	lastApplied  int
	lastNewIndex int // tracks the index of the last log added in the current term

	// volatile state (leaders)
	nextIndex  []int
	matchIndex []int

	// election state
	state           int
	lastHeard       time.Time
	electionTimeout time.Duration
	votes           int

	// application state
	applyCh chan ApplyMsg
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	logIndex := len(rf.log)
	logTerm := 0
	if logIndex > 0 {
		logTerm = rf.log[logIndex-1].Term
	}
	return logIndex, logTerm
}

func (rf *Raft) getPrevLogIndexAndTerm(server int) (int, int) {
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}
	return prevLogIndex, prevLogTerm
}

func (rf *Raft) getLogDiff(server int) []LogEntry {
	start := rf.nextIndex[server] - 1
	end := len(rf.log)

	logDiff := make([]LogEntry, end-start)
	if len(logDiff) == 0 {
		return logDiff
	}
	copy(logDiff, rf.log[start:end])
	// TODO: Does logDiff live on the stack? Will it get deallocated?
	return logDiff
}

func (rf *Raft) getHighestReplicatedLogIndex() int {
	sortedMatchIndex := make([]int, len(rf.matchIndex))
	copy(sortedMatchIndex, rf.matchIndex)
	sort.Ints(sortedMatchIndex)
	return sortedMatchIndex[len(rf.peers)/2]
}

func (rf *Raft) hasMajority() bool {
	return rf.votes > len(rf.peers)/2
}

func (rf *Raft) isLeader() bool {
	return rf.state == leader
}

func (rf *Raft) hasTimedOut(now time.Time) bool {
	return rf.lastHeard.Add(rf.electionTimeout).Before(now)
}

// forces a command to log and returns index of log record
func (rf *Raft) forceLog(command interface{}) int {
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := len(rf.log)
	rf.matchIndex[rf.me] = index
	return index
}

// applies all unapplied commits
func (rf *Raft) applyCommits() {
	for rf.lastApplied < rf.commitIndex && rf.lastApplied < len(rf.log) {
		rf.lastApplied++
		command := rf.log[rf.lastApplied-1].Command
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) updateCommitIndex(leaderCommit int) {
	if rf.lastNewIndex < leaderCommit {
		rf.commitIndex = rf.lastNewIndex
	} else {
		rf.commitIndex = leaderCommit
	}
}

func (rf *Raft) commitReplicatedLogs() {
	replicatedLogIndex := rf.getHighestReplicatedLogIndex()
	if replicatedLogIndex > rf.commitIndex && rf.log[replicatedLogIndex-1].Term == rf.currentTerm {
		rf.commitIndex = replicatedLogIndex
	}
}

func (rf *Raft) maybeVoteFor(candidate int, term int, logIndex int, logTerm int) bool {
	validTerm := term >= rf.currentTerm
	canVote := rf.votedFor == nil || *rf.votedFor == candidate
	upToDate := len(rf.log) == 0 ||
		rf.log[len(rf.log)-1].Term < logTerm ||
		rf.log[len(rf.log)-1].Term == logTerm && len(rf.log) <= logIndex

	if !(validTerm && canVote && upToDate) {
		return false
	}

	rf.votedFor = &candidate
	return true
}

func (rf *Raft) maybeAppend(term int, logIndex int, logTerm int, entries []LogEntry) bool {
	validTerm := term >= rf.currentTerm
	samePrevLog := logIndex <= 0 ||
		(logIndex-1 < len(rf.log) && rf.log[logIndex-1].Term == logTerm)

	if !(validTerm && samePrevLog) {
		return false
	}

	for _, entry := range entries {
		if len(rf.log) > logIndex {
			rf.log[logIndex] = entry
		} else {
			rf.log = append(rf.log, entry)
		}
		logIndex++
	}
	rf.lastNewIndex = logIndex
	return true
}

func (rf *Raft) initState() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = follower
	rf.lastHeard = time.Now()
	rf.electionTimeout = genRandomDuration(minElectionTimeoutMS, maxElectionTimeoutMS)
}

func (rf *Raft) maybeBecomeFollower(term int) bool {
	if term <= rf.currentTerm {
		return false
	}

	DPrintf("%v (term %v) becomes follower", rf.me, rf.currentTerm)
	rf.currentTerm = term
	rf.votedFor = nil
	rf.lastNewIndex = 0
	rf.state = follower
	// TODO: Is the following necessary?
	// rf.lastHeard = time.Now()
	return true

}

func (rf *Raft) becomeCandidate() {
	DPrintf("%v (term %v) becomes candidate", rf.me, rf.currentTerm)
	rf.currentTerm++
	rf.votedFor = &rf.me
	rf.lastNewIndex = 0
	rf.state = candidate
	rf.lastHeard = time.Now()
	rf.electionTimeout = genRandomDuration(minElectionTimeoutMS, maxElectionTimeoutMS)
	rf.votes = 1
}

func (rf *Raft) becomeLeader() {
	DPrintf("%v (term %v) becomes leader\n", rf.me, rf.currentTerm)
	rf.state = leader
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.isLeader()

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
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	// general
	Term int

	// entries
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	// commits
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.maybeBecomeFollower(args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = rf.maybeVoteFor(args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm)
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.maybeBecomeFollower(args.Term)
	if rf.currentTerm == args.Term {
		rf.lastHeard = time.Now()
	}

	reply.Term = rf.currentTerm
	reply.Success = rf.maybeAppend(args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	if reply.Success && rf.commitIndex < args.LeaderCommit {
		rf.updateCommitIndex(args.LeaderCommit)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.maybeBecomeFollower(reply.Term)
		if reply.VoteGranted {
			rf.votes++
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, retry bool) {
	reply := &AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.maybeBecomeFollower(reply.Term)
		if !rf.isLeader() {
			return
		}

		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else if retry {
			rf.nextIndex[server] = args.PrevLogIndex
			prevLogIndex, prevLogTerm := rf.getPrevLogIndexAndTerm(server)
			logDiff := rf.getLogDiff(server)
			retryArgs := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      logDiff,
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendAppendEntries(server, retryArgs, true)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if isLeader = rf.isLeader(); !isLeader {
		return index, term, isLeader
	}
	index = rf.forceLog(command)
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.initState()
	go rf.tick()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//
// Server routine acting as internal clock, called every unit of time as specified by tickFrequencyMS.
// if server is leader,              sends AppendEntries RPC to peers accordingly (heartbeats not included)
// if server is candidate,           count votes
// if server is candidate or server, start leader election when it hasn't heard from leader for a while.
//
func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// always reschedule this routine if server is not killed
	if !rf.killed() {
		defer time.AfterFunc(time.Duration(tickFrequencyMS)*time.Millisecond, rf.tick)
	}

	// always apply any unapplied commits
	rf.applyCommits()

	switch rf.state {
	case leader:
		// send AppendEntries RPC
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			logDiff := rf.getLogDiff(i)
			if len(logDiff) == 0 {
				continue
			}
			prevLogIndex, prevLogTerm := rf.getPrevLogIndexAndTerm(i)
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      logDiff,
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendAppendEntries(i, args, true)
		}
		rf.commitReplicatedLogs()
	case candidate:
		// count votes
		if hasMajority := rf.hasMajority(); hasMajority {
			rf.becomeLeader()
			go rf.heartbeat()
			break
		}
		fallthrough
	case follower:
		if !rf.hasTimedOut(time.Now()) {
			break
		}

		// leader election
		rf.becomeCandidate()
		lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendRequestVote(i, args)
		}
	}
}

//
// Leader routine that sends periodic AppendEntries heartbeats to peers.
//
func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// stop this routine if server is deposed
	if !rf.isLeader() {
		return
	}

	// always reschedule this routine if server is not killed
	if !rf.killed() {
		defer time.AfterFunc(time.Duration(heartbeatFrequencyMS)*time.Millisecond, rf.heartbeat)
	}

	// send empty AppendEntries
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		prevLogIndex, prevLogTerm := rf.getPrevLogIndexAndTerm(i)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendAppendEntries(i, args, false)
	}
}
