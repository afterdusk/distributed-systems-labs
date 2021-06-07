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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//
// Program constants.
//
const (
	tickFrequencyMS      = 20  // server clock frequency
	heartbeatFrequencyMS = 100 // leader heartbeat frequency
	minElectionTimeoutMS = 300
	maxElectionTimeoutMS = 800
	gobNilDummyValue     = -1 // dummy value to replace nil values for encoding with gob
)

//
// Raft server states.
//
type state int

const (
	follower state = iota
	candidate
	leader
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

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu         sync.Mutex          // Lock to protect shared access to this peer's state
	commitCond *sync.Cond          // Condition variable to synchronize on commit index
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	Me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()
	applyCh    chan ApplyMsg       // channel to send ApplyMsg to service

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	// fields should only be modified via setter functions
	currentTerm       int
	votedFor          *int
	log               []LogEntry
	logTerm           int // term of last log entry
	lastIncludedIndex int // offset of log due to snapshots
	lastIncludedTerm  int // term of last log entry in snapshot

	// volatile state
	commitIndex int // should only be modified via setter function
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	currentState    state         // follower, candidate or leader
	lastHeard       time.Time     // time that last valid AppendEntries was received
	electionTimeout time.Duration // election timeout
	lastNewIndex    int           // index of the last log added in the current term
	votes           int           // number of votes received in current term
	snapshotMsg     *ApplyMsg     // snapshot to apply
}

// sets commit index and broadcasts event via commitCond
func (rf *Raft) setCommitIndex(index int) {
	DPrintf("[Raft %v][setCommitIndex] index: %v\n", rf.Me, index)
	rf.commitIndex = index
	rf.commitCond.Broadcast()
}

// sets and persists currentTerm and votedFor
func (rf *Raft) setTermAndVotedFor(currentTerm int, votedFor *int) {
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.persister.SaveRaftState(rf.encodeState())
}

// sets and persists log entries starting from index provided
// returns index of last entry set
func (rf *Raft) setLog(index int, entries []LogEntry) int {
	DPrintf("[Raft %v][setLog][pre set] index: %v, rf.lastIncludedIndex: %v, entries: %v\n", rf.Me, index, rf.lastIncludedIndex, entries)
	adjustedIndex := index - rf.lastIncludedIndex - 1
	for _, entry := range entries {
		// ignore log entries that are inside of snapshot
		if adjustedIndex >= 0 {
			if len(rf.log) <= adjustedIndex {
				rf.log = append(rf.log, entry)
			} else if rf.log[adjustedIndex] != entry {
				// truncate conflicting entries
				rf.log = rf.log[:adjustedIndex]
				rf.log = append(rf.log, entry)
			}
			rf.logTerm = rf.log[adjustedIndex].Term
		}
		DPrintf("[Raft %v][setLog][mid set] index: %v, rf.lastIncludedIndex: %v, adjustedIndex: %v, rf.log: %v\n", rf.Me, index, rf.lastIncludedIndex, adjustedIndex, rf.log)
		adjustedIndex++
	}
	rf.persister.SaveRaftState(rf.encodeState())
	DPrintf("[Raft %v][setLog][post set] len(rf.log): %v\n", rf.Me, len(rf.log))
	return adjustedIndex + rf.lastIncludedIndex
}

// deletes the log up to and including an index
func (rf *Raft) clearLog(index int) {
	rf.log = rf.log[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	logIndex := len(rf.log) + rf.lastIncludedIndex
	logTerm := rf.logTerm
	return logIndex, logTerm
}

func (rf *Raft) getPrevLogIndexAndTerm(server int) (int, int) {
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.lastIncludedTerm
	if prevLogIndex > rf.lastIncludedIndex {
		prevLogTerm = rf.log[prevLogIndex-1-rf.lastIncludedIndex].Term
	}
	DPrintf("[Raft %v][getPrevLogIndexAndTerm] prevLogTerm: %v, prevLogIndex: %v, rf.log: %v\n", rf.Me, prevLogTerm, prevLogIndex, rf.log)
	return prevLogIndex, prevLogTerm
}

// returns log entries that are not present in a peer server
func (rf *Raft) getLogDiff(server int) []LogEntry {
	start := rf.nextIndex[server] - 1 - rf.lastIncludedIndex
	end := len(rf.log)
	DPrintf("[Raft %v][getLogDiff] target: %v, rf.nextIndex: %v, rf.lastIncludedIndex: %v, start: %v, end: %v\n", rf.Me, server, rf.nextIndex[server], rf.lastIncludedIndex, start, end)
	logDiff := make([]LogEntry, end-start)
	copy(logDiff, rf.log[start:end])
	return logDiff
}

// returns highest log entry replicated on a majority of servers
func (rf *Raft) getHighestReplicatedIndex() int {
	sortedMatchIndex := make([]int, len(rf.matchIndex))
	copy(sortedMatchIndex, rf.matchIndex)
	sort.Ints(sortedMatchIndex)
	DPrintf("[Raft %v][getHighestReplicatedIndex] sortedMatchIndex: %v\n", rf.Me, sortedMatchIndex)
	return sortedMatchIndex[(len(rf.peers)-1)/2]
}

// returns the first log index for a given term
func (rf *Raft) getFirstLogIndex(term int) int {
	return sort.Search(len(rf.log), func(i int) bool {
		return rf.log[i].Term >= term
	}) + 1 + rf.lastIncludedIndex
}

// returns true if the server can vote for a particular candidate
func (rf *Raft) canVoteFor(candidate int, term int, logIndex int, logTerm int) bool {
	validTerm := term >= rf.currentTerm
	canVote := rf.votedFor == nil || *rf.votedFor == candidate
	upToDate := len(rf.log) == 0 && rf.lastIncludedIndex == 0 ||
		rf.logTerm < logTerm ||
		rf.logTerm == logTerm && len(rf.log)+rf.lastIncludedIndex <= logIndex

	DPrintf("[Raft %v] canVoteFor %v: %v,validTerm: %v (term: %v, currentTerm: %v), canVote: %v, upToDate: %v", rf.Me, candidate, validTerm && canVote && upToDate, validTerm, term, rf.currentTerm, canVote, upToDate)
	return validTerm && canVote && upToDate
}

// returns true if the server can append a log with a given index and term
// if false, the first index of the conflicting term (if any) will be returned
func (rf *Raft) canAppend(term int, logIndex int, logTerm int) (bool, int) {
	adjustedIndex := logIndex - 1 - rf.lastIncludedIndex

	validTerm := term >= rf.currentTerm
	samePrevLog := logIndex == 0 ||
		(logIndex == rf.lastIncludedIndex && logTerm == rf.lastIncludedTerm) ||
		(adjustedIndex >= 0 && adjustedIndex < len(rf.log) && rf.log[adjustedIndex].Term == logTerm)

	DPrintf("[Raft %v][canAppend] validTerm: %v (term: %v, currentTerm: %v), samePrevLog: %v (rf.lastIncludedIndex: %v, logIndex: %v, logTerm:%v, rf.lastIncludedTerm: %v, rf.logTerm: %v, rf.log: %v)\n", rf.Me, validTerm, term, rf.currentTerm, samePrevLog, rf.lastIncludedIndex, logIndex, logTerm, rf.lastIncludedTerm, rf.logTerm, rf.log)

	// success, correct term
	if validTerm && samePrevLog {
		return true, -1
	}

	// failure, correct term but mismatched logs
	if validTerm {
		conflictFirstIndex := 1 + rf.lastIncludedIndex
		// first assume log is lagging leader
		if len(rf.log) > 0 {
			conflictFirstIndex = len(rf.log) + rf.lastIncludedIndex
		}
		// if log is not lagging but has conflicts
		if logIndex <= len(rf.log)+rf.lastIncludedIndex && adjustedIndex >= 0 {
			conflictTerm := rf.log[adjustedIndex].Term
			conflictFirstIndex = rf.getFirstLogIndex(conflictTerm)
		}
		return false, conflictFirstIndex
	}

	// failure, incorrect term
	return false, -1
}

func (rf *Raft) initState() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.currentState = follower
	rf.lastHeard = time.Now()
	rf.electionTimeout = genRandomDuration(minElectionTimeoutMS, maxElectionTimeoutMS)
}

func (rf *Raft) maybeBecomeFollower(term int) bool {
	if term <= rf.currentTerm {
		return false
	}

	rf.setTermAndVotedFor(term, nil)
	rf.lastNewIndex = 0
	rf.currentState = follower
	return true
}

func (rf *Raft) becomeCandidate() {
	rf.setTermAndVotedFor(rf.currentTerm+1, &rf.Me)
	rf.lastNewIndex = 0
	rf.currentState = candidate
	rf.lastHeard = time.Now()
	rf.electionTimeout = genRandomDuration(minElectionTimeoutMS, maxElectionTimeoutMS)
	rf.votes = 1
}

func (rf *Raft) becomeLeader() {
	DPrintf("[Raft %v] Becomes leader of term %v\n", rf.Me, rf.currentTerm)
	rf.currentState = leader
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log) + 1 + rf.lastIncludedIndex
		rf.matchIndex[i] = 0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	// DPrintf("[Raft %v][Lock][GetState]", rf.Me)
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.currentState == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) encodeState() []byte {
	// Your code here (2C).
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	if err := enc.Encode(rf.currentTerm); err != nil {
		log.Fatal(err)
	}
	// encode nil as gobNilDummyValue, since gob cannot encode nil pointers
	votedFor := gobNilDummyValue
	if rf.votedFor != nil {
		votedFor = *rf.votedFor
	}
	if err := enc.Encode(votedFor); err != nil {
		log.Fatal(err)
	}
	if err := enc.Encode(rf.log); err != nil {
		log.Fatal(err)
	}
	if err := enc.Encode(rf.lastIncludedIndex); err != nil {
		log.Fatal(err)
	}
	if err := enc.Encode(rf.logTerm); err != nil {
		log.Fatal(err)
	}
	if err := enc.Encode(rf.lastIncludedTerm); err != nil {
		log.Fatal(err)
	}
	data := buf.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) decodeState(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	if err := dec.Decode(&rf.currentTerm); err != nil {
		log.Fatal(err)
	}
	var votedFor int
	if err := dec.Decode(&votedFor); err != nil {
		log.Fatal(err)
	}
	if votedFor == gobNilDummyValue {
		rf.votedFor = nil
	} else {
		rf.votedFor = &votedFor
	}
	if err := dec.Decode(&rf.log); err != nil {
		log.Fatal(err)
	}
	if err := dec.Decode(&rf.lastIncludedIndex); err != nil {
		log.Fatal(err)
	}
	rf.commitIndex = rf.lastIncludedIndex
	if err := dec.Decode(&rf.logTerm); err != nil {
		log.Fatal(err)
	}
	if err := dec.Decode(&rf.lastIncludedTerm); err != nil {
		log.Fatal(err)
	}
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
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term               int
	Success            bool
	ConflictFirstIndex int // if failure, holds index of first entry with the conflicting term
}

//
// InstallSnapshot RPC arguments structure.
//
type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

//
// InstallSnapshot RPC reply structure.
//
type InstallSnapshotReply struct {
	Term int
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// DPrintf("[Raft %v][Lock][RequestVote]", rf.Me)
	defer rf.mu.Unlock()

	rf.maybeBecomeFollower(args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = rf.canVoteFor(args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm)
	if !reply.VoteGranted {
		return
	}

	// vote for candidate
	rf.setTermAndVotedFor(rf.currentTerm, &args.CandidateID)

	// reset last heard to prevent timeout before receiving heartbeat
	rf.lastHeard = time.Now()
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// DPrintf("[Raft %v][Lock][AppendEntries]", rf.Me)
	defer rf.mu.Unlock()

	rf.maybeBecomeFollower(args.Term)
	if rf.currentTerm == args.Term {
		rf.lastHeard = time.Now()
	}

	DPrintf("[Raft %v] Received AppendEntries: %v", rf.Me, args)
	reply.Term = rf.currentTerm
	reply.Success, reply.ConflictFirstIndex = rf.canAppend(args.Term, args.PrevLogIndex, args.PrevLogTerm)
	if !reply.Success {
		DPrintf("[Raft %v] Cannot append: %v", rf.Me, args)
		return
	}
	DPrintf("[Raft %v][AppendEntries] Can append: %v\n", rf.Me, args)

	// append new entries
	lastNewIndex := rf.setLog(args.PrevLogIndex+1, args.Entries)
	if rf.lastNewIndex < lastNewIndex {
		rf.lastNewIndex = lastNewIndex
	}

	// update commit index
	if rf.commitIndex < args.LeaderCommit {
		if rf.lastNewIndex < args.LeaderCommit {
			DPrintf("[Raft %v][AppendEntries] setCommitIndex rf.lastNewIndex: %v\n", rf.Me, rf.lastNewIndex)
			rf.setCommitIndex(rf.lastNewIndex)
		} else {
			DPrintf("[Raft %v][AppendEntries] setCommitIndex args.LeaderCommit: %v\n", rf.Me, args.LeaderCommit)
			rf.setCommitIndex(args.LeaderCommit)
		}
	}
}

//
// InstallSnapshot RPC handler.
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// DPrintf("[Raft %v] Waiting...", rf.me)
	rf.mu.Lock()
	// DPrintf("[Raft %v][Lock][InstallSnapshot]", rf.Me)
	defer rf.mu.Unlock()

	rf.maybeBecomeFollower(args.Term)
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		DPrintf("[Raft %v][InstallSnapshot] (term %v) failed on term %v\n", rf.Me, args.Term, rf.currentTerm)
		return
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		DPrintf("[Raft %v][InstallSnapshot] args.LastIncludedIndex (%v) > rf.lastIncludedIndex (%v) \n", rf.Me, args.LastIncludedIndex, rf.lastIncludedIndex)
		return
	}

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	// rf.mu.Unlock()
	// // TODO: If mutex is not released, two InstallSnapshots in quick succession will result in this lock beind held and preventing
	// // CondInstallSnapshot from acquiring lock
	// rf.applyCh <- applyMsg
	// rf.mu.Lock()
	// rf.lastApplied = args.LastIncludedIndex
	rf.snapshotMsg = &applyMsg
	rf.commitCond.Broadcast()
	DPrintf("[Raft %v][InstallSnapshot] snapshotMsg: %v\n", rf.Me, *rf.snapshotMsg)
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
		// DPrintf("[Raft %v][Lock][sendRequestVote]", rf.Me)
		defer rf.mu.Unlock()

		rf.maybeBecomeFollower(reply.Term)

		// terminate if original request expired
		if rf.currentState != candidate || args.Term != rf.currentTerm {
			return
		}

		// count vote if success
		if reply.VoteGranted {
			rf.votes++
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, retry bool) {
	reply := &AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		rf.mu.Lock()
		// DPrintf("[Raft %v][Lock][sendAppendEntries]", rf.Me)
		defer rf.mu.Unlock()
		DPrintf("[Raft %v][sendAppendEntries %v] reply received - reply: %v\n", rf.Me, server, reply)

		rf.maybeBecomeFollower(reply.Term)

		// terminate if original request expired
		if args.Term != rf.currentTerm {
			return
		}

		// process successful request, or retry
		if reply.Success {
			nextIndex := args.PrevLogIndex + len(args.Entries) + 1
			if rf.nextIndex[server] < nextIndex {
				rf.nextIndex[server] = nextIndex
			}
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			DPrintf("[Raft %v][sendAppendEntries %v] reply.Success - rf.nextIndex[server]: %v, rv.matchIndex[server]: %v\n", rf.Me, server, rf.nextIndex[server], rf.matchIndex[server])
		} else if retry {
			DPrintf("[Raft %v][sendAppendEntries %v] retry - ConflictFirstIndex: %v, nextIndex[server]: %v, lastIncludedIndex: %v\n", rf.Me, server, reply.ConflictFirstIndex, rf.nextIndex[server], rf.lastIncludedIndex)
			if reply.ConflictFirstIndex <= rf.lastIncludedIndex {
				installSnapshotArgs := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				go rf.sendInstallSnapshot(server, installSnapshotArgs)
				return
			}
			rf.nextIndex[server] = reply.ConflictFirstIndex
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	DPrintf("[Raft %v][sendInstallSnapshot] target: %v, args: %v\n", rf.Me, server, args)
	reply := &InstallSnapshotReply{}
	var ok bool
	if ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply); ok {
		DPrintf("[Raft %v][sendInstallSnapshot] target: %v, args: %v, reply: %v\n", rf.Me, server, args, reply)
		rf.mu.Lock()
		// DPrintf("[Raft %v][Lock][sendInstallSnapshot]", rf.Me)
		defer rf.mu.Unlock()

		rf.maybeBecomeFollower(reply.Term)
	}
	// DPrintf("Reply: %v\n", ok)
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
	// Your code here (2B).
	rf.mu.Lock()
	// DPrintf("[Raft %v][Lock][Start]", rf.Me)
	defer rf.mu.Unlock()

	// reject command if server is not leader
	index := len(rf.log) + 1 + rf.lastIncludedIndex
	term := rf.currentTerm
	if rf.currentState != leader {
		return index, term, false
	}

	// append the command to log
	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	DPrintf("[Raft %v][Start] rf.currrentState == leader: %v, index: %v, logEntry: %v\n", rf.Me, rf.currentState == leader, index, logEntry)
	rf.setLog(index, []LogEntry{logEntry})
	rf.matchIndex[rf.Me] = index
	return index, term, true
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

func (rf *Raft) Size() int {
	return rf.persister.RaftStateSize()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	DPrintf("[Raft %v][CondInstallSnapshot][waiting] lastIncludedTerm :%v, lastIncludedIndex: %v, rf.lastIncludedIndex: %v, len(rf.log): %v\n", rf.Me, lastIncludedTerm, lastIncludedIndex, rf.lastIncludedIndex, len(rf.log))
	rf.mu.Lock()
	// DPrintf("[Raft %v][Lock][CondInstallSnapshot]", rf.Me)
	defer rf.mu.Unlock()

	if lastIncludedTerm < rf.logTerm {
		return false
	}
	if lastIncludedIndex < len(rf.log)+rf.lastIncludedIndex {
		return false
	}

	rf.log = []LogEntry{}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.logTerm = lastIncludedTerm
	rf.lastIncludedTerm = lastIncludedTerm
	// TODO: Do we need this here?
	// rf.lastApplied = rf.lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	// DPrintf("[Raft %v][Lock][Snapshot]", rf.Me)
	defer rf.mu.Unlock()

	DPrintf("[Raft %v][Snapshot][Pre-truncation] index: %v, rf.lastIncludedIndex: %v, len(rf.log): %v\n", rf.Me, index, rf.lastIncludedIndex, len(rf.log))
	rf.lastIncludedTerm = rf.log[index-1-rf.lastIncludedIndex].Term
	rf.clearLog(index)
	for i := range rf.peers {
		if rf.nextIndex[i] <= index+1 {
			rf.nextIndex[i] = index + 1
		}
	}
	DPrintf("[Raft %v][Snapshot][Post-truncation] index: %v, rf.lastIncludedIndex: %v, len(rf.log): %v, snapshot: %v", rf.Me, index, rf.lastIncludedIndex, len(rf.log), snapshot)
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
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
	rf.commitCond = sync.NewCond(&rf.mu)
	rf.peers = peers
	rf.persister = persister
	rf.Me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	rf.initState()                               // initialize state
	rf.decodeState(rf.persister.ReadRaftState()) // load any persisted state before a crash
	DPrintf("[Raft %v][Make] snapshot: %v", rf.Me, rf.persister.ReadSnapshot())
	if snapshot := rf.persister.ReadSnapshot(); len(snapshot) > 0 {
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
		}
		// rf.applyCh <- applyMsg
		// rf.lastApplied = rf.lastIncludedIndex
		rf.snapshotMsg = &applyMsg
		DPrintf("[Raft %v][Make] rf.lastIncludedIndex: %v, rf.lastApplied: %v\n", rf.Me, rf.lastIncludedIndex, rf.lastApplied)
	}
	go rf.tick()  // start ticker goroutine
	go rf.apply() // start goroutine to apply commits in the background

	return rf
}

//
// Server routine acting as internal clock, called every unit of time as specified by tickFrequencyMS.
// if server is leader,              sends AppendEntries RPC to peers accordingly (heartbeats not included)
// if server is candidate,           count votes
// if server is candidate or server, start leader election when it hasn't heard from leader for a while.
//
func (rf *Raft) tick() {
	for !rf.killed() {
		rf.mu.Lock()
		// DPrintf("[Raft %v][Lock][Tick]", rf.Me)

		switch rf.currentState {
		case leader:
			// send logs to lagging followers
			for i := range rf.peers {
				if i == rf.Me {
					continue
				}
				// if rf.nextIndex[i] <= rf.lastIncludedIndex {
				// 	installSnapshotArgs := &InstallSnapshotArgs{
				// 		Term:              rf.currentTerm,
				// 		LastIncludedIndex: rf.lastIncludedIndex,
				// 		LastIncludedTerm:  rf.lastIncludedTerm,
				// 		Data:              rf.persister.ReadSnapshot(),
				// 	}
				// 	go rf.sendInstallSnapshot(i, installSnapshotArgs)
				// 	continue
				// }

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
			// commit replicated logs
			replicatedIndex := rf.getHighestReplicatedIndex()
			canUpdate := replicatedIndex > rf.commitIndex && rf.logTerm == rf.currentTerm
			if canUpdate {
				rf.setCommitIndex(replicatedIndex)
			}
		case candidate:
			// count votes
			hasMajority := rf.votes > len(rf.peers)/2
			if hasMajority {
				rf.becomeLeader()
				go rf.heartbeat()
				break
			}
			fallthrough
		case follower:
			hasTimedOut := rf.lastHeard.Add(rf.electionTimeout).Before(time.Now())
			if !hasTimedOut {
				break
			}
			// leader election
			rf.becomeCandidate()
			lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.Me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			for i := range rf.peers {
				if i == rf.Me {
					continue
				}
				go rf.sendRequestVote(i, args)
			}
		}

		rf.mu.Unlock()
		time.Sleep(tickFrequencyMS * time.Millisecond)
	}
}

//
// Server routine that applies commits in the background.
//
func (rf *Raft) apply() {
	for !rf.killed() {
		rf.mu.Lock()
		// DPrintf("[Raft %v][Lock][apply first]", rf.Me)

		// relinquish lock and sleep if no work to be done
		if (rf.lastApplied == rf.commitIndex || rf.lastApplied == len(rf.log)+rf.lastIncludedIndex) && rf.snapshotMsg == nil {
			rf.commitCond.Wait()
		}

		if rf.snapshotMsg != nil {
			applyMsg := *rf.snapshotMsg
			rf.snapshotMsg = nil
			if applyMsg.SnapshotIndex > rf.lastApplied {
				DPrintf("[Raft %v][apply] Snapshot: %v", rf.Me, applyMsg)
				// send message outside of CS as channel may block
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
				rf.lastApplied = applyMsg.SnapshotIndex
			}

		}

		// apply commits up until commitIndex
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < len(rf.log)+rf.lastIncludedIndex {
			DPrintf("[Raft %v][apply] rf.lastApplied: %v, rf.lastIncludedIndex: %v,  len(rf.log): %v\n", rf.Me, rf.lastApplied, rf.lastIncludedIndex, len(rf.log))
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			DPrintf("[Raft %v][apply] applyMsg: %v, log: %v\n", rf.Me, applyMsg, rf.log)
			// send message outside of CS as channel may block
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
			// DPrintf("[Raft %v][Lock][apply second]", rf.Me)
			rf.lastApplied++
		}

		rf.mu.Unlock()
	}
}

//
// Leader routine that sends periodic AppendEntries heartbeats to peers.
//
func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		// DPrintf("[Raft %v][Lock][heartbeat]", rf.Me)

		// stop this routine if server is deposed
		if rf.currentState != leader {
			rf.mu.Unlock()
			return
		}

		// send empty AppendEntries
		for i := range rf.peers {
			if i == rf.Me {
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

		rf.mu.Unlock()
		time.Sleep(heartbeatFrequencyMS * time.Millisecond)
	}
}
