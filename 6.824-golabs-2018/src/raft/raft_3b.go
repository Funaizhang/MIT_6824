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
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"time"

	"github.com/sasha-s/go-deadlock"
)

type State int

const (
	Debug           = 0
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
	// double the paper election timeouts because only 10 heartbeats per second
	MinElectionTimeout int = 400
	MaxElectionTimeout int = 800
	LeaderSleepTimeout int = 50
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

type ApplyMsg struct {
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
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	Snapshot     []byte
}

type Raft struct {
	//
	// A Go object implementing a single Raft peer.
	//

	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Based on Fig 2 of Raft paper
	state State
	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Log
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	// Volatile state on leaders; reinitialized after election
	nextIndex  []int
	matchIndex []int

	applyCh             chan ApplyMsg
	receivedHeartbeatCh chan bool
	voteGrantedCh       chan bool
	wonElectionCh       chan bool
}

func (rf *Raft) GetState() (int, bool) {
	// return currentTerm and whether this server
	// believes it is the leader.

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) Kill() {
	//
	// the tester calls Kill() when a Raft instance won't
	// be needed again. you are not required to do anything
	// in Kill(), but it might be convenient to (for example)
	// turn off debug output from this instance.
	//
	// Your code here, if desired.
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
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

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.state = Follower
	// Persistent state on all servers
	rf.currentTerm = 0
	rf.votedFor = -1
	firstLog := Log{
		Term:  0,
		Index: 0,
	}
	rf.log = append(rf.log, firstLog)
	// Volatile state on all servers
	rf.commitIndex = 0 // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	rf.lastApplied = 0 // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// Volatile state on leaders; reinitialized after election in startElection()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.receivedHeartbeatCh = make(chan bool, 100)
	rf.voteGrantedCh = make(chan bool, 100)
	rf.wonElectionCh = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	rf.persist()

	// create a background goroutine that will kick off leader election periodically
	// by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	// This way a peer will learn who is the leader, if there is already a leader,
	// or become the leader itself.
	go rf.startElection()

	return rf
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
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

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = rf.getLastLogIndex() + 1
	// if server is Leader, append command to its logs at index
	if isLeader {
		newLog := Log{
			Term:    term,
			Index:   index,
			Command: command,
		}
		rf.log = append(rf.log, newLog)
		DPrintf("Start -- Leader[%d] Term[%d]: new log %+v.\n", rf.me, rf.currentTerm, newLog)
		rf.persist()
	}

	return index, term, isLeader
}

func (rf *Raft) getSaveRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	//
	// save Raft's persistent state to stable storage,
	// where it can later be retrieved after a crash and restart.
	// see paper's Figure 2 for a description of what should be persistent.
	//
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.log)
	// data := w.Bytes()

	state := rf.getSaveRaftState()
	rf.persister.SaveRaftState(state)
}

func (rf *Raft) MakeSnapshot(data []byte, index int) {
	//
	// save Snapshot
	//

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer rf.persist()

	// check if snapshot can be taken
	if index <= rf.getFirstLogIndex() || index > rf.getLastLogIndex() {
		return
	}

	offsetIndex := index - rf.getFirstLogIndex()
	rf.log = rf.trimLog(index, rf.log[offsetIndex].Term, rf.log)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.getFirstLogIndex())
	e.Encode(rf.getFirstLogTerm())
	snapshot := append(w.Bytes(), data...)

	// DPrintf("MakeSnapshot -- Server[%d] Term[%d]: index[%d]\n", rf.me, rf.currentTerm, index)

	state := rf.getSaveRaftState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	//
	// restore previously persisted state.
	//

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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("INVESTIGATE: readPersist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	//
	// restore previous Snapshot
	// similar to readPersist()
	//

	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("INVESTIGATE: readSnapshot")
	} else {
		// same process as last part of InstallSnapshot()
		rf.mu.Lock()
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
		rf.log = rf.trimLog(lastIncludedIndex, lastIncludedTerm, rf.log)
		// rf.applySnapshot(lastIncludedIndex, lastIncludedTerm)
		rf.mu.Unlock()

		// tell kvServer
		newApplyMsg := ApplyMsg{
			CommandValid: false,
			Snapshot:     data,
		}
		go func() { rf.applyCh <- newApplyMsg }()
	}

}

type RequestVoteArgs struct {
	//
	// example RequestVote RPC arguments structure.
	// field names must start with capital letters!
	//
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	//
	// example RequestVote RPC reply structure.
	// field names must start with capital letters!
	//
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//
	// example RequestVote RPC handler.
	//
	// Your code here (2A, 2B).
	// Receiver behavior during election

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// determine if candidate log is up-to-date
	voterLogIndex := rf.getLastLogIndex()
	voterLogTerm := rf.getLastLogTerm()
	var candidateLogUptodate bool

	switch {
	// If the logs have last entries with different terms,
	// then the log with the later term is more up-to-date.
	case voterLogTerm < args.LastLogTerm:
		candidateLogUptodate = true
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	case (voterLogTerm == args.LastLogTerm && voterLogIndex <= args.LastLogIndex):
		candidateLogUptodate = true
	default:
		candidateLogUptodate = false
	}

	switch {
	// 1. Reply false if term < currentTerm (§5.1)
	case args.Term < rf.currentTerm:
		// DPrintf("Vote -- Voter[%d] Term[%d]; Candidate[%d] Term[%d]: did not vote, candidate term outdated.\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	case args.Term > rf.currentTerm:
		// DPrintf("Vote -- Voter[%d] Term[%d]; Candidate[%d] Term[%d]: converted to follower.\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.transitionFollower(args.Term)

	default:
		assert(args.Term == rf.currentTerm, "INVESTIGATE: Vote -- args.Term != rf.currentTerm")
	}

	// Now, args.Term = rf.currentTerm

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	assert(args.Term == rf.currentTerm, "INVESTIGATE: Vote -- args.Term != rf.currentTerm")
	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateLogUptodate {
		// DPrintf("Vote -- Voter[%d] Term[%d]; Candidate[%d] Term[%d]: voted Candidate in election.\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = args.Term
		rf.state = Follower
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		go func() { rf.voteGrantedCh <- true }()
	} else if !candidateLogUptodate {
		// DPrintf("Vote -- Voter[%d] Term[%d]; Candidate[%d] Term[%d]: did not vote, Candidate log outdated.\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		// DPrintf("Vote -- Voter[%d] Term[%d]; Candidate[%d] Term[%d]: did not vote, else case.\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) doRequestVote() {
	//
	// Candidate behavior during election
	//

	rf.mu.Lock()
	// DPrintf("Term[%d], Server[%d]: doRequestVote() called.\n", rf.currentTerm, rf.me)

	voteCount := 1 // voted for itself
	majority := len(rf.peers) / 2
	state := rf.state
	term := rf.currentTerm
	rf.mu.Unlock()

	// request vote from peer servers
	for server, _ := range rf.peers {
		if server != rf.me && state == Candidate {
			go func(i int) {
				rf.mu.Lock()
				// if I am still candidate for this term
				if rf.state != Candidate || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}
				// define RequestVoteArgs
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.getLastLogIndex(),
					LastLogTerm:  rf.getLastLogTerm(),
				}
				reply := RequestVoteReply{}
				rf.mu.Unlock()

				ok := rf.sendRequestVote(i, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// if peer server is alive
				if ok {
					// if I am still candidate for this term
					if rf.state != Candidate || rf.currentTerm != term {
						return
					}

					switch {
					// If RPC request or response contains term T > currentTerm:
					// set currentTerm = T, convert to follower (§5.1)
					case (reply.Term > rf.currentTerm):
						// DPrintf("Term[%d], Candidate[%d]: transitions from Candidate to Follower, due to higher Receiver[%d], Term[%d].\n", rf.currentTerm, rf.me, i, reply.Term)
						rf.transitionFollower(reply.Term)
					case reply.VoteGranted:
						// DPrintf("Term[%d], Candidate[%d]: received vote from Receiver[%d], Term[%d].\n", rf.currentTerm, rf.me, i, reply.Term)
						voteCount++
						if rf.state != Leader && voteCount > majority {
							DPrintf("Term[%d], Candidate[%d]: transitions from Candidate to Leader, won election.\n", rf.currentTerm, rf.me)
							rf.state = Leader
							rf.wonElectionCh <- true
						}
					}
				} else {
					return
				}
			}(server)
		}
	}
}

type AppendEntriesArgs struct {
	//
	// example AppendEntries RPC arguments structure.
	// field names must start with capital letters!
	//
	// Your data here (2A).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	//
	// example AppendEntries RPC reply structure.
	// field names must start with capital letters!
	//
	// Your data here (2A).
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	lastLogIndex := rf.getLastLogIndex()
	// followerLogTerm := rf.getLastLogTerm()

	switch {
	// 1. Reply false if term < currentTerm (§5.1)
	case args.Term < rf.currentTerm:
		// DPrintf("Append -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: did not append, leader Term outdated.\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	case args.Term > rf.currentTerm:
		// DPrintf("Append -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: transitions to Follower, due to higher leader Term.\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.transitionFollower(args.Term)
		reply.Term = args.Term
	case args.Term == rf.currentTerm:
		// DPrintf("Append -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: same term.\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.state = Follower
		reply.Term = args.Term
	}

	// now args.Term >= rf.currentTerm, Leader is valid
	go func() { rf.receivedHeartbeatCh <- true }()

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// Follower rf.log is shorter than prevLogIndex from Leader
	// DPrintf("Append -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: lastLogIndex[%d]; args.PrevLogIndex[%d].\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, lastLogIndex, args.PrevLogIndex)
	if args.PrevLogIndex > lastLogIndex {
		DPrintf("Append -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: Receiver index[%d] SHORTER than prevLogIndex[%d].\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, lastLogIndex, args.PrevLogIndex)
		reply.Success = false

		// If a follower does not have prevLogIndex in its log, it should return with
		// conflictIndex = len(log) and conflictTerm = None.
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	firstLogIndex := rf.getFirstLogIndex()

	// Log inconsistency; different Term at prevLogIndex from Leader
	// DPrintf("Append -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: args.PrevLogIndex[%d]; firstLogIndex[%d]; log %+v.\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, firstLogIndex, rf.log)

	if args.PrevLogIndex > firstLogIndex && rf.log[args.PrevLogIndex-firstLogIndex].Term != args.PrevLogTerm {
		DPrintf("Append -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: args.PrevLogIndex[%d]; firstLogIndex[%d]; log %+v.\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, firstLogIndex, rf.log)
		reply.Term = rf.currentTerm
		reply.Success = false

		// If a follower does have prevLogIndex in its log, but the term does not match,
		// it should return conflictTerm = log[prevLogIndex].Term,
		// and then search its log for the first index whose entry has term equal to conflictTerm.
		reply.ConflictTerm = rf.log[args.PrevLogIndex-firstLogIndex].Term
		for j := 1; j < len(rf.log); j++ { // there shouldn't be conflict at rf.log[0] after snapshot
			if rf.log[j].Term == reply.ConflictTerm {
				reply.ConflictIndex = j + firstLogIndex
				break
			}
		}
		return

	} else if args.PrevLogIndex == firstLogIndex && rf.log[args.PrevLogIndex-firstLogIndex].Term != args.PrevLogTerm {
		log.Fatal("INVESTIGATE args.PrevLogIndex == firstLogIndex && rf.log[args.PrevLogIndex-firstLogIndex].Term != args.PrevLogTerm")

		// DPrintf("Append -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: args.PrevLogIndex[%d]; log.PrevTerm[%d] ! = args.PrevLogTerm[%d]; log %v.\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, rf.log)

	} else if args.PrevLogIndex >= firstLogIndex {
		// Follower log is no shorter than Leader, and same PrevLogTerm for both
		// This means the logs are identical in all preceding entries.

		// 3. If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it (§5.3)

		appendIndex := len(args.Entries)
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex+1+i > lastLogIndex {
				appendIndex = i
				break
			}
			if rf.log[args.PrevLogIndex+1+i-firstLogIndex].Term != args.Entries[i].Term {
				rf.log = rf.log[:args.PrevLogIndex+1+i-firstLogIndex]
				appendIndex = i
				break
			}
		}

		// 4. Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries[appendIndex:]...)

		DPrintf("Append -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: current log %+v.\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.log)

		reply.Term = rf.currentTerm
		reply.Success = true

		// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			go rf.msgApplyCh()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) doAppendEntries() {
	// Leader behavior sending heartbeat or snapshot

	rf.mu.Lock()
	state := rf.state
	term := rf.currentTerm
	rf.mu.Unlock()

	for server, _ := range rf.peers {
		if server != rf.me && state == Leader {

			go func(i int) {

				// run for loop until one of these happens: I'm no longer leader or term changed;
				// reply successful; RPC reply not ok
				for {
					rf.mu.Lock()
					firstLogIndex := rf.getFirstLogIndex()
					nextIndex := rf.nextIndex[i]

					if nextIndex > firstLogIndex { // do AppendEntries
						// if I am still leader for this term
						if rf.state != Leader || rf.currentTerm != term {
							rf.mu.Unlock()
							return
						}

						// define AppendEntriesArgs
						emptyLog := make([]Log, 0)
						appendIndex := rf.nextIndex[i] - firstLogIndex
						prevLogTermIndex := rf.nextIndex[i] - 1 - firstLogIndex
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[i] - 1,
							PrevLogTerm:  rf.log[prevLogTermIndex].Term,
							Entries:      append(emptyLog, rf.log[appendIndex:]...),
							LeaderCommit: rf.commitIndex,
						}
						reply := AppendEntriesReply{}
						DPrintf("Term[%d] Leader[%d]: send AppendEntries RPC to Receiver[%d] args %+v.\n", rf.currentTerm, rf.me, i, args)
						rf.mu.Unlock()

						ok := rf.sendAppendEntries(i, &args, &reply)

						if ok {
							rf.mu.Lock()
							DPrintf("Term[%d] Leader[%d]: received reply of AppendEntries RPC from Receiver[%d] reply %+v.\n", rf.currentTerm, rf.me, i, reply)

							// if I am still Leader for this term
							if rf.state != Leader || rf.currentTerm != term {
								rf.mu.Unlock()
								return
							}

							firstLogIndex = rf.getFirstLogIndex()
							switch {
							case reply.Term > rf.currentTerm:
								// DPrintf("Term[%d] Leader[%d]: transitions from Leader to Follower, due to higher Receiver[%d], Term[%d].\n", rf.currentTerm, rf.me, i, reply.Term)
								rf.transitionFollower(reply.Term)
								rf.mu.Unlock()
								return

							// • If successful: update nextIndex and matchIndex for follower (§5.3)
							case reply.Success:
								rf.matchIndex[rf.me] = rf.getLastLogIndex()
								rf.nextIndex[i] = nextIndex + len(args.Entries)
								rf.matchIndex[i] = rf.nextIndex[i] - 1
								DPrintf("Term[%d] Leader[%d]; Receiver[%d] Term[%d]: nextIndex[%d] matchIndex[%d]; args.Entries %v.\n", rf.currentTerm, rf.me, i, reply.Term, rf.nextIndex[i], rf.matchIndex[i], args.Entries)

								// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
								// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
								// This should be checked every time rf.matchIndex[i] is updated
								for N := rf.matchIndex[i]; N > rf.commitIndex && rf.log[N-firstLogIndex].Term == rf.currentTerm; N-- {
									matchCount := 0
									for j := range rf.matchIndex {
										if rf.matchIndex[j] >= N {
											matchCount++
										}
									}
									if matchCount > len(rf.peers)/2 {
										rf.commitIndex = N
										DPrintf("Term[%d], Leader[%d]: Leader's commitIndex updated to %d.\n", rf.currentTerm, rf.me, rf.commitIndex)
										go rf.msgApplyCh()
										break
									}
								}
								rf.mu.Unlock()
								return

							// • If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
							case !reply.Success:
								// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
								// If it finds an entry in its log with that term, it should set nextIndex to be
								// the one beyond the index of the last entry in that term in its log.
								// If it does not find an entry with that term, it should set nextIndex = conflictIndex.

								rf.nextIndex[i] = reply.ConflictIndex
								if reply.ConflictTerm != -1 { // if ConflictTerm == -1, then just use ConflictIndex
									for j := 0; j < len(rf.log); j++ {
										if rf.log[j].Term == reply.ConflictTerm {
											rf.nextIndex[i] = j + 1 + firstLogIndex
										}
									}
								}
								// rf.nextIndex[i] = nextIndex - 1
								nextIndex = rf.nextIndex[i]

								rf.mu.Unlock()
							}
						} else { // RPC lost
							return
						}

					} else { // nextIndex <= rf.getFirstLogIndex(); send snapshot to slower follower
						// if I am still leader for this term
						if rf.state != Leader || rf.currentTerm != term {
							rf.mu.Unlock()
							return
						}
						// DPrintf("Term[%d], Leader[%d]: sending snapshot to Receiver[%d].\n", rf.currentTerm, rf.me, i)

						snapshot := rf.persister.ReadSnapshot()
						// define InstallSnapshotArgs
						args := InstallSnapshotArgs{
							Term:              rf.currentTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.getFirstLogIndex(),
							LastIncludedTerm:  rf.getFirstLogTerm(),
							Data:              snapshot,
						}
						reply := InstallSnapshotReply{}
						DPrintf("Term[%d] Leader[%d]: send InstallSnapshot RPC to Receiver[%d] args %+v.\n", rf.currentTerm, rf.me, i, args)
						rf.mu.Unlock()

						ok := rf.sendInstallSnapshot(i, &args, &reply)

						if ok {
							rf.mu.Lock()
							DPrintf("Term[%d] Leader[%d]: received reply of InstallSnapshot RPC from Receiver[%d] reply %+v.\n", rf.currentTerm, rf.me, i, reply)

							// if I am still Leader for this term
							if rf.state != Leader || rf.currentTerm != term {
								rf.mu.Unlock()
								return
							}

							switch {
							case reply.Term > rf.currentTerm:
								// DPrintf("Term[%d] Leader[%d]: transitions from Leader to Follower, due to higher Receiver[%d], Term[%d].\n", rf.currentTerm, rf.me, i, reply.Term)
								rf.transitionFollower(reply.Term)
								rf.mu.Unlock()
								return

							default:
								rf.matchIndex[i] = args.LastIncludedIndex
								rf.nextIndex[i] = args.LastIncludedIndex + 1
								nextIndex = rf.nextIndex[i]
								rf.mu.Unlock()
							}
						}
					}
				}
			}(server)
		}
	}
}

//
// util fucntion for doAppendEntries & AppendEntries RPC handler
//
func (rf *Raft) msgApplyCh() {
	// • If commitIndex > lastApplied: increment lastApplied,
	// apply log[lastApplied] to state machine (§5.3)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	firstLogIndex := rf.getFirstLogIndex()

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		msgLogIndex := rf.lastApplied - firstLogIndex

		newApplyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[msgLogIndex].Command,
			CommandIndex: rf.log[msgLogIndex].Index,
			CommandTerm:  rf.log[msgLogIndex].Term,
		}
		// DPrintf("Term[%d], Follower[%d]: sending to applyCh.\n", rf.currentTerm, rf.me)
		rf.applyCh <- newApplyMsg
		DPrintf("Term[%d], Server[%d]: sent to applyCh %v.\n", rf.currentTerm, rf.me, newApplyMsg)
	}

	assert(rf.commitIndex == rf.lastApplied, "INVESTIGATE: Append -- rf.commitIndex != rf.lastApplied\n")
}

type InstallSnapshotArgs struct {
	//
	// example InstallSnapshot RPC args structure.
	// field names must start with capital letters!
	//
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	//
	// example InstallSnapshot RPC reply structure.
	// field names must start with capital letters!
	//
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//
	// example InstallSnapshot RPC handler.
	//

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	switch {
	// 1. Reply immediately if term < currentTerm
	case args.Term < rf.currentTerm:
		DPrintf("Snapshot -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: did not snapshot, leader Term outdated.\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		return
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	case args.Term > rf.currentTerm:
		// DPrintf("Snapshot -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: transitions to Follower, due to higher leader Term.\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.transitionFollower(args.Term)
		reply.Term = args.Term
	case args.Term == rf.currentTerm:
		// DPrintf("Snapshot -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: same term.\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.state = Follower
		reply.Term = args.Term
	}

	// args.Term >= rf.currentTerm, Leader is valid
	go func() { rf.receivedHeartbeatCh <- true }()

	if args.LastIncludedIndex <= rf.commitIndex {
		// If instead the follower receives a snapshot that describes a prefix of its log
		// (due to retransmission or by mistake), then log entries covered by the snapshot are deleted
		// but entries following the snapshot are still valid and must be retained.
		DPrintf("Snapshot -- Receiver[%d] Term[%d]; Leader[%d] Term[%d]: args.LastIncludedIndex[%d] <= rf.commitIndex[%d].\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.LastIncludedIndex, rf.commitIndex)

	} else {
		// Usually the snapshot will contain new information not already in the recipient’s log.
		// The follower discards its entire log; it is all superseded by the snapshot and
		// may possibly have uncommitted entries that conflict with the snapshot.
		assert(args.LastIncludedIndex > rf.commitIndex, "INVESTIGATE: InstallSnapshot\n")

		// update from snapshot
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.log = rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)
		// rf.applySnapshot(lastIncludedIndex, lastIncludedTerm)
		rf.persist()

		// tell kvServer
		newApplyMsg := ApplyMsg{
			CommandValid: false,
			Snapshot:     args.Data,
		}
		go func() { rf.applyCh <- newApplyMsg }()

		// save snapshot
		raftState := rf.getSaveRaftState()
		rf.persister.SaveStateAndSnapshot(raftState, args.Data)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) trimLog(lastIncludedIndex int, lastIncludedTerm int, oldLog []Log) []Log {
	//
	// util fucntion for InstallSnapshot RPC handler
	// delete logs up to lastIncludedIndex
	//
	newLog := make([]Log, 0)
	snapshotLog := Log{
		Index: lastIncludedIndex,
		Term:  lastIncludedTerm,
	}
	newLog = append(newLog, snapshotLog)

	trimIndex := len(oldLog)
	for i := len(oldLog) - 1; i >= 0; i-- {
		if oldLog[i].Index == lastIncludedIndex && oldLog[i].Term == lastIncludedTerm {
			trimIndex = i + 1
			break
		}
	}
	newLog = append(newLog, oldLog[trimIndex:]...)
	return newLog
}

func (rf *Raft) applySnapshot(lastIncludedIndex int, lastIncludedTerm int) {
	//
	// util fucntion for InstallSnapshot RPC handler to do Snapshot updates
	//
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.log = rf.trimLog(lastIncludedIndex, lastIncludedTerm, rf.log)
}

func (rf *Raft) startElection() {
	//
	// go routine to kick off election & appendEntries
	//

	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			// • Respond to RPCs from candidates and leaders
			// • If election timeout elapses without receiving AppendEntries RPC
			// from current leader or granting vote to candidate: convert to candidate
			select {
			case <-rf.receivedHeartbeatCh:
				state = Follower
			case <-rf.voteGrantedCh:
				state = Follower
			case <-time.After(rf.getElectionTimeout()):
				rf.mu.Lock()
				DPrintf("Term[%d], Server[%d]: transitions from Follower to Candidate, due to timeout.\n", rf.currentTerm, rf.me)
				state = Candidate
				rf.mu.Unlock()
			}
			rf.mu.Lock()
			rf.state = state
			rf.mu.Unlock()

		case Candidate:
			rf.mu.Lock()
			// • On conversion to candidate, start election:
			// 	• Increment currentTerm
			// 	• Vote for self
			// 	• Reset election timer
			// 	• Send RequestVote RPCs to all other servers
			rf.currentTerm++
			rf.votedFor = rf.me
			// DPrintf("Term[%d], Candidate[%d]: Request vote.\n", rf.currentTerm, rf.me)
			rf.persist()
			rf.mu.Unlock()

			go rf.doRequestVote()

			// • If votes received from majority of servers: become leader
			// • If AppendEntries RPC received from new leader: convert to follower
			// • If election timeout elapses: start new election
			select {
			case <-rf.wonElectionCh:
				rf.mu.Lock()
				rf.state = Leader

				// nextIndex[] & matchIndex[] reinitialized after each election
				nextIdx := rf.getLastLogIndex() + 1
				for i := 0; i < len(rf.peers); i++ {
					// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
					rf.nextIndex[i] = nextIdx
					// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
					rf.matchIndex[i] = 0
				}
				rf.persist()
				rf.mu.Unlock()
			case <-rf.receivedHeartbeatCh:
				rf.mu.Lock()
				// DPrintf("Term[%d], Server[%d]: transitions from Candidate to Follower, due to receive heartbeat.\n", rf.currentTerm, rf.me)
				rf.state = Follower
				rf.mu.Unlock()
			case <-time.After(rf.getElectionTimeout()):
				rf.mu.Lock()
				DPrintf("Term[%d], Server[%d]: Candidate restarts election, due to timeout.\n", rf.currentTerm, rf.me)
				rf.state = Candidate
				rf.mu.Unlock()
			}

		case Leader:
			// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
			// repeat during idle periods to prevent election timeouts
			go rf.doAppendEntries()
			time.Sleep(time.Duration(LeaderSleepTimeout) * time.Millisecond)
		}
	}
}

func (rf *Raft) transitionFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) getElectionTimeout() time.Duration {
	random := rand.Intn(MaxElectionTimeout-MinElectionTimeout) + MinElectionTimeout
	timeout := time.Duration(random) * time.Millisecond
	return timeout
}

func (rf *Raft) getLastLogIndex() int {
	lastLogIndex := rf.log[len(rf.log)-1].Index
	return lastLogIndex
}
func (rf *Raft) getLastLogTerm() int {
	lastLogTerm := rf.log[len(rf.log)-1].Term
	return lastLogTerm
}
func (rf *Raft) getFirstLogIndex() int {
	firstLogIndex := rf.log[0].Index
	return firstLogIndex
}
func (rf *Raft) getFirstLogTerm() int {
	firstLogTerm := rf.log[0].Term
	return firstLogTerm
}

// helper function
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// helper function
func assert(pred bool, msgs ...interface{}) {
	if !pred {
		log.Fatal(msgs...)
	}
}
