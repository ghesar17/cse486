package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// commented out fields are not requried for implementing leader election

	// persistent 
	CurrentTerm int
	VotedFor    any
	// log         []LogEntry

	// volatile 
	// commitIndex int
	// lastAppliedIndex int

	// for leaders
	// nextIndex  []int
	// matchIndex []int

	// follower is 0, candidate is 1, leader is 2
	CurrentState int

	// timer for election
	// should be randomized each term
	ElectionTimeout *time.Timer

	// channel for applying log entries
	// applyCh chan ApplyMsg

	// channel to alert heartbeat was received
	// the leader's term will be passed through the channel
	heartbeatCh chan int

	// channel to alert vote request was received
	voterequestCh chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isleader bool
	term := rf.CurrentTerm
	if rf.CurrentState == 2 {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term int
	CandidateID int
	// lastLogIndex int
	// lastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

//
// AppendEntriesArgs structure for AppendEntries RPC arguments
//
type AppendEntriesArgs struct {
	Term int
	LeaderID int
	// prevLogIndex int
	// prevLogTerm int
	// entries []interface{}
}

//
// AppendEntriesReply structure for AppendEntries RPC reply
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// if i am out of date, set state to follower
	if args.Term > rf.CurrentTerm {
		fmt.Println("I'm ", rf.me, " and I'm out of date, now I'm a follower!!!")
		rf.CurrentState = 0
		rf.CurrentTerm = args.Term
		// since its a new term, set votedFor to nil
		rf.VotedFor = nil
		rf.heartbeatCh <- args.Term
		// rf.startElectionTimer()
	}

	 if args.Term == rf.CurrentTerm {
		fmt.Println("I'm ", rf.me, " and I received a heartbeat from ", args.LeaderID, "!!!")
		rf.heartbeatCh <- args.Term
		// rf.startElectionTimer()
	 }

	// check if the heartbeat is outdated, like from an old leader
	if args.Term < rf.CurrentTerm {
		fmt.Println("you're outdated buddy, get rejected old leader ", (args.LeaderID))	
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

	// if i am out of date, revert to follower
	if args.Term > rf.CurrentTerm {
		fmt.Println("I'm ", rf.me, " and I'm out of date, now I'm a follower!!!")
		rf.CurrentTerm = args.Term
		// since its a new term, set votedFor to nil
		rf.VotedFor = nil
		rf.CurrentState = 0
	}
	// if the candidate is out of date, reject
	if args.Term < rf.CurrentTerm {
		fmt.Println("you're outdated buddy, get rejected peer #", (args.CandidateID))
		reply.VoteGranted = false
		return	
	}
	// if havent voted yet in this term then vote for node
	if rf.VotedFor == nil || rf.VotedFor == args.CandidateID {
		fmt.Println("for sure I'll vote for ", (args.CandidateID))
		rf.VotedFor = args.CandidateID
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		// alert that I got a requestVote
		rf.voterequestCh <- 1
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Println("I'm ", args.CandidateID, "and I want your vote Mr.", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Println("I'm ", args.LeaderID, "and I'm the leader sending a heartbeat to ", server, "!")
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// returns a channel to receive vote responses
func (rf *Raft) askForVotes() <-chan bool {
	voteReplies := make(chan bool, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		// dont request a vote from yourself
		if i == rf.me {
			continue
		}
		voteRequest := RequestVoteArgs{
			Term: rf.CurrentTerm,
			CandidateID: rf.me,
		}
		voteReply := RequestVoteReply{}
		go func() {
			rf.sendRequestVote(i, voteRequest, &voteReply)
			voteReplies <- voteReply.VoteGranted
		}()
	}
	return voteReplies
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	return index, term, isLeader
}

func (rf *Raft) follower() {	
	for rf.CurrentState == 0 {
		select {
			// no heartbeats, start election
			case <-rf.ElectionTimeout.C:
				fmt.Println("I'm ", rf.me, " and I'm running for president. My term is ", rf.CurrentTerm + 1)
				rf.CurrentState = 1
			// received heartbeat, remain follower
			case <-rf.heartbeatCh:
				rf.startElectionTimer()
			// received vote request, remain follower
			case <-rf.voterequestCh:
				rf.startElectionTimer()
		}
	}
}

// use this method to start an election, handles all logic for candidates
func (rf *Raft) candidate() {

    rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
    rf.startElectionTimer()

	voteRepliesCh := rf.askForVotes()
	// votes for itself
	votes := 1

	for rf.CurrentState == 1 {
		select {
			case leaderTerm := <-rf.heartbeatCh:
				// check if leader’s term is at least as large as the candidate’s current term
				// if so then there is already a leader so revert to follower
				if leaderTerm >= rf.CurrentTerm {
					rf.CurrentState = 0
				}
			case isGranted := <-voteRepliesCh:
				if isGranted {
					votes++
				}
				// check if we got majority, if we did then become leader
				if votes >= (len(rf.peers)/2) + 1 {	
					fmt.Println("IM LEADERRRR")	
					rf.CurrentState = 2
				}
			// no heartbeats, reset election
			case <-rf.ElectionTimeout.C:
				rf.candidate()
		}
	}
}

func (rf *Raft) leader() {
    // keep sending heartbeats to maintain leader status, lets say every 50ms
    ticker := time.NewTicker(50 * time.Millisecond)

    for rf.CurrentState == 2 {
        select {
        case <-ticker.C:
            // send heartbeats to all followers
            for i := 0; i < len(rf.peers); i++ {
				// don't send a heartbeat to yourself
				if i == rf.me {
					continue
				}
                heartbeat := AppendEntriesArgs{
                    Term:         rf.CurrentTerm,
                    LeaderID:     rf.me,
                }
                heartbeatReply := AppendEntriesReply{}
                go func() {
                    rf.sendAppendEntries(i, heartbeat, &heartbeatReply)
                }()
            }
        }
    }
}


// start timer for randomized duration from 150-300 ms 
func (rf *Raft) startElectionTimer() {
	minDuration := 150 * time.Millisecond
    maxDuration := 300 * time.Millisecond

    duration := time.Duration(rand.Int63n(int64(maxDuration-minDuration)) + int64(minDuration))

    rf.ElectionTimeout = time.NewTimer(duration)
}

func (rf *Raft) Kill() {}

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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.heartbeatCh = make(chan int)
	rf.voterequestCh = make(chan int)

	// commented out fields are not required for leader election
	rf.CurrentTerm = 0
	rf.VotedFor = nil
	// rf.log =

	// rf.commitIndex = 0
	// rf.lastAppliedIndex = 0

	// rf.nextIndex = make([]int, len(rf.peers))
	// rf.matchIndex = make([]int, len(rf.peers))

	// initialize server as a follower
	rf.CurrentState = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		// begin timer
		rf.startElectionTimer()

		for {
			switch rf.CurrentState {
				case 0:
					rf.follower()
				case 1:
					rf.candidate()
				case 2:
					rf.leader()
			}
		}
  	  }()
	return rf
}