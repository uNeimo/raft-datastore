package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

// State describes the current role of a node in the Raft cluster.
type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

// RequestVoteRequest contains the information a candidate sends during an election.
type RequestVoteRequest struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteResponse reports whether a node granted its vote.
type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

// LogEntry is a command recorded by the leader in a specific term.
type LogEntry struct {
	Term    int
	Command Command
}

// Command describes a change to the key-value state machine.
type Command struct {
	Operation string
	Key       string
	Value     string
}

const (
	SetOperation    = "set"
	DeleteOperation = "delete"
)

// AppendEntriesRequest carries log entries and commit progress from the leader.
type AppendEntriesRequest struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesResponse reports whether a follower accepted the entries.
type AppendEntriesResponse struct {
	Term    int
	Success bool
}

// Node stores the Raft state needed for leader election.
type Node struct {
	mu          sync.Mutex
	id          string
	state       State
	term        int
	votedFor    string
	log         []LogEntry
	commitIndex int
	lastApplied int
	store       map[string]string
}

// NewNode creates a follower with no vote recorded for the initial term.
func NewNode(id string) *Node {
	return &Node{
		id: id, state: Follower, commitIndex: -1, lastApplied: -1,
		store: make(map[string]string),
	}
}

// Get returns a value from the leader's committed state.
func (n *Node) Get(key string) (string, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return "", false, errors.New("reads must be sent to the leader")
	}
	value, exists := n.store[key]
	return value, exists, nil
}

// GetLinearizable confirms leadership with a quorum before reading committed state.
func (n *Node) GetLinearizable(key string, peerIDs []string, transport AppendTransport) (string, bool, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return "", false, errors.New("reads must be sent to the leader")
	}
	term := n.term
	commitIndex := n.commitIndex
	commitTerm := 0
	if commitIndex >= 0 {
		commitTerm = n.log[commitIndex].Term
	}
	n.mu.Unlock()

	request := AppendEntriesRequest{
		Term: term, LeaderID: n.id, PrevLogIndex: commitIndex,
		PrevLogTerm: commitTerm, LeaderCommit: commitIndex,
	}
	confirmed := 1
	majority := (len(peerIDs)+1)/2 + 1
	for _, peerID := range peerIDs {
		response, err := transport.AppendEntries(peerID, request)
		if err != nil {
			continue
		}
		if response.Term > term {
			n.mu.Lock()
			n.becomeFollower(response.Term)
			n.mu.Unlock()
			return "", false, errors.New("leader discovered a newer term")
		}
		if response.Success {
			confirmed++
		}
	}
	if confirmed < majority {
		return "", false, errors.New("could not confirm leadership with a majority")
	}
	return n.Get(key)
}

// Set replicates a key and value through the Raft log.
func (n *Node) Set(key, value string, peerIDs []string, transport AppendTransport) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}
	return n.Replicate(Command{Operation: SetOperation, Key: key, Value: value}, peerIDs, transport)
}

// Delete removes a key after the command reaches a majority.
func (n *Node) Delete(key string, peerIDs []string, transport AppendTransport) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}
	return n.Replicate(Command{Operation: DeleteOperation, Key: key}, peerIDs, transport)
}

// LogStatus returns copies of the log and current commit index.
func (n *Node) LogStatus() ([]LogEntry, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	entries := append([]LogEntry(nil), n.log...)
	return entries, n.commitIndex
}

func (n *Node) ID() string {
	return n.id
}

// Status returns a consistent snapshot of the node's election state.
func (n *Node) Status() (State, int, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.term, n.votedFor
}

// ElectionTimeout returns a timeout in [minimum, maximum).
func ElectionTimeout(random *rand.Rand, minimum, maximum time.Duration) time.Duration {
	if maximum <= minimum {
		return minimum
	}
	return minimum + time.Duration(random.Int63n(int64(maximum-minimum)))
}

// HandleRequestVote applies Raft's term and one-vote-per-term rules.
func (n *Node) HandleRequestVote(request RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if request.Term < n.term {
		return RequestVoteResponse{Term: n.term}
	}

	if request.Term > n.term {
		n.becomeFollower(request.Term)
	}

	canVote := n.votedFor == "" || n.votedFor == request.CandidateID
	canVote = canVote && n.candidateLogIsCurrent(request.LastLogIndex, request.LastLogTerm)
	if canVote {
		n.votedFor = request.CandidateID
	}

	return RequestVoteResponse{Term: n.term, VoteGranted: canVote}
}

// HandleAppendEntries verifies the preceding entry before accepting leader data.
func (n *Node) HandleAppendEntries(request AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if request.Term < n.term {
		return AppendEntriesResponse{Term: n.term}
	}
	if request.Term > n.term {
		n.becomeFollower(request.Term)
	} else if n.state != Follower {
		n.state = Follower
	}

	if request.PrevLogIndex >= len(n.log) {
		return AppendEntriesResponse{Term: n.term}
	}
	if request.PrevLogIndex >= 0 && n.log[request.PrevLogIndex].Term != request.PrevLogTerm {
		return AppendEntriesResponse{Term: n.term}
	}

	insertAt := request.PrevLogIndex + 1
	entryOffset := 0
	for insertAt < len(n.log) && entryOffset < len(request.Entries) {
		if n.log[insertAt].Term != request.Entries[entryOffset].Term {
			n.log = n.log[:insertAt]
			break
		}
		insertAt++
		entryOffset++
	}
	if entryOffset < len(request.Entries) {
		n.log = append(n.log, request.Entries[entryOffset:]...)
	}

	if request.LeaderCommit > n.commitIndex {
		n.commitIndex = min(request.LeaderCommit, len(n.log)-1)
		n.applyCommittedEntries()
	}
	return AppendEntriesResponse{Term: n.term, Success: true}
}

// Campaign starts a new election and returns true when the node wins a majority.
func (n *Node) Campaign(peerIDs []string, transport VoteTransport) bool {
	n.mu.Lock()
	n.state = Candidate
	n.term++
	n.votedFor = n.id
	term := n.term
	n.mu.Unlock()

	votes := 1
	majority := (len(peerIDs)+1)/2 + 1
	n.mu.Lock()
	lastLogIndex, lastLogTerm := n.lastLogDetails()
	n.mu.Unlock()
	request := RequestVoteRequest{
		Term: term, CandidateID: n.id, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm,
	}

	for _, peerID := range peerIDs {
		response, err := transport.RequestVote(peerID, request)
		if err != nil {
			continue
		}

		n.mu.Lock()
		if response.Term > n.term {
			n.becomeFollower(response.Term)
			n.mu.Unlock()
			return false
		}
		n.mu.Unlock()

		if response.VoteGranted {
			votes++
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == Candidate && n.term == term && votes >= majority {
		n.state = Leader
		return true
	}
	return false
}

// Replicate appends a command and commits it after a majority accepts the entry.
func (n *Node) Replicate(command Command, peerIDs []string, transport AppendTransport) error {
	if command.Operation != SetOperation && command.Operation != DeleteOperation {
		return errors.New("unsupported state machine operation")
	}
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return errors.New("only the leader can replicate commands")
	}
	term := n.term
	previousIndex, previousTerm := n.lastLogDetails()
	n.log = append(n.log, LogEntry{Term: term, Command: command})
	entryIndex := len(n.log) - 1
	logSnapshot := append([]LogEntry(nil), n.log...)
	n.mu.Unlock()

	accepted := 1
	majority := (len(peerIDs)+1)/2 + 1
	for _, peerID := range peerIDs {
		for nextIndex := entryIndex; nextIndex >= 0; nextIndex-- {
			previousIndex = nextIndex - 1
			previousTerm = 0
			if previousIndex >= 0 {
				previousTerm = logSnapshot[previousIndex].Term
			}
			request := AppendEntriesRequest{
				Term: term, LeaderID: n.id, PrevLogIndex: previousIndex,
				PrevLogTerm: previousTerm, Entries: logSnapshot[nextIndex:],
				LeaderCommit: entryIndex - 1,
			}
			response, err := transport.AppendEntries(peerID, request)
			if err != nil {
				break
			}
			if response.Term > term {
				n.mu.Lock()
				n.becomeFollower(response.Term)
				n.mu.Unlock()
				return errors.New("leader discovered a newer term")
			}
			if response.Success {
				accepted++
				break
			}
		}
	}
	if accepted < majority {
		return errors.New("command was not accepted by a majority")
	}

	n.mu.Lock()
	n.commitIndex = entryIndex
	n.applyCommittedEntries()
	n.mu.Unlock()

	request := AppendEntriesRequest{
		Term: term, LeaderID: n.id, PrevLogIndex: entryIndex,
		PrevLogTerm: term, LeaderCommit: entryIndex,
	}
	for _, peerID := range peerIDs {
		_, _ = transport.AppendEntries(peerID, request)
	}
	return nil
}

func (n *Node) applyCommittedEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		command := n.log[n.lastApplied].Command
		switch command.Operation {
		case SetOperation:
			n.store[command.Key] = command.Value
		case DeleteOperation:
			delete(n.store, command.Key)
		}
	}
}

func (n *Node) candidateLogIsCurrent(candidateIndex, candidateTerm int) bool {
	localIndex, localTerm := n.lastLogDetails()
	if candidateTerm != localTerm {
		return candidateTerm > localTerm
	}
	return candidateIndex >= localIndex
}

func (n *Node) lastLogDetails() (int, int) {
	if len(n.log) == 0 {
		return -1, 0
	}
	lastIndex := len(n.log) - 1
	return lastIndex, n.log[lastIndex].Term
}

func (n *Node) becomeFollower(term int) {
	n.state = Follower
	n.term = term
	n.votedFor = ""
}
