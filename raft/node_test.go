package raft

import (
	"math/rand"
	"testing"
	"time"
)

func TestNodeGrantsOneVotePerTerm(t *testing.T) {
	node := NewNode("node-1")

	first := node.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: "node-2"})
	second := node.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: "node-3"})

	if !first.VoteGranted {
		t.Fatal("expected the first vote request to be granted")
	}
	if second.VoteGranted {
		t.Fatal("expected a second candidate in the same term to be rejected")
	}
}

func TestNodeRejectsStaleTerm(t *testing.T) {
	node := NewNode("node-1")
	node.HandleRequestVote(RequestVoteRequest{Term: 3, CandidateID: "node-2"})

	response := node.HandleRequestVote(RequestVoteRequest{Term: 2, CandidateID: "node-3"})

	if response.VoteGranted || response.Term != 3 {
		t.Fatalf("expected stale request to be rejected at term 3, got %+v", response)
	}
}

func TestCandidateBecomesLeaderWithMajority(t *testing.T) {
	candidate := NewNode("node-1")
	nodeTwo := NewNode("node-2")
	nodeThree := NewNode("node-3")
	transport := NewInMemoryTransport(nodeTwo, nodeThree)

	won := candidate.Campaign([]string{"node-2", "node-3"}, transport)
	state, term, _ := candidate.Status()

	if !won || state != Leader || term != 1 {
		t.Fatalf("expected node to lead term 1, got state=%s term=%d won=%v", state, term, won)
	}
}

func TestCandidateRemainsCandidateWithoutMajority(t *testing.T) {
	candidate := NewNode("node-1")
	transport := NewInMemoryTransport()

	won := candidate.Campaign([]string{"node-2", "node-3"}, transport)
	state, _, _ := candidate.Status()

	if won || state != Candidate {
		t.Fatalf("expected an unsuccessful election, got state=%s won=%v", state, won)
	}
}

func TestElectionTimeoutStaysWithinRange(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	minimum := 150 * time.Millisecond
	maximum := 300 * time.Millisecond

	for range 100 {
		timeout := ElectionTimeout(random, minimum, maximum)
		if timeout < minimum || timeout >= maximum {
			t.Fatalf("timeout %s is outside [%s, %s)", timeout, minimum, maximum)
		}
	}
}

func TestFollowerRejectsAppendWithMissingPreviousEntry(t *testing.T) {
	node := NewNode("node-2")

	response := node.HandleAppendEntries(AppendEntriesRequest{
		Term: 1, LeaderID: "node-1", PrevLogIndex: 0, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 1, Command: Command{Operation: SetOperation, Key: "color", Value: "blue"}}},
	})

	if response.Success {
		t.Fatal("expected follower to reject an entry with a missing predecessor")
	}
}

func TestLeaderReplicatesAndCommitsWithMajority(t *testing.T) {
	leader := NewNode("node-1")
	nodeTwo := NewNode("node-2")
	nodeThree := NewNode("node-3")
	transport := NewInMemoryTransport(nodeTwo, nodeThree)
	if !leader.Campaign([]string{"node-2", "node-3"}, transport) {
		t.Fatal("expected node-1 to win the election")
	}

	err := leader.Set("color", "blue", []string{"node-2", "node-3"}, transport)
	if err != nil {
		t.Fatalf("expected replication to succeed: %v", err)
	}

	for _, node := range []*Node{leader, nodeTwo, nodeThree} {
		entries, commitIndex := node.LogStatus()
		if len(entries) != 1 || entries[0].Command.Value != "blue" {
			t.Fatalf("node %s has unexpected log: %+v", node.ID(), entries)
		}
		if commitIndex != 0 {
			t.Fatalf("node %s has commit index %d, expected 0", node.ID(), commitIndex)
		}
	}
}

func TestLeaderDoesNotCommitWithoutMajority(t *testing.T) {
	leader := NewNode("node-1")
	nodeTwo := NewNode("node-2")
	nodeThree := NewNode("node-3")
	electionTransport := NewInMemoryTransport(nodeTwo, nodeThree)
	if !leader.Campaign([]string{"node-2", "node-3"}, electionTransport) {
		t.Fatal("expected node-1 to win the election")
	}

	unavailableTransport := NewInMemoryTransport()
	err := leader.Set("color", "blue", []string{"node-2", "node-3"}, unavailableTransport)
	_, commitIndex := leader.LogStatus()

	if err == nil {
		t.Fatal("expected replication without a majority to fail")
	}
	if commitIndex != -1 {
		t.Fatalf("expected entry to remain uncommitted, got commit index %d", commitIndex)
	}
}

func TestVoterRejectsCandidateWithOlderLog(t *testing.T) {
	node := NewNode("node-2")
	node.HandleAppendEntries(AppendEntriesRequest{
		Term: 2, LeaderID: "node-1", PrevLogIndex: -1,
		Entries: []LogEntry{{Term: 2, Command: Command{Operation: SetOperation, Key: "color", Value: "blue"}}}, LeaderCommit: -1,
	})

	response := node.HandleRequestVote(RequestVoteRequest{
		Term: 3, CandidateID: "node-3", LastLogIndex: -1, LastLogTerm: 0,
	})

	if response.VoteGranted {
		t.Fatal("expected candidate with an older log to be rejected")
	}
}

func TestLeaderCatchesUpFollowerThatMissedEntry(t *testing.T) {
	leader := NewNode("node-1")
	nodeTwo := NewNode("node-2")
	nodeThree := NewNode("node-3")
	electionTransport := NewInMemoryTransport(nodeTwo, nodeThree)
	if !leader.Campaign([]string{"node-2", "node-3"}, electionTransport) {
		t.Fatal("expected node-1 to win the election")
	}

	firstTransport := NewInMemoryTransport(nodeTwo)
	if err := leader.Set("color", "blue", []string{"node-2", "node-3"}, firstTransport); err != nil {
		t.Fatalf("expected first command to commit with node-2: %v", err)
	}

	secondTransport := NewInMemoryTransport(nodeTwo, nodeThree)
	if err := leader.Set("size", "large", []string{"node-2", "node-3"}, secondTransport); err != nil {
		t.Fatalf("expected second command to commit: %v", err)
	}

	entries, commitIndex := nodeThree.LogStatus()
	if len(entries) != 2 || entries[0].Command.Value != "blue" || entries[1].Command.Value != "large" {
		t.Fatalf("expected node-3 to catch up both entries, got %+v", entries)
	}
	if commitIndex != 1 {
		t.Fatalf("expected node-3 commit index 1, got %d", commitIndex)
	}
}

func TestCommittedSetIsAppliedToEveryNode(t *testing.T) {
	leader, followers, transport := electedThreeNodeCluster(t)

	if err := leader.Set("language", "go", []string{"node-2", "node-3"}, transport); err != nil {
		t.Fatalf("expected set command to commit: %v", err)
	}

	value, exists, err := leader.Get("language")
	if err != nil || !exists || value != "go" {
		t.Fatalf("expected leader to return go, got value=%q exists=%v err=%v", value, exists, err)
	}
	for _, follower := range followers {
		follower.mu.Lock()
		value := follower.store["language"]
		follower.mu.Unlock()
		if value != "go" {
			t.Fatalf("expected follower %s to apply committed value, got %q", follower.ID(), value)
		}
	}
}

func TestDeleteRemovesCommittedValue(t *testing.T) {
	leader, _, transport := electedThreeNodeCluster(t)
	peerIDs := []string{"node-2", "node-3"}
	if err := leader.Set("language", "go", peerIDs, transport); err != nil {
		t.Fatalf("expected set command to commit: %v", err)
	}
	if err := leader.Delete("language", peerIDs, transport); err != nil {
		t.Fatalf("expected delete command to commit: %v", err)
	}

	_, exists, err := leader.Get("language")
	if err != nil || exists {
		t.Fatalf("expected key to be deleted, got exists=%v err=%v", exists, err)
	}
}

func TestFollowerRejectsClientRead(t *testing.T) {
	follower := NewNode("node-1")

	_, _, err := follower.Get("language")

	if err == nil {
		t.Fatal("expected follower read to be rejected")
	}
}

func TestUncommittedSetDoesNotChangeStore(t *testing.T) {
	leader, _, _ := electedThreeNodeCluster(t)
	err := leader.Set("language", "go", []string{"node-2", "node-3"}, NewInMemoryTransport())
	if err == nil {
		t.Fatal("expected set without a majority to fail")
	}

	_, exists, getErr := leader.Get("language")
	if getErr != nil || exists {
		t.Fatalf("expected uncommitted value to remain invisible, got exists=%v err=%v", exists, getErr)
	}
}

func electedThreeNodeCluster(t *testing.T) (*Node, []*Node, *InMemoryTransport) {
	t.Helper()
	leader := NewNode("node-1")
	nodeTwo := NewNode("node-2")
	nodeThree := NewNode("node-3")
	transport := NewInMemoryTransport(nodeTwo, nodeThree)
	if !leader.Campaign([]string{"node-2", "node-3"}, transport) {
		t.Fatal("expected node-1 to win the election")
	}
	return leader, []*Node{nodeTwo, nodeThree}, transport
}
