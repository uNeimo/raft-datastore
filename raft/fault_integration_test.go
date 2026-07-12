package raft

import (
	"testing"
	"time"
)

func TestFaultTransportSimulatesNodeCrash(t *testing.T) {
	node := NewNode("node-2")
	transport := NewFaultTransport(node)
	transport.Crash("node-2")

	_, err := transport.RequestVote("node-2", RequestVoteRequest{Term: 1, CandidateID: "node-1"})
	if err == nil {
		t.Fatal("expected crashed node to be unavailable")
	}

	transport.Recover("node-2")
	response, err := transport.RequestVote("node-2", RequestVoteRequest{Term: 1, CandidateID: "node-1", LastLogIndex: -1})
	if err != nil || !response.VoteGranted {
		t.Fatalf("expected recovered node to vote, got response=%+v err=%v", response, err)
	}
}

func TestFaultTransportDropsRequest(t *testing.T) {
	node := NewNode("node-2")
	transport := NewFaultTransport(node)
	transport.DropNextRequest(VoteRPC)

	_, err := transport.RequestVote("node-2", RequestVoteRequest{Term: 1, CandidateID: "node-1"})
	_, term, votedFor := node.Status()
	if err == nil || term != 0 || votedFor != "" {
		t.Fatalf("expected request loss before delivery, got term=%d vote=%q err=%v", term, votedFor, err)
	}
}

func TestFaultTransportDropsResponseAfterDelivery(t *testing.T) {
	node := NewNode("node-2")
	transport := NewFaultTransport(node)
	transport.DropNextResponse(VoteRPC)

	_, err := transport.RequestVote("node-2", RequestVoteRequest{Term: 1, CandidateID: "node-1", LastLogIndex: -1})
	_, term, votedFor := node.Status()
	if err == nil || term != 1 || votedFor != "node-1" {
		t.Fatalf("expected response loss after delivery, got term=%d vote=%q err=%v", term, votedFor, err)
	}
}

func TestFaultTransportPartitionsDirectedLink(t *testing.T) {
	node := NewNode("node-2")
	transport := NewFaultTransport(node)
	transport.Partition("node-1", "node-2")

	_, err := transport.RequestVote("node-2", RequestVoteRequest{Term: 1, CandidateID: "node-1"})
	if err == nil {
		t.Fatal("expected partitioned link to block request")
	}

	transport.Heal("node-1", "node-2")
	_, err = transport.RequestVote("node-2", RequestVoteRequest{Term: 1, CandidateID: "node-1", LastLogIndex: -1})
	if err != nil {
		t.Fatalf("expected healed link to deliver request: %v", err)
	}
}

func TestFaultTransportDelaysRPC(t *testing.T) {
	node := NewNode("node-2")
	transport := NewFaultTransport(node)
	transport.SetDelay(VoteRPC, 20*time.Millisecond)
	started := time.Now()

	_, err := transport.RequestVote("node-2", RequestVoteRequest{Term: 1, CandidateID: "node-1", LastLogIndex: -1})

	if err != nil {
		t.Fatalf("expected delayed request to succeed: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond {
		t.Fatalf("expected at least 20ms delay, got %s", elapsed)
	}
}

func TestIsolatedLeaderCannotCommitOrServeLinearizableRead(t *testing.T) {
	leader := NewNode("node-1")
	nodeTwo := NewNode("node-2")
	nodeThree := NewNode("node-3")
	transport := NewFaultTransport(nodeTwo, nodeThree)
	peerIDs := []string{"node-2", "node-3"}
	if !leader.Campaign(peerIDs, transport) {
		t.Fatal("expected node-1 to win the election")
	}
	if err := leader.Set("status", "healthy", peerIDs, transport); err != nil {
		t.Fatalf("expected initial write to commit: %v", err)
	}

	transport.Partition("node-1", "node-2")
	transport.Partition("node-1", "node-3")
	if err := leader.Set("status", "isolated", peerIDs, transport); err == nil {
		t.Fatal("expected isolated leader write to fail")
	}
	if _, _, err := leader.GetLinearizable("status", peerIDs, transport); err == nil {
		t.Fatal("expected isolated leader read to fail quorum confirmation")
	}

	transport.Heal("node-1", "node-2")
	transport.Heal("node-1", "node-3")
	value, exists, err := leader.GetLinearizable("status", peerIDs, transport)
	if err != nil || !exists || value != "healthy" {
		t.Fatalf("expected healed quorum to read committed value, got value=%q exists=%v err=%v", value, exists, err)
	}
}
