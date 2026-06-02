package raft

import (
	"testing"
	"time"
)

func TestNetworkTransportRunsElectionAndReplication(t *testing.T) {
	leader := NewNode("node-1")
	nodeTwo := NewNode("node-2")
	nodeThree := NewNode("node-3")
	serverTwo := startTestRPCServer(t, nodeTwo)
	serverThree := startTestRPCServer(t, nodeThree)
	transport := NewNetworkTransport(map[string]string{
		"node-2": serverTwo.Address(),
		"node-3": serverThree.Address(),
	}, time.Second)
	peerIDs := []string{"node-2", "node-3"}

	if !leader.Campaign(peerIDs, transport) {
		t.Fatal("expected node-1 to win election over TCP")
	}
	if err := leader.Set("transport", "rpc", peerIDs, transport); err != nil {
		t.Fatalf("expected write to replicate over TCP: %v", err)
	}

	value, exists, err := leader.GetLinearizable("transport", peerIDs, transport)
	if err != nil || !exists || value != "rpc" {
		t.Fatalf("expected quorum read over TCP, got value=%q exists=%v err=%v", value, exists, err)
	}
}

func TestNetworkTransportRejectsUnknownNode(t *testing.T) {
	transport := NewNetworkTransport(nil, time.Second)

	_, err := transport.RequestVote("missing", RequestVoteRequest{})

	if err == nil {
		t.Fatal("expected missing node address to return an error")
	}
}

func startTestRPCServer(t *testing.T, node *Node) *RPCServer {
	t.Helper()
	server, err := StartRPCServer(node, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("start RPC server: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close RPC server: %v", err)
		}
	})
	return server
}
