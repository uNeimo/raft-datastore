package raft

import "fmt"

// VoteTransport delivers vote requests without tying the Raft logic to a network library.
type VoteTransport interface {
	RequestVote(nodeID string, request RequestVoteRequest) (RequestVoteResponse, error)
}

// AppendTransport delivers log replication requests.
type AppendTransport interface {
	AppendEntries(nodeID string, request AppendEntriesRequest) (AppendEntriesResponse, error)
}

// InMemoryTransport connects nodes directly for deterministic tests and local simulations.
type InMemoryTransport struct {
	nodes map[string]*Node
}

func NewInMemoryTransport(nodes ...*Node) *InMemoryTransport {
	transport := &InMemoryTransport{nodes: make(map[string]*Node)}
	for _, node := range nodes {
		transport.nodes[node.ID()] = node
	}
	return transport
}

func (t *InMemoryTransport) RequestVote(nodeID string, request RequestVoteRequest) (RequestVoteResponse, error) {
	node, exists := t.nodes[nodeID]
	if !exists {
		return RequestVoteResponse{}, fmt.Errorf("node %q is unavailable", nodeID)
	}
	return node.HandleRequestVote(request), nil
}

func (t *InMemoryTransport) AppendEntries(nodeID string, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	node, exists := t.nodes[nodeID]
	if !exists {
		return AppendEntriesResponse{}, fmt.Errorf("node %q is unavailable", nodeID)
	}
	return node.HandleAppendEntries(request), nil
}
