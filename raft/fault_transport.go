package raft

import (
	"errors"
	"sync"
	"time"
)

// RPCType identifies the kind of message affected by a fault rule.
type RPCType string

const (
	VoteRPC   RPCType = "request_vote"
	AppendRPC RPCType = "append_entries"
)

type networkLink struct {
	from string
	to   string
}

// FaultTransport adds deterministic failures around an in-memory transport.
type FaultTransport struct {
	base *InMemoryTransport

	mu            sync.Mutex
	crashed       map[string]bool
	blockedLinks  map[networkLink]bool
	requestDrops  map[RPCType]int
	responseDrops map[RPCType]int
	delays        map[RPCType]time.Duration
}

func NewFaultTransport(nodes ...*Node) *FaultTransport {
	return &FaultTransport{
		base:          NewInMemoryTransport(nodes...),
		crashed:       make(map[string]bool),
		blockedLinks:  make(map[networkLink]bool),
		requestDrops:  make(map[RPCType]int),
		responseDrops: make(map[RPCType]int),
		delays:        make(map[RPCType]time.Duration),
	}
}

func (t *FaultTransport) Crash(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.crashed[nodeID] = true
}

func (t *FaultTransport) Recover(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.crashed, nodeID)
}

func (t *FaultTransport) Partition(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blockedLinks[networkLink{from: from, to: to}] = true
}

func (t *FaultTransport) Heal(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.blockedLinks, networkLink{from: from, to: to})
}

func (t *FaultTransport) DropNextRequest(rpcType RPCType) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.requestDrops[rpcType]++
}

func (t *FaultTransport) DropNextResponse(rpcType RPCType) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.responseDrops[rpcType]++
}

func (t *FaultTransport) SetDelay(rpcType RPCType, delay time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.delays[rpcType] = delay
}

func (t *FaultTransport) RequestVote(nodeID string, request RequestVoteRequest) (RequestVoteResponse, error) {
	delay, dropRequest, dropResponse, err := t.prepare(request.CandidateID, nodeID, VoteRPC)
	if err != nil || dropRequest {
		return RequestVoteResponse{}, errors.New("vote request was not delivered")
	}
	time.Sleep(delay)
	response, err := t.base.RequestVote(nodeID, request)
	if err != nil || dropResponse {
		return RequestVoteResponse{}, errors.New("vote response was not delivered")
	}
	return response, nil
}

func (t *FaultTransport) AppendEntries(nodeID string, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	delay, dropRequest, dropResponse, err := t.prepare(request.LeaderID, nodeID, AppendRPC)
	if err != nil || dropRequest {
		return AppendEntriesResponse{}, errors.New("append request was not delivered")
	}
	time.Sleep(delay)
	response, err := t.base.AppendEntries(nodeID, request)
	if err != nil || dropResponse {
		return AppendEntriesResponse{}, errors.New("append response was not delivered")
	}
	return response, nil
}

func (t *FaultTransport) prepare(from, to string, rpcType RPCType) (time.Duration, bool, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.crashed[from] || t.crashed[to] {
		return 0, false, false, errors.New("source or destination node is crashed")
	}
	if t.blockedLinks[networkLink{from: from, to: to}] {
		return 0, false, false, errors.New("network link is partitioned")
	}

	dropRequest := t.requestDrops[rpcType] > 0
	if dropRequest {
		t.requestDrops[rpcType]--
	}
	dropResponse := t.responseDrops[rpcType] > 0
	if dropResponse {
		t.responseDrops[rpcType]--
	}
	return t.delays[rpcType], dropRequest, dropResponse, nil
}
