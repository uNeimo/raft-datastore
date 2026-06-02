package raft

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const defaultRPCTimeout = 500 * time.Millisecond

type raftRPCService struct {
	node *Node
}

func (s *raftRPCService) RequestVote(request RequestVoteRequest, response *RequestVoteResponse) error {
	*response = s.node.HandleRequestVote(request)
	return nil
}

func (s *raftRPCService) AppendEntries(request AppendEntriesRequest, response *AppendEntriesResponse) error {
	*response = s.node.HandleAppendEntries(request)
	return nil
}

// RPCServer exposes one Raft node over TCP.
type RPCServer struct {
	listener net.Listener
	close    sync.Once
}

// StartRPCServer listens on an address such as "127.0.0.1:7002".
// Port zero may be used in tests to select an available port.
func StartRPCServer(node *Node, address string) (*RPCServer, error) {
	server := rpc.NewServer()
	if err := server.RegisterName("Raft", &raftRPCService{node: node}); err != nil {
		return nil, fmt.Errorf("register Raft RPC service: %w", err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for Raft RPC: %w", err)
	}

	rpcServer := &RPCServer{listener: listener}
	go rpcServer.serve(server)
	return rpcServer, nil
}

func (s *RPCServer) Address() string {
	return s.listener.Addr().String()
}

func (s *RPCServer) Close() error {
	var closeError error
	s.close.Do(func() {
		closeError = s.listener.Close()
	})
	return closeError
}

func (s *RPCServer) serve(server *rpc.Server) {
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			return
		}
		go server.ServeConn(connection)
	}
}

// NetworkTransport sends Raft messages to node addresses over TCP.
type NetworkTransport struct {
	addresses map[string]string
	timeout   time.Duration
}

func NewNetworkTransport(addresses map[string]string, timeout time.Duration) *NetworkTransport {
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}
	addressCopy := make(map[string]string, len(addresses))
	for nodeID, address := range addresses {
		addressCopy[nodeID] = address
	}
	return &NetworkTransport{addresses: addressCopy, timeout: timeout}
}

func (t *NetworkTransport) RequestVote(nodeID string, request RequestVoteRequest) (RequestVoteResponse, error) {
	var response RequestVoteResponse
	err := t.call(nodeID, "Raft.RequestVote", request, &response)
	return response, err
}

func (t *NetworkTransport) AppendEntries(nodeID string, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	var response AppendEntriesResponse
	err := t.call(nodeID, "Raft.AppendEntries", request, &response)
	return response, err
}

func (t *NetworkTransport) call(nodeID, method string, request, response any) error {
	address, exists := t.addresses[nodeID]
	if !exists {
		return fmt.Errorf("no RPC address configured for node %q", nodeID)
	}
	connection, err := net.DialTimeout("tcp", address, t.timeout)
	if err != nil {
		return fmt.Errorf("connect to node %q: %w", nodeID, err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(t.timeout)); err != nil {
		return fmt.Errorf("set RPC deadline: %w", err)
	}

	client := rpc.NewClient(connection)
	defer client.Close()
	if err := client.Call(method, request, response); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return fmt.Errorf("call node %q: connection closed", nodeID)
		}
		return fmt.Errorf("call node %q: %w", nodeID, err)
	}
	return nil
}
