package main

import (
	"fmt"
	"log"
	"time"

	"github.com/uNeimo/raft-datastore/raft"
)

func main() {
	leader := raft.NewNode("node-1")
	nodeTwo := raft.NewNode("node-2")
	nodeThree := raft.NewNode("node-3")

	serverTwo := mustStartServer(nodeTwo)
	defer serverTwo.Close()
	serverThree := mustStartServer(nodeThree)
	defer serverThree.Close()

	transport := raft.NewNetworkTransport(map[string]string{
		"node-2": serverTwo.Address(),
		"node-3": serverThree.Address(),
	}, time.Second)
	peerIDs := []string{"node-2", "node-3"}

	if !leader.Campaign(peerIDs, transport) {
		log.Fatal("node-1 did not receive a majority vote")
	}
	if err := leader.Set("course", "distributed systems", peerIDs, transport); err != nil {
		log.Fatalf("replicate value: %v", err)
	}
	value, exists, err := leader.GetLinearizable("course", peerIDs, transport)
	if err != nil {
		log.Fatalf("read value: %v", err)
	}

	fmt.Printf("leader=node-1 key=course value=%q exists=%v\n", value, exists)
}

func mustStartServer(node *raft.Node) *raft.RPCServer {
	server, err := raft.StartRPCServer(node, "127.0.0.1:0")
	if err != nil {
		log.Fatalf("start server for %s: %v", node.ID(), err)
	}
	return server
}
