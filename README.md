# Raft-Based Distributed Datastore

I built this project to understand how a replicated datastore stays consistent when nodes fail or cannot communicate. It implements the main Raft consensus rules in Go and uses a small key-value state machine so the behavior is easy to observe.

## What I implemented

- leader election with randomized timeout calculation and one vote per term;
- replicated logs with consistency checks and follower catch-up;
- majority-quorum commits for `set` and `delete` commands;
- quorum-confirmed linearizable reads;
- TCP communication using Go's `net/rpc` package; and
- deterministic fault injection for crashes, request loss, response loss, partitions, and delays.

## Project structure

```text
cmd/demo/                 Runnable three-node TCP demonstration
raft/node.go              Consensus and key-value state
raft/transport.go         In-memory test transport
raft/rpc_transport.go     TCP RPC server and client transport
raft/fault_transport.go   Deterministic failure controls
raft/*_test.go            Unit and integration tests
```

## Run the project

I use Go 1.26 or newer. The project has no third-party dependencies.

```bash
go run ./cmd/demo
go test ./...
go test -race ./...
```

The demo starts three nodes on local ephemeral ports, elects `node-1`, replicates a value, and confirms the read with a majority.

## Design decisions

I separated Raft logic from message delivery through small transport interfaces. This lets the same node implementation run against direct in-memory calls, deterministic failures, or real TCP connections. Tests use explicit campaigns instead of sleeping for election timers, which keeps failures repeatable.

An unsuccessful write can remain in the leader's log and commit later, even though the client received an error. This uncertainty is normal for distributed writes when a response is lost. A production client would attach request IDs and retry safely.

## Current limitations

This is an educational implementation rather than a production database. State is held in memory, membership is fixed, election scheduling is driven by the caller, and logs are not compacted into snapshots. The TCP demo runs on one machine, while the fault harness provides deterministic coverage of multi-node failure behavior.

## Testing results

I test election safety, stale terms and logs, quorum commits, follower recovery, state-machine application, fault injection, partitioned reads and writes, and real TCP replication. I also run the race detector to check synchronized node and transport state.
