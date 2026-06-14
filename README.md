# Distributed Inventory System

A distributed systems project for the MC714 course at Unicamp, implementing classic distributed algorithms over a network of communicating nodes.

## Background

In distributed systems, nodes must coordinate without shared memory or a central authority. This project demonstrates three foundational techniques:

- **Logical clocks (Lamport):** Since nodes have independent clocks, a logical counter provides a consistent ordering of events across the system. Each message carries the sender's timestamp; receivers advance their own clock accordingly.

- **Distributed mutual exclusion (Ricart-Agrawala):** When multiple nodes want to access a shared resource simultaneously, they must coordinate to ensure only one enters the critical section at a time. Ricart-Agrawala achieves this through message passing: a node that wants access broadcasts a request and only proceeds once every other node has replied.

- **Leader election (Bully algorithm):** The system elects a single leader responsible for privileged operations (inventory restock). The Bully algorithm works by having a node challenge all peers with higher IDs; if none respond, it declares itself the leader. A heartbeat mechanism detects leader failure and triggers re-election.

## Project

Four nodes share an inventory counter. Each node periodically tries to check out one unit (decrement the counter), protected by the Ricart-Agrawala mutex. The elected leader periodically restocks the inventory, and after each state change the modifying node broadcasts the new count to all peers.

All inter-node communication is done via gRPC.

## How to Run

**Requirements:** Docker and Docker Compose.

```bash
docker compose up --build
```

This starts four nodes (`node1`–`node4`) on a shared Docker network. Logs from all nodes are printed to stdout, prefixed with the node ID and current Lamport timestamp.

To stop:
```bash
docker compose down
```

To stop a specific container to verify a new election:
```bash
docker container stop <container ID>
```

## Components

### `src/lamport_clock.py` — Lamport Clock

Thread-safe logical clock. `tick()` increments and returns the current timestamp; `update(t)` advances the clock to `max(local, t) + 1` upon receiving a remote timestamp.

### `src/mutex.py` — Ricart-Agrawala Mutex

Implements distributed mutual exclusion. `request_cs(stubs)` broadcasts a `RequestCS` RPC to all peers and blocks until all have replied. `on_request()` is called by the gRPC server when a peer requests access — it defers the reply if the local node has higher priority (i.e. is currently holding or has an earlier-timestamped pending request). `release_cs()` unblocks all deferred replies.

### `src/election.py` — Bully Election

Implements leader election. `start_election()` sends `Election` messages to all peers with a higher node ID; if none acknowledge within the timeout, the node declares itself leader and broadcasts a `Coordinator` message. `on_election()` re-triggers election when challenged by a lower-ID peer. The `heartbeat_loop` in `node.py` pings the current leader periodically and calls `start_election()` if the leader is unreachable.

### `src/inventory.py` — Inventory State

Thread-safe inventory counter. `checkout()` decrements by one and returns whether it succeeded. `restock(amount)` adds units. `sync(new_count)` overwrites the local count with a value received from another node.

### `src/node.py` — Node Entry Point

Wires all components together. Starts the gRPC server, waits for all peers to come online, then launches three daemon threads:

- **`checkout_loop`** — repeatedly acquires the mutex, attempts a checkout, then broadcasts the new inventory count.
- **`restock_loop`** — runs on all nodes but only acts if the local node is the current leader; restocks every 10 seconds.
- **`heartbeat_loop`** — pings the leader every 3 seconds and triggers re-election on failure.

### `proto/inventory.proto` — gRPC Service Definition

Defines the `NodeService` with RPCs for each protocol: `RequestCS`/`CSReply` (mutex), `Election`/`Coordinator` (leader election), `Ping` (heartbeat), `SyncInventory` (state broadcast), and `SendMessage` (startup handshake).

### `generated/` — gRPC Stubs

Auto-generated Python stubs from the proto file. Regenerate with:

```bash
python -m grpc_tools.protoc -I proto --python_out=generated --grpc_python_out=generated proto/inventory.proto
```
