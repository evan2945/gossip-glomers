# Gossip Glomers (Distributed Systems Benchmarks)

A collection of distributed systems algorithms and fault-tolerant primitives implemented in **Go** and validated using [Maelstrom](https://github.com/jepsen-io/maelstrom) (Fly.io's distributed systems challenge suite).

---

## Overview

This repository showcases implementations of classic distributed systems challenges—ranging from broadcast messaging to partitioned distributed logs—focusing on correctness under network partitions, high throughput, and strict linearizability verified by Jepsen tests.

---

## Completed Challenges

### 1. Echo (`echo`)
* **Goal:** Basic single-node echo server handling client message requests.
* **Concepts:** Maelstrom node initialization and JSON-RPC message handling.

### 2. Unique ID Generation (`unique-ids`)
* **Goal:** Generate globally unique IDs across a distributed cluster without centralized coordination.
* **Concepts:** Node ID prefixing combined with monotonic counter increments/UUID generation.

### 3. Broadcast (`broadcast`)
* **Goal:** Multi-node gossip protocol to efficiently disseminate client-submitted messages across all cluster nodes.
* **Concepts:** 
  * Anti-entropy background gossip loops.
  * Fault-tolerant message deduplication and topology management.
  * Handling network delays and dropped packets.

### 4. Grow-Only Counter (`counter`)
* **Goal:** Fault-tolerant distributed counter supporting concurrent increments across multiple nodes.
* **Concepts:** Utilizing Maelstrom's linearizable KV store (`lin-kv`) with Compare-And-Swap (CAS) loops to ensure safety and prevent lost updates.

### 5. Kafka-Style Log (`kafka`) — *Challenges 5a–5c*
Engineered a high-throughput, partitioned distributed commit log in Go:
* **5a (Single-Node Log):** Built in-memory log storage protected by `sync.RWMutex` handling `send`, `poll`, `commit_offsets`, and `list_committed_offsets`.
* **5b (Multi-Node CAS Log):** Scaled across multiple nodes using Maelstrom's `lin-kv` store with retry-based CAS loops.
* **5c (Partitioned Single-Owner Architecture):** Eliminated `lin-kv` write contention by introducing:
  * **FNV-1a Key Sharding:** Deterministically routing partition keys to single owner nodes (`getOwner`).
  * **Local In-Memory Mutations:** Partition owners maintain and query logs directly in local Go memory.
  * **Inter-Node RPC Proxying:** Forwarding non-owned key requests via targeted synchronous `SyncRPC` sub-requests.
  * **Performance:** Achieved **1.46 server messages/op (MPO)** and **99.95% availability** under 1,000 req/s load with zero linearizability violations (`:valid? true`).
