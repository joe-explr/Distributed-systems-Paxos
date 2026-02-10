# Multi-Paxos Distributed Banking System

A fault-tolerant distributed banking system built on **Multi-Paxos**, supporting continuous transaction processing with leader election, crash fault tolerance, and exactly-once semantics.

This project implements a **stable-leader Paxos** variant to efficiently replicate a sequence of transactions across replicas while tolerating node failures.

---

## 🚀 Features

- Multi-Paxos with **stable leader optimization**
- Automatic **leader election and recovery**
- Exactly-once client semantics using timestamps
- Crash fault tolerance (fail-stop model)
- Deterministic total order execution
- Distributed key-value datastore (bank balances)
- Client retry and leader redirection logic
- Debugging & observability via introspection commands

---

## 🏗️ System Architecture

- **Nodes:** 5 replicas (2f + 1, f = 2)
- **Clients:** 10 logical clients
- **Replication:** State machine replication
- **Fault Model:** Crash failures
- **Communication:** RPC / TCP-based message passing
- **Execution Model:** Out-of-order proposal, in-order execution

Each node maintains:
- Accepted log
- Commit log
- Execution pointer
- Client reply cache (for exactly-once semantics)

---

## 🔁 Protocol Overview

### Normal Operation
1. Client sends `(sender, receiver, amount)` to leader
2. Leader assigns a monotonically increasing **sequence number**
3. Leader broadcasts `ACCEPT(b, s, txn)`
4. Replicas respond with `ACCEPTED`
5. Leader commits after majority and broadcasts `COMMIT`
6. All nodes execute transaction in order

### Leader Election
- Triggered by timeout
- Uses `PREPARE / PROMISE`
- Accept logs are merged to avoid lost updates
- Missing sequence numbers are filled with **no-op**

---

## 🧪 Supported Commands

- `PrintDB()` — Print balances on all nodes
- `PrintLog(node)` — Inspect Paxos logs
- `PrintStatus(seq)` — View status (Accepted / Committed / Executed)
- `PrintView()` — Show leader changes

---

## ▶️ Running the System

```bash
# Build
make

# Run nodes
./node --id 1
./node --id 2
...

# Run client
./client input.csv
