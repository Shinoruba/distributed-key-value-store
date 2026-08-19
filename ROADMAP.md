## Milestones

### Phase 1: Local In-Memory Storage Engine
- [x] Implement core `KeyValueStore` / `StorageEngine` class supporting `GET`, `SET`, `DEL`.
- [x] Implement thread-safety via `std::shared_mutex` (reader-writer lock pattern).
- [ ] Add TTL (Time-To-Live) expiration handling and periodic cleanup.
- [ ] Add basic Write-Ahead Logging (WAL) for append-only crash recovery.
- [x] Write unit tests verifying concurrent read/write isolation.

### Phase 2: Standalone Asio Networking & Protocol
- [ ] Define the binary/text wire protocol (length-prefixed frames or RESP/JSON).
- [ ] Implement `TcpServer` with asynchronous accept loops using standalone Asio.
- [ ] Implement `Session` class managing per-connection reading, command dispatch, and write queues.
- [ ] Handle client disconnections, partial reads, and malformed frames gracefully.
- [ ] Integration test: send concurrent client requests to the server over TCP.

### Phase 3: Distributed Consensus & Replication
- [ ] Define cluster membership configuration (node IDs, endpoints).
- [ ] Implement RPC protocol between nodes (AppendEntries, RequestVote).
- [ ] Implement Leader Election with randomized heartbeat/election timers.
- [ ] Implement Log Replication across quorum before committing to the local store.
- [ ] Add client forwarding (redirect non-leader writes to current leader).

### Phase 4: Fault Tolerance, Compaction & Tooling
- [ ] Implement Log Compaction and Snapshotting to limit WAL growth.
- [ ] Handle network partitions and node re-joins with catch-up synchronization.
- [ ] Build a lightweight CLI client utility for interacting with the cluster.
- [ ] Benchmark throughput and p99 latency under concurrent workloads.
