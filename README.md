Before building the project, ensure you have the following installed on your machine:
- **C++ Compiler:** C++17 or newer (MSVC 2019/2022/2026, GCC 9+, or Clang 10+)
- **Build System:** [CMake](https://cmake.org/download/) (v3.15 or newer)
- **Dependencies:** Header-only standalone Asio managed automatically via CMake `FetchContent`.

---

## Building the Project

```powershell
# Configure build
cmake -B build -S .

# Compile all targets in Release mode
cmake --build build --config Release
```

The build produces three standalone executables in `build/DistributedKVStore/Release/`:
1. `kvstore-server.exe` — The cluster node server daemon.
2. `kvstore-cli.exe` — Interactive CLI client REPL and one-shot utility.
3. `kvstore-tests.exe` — Comprehensive automated test suite.

---

## Running the Cluster & CLI

### 1. Launch a 3-Node Local Cluster

**Node 1 (Terminal 1):**
```powershell
.\build\DistributedKVStore\Release\kvstore-server.exe --id node_1 --port 6379 --raft-port 7001 --peers "node_2=127.0.0.1:7002,node_3=127.0.0.1:7003" --wal "data/node1.wal"
```

**Node 2 (Terminal 2):**
```powershell
.\build\DistributedKVStore\Release\kvstore-server.exe --id node_2 --port 6380 --raft-port 7002 --peers "node_1=127.0.0.1:7001,node_3=127.0.0.1:7003" --wal "data/node2.wal"
```

**Node 3 (Terminal 3):**
```powershell
.\build\DistributedKVStore\Release\kvstore-server.exe --id node_3 --port 6381 --raft-port 7003 --peers "node_1=127.0.0.1:7001,node_2=127.0.0.1:7002" --wal "data/node3.wal"
```

---

### 2. Interactive CLI Client

Launch the interactive REPL:
```powershell
.\build\DistributedKVStore\Release\kvstore-cli.exe --port 6379
```

#### Example CLI Session:
```text
127.0.0.1:6379> PING
"PONG"  [120.4 µs]

127.0.0.1:6379> SET user:100 "Alice Smith"
OK  [145.2 µs]

127.0.0.1:6379> GET user:100
"Alice Smith"  [110.1 µs]

127.0.0.1:6379> STATS
"1"  [95.4 µs]

127.0.0.1:6379> DEL user:100
OK  [138.6 µs]

127.0.0.1:6379> GET user:100
(nil)  [102.3 µs]
```

#### One-Shot Command Mode:
```powershell
.\build\DistributedKVStore\Release\kvstore-cli.exe --port 6379 SET mykey myvalue
.\build\DistributedKVStore\Release\kvstore-cli.exe --port 6379 GET mykey
```

---

### 3. Running Automated Tests

```powershell
.\build\DistributedKVStore\Release\kvstore-tests.exe
```