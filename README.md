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

The build produces four standalone executables in `build/DistributedKVStore/Release/`:
1. **`kvstore-server.exe`** — The cluster node server daemon.
2. **`kvstore-cli.exe`** — Interactive CLI client REPL and one-shot utility.
3. **`kvstore-bench.exe`** — High-performance multi-threaded benchmark tool.
4. **`kvstore-tests.exe`** — Comprehensive automated integration test suite.

---

## Cluster Management Scripts

### Start 3-Node Cluster
```powershell
# Windows PowerShell
.\scripts\start_cluster.ps1

# Linux / macOS Bash
./scripts/start_cluster.sh
```

### Stop Cluster
```powershell
# Windows PowerShell
.\scripts\stop_cluster.ps1

# Linux / macOS Bash
./scripts/stop_cluster.sh
```

---

## Interactive CLI Client

Launch the interactive REPL:
```powershell
.\build\DistributedKVStore\Release\kvstore-cli.exe --port 6380
```

#### Example Session:
```text
127.0.0.1:6380> PING
"PONG"  [145.2 µs]

127.0.0.1:6380> SET session:101 "user_token_abc" EX 60
OK  [180.4 µs]

127.0.0.1:6380> TTL session:101
(integer) 59420 ms (59s remaining)  [95.1 µs]

127.0.0.1:6380> GET session:101
"user_token_abc"  [110.3 µs]

127.0.0.1:6380> STATS
"1"  [85.6 µs]

127.0.0.1:6380> DEL session:101
OK  [135.2 µs]
```

---

## Benchmarking (`kvstore-bench`)

Run the benchmark against a running cluster or node:
```powershell
.\build\DistributedKVStore\Release\kvstore-bench.exe --port 6380 --clients 16 --requests 50000 --ratio 1:4 --keyspace 10000
```

#### Sample Benchmark Output:
```text
======================================================
 DistributedKVStore Benchmark Runner                  
======================================================
Target:        127.0.0.1:6380
Clients:       16 threads
Requests:      50000
Keyspace:      10000 keys
Workload:      1 SETs / 4 GETs (20.0% writes)
Value Size:    64 bytes
======================================================
Connecting clients...
Starting benchmark...

======================================================
 Benchmark Results                                    
======================================================
Total Duration:       0.5007 seconds
Total Requests:       50000
Throughput:           99866.96 ops/sec
Successful Ops:       50000
Redirects:            0
Errors:               0
------------------------------------------------------
Latency Distribution:
  Min:                39 µs
  Avg:                124.5 µs
  p50 (Median):       99 µs
  p90:                145 µs
  p95:                178 µs
  p99:                338 µs
  p99.9:              3370 µs
  Max:                165692 µs
======================================================
```

---

## Running Automated Test Suite

```powershell
.\build\DistributedKVStore\Release\kvstore-tests.exe
```