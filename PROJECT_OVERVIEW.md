# Project Overview: Distributed Key-Value Store
A distributed, fault-tolerant, in-memory key-value store built in C++17.

## Core Architectural Goals
- **Networking:** Asynchronous I/O via standalone Asio (no Boost dependencies).
- **Consensus/Replication:** Raft consensus algorithm for distributed state replication.
- **Storage Engine:** Thread-safe in-memory hashtable with write-ahead logging (WAL).
- **Protocol:** Custom binary framing / JSON-based command parsing over TCP.

## Engineering Standards & Rules
- Use modern C++17 idioms (RAII, smart pointers, `std::string_view`).
- Rely exclusively on `FetchContent` in root `CMakeLists.txt` for external dependencies.
- Header-only Asio must be linked via the interface target `asio`.
- Avoid premature optimizations; prioritize clear abstractions and thread safety first.

## Boundaries & Constraints
- Do NOT introduce raw threads where Asio `io_context` strand patterns apply.
- Do NOT add heavy third-party dependencies without prior discussion.
- Keep build scripts portable across MSVC (Windows) and Clang/GCC (Linux).