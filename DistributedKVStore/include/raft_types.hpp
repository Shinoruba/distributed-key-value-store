#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage_engine.hpp"

namespace distributed_kv {

using NodeId = std::string;
using Term = uint64_t;
using LogIndex = uint64_t;

enum class NodeRole : uint8_t {
    FOLLOWER = 1,
    CANDIDATE = 2,
    LEADER = 3
};

struct PeerConfig {
    NodeId id;
    std::string host{"127.0.0.1"};
    uint16_t port{0};
};

struct LogEntry {
    Term term{0};
    LogIndex index{0};
    Command command;
};

} // namespace distributed_kv