#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "raft_types.hpp"

namespace distributed_kv {

enum class RaftRpcType : uint8_t {
    REQUEST_VOTE_REQ = 1,
    REQUEST_VOTE_RESP = 2,
    APPEND_ENTRIES_REQ = 3,
    APPEND_ENTRIES_RESP = 4,
    INSTALL_SNAPSHOT_REQ = 5,
    INSTALL_SNAPSHOT_RESP = 6
};

struct RequestVoteArgs {
    Term term{0};
    NodeId candidate_id;
    LogIndex last_log_index{0};
    Term last_log_term{0};
};

struct RequestVoteReply {
    Term term{0};
    bool vote_granted{false};
};

struct AppendEntriesArgs {
    Term term{0};
    NodeId leader_id;
    LogIndex prev_log_index{0};
    Term prev_log_term{0};
    std::vector<LogEntry> entries;
    LogIndex leader_commit{0};
};

struct AppendEntriesReply {
    Term term{0};
    bool success{false};
    LogIndex match_index{0};
};

struct InstallSnapshotArgs {
    Term term{0};
    NodeId leader_id;
    LogIndex last_included_index{0};
    Term last_included_term{0};
    std::unordered_map<std::string, std::string> data;
};

struct InstallSnapshotReply {
    Term term{0};
};

class RaftRpcSerializer {
public:
    static std::vector<uint8_t> serialize_request_vote_args(const RequestVoteArgs& args);
    static std::optional<RequestVoteArgs> deserialize_request_vote_args(const uint8_t* data, size_t size);

    static std::vector<uint8_t> serialize_request_vote_reply(const RequestVoteReply& reply);
    static std::optional<RequestVoteReply> deserialize_request_vote_reply(const uint8_t* data, size_t size);

    static std::vector<uint8_t> serialize_append_entries_args(const AppendEntriesArgs& args);
    static std::optional<AppendEntriesArgs> deserialize_append_entries_args(const uint8_t* data, size_t size);

    static std::vector<uint8_t> serialize_append_entries_reply(const AppendEntriesReply& reply);
    static std::optional<AppendEntriesReply> deserialize_append_entries_reply(const uint8_t* data, size_t size);

    static std::vector<uint8_t> serialize_install_snapshot_args(const InstallSnapshotArgs& args);
    static std::optional<InstallSnapshotArgs> deserialize_install_snapshot_args(const uint8_t* data, size_t size);

    static std::vector<uint8_t> serialize_install_snapshot_reply(const InstallSnapshotReply& reply);
    static std::optional<InstallSnapshotReply> deserialize_install_snapshot_reply(const uint8_t* data, size_t size);
};

} // namespace distributed_kv