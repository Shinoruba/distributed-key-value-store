#include "raft_rpc.hpp"

#include <cstring>

namespace distributed_kv {

std::vector<uint8_t> RaftRpcSerializer::serialize_request_vote_args(const RequestVoteArgs& args) {
    const uint32_t cid_len = static_cast<uint32_t>(args.candidate_id.size());
    const uint32_t payload_len = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + cid_len + sizeof(uint64_t) + sizeof(uint64_t);

    std::vector<uint8_t> buffer;
    buffer.reserve(sizeof(uint32_t) + payload_len);

    const uint8_t* p_len = reinterpret_cast<const uint8_t*>(&payload_len);
    buffer.insert(buffer.end(), p_len, p_len + sizeof(uint32_t));

    buffer.push_back(static_cast<uint8_t>(RaftRpcType::REQUEST_VOTE_REQ));

    const uint8_t* term_bytes = reinterpret_cast<const uint8_t*>(&args.term);
    buffer.insert(buffer.end(), term_bytes, term_bytes + sizeof(uint64_t));

    const uint8_t* cid_len_bytes = reinterpret_cast<const uint8_t*>(&cid_len);
    buffer.insert(buffer.end(), cid_len_bytes, cid_len_bytes + sizeof(uint32_t));
    buffer.insert(buffer.end(), args.candidate_id.begin(), args.candidate_id.end());

    const uint8_t* last_idx_bytes = reinterpret_cast<const uint8_t*>(&args.last_log_index);
    buffer.insert(buffer.end(), last_idx_bytes, last_idx_bytes + sizeof(uint64_t));

    const uint8_t* last_term_bytes = reinterpret_cast<const uint8_t*>(&args.last_log_term);
    buffer.insert(buffer.end(), last_term_bytes, last_term_bytes + sizeof(uint64_t));

    return buffer;
}

std::optional<RequestVoteArgs> RaftRpcSerializer::deserialize_request_vote_args(const uint8_t* data, size_t size) {
    if (size < sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t)) {
        return std::nullopt;
    }
    if (data[0] != static_cast<uint8_t>(RaftRpcType::REQUEST_VOTE_REQ)) {
        return std::nullopt;
    }

    RequestVoteArgs args;
    size_t offset = 1;

    args.term = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    uint32_t cid_len = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += sizeof(uint32_t);

    if (offset + cid_len + sizeof(uint64_t) + sizeof(uint64_t) > size) {
        return std::nullopt;
    }

    args.candidate_id.assign(reinterpret_cast<const char*>(data + offset), cid_len);
    offset += cid_len;

    args.last_log_index = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    args.last_log_term = *reinterpret_cast<const uint64_t*>(data + offset);

    return args;
}

std::vector<uint8_t> RaftRpcSerializer::serialize_request_vote_reply(const RequestVoteReply& reply) {
    const uint32_t payload_len = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint8_t);

    std::vector<uint8_t> buffer;
    buffer.reserve(sizeof(uint32_t) + payload_len);

    const uint8_t* p_len = reinterpret_cast<const uint8_t*>(&payload_len);
    buffer.insert(buffer.end(), p_len, p_len + sizeof(uint32_t));

    buffer.push_back(static_cast<uint8_t>(RaftRpcType::REQUEST_VOTE_RESP));

    const uint8_t* term_bytes = reinterpret_cast<const uint8_t*>(&reply.term);
    buffer.insert(buffer.end(), term_bytes, term_bytes + sizeof(uint64_t));

    buffer.push_back(reply.vote_granted ? 1 : 0);

    return buffer;
}

std::optional<RequestVoteReply> RaftRpcSerializer::deserialize_request_vote_reply(const uint8_t* data, size_t size) {
    if (size < sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint8_t)) {
        return std::nullopt;
    }
    if (data[0] != static_cast<uint8_t>(RaftRpcType::REQUEST_VOTE_RESP)) {
        return std::nullopt;
    }

    RequestVoteReply reply;
    size_t offset = 1;

    reply.term = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    reply.vote_granted = (data[offset] != 0);

    return reply;
}

std::vector<uint8_t> RaftRpcSerializer::serialize_append_entries_args(const AppendEntriesArgs& args) {
    const uint32_t lid_len = static_cast<uint32_t>(args.leader_id.size());
    const uint32_t num_entries = static_cast<uint32_t>(args.entries.size());

    uint32_t payload_len = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + lid_len +
                           sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);

    for (const auto& entry : args.entries) {
        payload_len += sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint8_t) +
                       sizeof(uint32_t) + static_cast<uint32_t>(entry.command.key.size()) +
                       sizeof(uint32_t) + static_cast<uint32_t>(entry.command.value.size());
    }

    std::vector<uint8_t> buffer;
    buffer.reserve(sizeof(uint32_t) + payload_len);

    const uint8_t* p_len = reinterpret_cast<const uint8_t*>(&payload_len);
    buffer.insert(buffer.end(), p_len, p_len + sizeof(uint32_t));

    buffer.push_back(static_cast<uint8_t>(RaftRpcType::APPEND_ENTRIES_REQ));

    const uint8_t* term_bytes = reinterpret_cast<const uint8_t*>(&args.term);
    buffer.insert(buffer.end(), term_bytes, term_bytes + sizeof(uint64_t));

    const uint8_t* lid_len_bytes = reinterpret_cast<const uint8_t*>(&lid_len);
    buffer.insert(buffer.end(), lid_len_bytes, lid_len_bytes + sizeof(uint32_t));
    buffer.insert(buffer.end(), args.leader_id.begin(), args.leader_id.end());

    const uint8_t* prev_idx_bytes = reinterpret_cast<const uint8_t*>(&args.prev_log_index);
    buffer.insert(buffer.end(), prev_idx_bytes, prev_idx_bytes + sizeof(uint64_t));

    const uint8_t* prev_term_bytes = reinterpret_cast<const uint8_t*>(&args.prev_log_term);
    buffer.insert(buffer.end(), prev_term_bytes, prev_term_bytes + sizeof(uint64_t));

    const uint8_t* commit_bytes = reinterpret_cast<const uint8_t*>(&args.leader_commit);
    buffer.insert(buffer.end(), commit_bytes, commit_bytes + sizeof(uint64_t));

    const uint8_t* num_ent_bytes = reinterpret_cast<const uint8_t*>(&num_entries);
    buffer.insert(buffer.end(), num_ent_bytes, num_ent_bytes + sizeof(uint32_t));

    for (const auto& entry : args.entries) {
        const uint8_t* e_term = reinterpret_cast<const uint8_t*>(&entry.term);
        buffer.insert(buffer.end(), e_term, e_term + sizeof(uint64_t));

        const uint8_t* e_idx = reinterpret_cast<const uint8_t*>(&entry.index);
        buffer.insert(buffer.end(), e_idx, e_idx + sizeof(uint64_t));

        buffer.push_back(static_cast<uint8_t>(entry.command.type));

        uint32_t k_len = static_cast<uint32_t>(entry.command.key.size());
        const uint8_t* k_len_bytes = reinterpret_cast<const uint8_t*>(&k_len);
        buffer.insert(buffer.end(), k_len_bytes, k_len_bytes + sizeof(uint32_t));
        buffer.insert(buffer.end(), entry.command.key.begin(), entry.command.key.end());

        uint32_t v_len = static_cast<uint32_t>(entry.command.value.size());
        const uint8_t* v_len_bytes = reinterpret_cast<const uint8_t*>(&v_len);
        buffer.insert(buffer.end(), v_len_bytes, v_len_bytes + sizeof(uint32_t));
        buffer.insert(buffer.end(), entry.command.value.begin(), entry.command.value.end());
    }

    return buffer;
}

std::optional<AppendEntriesArgs> RaftRpcSerializer::deserialize_append_entries_args(const uint8_t* data, size_t size) {
    if (size < sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t)) {
        return std::nullopt;
    }
    if (data[0] != static_cast<uint8_t>(RaftRpcType::APPEND_ENTRIES_REQ)) {
        return std::nullopt;
    }

    AppendEntriesArgs args;
    size_t offset = 1;

    args.term = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    uint32_t lid_len = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += sizeof(uint32_t);

    if (offset + lid_len + sizeof(uint64_t) * 3 + sizeof(uint32_t) > size) {
        return std::nullopt;
    }

    args.leader_id.assign(reinterpret_cast<const char*>(data + offset), lid_len);
    offset += lid_len;

    args.prev_log_index = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    args.prev_log_term = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    args.leader_commit = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    uint32_t num_entries = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += sizeof(uint32_t);

    args.entries.reserve(num_entries);
    for (uint32_t i = 0; i < num_entries; ++i) {
        if (offset + sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) > size) {
            return std::nullopt;
        }

        LogEntry entry;
        entry.term = *reinterpret_cast<const uint64_t*>(data + offset);
        offset += sizeof(uint64_t);

        entry.index = *reinterpret_cast<const uint64_t*>(data + offset);
        offset += sizeof(uint64_t);

        entry.command.type = static_cast<CommandType>(data[offset]);
        offset += sizeof(uint8_t);

        uint32_t k_len = *reinterpret_cast<const uint32_t*>(data + offset);
        offset += sizeof(uint32_t);

        if (offset + k_len + sizeof(uint32_t) > size) {
            return std::nullopt;
        }

        entry.command.key.assign(reinterpret_cast<const char*>(data + offset), k_len);
        offset += k_len;

        uint32_t v_len = *reinterpret_cast<const uint32_t*>(data + offset);
        offset += sizeof(uint32_t);

        if (offset + v_len > size) {
            return std::nullopt;
        }

        entry.command.value.assign(reinterpret_cast<const char*>(data + offset), v_len);
        offset += v_len;

        args.entries.push_back(std::move(entry));
    }

    return args;
}

std::vector<uint8_t> RaftRpcSerializer::serialize_append_entries_reply(const AppendEntriesReply& reply) {
    const uint32_t payload_len = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint64_t);

    std::vector<uint8_t> buffer;
    buffer.reserve(sizeof(uint32_t) + payload_len);

    const uint8_t* p_len = reinterpret_cast<const uint8_t*>(&payload_len);
    buffer.insert(buffer.end(), p_len, p_len + sizeof(uint32_t));

    buffer.push_back(static_cast<uint8_t>(RaftRpcType::APPEND_ENTRIES_RESP));

    const uint8_t* term_bytes = reinterpret_cast<const uint8_t*>(&reply.term);
    buffer.insert(buffer.end(), term_bytes, term_bytes + sizeof(uint64_t));

    buffer.push_back(reply.success ? 1 : 0);

    const uint8_t* match_bytes = reinterpret_cast<const uint8_t*>(&reply.match_index);
    buffer.insert(buffer.end(), match_bytes, match_bytes + sizeof(uint64_t));

    return buffer;
}

std::optional<AppendEntriesReply> RaftRpcSerializer::deserialize_append_entries_reply(const uint8_t* data, size_t size) {
    if (size < sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint64_t)) {
        return std::nullopt;
    }
    if (data[0] != static_cast<uint8_t>(RaftRpcType::APPEND_ENTRIES_RESP)) {
        return std::nullopt;
    }

    AppendEntriesReply reply;
    size_t offset = 1;

    reply.term = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    reply.success = (data[offset] != 0);
    offset += sizeof(uint8_t);

    reply.match_index = *reinterpret_cast<const uint64_t*>(data + offset);

    return reply;
}

std::vector<uint8_t> RaftRpcSerializer::serialize_install_snapshot_args(const InstallSnapshotArgs& args) {
    const uint32_t lid_len = static_cast<uint32_t>(args.leader_id.size());
    const uint32_t num_pairs = static_cast<uint32_t>(args.data.size());

    uint32_t payload_len = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + lid_len +
                           sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);

    for (const auto& [k, v] : args.data) {
        payload_len += sizeof(uint32_t) + static_cast<uint32_t>(k.size()) +
                       sizeof(uint32_t) + static_cast<uint32_t>(v.size());
    }

    std::vector<uint8_t> buffer;
    buffer.reserve(sizeof(uint32_t) + payload_len);

    const uint8_t* p_len = reinterpret_cast<const uint8_t*>(&payload_len);
    buffer.insert(buffer.end(), p_len, p_len + sizeof(uint32_t));

    buffer.push_back(static_cast<uint8_t>(RaftRpcType::INSTALL_SNAPSHOT_REQ));

    const uint8_t* term_bytes = reinterpret_cast<const uint8_t*>(&args.term);
    buffer.insert(buffer.end(), term_bytes, term_bytes + sizeof(uint64_t));

    const uint8_t* lid_len_bytes = reinterpret_cast<const uint8_t*>(&lid_len);
    buffer.insert(buffer.end(), lid_len_bytes, lid_len_bytes + sizeof(uint32_t));
    buffer.insert(buffer.end(), args.leader_id.begin(), args.leader_id.end());

    const uint8_t* last_idx_bytes = reinterpret_cast<const uint8_t*>(&args.last_included_index);
    buffer.insert(buffer.end(), last_idx_bytes, last_idx_bytes + sizeof(uint64_t));

    const uint8_t* last_term_bytes = reinterpret_cast<const uint8_t*>(&args.last_included_term);
    buffer.insert(buffer.end(), last_term_bytes, last_term_bytes + sizeof(uint64_t));

    const uint8_t* num_pairs_bytes = reinterpret_cast<const uint8_t*>(&num_pairs);
    buffer.insert(buffer.end(), num_pairs_bytes, num_pairs_bytes + sizeof(uint32_t));

    for (const auto& [k, v] : args.data) {
        uint32_t k_len = static_cast<uint32_t>(k.size());
        const uint8_t* k_len_bytes = reinterpret_cast<const uint8_t*>(&k_len);
        buffer.insert(buffer.end(), k_len_bytes, k_len_bytes + sizeof(uint32_t));
        buffer.insert(buffer.end(), k.begin(), k.end());

        uint32_t v_len = static_cast<uint32_t>(v.size());
        const uint8_t* v_len_bytes = reinterpret_cast<const uint8_t*>(&v_len);
        buffer.insert(buffer.end(), v_len_bytes, v_len_bytes + sizeof(uint32_t));
        buffer.insert(buffer.end(), v.begin(), v.end());
    }

    return buffer;
}

std::optional<InstallSnapshotArgs> RaftRpcSerializer::deserialize_install_snapshot_args(const uint8_t* data, size_t size) {
    if (size < sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t)) {
        return std::nullopt;
    }
    if (data[0] != static_cast<uint8_t>(RaftRpcType::INSTALL_SNAPSHOT_REQ)) {
        return std::nullopt;
    }

    InstallSnapshotArgs args;
    size_t offset = 1;

    args.term = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    uint32_t lid_len = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += sizeof(uint32_t);

    if (offset + lid_len + sizeof(uint64_t) * 2 + sizeof(uint32_t) > size) {
        return std::nullopt;
    }

    args.leader_id.assign(reinterpret_cast<const char*>(data + offset), lid_len);
    offset += lid_len;

    args.last_included_index = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    args.last_included_term = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += sizeof(uint64_t);

    uint32_t num_pairs = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += sizeof(uint32_t);

    for (uint32_t i = 0; i < num_pairs; ++i) {
        if (offset + sizeof(uint32_t) > size) return std::nullopt;
        uint32_t k_len = *reinterpret_cast<const uint32_t*>(data + offset);
        offset += sizeof(uint32_t);

        if (offset + k_len + sizeof(uint32_t) > size) return std::nullopt;
        std::string k(reinterpret_cast<const char*>(data + offset), k_len);
        offset += k_len;

        uint32_t v_len = *reinterpret_cast<const uint32_t*>(data + offset);
        offset += sizeof(uint32_t);

        if (offset + v_len > size) return std::nullopt;
        std::string v(reinterpret_cast<const char*>(data + offset), v_len);
        offset += v_len;

        args.data.emplace(std::move(k), std::move(v));
    }

    return args;
}

std::vector<uint8_t> RaftRpcSerializer::serialize_install_snapshot_reply(const InstallSnapshotReply& reply) {
    const uint32_t payload_len = sizeof(uint8_t) + sizeof(uint64_t);

    std::vector<uint8_t> buffer;
    buffer.reserve(sizeof(uint32_t) + payload_len);

    const uint8_t* p_len = reinterpret_cast<const uint8_t*>(&payload_len);
    buffer.insert(buffer.end(), p_len, p_len + sizeof(uint32_t));

    buffer.push_back(static_cast<uint8_t>(RaftRpcType::INSTALL_SNAPSHOT_RESP));

    const uint8_t* term_bytes = reinterpret_cast<const uint8_t*>(&reply.term);
    buffer.insert(buffer.end(), term_bytes, term_bytes + sizeof(uint64_t));

    return buffer;
}

std::optional<InstallSnapshotReply> RaftRpcSerializer::deserialize_install_snapshot_reply(const uint8_t* data, size_t size) {
    if (size < sizeof(uint8_t) + sizeof(uint64_t)) {
        return std::nullopt;
    }
    if (data[0] != static_cast<uint8_t>(RaftRpcType::INSTALL_SNAPSHOT_RESP)) {
        return std::nullopt;
    }

    InstallSnapshotReply reply;
    size_t offset = 1;

    reply.term = *reinterpret_cast<const uint64_t*>(data + offset);

    return reply;
}

} // namespace distributed_kv