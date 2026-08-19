#pragma once

#include <asio.hpp>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "raft_rpc.hpp"
#include "raft_types.hpp"

namespace distributed_kv {

class StorageEngine;

class RaftNode : public std::enable_shared_from_this<RaftNode> {
public:
    RaftNode(NodeId node_id, std::string host, uint16_t port,
             std::vector<PeerConfig> peers, asio::io_context& ioc,
             StorageEngine& engine);
    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    void start();
    void stop();

    void set_peers(std::vector<PeerConfig> peers);

    NodeRole role() const;
    bool is_leader() const;
    Term current_term() const;
    NodeId node_id() const;
    NodeId leader_id() const;
    uint16_t port() const;
    size_t log_size() const;
    LogIndex commit_index() const;
    LogIndex last_applied() const;
    bool is_running() const noexcept;

    void propose(const Command& cmd,
                 std::function<void(bool success, std::string leader_id)> callback);

    bool propose(const Command& cmd,
                 std::chrono::milliseconds timeout = std::chrono::milliseconds(2000));

    RequestVoteReply handle_request_vote(const RequestVoteArgs& args);
    AppendEntriesReply handle_append_entries(const AppendEntriesArgs& args);

private:
    void do_accept();
    void reset_election_timeout();
    void start_election();
    void become_leader();
    void become_follower(Term term, NodeId leader_id = "");
    void broadcast_heartbeats();
    void broadcast_request_vote();
    void replicate_to_peer(const PeerConfig& peer);
    void check_and_advance_commit_index();
    void apply_entries_to_state_machine();

    void send_rpc_async(const PeerConfig& peer, std::vector<uint8_t> payload,
                        std::function<void(const std::vector<uint8_t>&, const asio::error_code&)> callback);

    NodeId node_id_;
    std::string host_;
    uint16_t port_{0};
    std::vector<PeerConfig> peers_;
    asio::io_context& ioc_;
    asio::strand<asio::any_io_executor> strand_;
    asio::ip::tcp::acceptor acceptor_;
    StorageEngine& engine_;

    std::atomic<bool> running_{false};
    NodeRole role_{NodeRole::FOLLOWER};
    Term current_term_{0};
    NodeId voted_for_;
    NodeId current_leader_id_;
    std::vector<LogEntry> log_;

    LogIndex commit_index_{0};
    LogIndex last_applied_{0};

    std::unordered_map<NodeId, LogIndex> next_index_;
    std::unordered_map<NodeId, LogIndex> match_index_;
    size_t votes_granted_{0};

    std::unordered_map<LogIndex, std::vector<std::function<void(bool, std::string)>>> pending_callbacks_;

    asio::steady_timer election_timer_;
    asio::steady_timer heartbeat_timer_;
    std::mt19937_64 rng_;
};

} // namespace distributed_kv
