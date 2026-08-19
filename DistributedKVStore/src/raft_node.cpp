#include "raft_node.hpp"
#include "storage_engine.hpp"

#include <chrono>

namespace distributed_kv {

RaftNode::RaftNode(NodeId node_id, std::string host, uint16_t port,
                   std::vector<PeerConfig> peers, asio::io_context& ioc,
                   StorageEngine& engine)
    : node_id_(std::move(node_id)),
      host_(std::move(host)),
      port_(port),
      peers_(std::move(peers)),
      ioc_(ioc),
      strand_(asio::make_strand(ioc.get_executor())),
      acceptor_(ioc, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)),
      engine_(engine),
      election_timer_(ioc),
      heartbeat_timer_(ioc),
      rng_(std::random_device{}() + std::hash<std::string>{}(node_id_)) {
    // Index 0 dummy entry
    log_.push_back(LogEntry{0, 0, Command::make_noop()});

    asio::error_code ec;
    auto ep = acceptor_.local_endpoint(ec);
    if (!ec) {
        port_ = ep.port();
    }
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::set_peers(std::vector<PeerConfig> peers) {
    asio::post(strand_, [this, self = shared_from_this(), peers = std::move(peers)]() mutable {
        peers_ = std::move(peers);
    });
}

void RaftNode::start() {
    if (!running_.exchange(true)) {
        asio::post(strand_, [this, self = shared_from_this()]() {
            do_accept();
            reset_election_timeout();
        });
    }
}

void RaftNode::stop() {
    if (running_.exchange(false)) {
        asio::post(strand_, [this, self = shared_from_this()]() {
            asio::error_code ec;
            election_timer_.cancel();
            heartbeat_timer_.cancel();
            acceptor_.close(ec);
            role_ = NodeRole::FOLLOWER;

            for (auto& [idx, cbs] : pending_callbacks_) {
                for (auto& cb : cbs) {
                    cb(false, "");
                }
            }
            pending_callbacks_.clear();
        });
    }
}

NodeRole RaftNode::role() const {
    return role_;
}

bool RaftNode::is_leader() const {
    return role_ == NodeRole::LEADER;
}

Term RaftNode::current_term() const {
    return current_term_;
}

NodeId RaftNode::node_id() const {
    return node_id_;
}

NodeId RaftNode::leader_id() const {
    return current_leader_id_;
}

uint16_t RaftNode::port() const {
    return port_;
}

size_t RaftNode::log_size() const {
    return log_.size() > 0 ? log_.size() - 1 : 0;
}

LogIndex RaftNode::commit_index() const {
    return commit_index_;
}

LogIndex RaftNode::last_applied() const {
    return last_applied_;
}

bool RaftNode::is_running() const noexcept {
    return running_.load();
}

void RaftNode::propose(const Command& cmd,
                       std::function<void(bool success, std::string leader_id)> callback) {
    asio::post(strand_, [this, self = shared_from_this(), cmd, callback = std::move(callback)]() mutable {
        if (!running_ || role_ != NodeRole::LEADER) {
            callback(false, current_leader_id_);
            return;
        }

        const LogIndex new_index = static_cast<LogIndex>(log_.size());
        LogEntry entry{current_term_, new_index, cmd};
        log_.push_back(std::move(entry));
        pending_callbacks_[new_index].push_back(std::move(callback));

        if (peers_.empty()) {
            commit_index_ = new_index;
            apply_entries_to_state_machine();
            return;
        }

        for (const auto& peer : peers_) {
            replicate_to_peer(peer);
        }
    });
}

bool RaftNode::propose(const Command& cmd, std::chrono::milliseconds timeout) {
    auto prom = std::make_shared<std::promise<std::pair<bool, std::string>>>();
    auto fut = prom->get_future();

    propose(cmd, [prom](bool success, std::string leader_id) {
        prom->set_value({success, std::move(leader_id)});
    });

    if (fut.wait_for(timeout) == std::future_status::ready) {
        return fut.get().first;
    }
    return false;
}

void RaftNode::reset_election_timeout() {
    if (!running_) return;

    election_timer_.cancel();
    std::uniform_int_distribution<int> dist(150, 300);
    int timeout_ms = dist(rng_);

    election_timer_.expires_after(std::chrono::milliseconds(timeout_ms));
    election_timer_.async_wait(asio::bind_executor(strand_, [this, self = shared_from_this()](const asio::error_code& ec) {
        if (!ec && running_ && role_ != NodeRole::LEADER) {
            start_election();
        }
    }));
}

void RaftNode::start_election() {
    if (!running_ || role_ == NodeRole::LEADER) return;

    role_ = NodeRole::CANDIDATE;
    ++current_term_;
    voted_for_ = node_id_;
    votes_granted_ = 1;
    current_leader_id_.clear();

    reset_election_timeout();

    const size_t total_nodes = peers_.size() + 1;
    const size_t quorum = (total_nodes / 2) + 1;

    if (votes_granted_ >= quorum) {
        become_leader();
        return;
    }

    broadcast_request_vote();
}

void RaftNode::become_leader() {
    if (!running_) return;

    role_ = NodeRole::LEADER;
    current_leader_id_ = node_id_;
    election_timer_.cancel();

    next_index_.clear();
    match_index_.clear();
    const LogIndex last_idx = log_.empty() ? 0 : static_cast<LogIndex>(log_.size() - 1);
    for (const auto& peer : peers_) {
        next_index_[peer.id] = last_idx + 1;
        match_index_[peer.id] = 0;
    }

    broadcast_heartbeats();
}

void RaftNode::become_follower(Term term, NodeId leader_id) {
    role_ = NodeRole::FOLLOWER;
    current_term_ = term;
    voted_for_.clear();
    current_leader_id_ = std::move(leader_id);
    heartbeat_timer_.cancel();

    for (auto& [idx, cbs] : pending_callbacks_) {
        for (auto& cb : cbs) {
            cb(false, current_leader_id_);
        }
    }
    pending_callbacks_.clear();

    reset_election_timeout();
}

void RaftNode::broadcast_request_vote() {
    if (!running_ || role_ != NodeRole::CANDIDATE) return;

    RequestVoteArgs args;
    args.term = current_term_;
    args.candidate_id = node_id_;
    args.last_log_index = log_.empty() ? 0 : log_.back().index;
    args.last_log_term = log_.empty() ? 0 : log_.back().term;

    auto payload = RaftRpcSerializer::serialize_request_vote_args(args);
    const size_t quorum = ((peers_.size() + 1) / 2) + 1;
    const Term election_term = current_term_;

    for (const auto& peer : peers_) {
        send_rpc_async(peer, payload, [this, self = shared_from_this(), election_term, quorum](const std::vector<uint8_t>& resp_data, const asio::error_code& ec) {
            if (ec || !running_ || role_ != NodeRole::CANDIDATE || current_term_ != election_term) {
                return;
            }

            if (resp_data.size() <= sizeof(uint32_t)) return;
            auto reply = RaftRpcSerializer::deserialize_request_vote_reply(resp_data.data() + sizeof(uint32_t), resp_data.size() - sizeof(uint32_t));
            if (!reply) return;

            if (reply->term > current_term_) {
                become_follower(reply->term);
                return;
            }

            if (reply->vote_granted) {
                ++votes_granted_;
                if (votes_granted_ >= quorum && role_ == NodeRole::CANDIDATE) {
                    become_leader();
                }
            }
        });
    }
}

void RaftNode::replicate_to_peer(const PeerConfig& peer) {
    if (!running_ || role_ != NodeRole::LEADER) return;

    LogIndex p_next = next_index_[peer.id];
    if (p_next == 0) p_next = 1;
    if (p_next > log_.size()) p_next = static_cast<LogIndex>(log_.size());

    const LogIndex prev_idx = p_next - 1;
    const Term prev_term = log_[prev_idx].term;

    AppendEntriesArgs args;
    args.term = current_term_;
    args.leader_id = node_id_;
    args.prev_log_index = prev_idx;
    args.prev_log_term = prev_term;
    args.leader_commit = commit_index_;

    for (size_t i = p_next; i < log_.size(); ++i) {
        args.entries.push_back(log_[i]);
    }

    auto payload = RaftRpcSerializer::serialize_append_entries_args(args);
    const Term req_term = current_term_;

    send_rpc_async(peer, payload, [this, self = shared_from_this(), peer_id = peer.id, req_term](const std::vector<uint8_t>& resp_data, const asio::error_code& ec) {
        if (ec || !running_ || role_ != NodeRole::LEADER || current_term_ != req_term) {
            return;
        }

        if (resp_data.size() <= sizeof(uint32_t)) return;
        auto reply = RaftRpcSerializer::deserialize_append_entries_reply(resp_data.data() + sizeof(uint32_t), resp_data.size() - sizeof(uint32_t));
        if (!reply) return;

        if (reply->term > current_term_) {
            become_follower(reply->term);
            return;
        }

        if (reply->success) {
            match_index_[peer_id] = std::max(match_index_[peer_id], reply->match_index);
            next_index_[peer_id] = match_index_[peer_id] + 1;
            check_and_advance_commit_index();
        } else {
            if (next_index_[peer_id] > 1) {
                --next_index_[peer_id];
                for (const auto& p : peers_) {
                    if (p.id == peer_id) {
                        replicate_to_peer(p);
                        break;
                    }
                }
            }
        }
    });
}

void RaftNode::broadcast_heartbeats() {
    if (!running_ || role_ != NodeRole::LEADER) return;

    for (const auto& peer : peers_) {
        replicate_to_peer(peer);
    }

    heartbeat_timer_.expires_after(std::chrono::milliseconds(50));
    heartbeat_timer_.async_wait(asio::bind_executor(strand_, [this, self = shared_from_this()](const asio::error_code& ec) {
        if (!ec && running_ && role_ == NodeRole::LEADER) {
            broadcast_heartbeats();
        }
    }));
}

void RaftNode::check_and_advance_commit_index() {
    if (role_ != NodeRole::LEADER) return;

    for (LogIndex n = log_.size() - 1; n > commit_index_; --n) {
        if (log_[n].term != current_term_) continue;

        size_t count = 1;
        for (const auto& peer : peers_) {
            if (match_index_[peer.id] >= n) {
                ++count;
            }
        }

        const size_t quorum = ((peers_.size() + 1) / 2) + 1;
        if (count >= quorum) {
            commit_index_ = n;
            apply_entries_to_state_machine();
            break;
        }
    }
}

void RaftNode::apply_entries_to_state_machine() {
    while (commit_index_ > last_applied_) {
        ++last_applied_;
        if (last_applied_ < log_.size()) {
            const auto& entry = log_[last_applied_];
            engine_.apply(entry.command);

            auto it = pending_callbacks_.find(last_applied_);
            if (it != pending_callbacks_.end()) {
                for (auto& cb : it->second) {
                    cb(true, node_id_);
                }
                pending_callbacks_.erase(it);
            }
        }
    }
}

RequestVoteReply RaftNode::handle_request_vote(const RequestVoteArgs& args) {
    if (args.term < current_term_) {
        return {current_term_, false};
    }

    if (args.term > current_term_) {
        become_follower(args.term);
    }

    const Term last_term = log_.empty() ? 0 : log_.back().term;
    const LogIndex last_idx = log_.empty() ? 0 : log_.back().index;
    const bool log_ok = (args.last_log_term > last_term) ||
                        (args.last_log_term == last_term && args.last_log_index >= last_idx);

    if ((voted_for_.empty() || voted_for_ == args.candidate_id) && log_ok) {
        voted_for_ = args.candidate_id;
        reset_election_timeout();
        return {current_term_, true};
    }

    return {current_term_, false};
}

AppendEntriesReply RaftNode::handle_append_entries(const AppendEntriesArgs& args) {
    if (args.term < current_term_) {
        return {current_term_, false, 0};
    }

    if (args.term > current_term_ || role_ != NodeRole::FOLLOWER) {
        become_follower(args.term, args.leader_id);
    } else {
        current_leader_id_ = args.leader_id;
        reset_election_timeout();
    }

    if (args.prev_log_index >= log_.size()) {
        return {current_term_, false, log_.empty() ? 0 : static_cast<LogIndex>(log_.size() - 1)};
    }

    if (log_[args.prev_log_index].term != args.prev_log_term) {
        log_.resize(args.prev_log_index);
        return {current_term_, false, log_.empty() ? 0 : static_cast<LogIndex>(log_.size() - 1)};
    }

    LogIndex insert_idx = args.prev_log_index + 1;
    for (const auto& entry : args.entries) {
        if (insert_idx < log_.size()) {
            if (log_[insert_idx].term != entry.term) {
                log_.resize(insert_idx);
                log_.push_back(entry);
            }
        } else {
            log_.push_back(entry);
        }
        ++insert_idx;
    }

    if (args.leader_commit > commit_index_) {
        commit_index_ = std::min(args.leader_commit, static_cast<LogIndex>(log_.size() - 1));
        apply_entries_to_state_machine();
    }

    return {current_term_, true, static_cast<LogIndex>(log_.size() - 1)};
}

void RaftNode::do_accept() {
    acceptor_.async_accept([this, self = shared_from_this()](const asio::error_code& ec, asio::ip::tcp::socket socket) {
        if (!ec) {
            auto sock_ptr = std::make_shared<asio::ip::tcp::socket>(std::move(socket));
            auto len_buf = std::make_shared<uint32_t>(0);

            asio::async_read(*sock_ptr, asio::buffer(len_buf.get(), sizeof(uint32_t)),
                asio::bind_executor(strand_, [this, self, sock_ptr, len_buf](const asio::error_code& rec, size_t) {
                    if (rec || *len_buf == 0 || *len_buf > 64 * 1024 * 1024) return;

                    auto payload = std::make_shared<std::vector<uint8_t>>(*len_buf);
                    asio::async_read(*sock_ptr, asio::buffer(payload->data(), *len_buf),
                        asio::bind_executor(strand_, [this, self, sock_ptr, payload](const asio::error_code& pec, size_t) {
                            if (pec || payload->empty()) return;

                            std::vector<uint8_t> resp;
                            const uint8_t type = (*payload)[0];

                            if (type == static_cast<uint8_t>(RaftRpcType::REQUEST_VOTE_REQ)) {
                                auto args = RaftRpcSerializer::deserialize_request_vote_args(payload->data(), payload->size());
                                if (args) {
                                    auto reply = handle_request_vote(*args);
                                    resp = RaftRpcSerializer::serialize_request_vote_reply(reply);
                                }
                            } else if (type == static_cast<uint8_t>(RaftRpcType::APPEND_ENTRIES_REQ)) {
                                auto args = RaftRpcSerializer::deserialize_append_entries_args(payload->data(), payload->size());
                                if (args) {
                                    auto reply = handle_append_entries(*args);
                                    resp = RaftRpcSerializer::serialize_append_entries_reply(reply);
                                }
                            }

                            if (!resp.empty()) {
                                auto resp_ptr = std::make_shared<std::vector<uint8_t>>(std::move(resp));
                                asio::async_write(*sock_ptr, asio::buffer(*resp_ptr),
                                    asio::bind_executor(strand_, [sock_ptr, resp_ptr](const asio::error_code&, size_t) {
                                        asio::error_code ignored;
                                        sock_ptr->shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
                                        sock_ptr->close(ignored);
                                    }));
                            }
                        }));
                }));
        }

        if (running_) {
            do_accept();
        }
    });
}

void RaftNode::send_rpc_async(const PeerConfig& peer, std::vector<uint8_t> payload,
                              std::function<void(const std::vector<uint8_t>&, const asio::error_code&)> callback) {
    auto sock = std::make_shared<asio::ip::tcp::socket>(ioc_);
    auto payload_ptr = std::make_shared<std::vector<uint8_t>>(std::move(payload));
    auto endpoint = asio::ip::tcp::endpoint(asio::ip::make_address(peer.host), peer.port);

    sock->async_connect(endpoint, asio::bind_executor(strand_, [this, self = shared_from_this(), sock, payload_ptr, callback](const asio::error_code& ec) {
        if (ec) {
            callback({}, ec);
            return;
        }

        asio::async_write(*sock, asio::buffer(*payload_ptr), asio::bind_executor(strand_, [this, self, sock, callback](const asio::error_code& wec, size_t) {
            if (wec) {
                callback({}, wec);
                return;
            }

            auto len_buf = std::make_shared<uint32_t>(0);
            asio::async_read(*sock, asio::buffer(len_buf.get(), sizeof(uint32_t)),
                asio::bind_executor(strand_, [this, self, sock, len_buf, callback](const asio::error_code& rec, size_t) {
                    if (rec || *len_buf == 0 || *len_buf > 64 * 1024 * 1024) {
                        callback({}, rec ? rec : asio::error::make_error_code(asio::error::fault));
                        return;
                    }

                    auto resp_buf = std::make_shared<std::vector<uint8_t>>(sizeof(uint32_t) + *len_buf);
                    const uint8_t* len_bytes = reinterpret_cast<const uint8_t*>(len_buf.get());
                    std::copy(len_bytes, len_bytes + sizeof(uint32_t), resp_buf->data());

                    asio::async_read(*sock, asio::buffer(resp_buf->data() + sizeof(uint32_t), *len_buf),
                        asio::bind_executor(strand_, [sock, resp_buf, callback](const asio::error_code& pec, size_t) {
                            callback(*resp_buf, pec);
                        }));
                }));
        }));
    }));
}

} // namespace distributed_kv
