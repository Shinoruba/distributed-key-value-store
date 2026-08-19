#pragma once

#include <asio.hpp>
#include <deque>
#include <memory>
#include <vector>

#include "protocol.hpp"

namespace distributed_kv {

class StorageEngine;
class RaftNode;

class TcpSession : public std::enable_shared_from_this<TcpSession> {
public:
    TcpSession(asio::ip::tcp::socket socket, StorageEngine& engine,
               std::shared_ptr<RaftNode> raft = nullptr);
    ~TcpSession() = default;

    TcpSession(const TcpSession&) = delete;
    TcpSession& operator=(const TcpSession&) = delete;

    void start();
    void stop();

private:
    void read_header();
    void read_payload(uint32_t payload_len);
    void handle_request(const Request& req);
    void queue_response(const Response& res);
    void do_write();

    asio::ip::tcp::socket socket_;
    asio::strand<asio::any_io_executor> strand_;
    StorageEngine& engine_;
    std::shared_ptr<RaftNode> raft_;

    uint32_t read_payload_len_{0};
    std::vector<uint8_t> read_buffer_;
    std::deque<std::vector<uint8_t>> write_queue_;
};

} // namespace distributed_kv