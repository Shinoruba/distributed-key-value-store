#pragma once

#include <asio.hpp>
#include <atomic>
#include <cstdint>
#include <memory>

namespace distributed_kv {

class StorageEngine;

class TcpServer {
public:
    TcpServer(asio::io_context& ioc, uint16_t port, StorageEngine& engine);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void start();
    void stop();

    uint16_t port() const;
    bool is_running() const noexcept;

private:
    void do_accept();

    asio::io_context& ioc_;
    asio::ip::tcp::acceptor acceptor_;
    StorageEngine& engine_;
    std::atomic<bool> running_{false};
};

} // namespace distributed_kv
