#include "tcp_server.hpp"
#include "tcp_session.hpp"

namespace distributed_kv {

TcpServer::TcpServer(asio::io_context& ioc, uint16_t port, StorageEngine& engine)
    : ioc_(ioc),
      acceptor_(ioc, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)),
      engine_(engine) {}

TcpServer::~TcpServer() {
    stop();
}

void TcpServer::start() {
    if (!running_.exchange(true)) {
        do_accept();
    }
}

void TcpServer::stop() {
    if (running_.exchange(false)) {
        asio::error_code ec;
        acceptor_.close(ec);
    }
}

uint16_t TcpServer::port() const {
    asio::error_code ec;
    auto endpoint = acceptor_.local_endpoint(ec);
    if (!ec) {
        return endpoint.port();
    }
    return 0;
}

bool TcpServer::is_running() const noexcept {
    return running_.load();
}

void TcpServer::do_accept() {
    acceptor_.async_accept([this](const asio::error_code& ec, asio::ip::tcp::socket socket) {
        if (!ec) {
            std::make_shared<TcpSession>(std::move(socket), engine_)->start();
        }
        if (running_.load()) {
            do_accept();
        }
    });
}

} // namespace distributed_kv
