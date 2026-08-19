#include "tcp_session.hpp"
#include "storage_engine.hpp"

namespace distributed_kv {

TcpSession::TcpSession(asio::ip::tcp::socket socket, StorageEngine& engine)
    : socket_(std::move(socket)),
      strand_(asio::make_strand(socket_.get_executor())),
      engine_(engine) {}

void TcpSession::start() {
    read_header();
}

void TcpSession::stop() {
    asio::post(strand_, [self = shared_from_this()]() {
        asio::error_code ec;
        self->socket_.close(ec);
    });
}

void TcpSession::read_header() {
    auto self = shared_from_this();
    asio::async_read(
        socket_,
        asio::buffer(&read_payload_len_, sizeof(uint32_t)),
        asio::bind_executor(strand_, [this, self](const asio::error_code& ec, size_t /*length*/) {
            if (ec) {
                return;
            }

            if (read_payload_len_ == 0 || read_payload_len_ > 64 * 1024 * 1024) {
                stop();
                return;
            }

            read_payload(read_payload_len_);
        })
    );
}

void TcpSession::read_payload(uint32_t payload_len) {
    auto self = shared_from_this();
    read_buffer_.resize(payload_len);

    asio::async_read(
        socket_,
        asio::buffer(read_buffer_.data(), payload_len),
        asio::bind_executor(strand_, [this, self](const asio::error_code& ec, size_t /*length*/) {
            if (ec) {
                return;
            }

            auto req = Protocol::deserialize_request(read_buffer_.data(), read_buffer_.size());
            if (!req) {
                queue_response(Response::error("Malformed request"));
            } else {
                handle_request(*req);
            }

            read_header();
        })
    );
}

void TcpSession::handle_request(const Request& req) {
    switch (req.op) {
        case OpCode::PING:
            queue_response(Response::ok("PONG", "PONG"));
            break;

        case OpCode::SET:
            engine_.set(req.key, req.value);
            queue_response(Response::ok("", "OK"));
            break;

        case OpCode::GET: {
            auto val = engine_.get(req.key);
            if (val) {
                queue_response(Response::ok(*val, "OK"));
            } else {
                queue_response(Response::not_found("Key not found"));
            }
            break;
        }

        case OpCode::DEL: {
            if (engine_.del(req.key)) {
                queue_response(Response::ok("", "OK"));
            } else {
                queue_response(Response::not_found("Key not found"));
            }
            break;
        }

        case OpCode::STATS: {
            queue_response(Response::ok(std::to_string(engine_.size()), "OK"));
            break;
        }

        default:
            queue_response(Response::error("Unknown opcode"));
            break;
    }
}

void TcpSession::queue_response(const Response& res) {
    auto data = Protocol::serialize_response(res);
    asio::post(strand_, [this, self = shared_from_this(), data = std::move(data)]() mutable {
        bool write_in_progress = !write_queue_.empty();
        write_queue_.push_back(std::move(data));
        if (!write_in_progress) {
            do_write();
        }
    });
}

void TcpSession::do_write() {
    auto self = shared_from_this();
    asio::async_write(
        socket_,
        asio::buffer(write_queue_.front()),
        asio::bind_executor(strand_, [this, self](const asio::error_code& ec, size_t /*length*/) {
            if (ec) {
                stop();
                return;
            }

            write_queue_.pop_front();
            if (!write_queue_.empty()) {
                do_write();
            }
        })
    );
}

} // namespace distributed_kv