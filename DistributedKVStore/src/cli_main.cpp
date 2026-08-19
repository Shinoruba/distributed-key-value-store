#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <memory>
#include <asio.hpp>

#include "protocol.hpp"

using namespace distributed_kv;

static const char* COLOR_RESET   = "\033[0m";
static const char* COLOR_GREEN   = "\033[32m";
static const char* COLOR_RED     = "\033[31m";
static const char* COLOR_YELLOW  = "\033[33m";
static const char* COLOR_CYAN    = "\033[36m";
static const char* COLOR_BOLD    = "\033[1m";
static const char* COLOR_MAGENTA = "\033[35m";

static void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options] [command [args...]]\n"
              << "Options:\n"
              << "  --host <ip>        Server Host (default: 127.0.0.1)\n"
              << "  --port <port>      Server Port (default: 6379)\n"
              << "  --help             Show help message\n\n"
              << "Examples:\n"
              << "  " << prog << "                             # Starts interactive REPL\n"
              << "  " << prog << " SET mykey hello_world      # One-shot command\n"
              << "  " << prog << " GET mykey\n";
}

struct ExecutionResult {
    bool success{false};
    Response response;
    double latency_us{0.0};
    std::string error_message;
};

static ExecutionResult send_command(asio::ip::tcp::socket& sock, const Request& req) {
    ExecutionResult result;
    auto start = std::chrono::high_resolution_clock::now();

    try {
        auto wire_bytes = Protocol::serialize_request(req);
        asio::write(sock, asio::buffer(wire_bytes));

        uint32_t payload_len = 0;
        asio::read(sock, asio::buffer(&payload_len, sizeof(uint32_t)));

        if (payload_len == 0 || payload_len > 64 * 1024 * 1024) {
            result.error_message = "Invalid response length from server";
            return result;
        }

        std::vector<uint8_t> payload(payload_len);
        asio::read(sock, asio::buffer(payload.data(), payload_len));

        auto resp = Protocol::deserialize_response(payload.data(), payload.size());
        if (!resp) {
            result.error_message = "Failed to deserialize server response";
            return result;
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::micro> elapsed = end - start;

        result.success = true;
        result.response = *resp;
        result.latency_us = elapsed.count();
    } catch (const std::exception& ex) {
        result.error_message = ex.what();
    }

    return result;
}

static void print_result(const ExecutionResult& res) {
    if (!res.success) {
        std::cout << COLOR_RED << "(error) " << res.error_message << COLOR_RESET << std::endl;
        return;
    }

    std::string latency_str;
    if (res.latency_us >= 1000.0) {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2) << (res.latency_us / 1000.0) << " ms";
        latency_str = ss.str();
    } else {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(1) << res.latency_us << " \xC2\xB5s";
        latency_str = ss.str();
    }

    if (res.response.status == StatusCode::OK) {
        if (!res.response.value.empty()) {
            std::cout << COLOR_GREEN << "\"" << res.response.value << "\"" << COLOR_RESET;
        } else {
            std::cout << COLOR_GREEN << "OK" << COLOR_RESET;
        }
    } else if (res.response.status == StatusCode::NOT_FOUND) {
        std::cout << COLOR_YELLOW << "(nil)" << COLOR_RESET;
    } else {
        if (res.response.message.rfind("NOT_LEADER:", 0) == 0) {
            std::string leader_id = res.response.message.substr(11);
            std::cout << COLOR_MAGENTA << "(redirect) Not cluster leader. Current leader: "
                      << (leader_id.empty() ? "electing..." : leader_id) << COLOR_RESET;
        } else {
            std::cout << COLOR_RED << "(error) " << res.response.message << COLOR_RESET;
        }
    }

    std::cout << "  " << COLOR_CYAN << "[" << latency_str << "]" << COLOR_RESET << std::endl;
}

static std::vector<std::string> tokenize_line(const std::string& line) {
    std::vector<std::string> tokens;
    std::stringstream ss(line);
    std::string token;
    while (ss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 6379;
    std::vector<std::string> cmd_args;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else {
            cmd_args.push_back(arg);
        }
    }

    asio::io_context ioc;
    asio::ip::tcp::socket socket(ioc);

    auto connect_socket = [&]() -> bool {
        try {
            if (socket.is_open()) {
                asio::error_code ec;
                socket.close(ec);
            }
            socket.connect(asio::ip::tcp::endpoint(asio::ip::make_address(host), port));
            return true;
        } catch (const std::exception&) {
            return false;
        }
    };

    if (!connect_socket()) {
        std::cerr << COLOR_RED << "Could not connect to DistributedKVStore at " << host << ":" << port << COLOR_RESET << std::endl;
        return 1;
    }

    // One-shot command mode
    if (!cmd_args.empty()) {
        std::string op = cmd_args[0];
        for (auto& c : op) c = static_cast<char>(std::toupper(c));

        if (op == "PING") {
            print_result(send_command(socket, Request::make_ping()));
        } else if (op == "SET" && cmd_args.size() >= 3) {
            std::string val;
            for (size_t i = 2; i < cmd_args.size(); ++i) {
                if (i > 2) val += " ";
                val += cmd_args[i];
            }
            print_result(send_command(socket, Request::make_set(cmd_args[1], val)));
        } else if (op == "GET" && cmd_args.size() >= 2) {
            print_result(send_command(socket, Request::make_get(cmd_args[1])));
        } else if (op == "DEL" && cmd_args.size() >= 2) {
            print_result(send_command(socket, Request::make_del(cmd_args[1])));
        } else if (op == "STATS") {
            print_result(send_command(socket, Request::make_stats()));
        } else {
            std::cerr << COLOR_RED << "Unknown command or invalid arguments." << COLOR_RESET << std::endl;
            return 1;
        }
        return 0;
    }

    // Interactive REPL Mode
    std::cout << COLOR_BOLD << "======================================================\n"
              << " DistributedKVStore Interactive CLI                   \n"
              << "======================================================\n" << COLOR_RESET
              << "Connected to " << COLOR_CYAN << host << ":" << port << COLOR_RESET << "\n"
              << "Type " << COLOR_BOLD << "HELP" << COLOR_RESET << " for available commands or "
              << COLOR_BOLD << "EXIT" << COLOR_RESET << " to quit.\n" << std::endl;

    std::string line;
    while (true) {
        std::cout << COLOR_CYAN << host << ":" << port << "> " << COLOR_RESET;
        if (!std::getline(std::cin, line)) {
            break;
        }

        auto tokens = tokenize_line(line);
        if (tokens.empty()) continue;

        std::string cmd = tokens[0];
        for (auto& c : cmd) c = static_cast<char>(std::toupper(c));

        if (cmd == "EXIT" || cmd == "QUIT") {
            break;
        }

        if (cmd == "HELP") {
            std::cout << COLOR_BOLD << "Commands:\n" << COLOR_RESET
                      << "  PING                   - Check server liveness\n"
                      << "  SET <key> <value>      - Set key to value (Raft replicated)\n"
                      << "  GET <key>              - Retrieve value by key\n"
                      << "  DEL <key>              - Delete key (Raft replicated)\n"
                      << "  STATS                  - Show total stored keys\n"
                      << "  EXIT / QUIT            - Exit CLI\n";
            continue;
        }

        if (cmd == "PING") {
            print_result(send_command(socket, Request::make_ping()));
        } else if (cmd == "SET") {
            if (tokens.size() < 3) {
                std::cout << COLOR_RED << "(error) Syntax: SET <key> <value>" << COLOR_RESET << std::endl;
                continue;
            }
            std::string val;
            for (size_t i = 2; i < tokens.size(); ++i) {
                if (i > 2) val += " ";
                val += tokens[i];
            }
            print_result(send_command(socket, Request::make_set(tokens[1], val)));
        } else if (cmd == "GET") {
            if (tokens.size() < 2) {
                std::cout << COLOR_RED << "(error) Syntax: GET <key>" << COLOR_RESET << std::endl;
                continue;
            }
            print_result(send_command(socket, Request::make_get(tokens[1])));
        } else if (cmd == "DEL") {
            if (tokens.size() < 2) {
                std::cout << COLOR_RED << "(error) Syntax: DEL <key>" << COLOR_RESET << std::endl;
                continue;
            }
            print_result(send_command(socket, Request::make_del(tokens[1])));
        } else if (cmd == "STATS") {
            print_result(send_command(socket, Request::make_stats()));
        } else {
            std::cout << COLOR_RED << "(error) Unknown command '" << tokens[0] << "'. Type HELP." << COLOR_RESET << std::endl;
        }
    }

    std::cout << "Bye!" << std::endl;
    return 0;
}