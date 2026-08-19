#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <thread>
#include <asio.hpp>

#include "storage_engine.hpp"
#include "wal.hpp"
#include "tcp_server.hpp"
#include "raft_node.hpp"
#include "raft_types.hpp"

using namespace distributed_kv;

static void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  --id <node_id>            Node ID (default: node_1)\n"
              << "  --host <bind_host>        Bind IP (default: 127.0.0.1)\n"
              << "  --port <client_port>      Client TCP Port (default: 6379)\n"
              << "  --raft-port <raft_port>   Raft Consensus Port (default: 7001)\n"
              << "  --peers <id=ip:port,...>  Comma-separated peer cluster list\n"
              << "  --wal <path>              Path to Write-Ahead Log (optional)\n"
              << "  --threads <count>         Worker thread count (default: auto)\n"
              << "  --help                    Show help message\n";
}

int main(int argc, char* argv[]) {
    std::string node_id = "node_1";
    std::string host = "127.0.0.1";
    uint16_t client_port = 6379;
    uint16_t raft_port = 7001;
    std::string peers_str = "";
    std::string wal_path = "";
    unsigned int num_threads = std::max(2u, std::thread::hardware_concurrency());

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--id" && i + 1 < argc) {
            node_id = argv[++i];
        } else if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            client_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--raft-port" && i + 1 < argc) {
            raft_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--peers" && i + 1 < argc) {
            peers_str = argv[++i];
        } else if (arg == "--wal" && i + 1 < argc) {
            wal_path = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = static_cast<unsigned int>(std::stoi(argv[++i]));
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }

    std::vector<PeerConfig> peers;
    if (!peers_str.empty()) {
        std::stringstream ss(peers_str);
        std::string item;
        while (std::getline(ss, item, ',')) {
            size_t eq = item.find('=');
            size_t col = item.find(':', eq);
            if (eq != std::string::npos && col != std::string::npos) {
                PeerConfig peer;
                peer.id = item.substr(0, eq);
                peer.host = item.substr(eq + 1, col - eq - 1);
                peer.port = static_cast<uint16_t>(std::stoi(item.substr(col + 1)));
                peers.push_back(std::move(peer));
            }
        }
    }

    std::cout << "======================================================\n"
              << " DistributedKVStore Server Node                       \n"
              << "======================================================\n"
              << "Node ID:      " << node_id << "\n"
              << "Host:         " << host << "\n"
              << "Client Port:  " << client_port << "\n"
              << "Raft Port:    " << raft_port << "\n"
              << "Peers:        " << (peers.empty() ? "(standalone)" : peers_str) << "\n"
              << "WAL Path:     " << (wal_path.empty() ? "(disabled)" : wal_path) << "\n"
              << "Workers:      " << num_threads << "\n"
              << "======================================================\n"
              << std::endl;

    try {
        asio::io_context ioc;

        std::shared_ptr<WAL> wal = nullptr;
        if (!wal_path.empty()) {
            wal = std::make_shared<WAL>(wal_path);
        }

        StorageEngine engine(wal);
        if (wal) {
            size_t recovered = wal->recover(engine);
            std::cout << "[WAL] Replayed " << recovered << " operations from log on startup." << std::endl;
        }

        auto raft = std::make_shared<RaftNode>(node_id, host, raft_port, peers, ioc, engine);
        raft->start();

        TcpServer server(ioc, client_port, engine, raft);
        server.start();

        std::cout << "[Server] Ready and listening for client connections on " << host << ":" << client_port << std::endl;

        asio::signal_set signals(ioc, SIGINT, SIGTERM);
        signals.async_wait([&](const asio::error_code&, int sig) {
            std::cout << "\n[Signal " << sig << " received] Initiating graceful shutdown..." << std::endl;
            server.stop();
            raft->stop();
            ioc.stop();
        });

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (unsigned int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&ioc]() {
                ioc.run();
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        std::cout << "[Server] Shutdown complete." << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "Fatal Error: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}