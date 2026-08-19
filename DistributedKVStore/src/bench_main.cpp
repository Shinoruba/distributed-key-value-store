#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <iomanip>
#include <algorithm>
#include <sstream>
#include <numeric>
#include <asio.hpp>

#include "protocol.hpp"

using namespace distributed_kv;

static const char* COLOR_RESET   = "\033[0m";
static const char* COLOR_GREEN   = "\033[32m";
static const char* COLOR_RED     = "\033[31m";
static const char* COLOR_YELLOW  = "\033[33m";
static const char* COLOR_CYAN    = "\033[36m";
static const char* COLOR_BOLD    = "\033[1m";

static void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  --host <ip>             Server Host (default: 127.0.0.1)\n"
              << "  --port <port>           Server Port (default: 6379)\n"
              << "  --clients <count>       Concurrent client threads (default: 8)\n"
              << "  --requests <count>      Total requests to execute (default: 50000)\n"
              << "  --keyspace <count>      Number of distinct keys (default: 10000)\n"
              << "  --ratio <set:get>       Write-to-read ratio (default: 1:1, e.g. 1:9)\n"
              << "  --val-size <bytes>      Value payload size in bytes (default: 64)\n"
              << "  --help                  Show help message\n";
}

struct ClientStats {
    uint64_t total_requests{0};
    uint64_t successful_ops{0};
    uint64_t redirected_ops{0};
    uint64_t error_ops{0};
    std::vector<uint32_t> latencies_us;
};

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 6379;
    unsigned int num_clients = 8;
    uint64_t total_requests = 50000;
    uint64_t keyspace_size = 10000;
    std::string ratio_str = "1:1";
    size_t val_size = 64;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--clients" && i + 1 < argc) {
            num_clients = static_cast<unsigned int>(std::stoul(argv[++i]));
        } else if (arg == "--requests" && i + 1 < argc) {
            total_requests = std::stoull(argv[++i]);
        } else if (arg == "--keyspace" && i + 1 < argc) {
            keyspace_size = std::stoull(argv[++i]);
        } else if (arg == "--ratio" && i + 1 < argc) {
            ratio_str = argv[++i];
        } else if (arg == "--val-size" && i + 1 < argc) {
            val_size = std::stoul(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }

    int set_ratio = 1;
    int get_ratio = 1;
    size_t colon_pos = ratio_str.find(':');
    if (colon_pos != std::string::npos) {
        set_ratio = std::stoi(ratio_str.substr(0, colon_pos));
        get_ratio = std::stoi(ratio_str.substr(colon_pos + 1));
    }
    int total_ratio = set_ratio + get_ratio;
    if (total_ratio <= 0) {
        set_ratio = 1; get_ratio = 1; total_ratio = 2;
    }

    std::string sample_value(val_size, 'X');

    std::cout << COLOR_BOLD << "======================================================\n"
              << " DistributedKVStore Benchmark Runner                  \n"
              << "======================================================\n" << COLOR_RESET
              << "Target:        " << COLOR_CYAN << host << ":" << port << COLOR_RESET << "\n"
              << "Clients:       " << num_clients << " threads\n"
              << "Requests:      " << total_requests << "\n"
              << "Keyspace:      " << keyspace_size << " keys\n"
              << "Workload:      " << set_ratio << " SETs / " << get_ratio << " GETs ("
              << std::fixed << std::setprecision(1) << (100.0 * set_ratio / total_ratio) << "% writes)\n"
              << "Value Size:    " << val_size << " bytes\n"
              << "======================================================\n"
              << "Connecting clients..." << std::endl;

    std::vector<ClientStats> client_stats(num_clients);
    std::vector<std::thread> threads;
    threads.reserve(num_clients);

    std::atomic<bool> start_flag{false};
    std::atomic<unsigned int> ready_clients{0};

    uint64_t reqs_per_client = total_requests / num_clients;

    for (unsigned int c = 0; c < num_clients; ++c) {
        threads.emplace_back([&, c, reqs_per_client]() {
            ClientStats& stats = client_stats[c];
            stats.latencies_us.reserve(reqs_per_client);

            try {
                asio::io_context ioc;
                asio::ip::tcp::socket sock(ioc);
                sock.connect(asio::ip::tcp::endpoint(asio::ip::make_address(host), port));

                ready_clients.fetch_add(1, std::memory_order_release);
                while (!start_flag.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                std::mt19937_64 rng(1337 + c);
                std::uniform_int_distribution<uint64_t> key_dist(0, keyspace_size - 1);
                std::uniform_int_distribution<int> op_dist(0, total_ratio - 1);

                for (uint64_t i = 0; i < reqs_per_client; ++i) {
                    uint64_t k = key_dist(rng);
                    std::string key = "bench_k_" + std::to_string(k);
                    bool is_set = (op_dist(rng) < set_ratio);

                    Request req = is_set ? Request::make_set(key, sample_value) : Request::make_get(key);
                    auto req_bytes = Protocol::serialize_request(req);

                    auto t_start = std::chrono::high_resolution_clock::now();
                    asio::write(sock, asio::buffer(req_bytes));

                    uint32_t resp_len = 0;
                    asio::read(sock, asio::buffer(&resp_len, sizeof(uint32_t)));

                    std::vector<uint8_t> payload(resp_len);
                    asio::read(sock, asio::buffer(payload.data(), resp_len));

                    auto t_end = std::chrono::high_resolution_clock::now();
                    uint32_t lat_us = static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count());
                    stats.latencies_us.push_back(lat_us);

                    auto resp = Protocol::deserialize_response(payload.data(), payload.size());
                    if (resp) {
                        if (resp->status == StatusCode::OK || resp->status == StatusCode::NOT_FOUND) {
                            ++stats.successful_ops;
                        } else if (resp->message.rfind("NOT_LEADER:", 0) == 0) {
                            ++stats.redirected_ops;
                        } else {
                            ++stats.error_ops;
                        }
                    } else {
                        ++stats.error_ops;
                    }
                    ++stats.total_requests;
                }

                sock.close();
            } catch (const std::exception&) {
                ++stats.error_ops;
            }
        });
    }

    while (ready_clients.load(std::memory_order_acquire) < num_clients) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::cout << "Starting benchmark..." << std::endl;
    auto bench_start = std::chrono::high_resolution_clock::now();
    start_flag.store(true, std::memory_order_release);

    for (auto& t : threads) {
        t.join();
    }
    auto bench_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> total_duration = bench_end - bench_start;

    std::vector<uint32_t> all_latencies;
    all_latencies.reserve(total_requests);

    uint64_t total_success = 0;
    uint64_t total_redirects = 0;
    uint64_t total_errors = 0;

    for (const auto& s : client_stats) {
        total_success += s.successful_ops;
        total_redirects += s.redirected_ops;
        total_errors += s.error_ops;
        all_latencies.insert(all_latencies.end(), s.latencies_us.begin(), s.latencies_us.end());
    }

    if (all_latencies.empty()) {
        std::cerr << COLOR_RED << "Benchmark failed: no operations completed." << COLOR_RESET << std::endl;
        return 1;
    }

    std::sort(all_latencies.begin(), all_latencies.end());

    double qps = static_cast<double>(all_latencies.size()) / total_duration.count();
    uint32_t min_lat = all_latencies.front();
    uint32_t max_lat = all_latencies.back();
    double avg_lat = std::accumulate(all_latencies.begin(), all_latencies.end(), 0.0) / all_latencies.size();

    auto get_p = [&](double p) -> uint32_t {
        size_t idx = static_cast<size_t>(p * all_latencies.size() / 100.0);
        if (idx >= all_latencies.size()) idx = all_latencies.size() - 1;
        return all_latencies[idx];
    };

    uint32_t p50 = get_p(50.0);
    uint32_t p90 = get_p(90.0);
    uint32_t p95 = get_p(95.0);
    uint32_t p99 = get_p(99.0);
    uint32_t p999 = get_p(99.9);

    std::cout << "\n" << COLOR_BOLD << "======================================================\n"
              << " Benchmark Results                                    \n"
              << "======================================================\n" << COLOR_RESET
              << "Total Duration:       " << std::fixed << std::setprecision(4) << total_duration.count() << " seconds\n"
              << "Total Requests:       " << all_latencies.size() << "\n"
              << "Throughput:           " << COLOR_GREEN << COLOR_BOLD << std::fixed << std::setprecision(2) << qps << " ops/sec" << COLOR_RESET << "\n"
              << "Successful Ops:       " << COLOR_GREEN << total_success << COLOR_RESET << "\n"
              << "Redirects:            " << (total_redirects > 0 ? COLOR_YELLOW : COLOR_RESET) << total_redirects << COLOR_RESET << "\n"
              << "Errors:               " << (total_errors > 0 ? COLOR_RED : COLOR_RESET) << total_errors << COLOR_RESET << "\n"
              << "------------------------------------------------------\n"
              << "Latency Distribution:\n"
              << "  Min:                " << min_lat << " 탎\n"
              << "  Avg:                " << std::fixed << std::setprecision(1) << avg_lat << " 탎\n"
              << "  p50 (Median):       " << p50 << " 탎\n"
              << "  p90:                " << p90 << " 탎\n"
              << "  p95:                " << p95 << " 탎\n"
              << "  p99:                " << p99 << " 탎\n"
              << "  p99.9:              " << p999 << " 탎\n"
              << "  Max:                " << max_lat << " 탎\n"
              << "======================================================\n" << std::endl;

    return 0;
}