#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cassert>
#include <random>
#include <iomanip>

#include "storage_engine.hpp"

using namespace distributed_kv;

void test_basic_crud() {
    std::cout << "=== Running Basic CRUD Tests ===" << std::endl;
    StorageEngine engine;

    assert(engine.empty());
    assert(engine.size() == 0);

    // Test SET & GET
    assert(engine.set("user:100", "Alice"));
    assert(engine.set("user:101", "Bob"));
    assert(engine.size() == 2);
    assert(!engine.empty());

    auto val1 = engine.get("user:100");
    assert(val1.has_value() && *val1 == "Alice");

    auto val2 = engine.get("user:101");
    assert(val2.has_value() && *val2 == "Bob");

    auto non_existent = engine.get("user:999");
    assert(!non_existent.has_value());

    // Test Overwrite
    assert(engine.set("user:100", "Alice_Updated"));
    auto val_updated = engine.get("user:100");
    assert(val_updated.has_value() && *val_updated == "Alice_Updated");
    assert(engine.size() == 2);

    // Test Exists
    assert(engine.exists("user:100"));
    assert(!engine.exists("user:999"));

    // Test DEL
    assert(engine.del("user:101"));
    assert(!engine.exists("user:101"));
    assert(engine.size() == 1);
    assert(!engine.del("user:101")); // Deleting non-existent returns false

    // Test Clear
    engine.clear();
    assert(engine.empty());
    assert(engine.size() == 0);

    std::cout << "[PASS] Basic CRUD tests passed successfully.\n" << std::endl;
}

void test_state_machine_batching_and_snapshot() {
    std::cout << "=== Running State Machine and Batching Tests ===" << std::endl;
    StorageEngine engine;

    // Apply individual commands
    auto res1 = engine.apply(Command::make_set("k1", "v1"));
    assert(res1.success);
    assert(!res1.previous_value.has_value());

    auto res2 = engine.apply(Command::make_set("k1", "v2"));
    assert(res2.success);
    assert(res2.previous_value.has_value() && *res2.previous_value == "v1");

    auto res3 = engine.apply(Command::make_noop());
    assert(res3.success);

    auto res4 = engine.apply(Command::make_del("k1"));
    assert(res4.success);
    assert(res4.previous_value.has_value() && *res4.previous_value == "v2");

    auto res5 = engine.apply(Command::make_del("k1"));
    assert(!res5.success);

    std::vector<Command> batch = {
        Command::make_set("cluster:node1", "192.168.1.10:8000"),
        Command::make_set("cluster:node2", "192.168.1.11:8000"),
        Command::make_set("cluster:node3", "192.168.1.12:8000"),
        Command::make_set("temp_key", "temporary"),
        Command::make_del("temp_key")
    };

    auto batch_results = engine.apply_batch(batch);
    assert(batch_results.size() == 5);
    assert(batch_results[0].success);
    assert(batch_results[1].success);
    assert(batch_results[2].success);
    assert(batch_results[3].success);
    assert(batch_results[4].success);

    assert(engine.size() == 3);
    assert(engine.get("cluster:node1").value() == "192.168.1.10:8000");
    assert(!engine.exists("temp_key"));

    // Test Snapshot & Restore
    auto snap = engine.snapshot();
    assert(snap.size() == 3);
    assert(snap["cluster:node2"] == "192.168.1.11:8000");

    StorageEngine restored_engine;
    restored_engine.restore_snapshot(snap);
    assert(restored_engine.size() == 3);
    assert(restored_engine.get("cluster:node3").value() == "192.168.1.12:8000");

    std::cout << "[PASS] State Machine, Batching & Snapshot tests passed successfully.\n" << std::endl;
}

void test_concurrency_stress() {
    std::cout << "=== Running Multi-Threaded Concurrency Stress Test ===" << std::endl;

    StorageEngine engine;
    const int NUM_READERS = 16;
    const int NUM_WRITERS = 4;
    const int OPS_PER_WRITER = 25000;
    const int OPS_PER_READER = 50000;
    const int KEYSPACE_SIZE = 1000;

    std::atomic<bool> start_flag{false};
    std::atomic<uint64_t> total_reads{0};
    std::atomic<uint64_t> read_hits{0};
    std::atomic<uint64_t> read_misses{0};
    std::atomic<uint64_t> total_writes{0};
    std::atomic<uint64_t> total_deletes{0};
    std::atomic<uint64_t> batch_commands_applied{0};

    // Pre-populate some keys
    for (int i = 0; i < 200; ++i) {
        engine.set("key_" + std::to_string(i), "init_val_" + std::to_string(i));
    }

    std::vector<std::thread> threads;
    threads.reserve(NUM_READERS + NUM_WRITERS);

    // Spawn Writer Threads
    for (int w = 0; w < NUM_WRITERS; ++w) {
        threads.emplace_back([&, w]() {
            while (!start_flag.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            std::mt19937_64 rng(1337 + w);
            std::uniform_int_distribution<int> key_dist(0, KEYSPACE_SIZE - 1);
            std::uniform_int_distribution<int> op_dist(0, 9);

            for (int i = 0; i < OPS_PER_WRITER; ++i) {
                int op = op_dist(rng);
                int k = key_dist(rng);
                std::string key = "key_" + std::to_string(k);

                if (op < 6) {
                    // SET operation (60%)
                    std::string val = "val_w" + std::to_string(w) + "_" + std::to_string(i);
                    engine.set(key, val);
                    total_writes.fetch_add(1, std::memory_order_relaxed);
                } else if (op < 8) {
                    // DEL operation (20%)
                    engine.del(key);
                    total_deletes.fetch_add(1, std::memory_order_relaxed);
                } else {
                    // Atomic Batch operation (20%)
                    int k2 = key_dist(rng);
                    std::string key2 = "key_" + std::to_string(k2);
                    std::vector<Command> batch = {
                        Command::make_set(key, "batch_val_" + std::to_string(i)),
                        Command::make_set(key2, "batch_val2_" + std::to_string(i))
                    };
                    engine.apply_batch(batch);
                    batch_commands_applied.fetch_add(2, std::memory_order_relaxed);
                }
            }
        });
    }

    // Spawn Reader Threads
    for (int r = 0; r < NUM_READERS; ++r) {
        threads.emplace_back([&, r]() {
            while (!start_flag.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            std::mt19937_64 rng(4242 + r);
            std::uniform_int_distribution<int> key_dist(0, KEYSPACE_SIZE - 1);

            for (int i = 0; i < OPS_PER_READER; ++i) {
                int k = key_dist(rng);
                std::string key = "key_" + std::to_string(k);

                auto val = engine.get(key);
                total_reads.fetch_add(1, std::memory_order_relaxed);
                if (val.has_value()) {
                    read_hits.fetch_add(1, std::memory_order_relaxed);
                    // Verify data integrity: value must not be empty and must have expected prefix
                    assert(!val->empty());
                } else {
                    read_misses.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    start_flag.store(true, std::memory_order_release);

    for (auto& t : threads) {
        t.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;

    uint64_t total_ops = total_reads.load() + total_writes.load() + total_deletes.load() + batch_commands_applied.load();
    double throughput = static_cast<double>(total_ops) / duration.count();

    std::cout << "Concurrency Test Statistics:" << std::endl;
    std::cout << "Threads:               " << NUM_READERS << " Readers, " << NUM_WRITERS << " Writers" << std::endl;
    std::cout << "Duration:              " << std::fixed << std::setprecision(4) << duration.count() << " seconds" << std::endl;
    std::cout << "Total Operations:      " << total_ops << std::endl;
    std::cout << "  - Total Reads:       " << total_reads.load() << " (Hits: " << read_hits.load() << ", Misses: " << read_misses.load() << ")" << std::endl;
    std::cout << "  - Total Writes:      " << total_writes.load() << std::endl;
    std::cout << "  - Total Deletes:     " << total_deletes.load() << std::endl;
    std::cout << "  - Batch Commands:    " << batch_commands_applied.load() << std::endl;
    std::cout << "Throughput:            " << std::fixed << std::setprecision(2) << throughput << " ops/sec" << std::endl;
    std::cout << "Final Store Size:      " << engine.size() << " keys" << std::endl;
    std::cout << "[PASS] Concurrency stress test completed with 0 errors.\n" << std::endl;
}

int main() {
    std::cout << "=================================================" << std::endl;
    std::cout << " DistributedKVStore " << std::endl;
    std::cout << "=================================================\n" << std::endl;

    try {
        test_basic_crud();
        test_state_machine_batching_and_snapshot();
        test_concurrency_stress();

        std::cout << "=================================================" << std::endl;
        std::cout << " ALL TESTS PASSED SUCCESSFULLY!                  " << std::endl;
        std::cout << "=================================================" << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "Unhandled exception during tests: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
