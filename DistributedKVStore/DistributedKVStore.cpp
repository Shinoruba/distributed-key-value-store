#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cassert>
#include <random>
#include <iomanip>
#include <filesystem>
#include <fstream>
#include <memory>
#include <asio.hpp>

#include "storage_engine.hpp"
#include "wal.hpp"
#include "protocol.hpp"
#include "tcp_server.hpp"
#include "raft_node.hpp"
#include "raft_rpc.hpp"

using namespace distributed_kv;

void test_basic_crud() {
    std::cout << "=== Running Basic CRUD Tests ===" << std::endl;
    StorageEngine engine;

    assert(engine.empty());
    assert(engine.size() == 0);

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

    assert(engine.set("user:100", "Alice_Updated"));
    auto val_updated = engine.get("user:100");
    assert(val_updated.has_value() && *val_updated == "Alice_Updated");
    assert(engine.size() == 2);

    assert(engine.exists("user:100"));
    assert(!engine.exists("user:999"));

    assert(engine.del("user:101"));
    assert(!engine.exists("user:101"));
    assert(engine.size() == 1);
    assert(!engine.del("user:101"));

    engine.clear();
    assert(engine.empty());
    assert(engine.size() == 0);

    std::cout << "[PASS] Basic CRUD tests passed successfully.\n" << std::endl;
}

void test_state_machine_batching_and_snapshot() {
    std::cout << "=== Running State Machine & Batching Tests ===" << std::endl;
    StorageEngine engine;

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

    auto snap = engine.snapshot();
    assert(snap.size() == 3);
    assert(snap["cluster:node2"] == "192.168.1.11:8000");

    StorageEngine restored_engine;
    restored_engine.restore_snapshot(snap);
    assert(restored_engine.size() == 3);
    assert(restored_engine.get("cluster:node3").value() == "192.168.1.12:8000");

    std::cout << "[PASS] State Machine, Batching & Snapshot tests passed successfully.\n" << std::endl;
}

void test_wal_durability_and_recovery() {
    std::cout << "=== Running WAL Durability & Crash Recovery Tests ===" << std::endl;
    std::filesystem::path wal_path = "test_data/wal_recovery.log";
    std::filesystem::remove(wal_path);

    std::unordered_map<std::string, std::string> expected_state;

    {
        auto wal = std::make_shared<WAL>(wal_path);
        StorageEngine engine(wal);

        for (int i = 0; i < 50; ++i) {
            std::string k = "user:" + std::to_string(i);
            std::string v = "payload_" + std::to_string(i * 10);
            engine.set(k, v);
            expected_state[k] = v;
        }

        engine.set("user:10", "payload_10_updated");
        expected_state["user:10"] = "payload_10_updated";

        engine.del("user:25");
        expected_state.erase("user:25");

        engine.apply(Command::make_set("cluster:leader", "node_1"));
        expected_state["cluster:leader"] = "node_1";

        std::vector<Command> batch = {
            Command::make_set("batch_k1", "batch_v1"),
            Command::make_set("batch_k2", "batch_v2"),
            Command::make_del("user:30")
        };
        engine.apply_batch(batch);
        expected_state["batch_k1"] = "batch_v1";
        expected_state["batch_k2"] = "batch_v2";
        expected_state.erase("user:30");

        assert(engine.size() == expected_state.size());
    }

    {
        StorageEngine recovered_engine;
        WAL recovery_wal(wal_path);

        size_t replayed = recovery_wal.recover(recovered_engine);
        (void)replayed;
        assert(replayed > 0);
        assert(recovered_engine.size() == expected_state.size());

        for (const auto& [k, v] : expected_state) {
            auto val = recovered_engine.get(k);
            assert(val.has_value());
            assert(*val == v);
        }

        assert(!recovered_engine.exists("user:25"));
        assert(!recovered_engine.exists("user:30"));
    }

    std::cout << "[PASS] WAL Durability and Recovery verified successfully.\n" << std::endl;
}

void test_wal_corruption_and_truncation() {
    std::cout << "=== Running WAL Corruption & Truncation Tests ===" << std::endl;
    std::filesystem::path wal_path = "test_data/wal_corrupt.log";
    std::filesystem::remove(wal_path);

    {
        WAL wal(wal_path);
        wal.append_set("k1", "v1");
        wal.append_set("k2", "v2");
        wal.append_set("k3", "v3");
        wal.close();
    }

    {
        std::ofstream out(wal_path, std::ios::binary | std::ios::app);
        const char garbage[] = { '\xFF', '\x00', '\xDE', '\xAD', '\xBE', '\xEF' };
        out.write(garbage, sizeof(garbage));
    }

    {
        StorageEngine engine;
        WAL recovery_wal(wal_path);
        size_t replayed = recovery_wal.recover(engine);
        (void)replayed;
        assert(replayed == 3);
        assert(engine.size() == 3);
        assert(engine.get("k1").value() == "v1");
        assert(engine.get("k2").value() == "v2");
        assert(engine.get("k3").value() == "v3");
    }

    {
        WAL wal(wal_path);
        assert(wal.size_bytes() > 0);
        wal.truncate();
        assert(wal.size_bytes() == 0);

        wal.append_set("fresh_key", "fresh_value");
        StorageEngine engine;
        size_t replayed = wal.recover(engine);
        (void)replayed;
        assert(replayed == 1);
        assert(engine.get("fresh_key").value() == "fresh_value");
    }

    std::filesystem::remove_all("test_data");
    std::cout << "[PASS] WAL Corruption handling and Truncation verified successfully.\n" << std::endl;
}

void test_protocol_serialization() {
    std::cout << "=== Running Protocol Wire Serialization Tests ===" << std::endl;

    auto ping_req = Request::make_ping();
    auto ping_bytes = Protocol::serialize_request(ping_req);
    auto ping_parsed = Protocol::deserialize_request(ping_bytes.data() + 4, ping_bytes.size() - 4);
    assert(ping_parsed.has_value());
    assert(ping_parsed->op == OpCode::PING);

    auto set_req = Request::make_set("user:key_99", "custom_binary_payload");
    auto set_bytes = Protocol::serialize_request(set_req);
    auto set_parsed = Protocol::deserialize_request(set_bytes.data() + 4, set_bytes.size() - 4);
    assert(set_parsed.has_value());
    assert(set_parsed->op == OpCode::SET);
    assert(set_parsed->key == "user:key_99");
    assert(set_parsed->value == "custom_binary_payload");

    auto get_req = Request::make_get("my_key");
    auto get_bytes = Protocol::serialize_request(get_req);
    auto get_parsed = Protocol::deserialize_request(get_bytes.data() + 4, get_bytes.size() - 4);
    assert(get_parsed.has_value() && get_parsed->op == OpCode::GET && get_parsed->key == "my_key");

    auto res_ok = Response::ok("hello_world", "OK");
    auto res_ok_bytes = Protocol::serialize_response(res_ok);
    auto res_ok_parsed = Protocol::deserialize_response(res_ok_bytes.data() + 4, res_ok_bytes.size() - 4);
    assert(res_ok_parsed.has_value());
    assert(res_ok_parsed->status == StatusCode::OK);
    assert(res_ok_parsed->value == "hello_world");
    assert(res_ok_parsed->message == "OK");

    auto res_nf = Response::not_found("No such item");
    auto res_nf_bytes = Protocol::serialize_response(res_nf);
    auto res_nf_parsed = Protocol::deserialize_response(res_nf_bytes.data() + 4, res_nf_bytes.size() - 4);
    assert(res_nf_parsed.has_value());
    assert(res_nf_parsed->status == StatusCode::NOT_FOUND);
    assert(res_nf_parsed->message == "No such item");

    std::cout << "[PASS] Protocol serialization & deserialization verified.\n" << std::endl;
}

static Response send_request_sync(asio::ip::tcp::socket& sock, const Request& req) {
    auto req_data = Protocol::serialize_request(req);
    asio::write(sock, asio::buffer(req_data));

    uint32_t payload_len = 0;
    asio::read(sock, asio::buffer(&payload_len, sizeof(uint32_t)));

    std::vector<uint8_t> payload(payload_len);
    asio::read(sock, asio::buffer(payload.data(), payload_len));

    auto res = Protocol::deserialize_response(payload.data(), payload.size());
    assert(res.has_value());
    return *res;
}

void test_tcp_server_async_multiclient() {
    std::cout << "=== Running Async TCP Server Multi-Client Integration Test ===" << std::endl;

    StorageEngine engine;
    asio::io_context ioc;

    TcpServer server(ioc, 0, engine);
    server.start();

    uint16_t port = server.port();
    assert(port != 0);

    const int NUM_WORKER_THREADS = 4;
    std::vector<std::thread> workers;
    workers.reserve(NUM_WORKER_THREADS);
    for (int i = 0; i < NUM_WORKER_THREADS; ++i) {
        workers.emplace_back([&ioc]() {
            ioc.run();
        });
    }

    const int NUM_CLIENTS = 8;
    const int REQUESTS_PER_CLIENT = 200;
    std::atomic<uint64_t> successful_ops{0};
    std::vector<std::thread> client_threads;
    client_threads.reserve(NUM_CLIENTS);

    for (int c = 0; c < NUM_CLIENTS; ++c) {
        client_threads.emplace_back([&, c, port]() {
            try {
                asio::io_context client_ioc;
                asio::ip::tcp::socket sock(client_ioc);
                sock.connect(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));

                auto ping_res = send_request_sync(sock, Request::make_ping());
                assert(ping_res.status == StatusCode::OK);
                assert(ping_res.value == "PONG");
                successful_ops.fetch_add(1, std::memory_order_relaxed);

                for (int i = 0; i < REQUESTS_PER_CLIENT; ++i) {
                    std::string key = "client_" + std::to_string(c) + "_key_" + std::to_string(i);
                    std::string val = "val_" + std::to_string(c) + "_" + std::to_string(i);

                    auto set_res = send_request_sync(sock, Request::make_set(key, val));
                    assert(set_res.status == StatusCode::OK);
                    successful_ops.fetch_add(1, std::memory_order_relaxed);

                    auto get_res = send_request_sync(sock, Request::make_get(key));
                    assert(get_res.status == StatusCode::OK);
                    assert(get_res.value == val);
                    successful_ops.fetch_add(1, std::memory_order_relaxed);

                    if (i % 2 == 0) {
                        auto del_res = send_request_sync(sock, Request::make_del(key));
                        assert(del_res.status == StatusCode::OK);
                        successful_ops.fetch_add(1, std::memory_order_relaxed);

                        auto get_after_del = send_request_sync(sock, Request::make_get(key));
                        assert(get_after_del.status == StatusCode::NOT_FOUND);
                        successful_ops.fetch_add(1, std::memory_order_relaxed);
                    }
                }

                auto stats_res = send_request_sync(sock, Request::make_stats());
                assert(stats_res.status == StatusCode::OK);
                successful_ops.fetch_add(1, std::memory_order_relaxed);

                sock.close();
            } catch (const std::exception& ex) {
                std::cerr << "Client error: " << ex.what() << std::endl;
                assert(false);
            }
        });
    }

    for (auto& t : client_threads) {
        t.join();
    }

    server.stop();
    ioc.stop();

    for (auto& w : workers) {
        w.join();
    }

    std::cout << "[PASS] Async TCP Server Multi-Client integration tests passed.\n" << std::endl;
}

void test_raft_rpc_serialization() {
    std::cout << "=== Running Raft RPC Serialization Tests ===" << std::endl;

    RequestVoteArgs rv_args;
    rv_args.term = 42;
    rv_args.candidate_id = "node_candidate_99";
    rv_args.last_log_index = 1005;
    rv_args.last_log_term = 41;

    auto rv_bytes = RaftRpcSerializer::serialize_request_vote_args(rv_args);
    auto rv_deser = RaftRpcSerializer::deserialize_request_vote_args(rv_bytes.data() + 4, rv_bytes.size() - 4);
    assert(rv_deser.has_value());
    assert(rv_deser->term == 42);
    assert(rv_deser->candidate_id == "node_candidate_99");
    assert(rv_deser->last_log_index == 1005);
    assert(rv_deser->last_log_term == 41);

    RequestVoteReply rv_reply{42, true};
    auto rvr_bytes = RaftRpcSerializer::serialize_request_vote_reply(rv_reply);
    auto rvr_deser = RaftRpcSerializer::deserialize_request_vote_reply(rvr_bytes.data() + 4, rvr_bytes.size() - 4);
    assert(rvr_deser.has_value());
    assert(rvr_deser->term == 42);
    assert(rvr_deser->vote_granted == true);

    AppendEntriesArgs ae_args;
    ae_args.term = 55;
    ae_args.leader_id = "leader_node_1";
    ae_args.prev_log_index = 100;
    ae_args.prev_log_term = 54;
    ae_args.leader_commit = 98;
    ae_args.entries.push_back({55, 101, Command::make_set("k_raft", "v_raft")});

    auto ae_bytes = RaftRpcSerializer::serialize_append_entries_args(ae_args);
    auto ae_deser = RaftRpcSerializer::deserialize_append_entries_args(ae_bytes.data() + 4, ae_bytes.size() - 4);
    assert(ae_deser.has_value());
    assert(ae_deser->term == 55);
    assert(ae_deser->leader_id == "leader_node_1");
    assert(ae_deser->entries.size() == 1);
    assert(ae_deser->entries[0].command.key == "k_raft");
    assert(ae_deser->entries[0].command.value == "v_raft");

    AppendEntriesReply ae_reply{55, true, 101};
    auto aer_bytes = RaftRpcSerializer::serialize_append_entries_reply(ae_reply);
    auto aer_deser = RaftRpcSerializer::deserialize_append_entries_reply(aer_bytes.data() + 4, aer_bytes.size() - 4);
    assert(aer_deser.has_value());
    assert(aer_deser->term == 55);
    assert(aer_deser->success == true);
    assert(aer_deser->match_index == 101);

    std::cout << "[PASS] Raft RPC Serialization tests passed.\n" << std::endl;
}

void test_raft_3node_cluster_leader_election_and_failover() {
    std::cout << "=== Running 3-Node Raft Cluster Leader Election & Failover Test ===" << std::endl;

    asio::io_context ioc;
    StorageEngine engine1, engine2, engine3;

    auto node1 = std::make_shared<RaftNode>("node_1", "127.0.0.1", static_cast<uint16_t>(0), std::vector<PeerConfig>{}, ioc, engine1);
    auto node2 = std::make_shared<RaftNode>("node_2", "127.0.0.1", static_cast<uint16_t>(0), std::vector<PeerConfig>{}, ioc, engine2);
    auto node3 = std::make_shared<RaftNode>("node_3", "127.0.0.1", static_cast<uint16_t>(0), std::vector<PeerConfig>{}, ioc, engine3);

    node1->set_peers({{"node_2", "127.0.0.1", node2->port()}, {"node_3", "127.0.0.1", node3->port()}});
    node2->set_peers({{"node_1", "127.0.0.1", node1->port()}, {"node_3", "127.0.0.1", node3->port()}});
    node3->set_peers({{"node_1", "127.0.0.1", node1->port()}, {"node_2", "127.0.0.1", node2->port()}});

    std::cout << "Cluster Nodes initialized on ports: "
              << "node_1=" << node1->port() << ", node_2=" << node2->port() << ", node_3=" << node3->port() << std::endl;

    node1->start();
    node2->start();
    node3->start();

    const int NUM_WORKER_THREADS = 4;
    std::vector<std::thread> workers;
    workers.reserve(NUM_WORKER_THREADS);
    for (int i = 0; i < NUM_WORKER_THREADS; ++i) {
        workers.emplace_back([&ioc]() {
            ioc.run();
        });
    }

    std::shared_ptr<RaftNode> leader = nullptr;
    for (int retry = 0; retry < 40; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        int leader_count = 0;
        if (node1->is_leader()) { ++leader_count; leader = node1; }
        if (node2->is_leader()) { ++leader_count; leader = node2; }
        if (node3->is_leader()) { ++leader_count; leader = node3; }

        if (leader_count == 1) {
            break;
        }
    }

    assert(leader != nullptr);
    std::cout << "Initial Leader elected: " << leader->node_id() << " (Term: " << leader->current_term() << ")" << std::endl;

    int initial_leaders = (node1->is_leader() ? 1 : 0) + (node2->is_leader() ? 1 : 0) + (node3->is_leader() ? 1 : 0);
    (void)initial_leaders;
    assert(initial_leaders == 1);

    std::string dead_leader_id = leader->node_id();
    std::cout << "Simulating crash of leader " << dead_leader_id << "..." << std::endl;
    leader->stop();

    std::shared_ptr<RaftNode> new_leader = nullptr;
    for (int retry = 0; retry < 50; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        int leader_count = 0;
        if (node1->is_running() && node1->is_leader()) { ++leader_count; new_leader = node1; }
        if (node2->is_running() && node2->is_leader()) { ++leader_count; new_leader = node2; }
        if (node3->is_running() && node3->is_leader()) { ++leader_count; new_leader = node3; }

        if (leader_count == 1 && new_leader->node_id() != dead_leader_id) {
            break;
        }
    }

    assert(new_leader != nullptr);
    assert(new_leader->node_id() != dead_leader_id);
    std::cout << "New Leader successfully elected after failover: " << new_leader->node_id()
              << " (Term: " << new_leader->current_term() << ")" << std::endl;

    node1->stop();
    node2->stop();
    node3->stop();
    ioc.stop();

    for (auto& w : workers) {
        w.join();
    }

    std::cout << "[PASS] 3-Node Raft Cluster Leader Election & Failover tests passed.\n" << std::endl;
}

void test_raft_log_replication_and_catchup() {
    std::cout << "=== Running Raft Log Replication, Quorum Commit & Catch-Up Test ===" << std::endl;

    asio::io_context ioc;
    StorageEngine engine1, engine2, engine3;

    auto node1 = std::make_shared<RaftNode>("node_1", "127.0.0.1", static_cast<uint16_t>(0), std::vector<PeerConfig>{}, ioc, engine1);
    auto node2 = std::make_shared<RaftNode>("node_2", "127.0.0.1", static_cast<uint16_t>(0), std::vector<PeerConfig>{}, ioc, engine2);
    auto node3 = std::make_shared<RaftNode>("node_3", "127.0.0.1", static_cast<uint16_t>(0), std::vector<PeerConfig>{}, ioc, engine3);

    node1->set_peers({{"node_2", "127.0.0.1", node2->port()}, {"node_3", "127.0.0.1", node3->port()}});
    node2->set_peers({{"node_1", "127.0.0.1", node1->port()}, {"node_3", "127.0.0.1", node3->port()}});
    node3->set_peers({{"node_1", "127.0.0.1", node1->port()}, {"node_2", "127.0.0.1", node2->port()}});

    node1->start();
    node2->start();
    node3->start();

    const int NUM_WORKER_THREADS = 4;
    std::vector<std::thread> workers;
    workers.reserve(NUM_WORKER_THREADS);
    for (int i = 0; i < NUM_WORKER_THREADS; ++i) {
        workers.emplace_back([&ioc]() {
            ioc.run();
        });
    }

    // 1. Wait for leader
    std::shared_ptr<RaftNode> leader = nullptr;
    for (int retry = 0; retry < 40; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (node1->is_leader()) leader = node1;
        else if (node2->is_leader()) leader = node2;
        else if (node3->is_leader()) leader = node3;
        if (leader) break;
    }
    assert(leader != nullptr);
    std::cout << "Leader for replication test: " << leader->node_id() << std::endl;

    // 2. Propose 100 SET commands to leader
    for (int i = 0; i < 100; ++i) {
        std::string key = "replicated_k" + std::to_string(i);
        std::string val = "replicated_v" + std::to_string(i);
        bool ok = leader->propose(Command::make_set(key, val));
        (void)ok;
        assert(ok);
    }
    std::cout << "Successfully proposed and committed 100 commands to leader." << std::endl;

    // Wait for followers to apply commit index
    for (int retry = 0; retry < 40; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (engine1.size() == 100 && engine2.size() == 100 && engine3.size() == 100) {
            break;
        }
    }

    assert(engine1.size() == 100);
    assert(engine2.size() == 100);
    assert(engine3.size() == 100);

    for (int i = 0; i < 100; ++i) {
        std::string key = "replicated_k" + std::to_string(i);
        std::string val = "replicated_v" + std::to_string(i);
        assert(engine1.get(key).value() == val);
        assert(engine2.get(key).value() == val);
        assert(engine3.get(key).value() == val);
    }
    std::cout << "All 3 nodes verified with 100 identical key-value entries." << std::endl;

    // 3. Disconnect one follower (simulate network failure)
    std::shared_ptr<RaftNode> follower_to_partition = nullptr;
    std::shared_ptr<RaftNode> other_follower = nullptr;

    if (node1 != leader) {
        follower_to_partition = node1;
        other_follower = (node2 != leader) ? node2 : node3;
    } else if (node2 != leader) {
        follower_to_partition = node2;
        other_follower = node3;
    } else {
        follower_to_partition = node3;
        other_follower = node2;
    }

    std::cout << "Simulating network partition on follower " << follower_to_partition->node_id() << "..." << std::endl;
    follower_to_partition->stop();

    // 4. Propose 50 more commands to the leader while 1 follower is partitioned (2/3 quorum)
    for (int i = 100; i < 150; ++i) {
        std::string key = "replicated_k" + std::to_string(i);
        std::string val = "replicated_v" + std::to_string(i);
        bool ok = leader->propose(Command::make_set(key, val));
        (void)ok;
        assert(ok);
    }
    std::cout << "Successfully committed 50 additional writes across 2/3 quorum." << std::endl;

    StorageEngine& leader_eng = (leader == node1 ? engine1 : (leader == node2 ? engine2 : engine3));
    StorageEngine& other_eng = (other_follower == node1 ? engine1 : (other_follower == node2 ? engine2 : engine3));
    StorageEngine& part_eng = (follower_to_partition == node1 ? engine1 : (follower_to_partition == node2 ? engine2 : engine3));

    // Wait for other follower to apply
    for (int retry = 0; retry < 40; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (other_eng.size() == 150) {
            break;
        }
    }

    (void)leader_eng;
    (void)other_eng;
    (void)part_eng;
    assert(leader_eng.size() == 150);
    assert(other_eng.size() == 150);
    assert(part_eng.size() == 100);

    // 5. Reconnect the partitioned follower and assert catch-up
    std::cout << "Reconnecting partitioned follower " << follower_to_partition->node_id() << " to cluster..." << std::endl;
    follower_to_partition->start();

    // Wait for leader heartbeat / replication to catch up the reconnected follower
    for (int retry = 0; retry < 50; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (part_eng.size() == 150) {
            break;
        }
    }

    assert(part_eng.size() == 150);
    for (int i = 0; i < 150; ++i) {
        std::string key = "replicated_k" + std::to_string(i);
        std::string val = "replicated_v" + std::to_string(i);
        assert(part_eng.get(key).value() == val);
    }
    std::cout << "Partitioned follower successfully caught up to 150 entries." << std::endl;

    // Teardown
    node1->stop();
    node2->stop();
    node3->stop();
    ioc.stop();

    for (auto& w : workers) {
        w.join();
    }

    std::cout << "[PASS] Raft Log Replication, Quorum Commit & Catch-Up tests passed.\n" << std::endl;
}

void test_raft_snapshot_compaction_and_install() {
    std::cout << "=== Running Raft Snapshot Compaction & InstallSnapshot Test ===" << std::endl;

    asio::io_context ioc;
    StorageEngine engine1, engine2, engine3;

    auto node1 = std::make_shared<RaftNode>("node_1", "127.0.0.1", static_cast<uint16_t>(0), std::vector<PeerConfig>{}, ioc, engine1);
    auto node2 = std::make_shared<RaftNode>("node_2", "127.0.0.1", static_cast<uint16_t>(0), std::vector<PeerConfig>{}, ioc, engine2);
    auto node3 = std::make_shared<RaftNode>("node_3", "127.0.0.1", static_cast<uint16_t>(0), std::vector<PeerConfig>{}, ioc, engine3);

    node1->set_peers({{"node_2", "127.0.0.1", node2->port()}, {"node_3", "127.0.0.1", node3->port()}});
    node2->set_peers({{"node_1", "127.0.0.1", node1->port()}, {"node_3", "127.0.0.1", node3->port()}});
    node3->set_peers({{"node_1", "127.0.0.1", node1->port()}, {"node_2", "127.0.0.1", node2->port()}});

    node1->start();
    node2->start();
    node3->start();

    const int NUM_WORKER_THREADS = 4;
    std::vector<std::thread> workers;
    workers.reserve(NUM_WORKER_THREADS);
    for (int i = 0; i < NUM_WORKER_THREADS; ++i) {
        workers.emplace_back([&ioc]() {
            ioc.run();
        });
    }

    std::shared_ptr<RaftNode> leader = nullptr;
    for (int retry = 0; retry < 40; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (node1->is_leader()) leader = node1;
        else if (node2->is_leader()) leader = node2;
        else if (node3->is_leader()) leader = node3;
        if (leader) break;
    }
    assert(leader != nullptr);
    std::cout << "Leader for snapshot test: " << leader->node_id() << std::endl;

    // 1. Propose 200 items
    for (int i = 0; i < 200; ++i) {
        std::string key = "snap_k" + std::to_string(i);
        std::string val = "snap_v" + std::to_string(i);
        bool ok = leader->propose(Command::make_set(key, val));
        (void)ok;
        assert(ok);
    }
    std::cout << "Committed 200 initial entries across cluster." << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 2. Identify a follower to simulate severe lag (stop it before compaction)
    std::shared_ptr<RaftNode> lagging_follower = (node1 != leader ? node1 : node2);
    StorageEngine& lagging_eng = (lagging_follower == node1 ? engine1 : engine2);

    std::cout << "Stopping follower " << lagging_follower->node_id() << " before snapshot..." << std::endl;
    lagging_follower->stop();

    // 3. Compact log on leader up to index 150
    std::cout << "Leader taking snapshot and compacting log up to index 150..." << std::endl;
    bool snap_ok = leader->take_snapshot(150);
    (void)snap_ok;
    assert(snap_ok);
    assert(leader->last_included_index() == 150);
    assert(leader->in_memory_log_size() <= 55);
    std::cout << "Leader in-memory log successfully compacted to " << leader->in_memory_log_size() << " entries." << std::endl;

    // 4. Propose 50 more items while follower is still down
    for (int i = 200; i < 250; ++i) {
        std::string key = "snap_k" + std::to_string(i);
        std::string val = "snap_v" + std::to_string(i);
        bool ok = leader->propose(Command::make_set(key, val));
        (void)ok;
        assert(ok);
    }

    // 5. Restart the lagging follower; leader must stream snapshot via InstallSnapshot RPC
    std::cout << "Restarting lagging follower " << lagging_follower->node_id() << " to trigger InstallSnapshot..." << std::endl;
    lagging_follower->start();

    // Wait for lagging follower to install snapshot and catch up to 250 entries
    for (int retry = 0; retry < 60; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (lagging_eng.size() == 250) {
            break;
        }
    }

    assert(lagging_eng.size() == 250);
    for (int i = 0; i < 250; ++i) {
        std::string key = "snap_k" + std::to_string(i);
        std::string val = "snap_v" + std::to_string(i);
        assert(lagging_eng.get(key).value() == val);
    }

    std::cout << "Lagging follower successfully installed snapshot and caught up to 250 entries!" << std::endl;

    // Teardown
    node1->stop();
    node2->stop();
    node3->stop();
    ioc.stop();

    for (auto& w : workers) {
        w.join();
    }

    std::cout << "[PASS] Raft Snapshot Compaction & InstallSnapshot tests passed.\n" << std::endl;
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

    for (int i = 0; i < 200; ++i) {
        engine.set("key_" + std::to_string(i), "init_val_" + std::to_string(i));
    }

    std::vector<std::thread> threads;
    threads.reserve(NUM_READERS + NUM_WRITERS);

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
                    std::string val = "val_w" + std::to_string(w) + "_" + std::to_string(i);
                    engine.set(key, val);
                    total_writes.fetch_add(1, std::memory_order_relaxed);
                } else if (op < 8) {
                    engine.del(key);
                    total_deletes.fetch_add(1, std::memory_order_relaxed);
                } else {
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

    std::cout << "--- Concurrency Test Statistics ---" << std::endl;
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
        test_wal_durability_and_recovery();
        test_wal_corruption_and_truncation();
        test_protocol_serialization();
        test_tcp_server_async_multiclient();
        test_raft_rpc_serialization();
        test_raft_3node_cluster_leader_election_and_failover();
        test_raft_log_replication_and_catchup();
        test_raft_snapshot_compaction_and_install();
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