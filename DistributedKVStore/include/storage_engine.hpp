#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace distributed_kv {

class WAL;

enum class CommandType : uint8_t {
    SET = 1,
    DEL = 2,
    CLEAR = 3,
    NOOP = 4
};

struct Command {
    CommandType type{CommandType::NOOP};
    std::string key;
    std::string value;
    uint64_t ttl_ms{0};

    Command() = default;
    Command(CommandType t, std::string k, std::string v = "", uint64_t ttl = 0)
        : type(t), key(std::move(k)), value(std::move(v)), ttl_ms(ttl) {}

    static Command make_set(std::string key, std::string value, uint64_t ttl_ms = 0) {
        return Command(CommandType::SET, std::move(key), std::move(value), ttl_ms);
    }

    static Command make_del(std::string key) {
        return Command(CommandType::DEL, std::move(key));
    }

    static Command make_clear() {
        return Command(CommandType::CLEAR, "");
    }

    static Command make_noop() {
        return Command(CommandType::NOOP, "");
    }
};

struct CommandResult {
    bool success{false};
    std::optional<std::string> previous_value{std::nullopt};
    std::string error_message;

    static CommandResult ok(std::optional<std::string> prev = std::nullopt) {
        CommandResult res;
        res.success = true;
        res.previous_value = std::move(prev);
        return res;
    }

    static CommandResult failure(std::string err) {
        CommandResult res;
        res.success = false;
        res.error_message = std::move(err);
        return res;
    }
};

struct ValueEntry {
    std::string value;
    std::optional<std::chrono::steady_clock::time_point> expire_at{std::nullopt};

    bool is_expired() const {
        return expire_at.has_value() && std::chrono::steady_clock::now() >= *expire_at;
    }
};

class StorageEngine {
public:
    explicit StorageEngine(std::shared_ptr<WAL> wal = nullptr);
    ~StorageEngine() = default;

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;

    StorageEngine(StorageEngine&& other) noexcept;
    StorageEngine& operator=(StorageEngine&& other) noexcept;

    // --- Key-Value Point Operations ---

    bool set(std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms = std::nullopt);
    std::optional<std::string> get(std::string_view key);
    bool del(std::string_view key);
    bool exists(std::string_view key) const;
    int64_t ttl(std::string_view key) const;
    size_t purge_expired(size_t max_keys_to_check = 100);

    size_t size() const;
    bool empty() const;
    void clear();

    // --- Deterministic State Machine Operations (Raft) ---

    CommandResult apply(const Command& cmd);
    std::vector<CommandResult> apply_batch(const std::vector<Command>& batch);

    // --- Snapshotting & WAL Integration ---

    std::unordered_map<std::string, std::string> snapshot() const;
    void restore_snapshot(const std::unordered_map<std::string, std::string>& data);

    void attach_wal(std::shared_ptr<WAL> wal);
    std::shared_ptr<WAL> wal() const noexcept;

private:
    CommandResult apply_unlocked(const Command& cmd);

    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, ValueEntry> store_;
    std::shared_ptr<WAL> wal_{nullptr};
};

} // namespace distributed_kv