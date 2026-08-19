#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <unordered_map>
#include <vector>
#include <shared_mutex>
#include <mutex>
#include <cstdint>

namespace distributed_kv {

enum class CommandType : uint8_t {
    SET,
    DEL,
    NOOP
};

struct Command {
    CommandType type{CommandType::NOOP};
    std::string key;
    std::string value;

    Command() = default;
    Command(CommandType t, std::string k, std::string v = "")
        : type(t), key(std::move(k)), value(std::move(v)) {}

    static Command make_set(std::string key, std::string value) {
        return Command(CommandType::SET, std::move(key), std::move(value));
    }

    static Command make_del(std::string key) {
        return Command(CommandType::DEL, std::move(key));
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

class StorageEngine {
public:
    StorageEngine() = default;
    ~StorageEngine() = default;

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;

    StorageEngine(StorageEngine&& other) noexcept;
    StorageEngine& operator=(StorageEngine&& other) noexcept;

    // --- Key-Value Point Operations ---

    bool set(std::string_view key, std::string_view value);

    std::optional<std::string> get(std::string_view key) const;

    bool del(std::string_view key);

    bool exists(std::string_view key) const;

    size_t size() const;

    bool empty() const;

    void clear();

    // --- Deterministic State Machine Operations (Raft) ---

    CommandResult apply(const Command& cmd);

    std::vector<CommandResult> apply_batch(const std::vector<Command>& batch);

    // --- Snapshotting Support ---

    std::unordered_map<std::string, std::string> snapshot() const;

    void restore_snapshot(const std::unordered_map<std::string, std::string>& data);

private:
    CommandResult apply_unlocked(const Command& cmd);

    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::string> store_;
};

} // namespace distributed_kv