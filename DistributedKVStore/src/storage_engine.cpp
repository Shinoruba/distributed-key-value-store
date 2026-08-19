#include "storage_engine.hpp"
#include "wal.hpp"

namespace distributed_kv {

StorageEngine::StorageEngine(std::shared_ptr<WAL> wal)
    : wal_(std::move(wal)) {}

StorageEngine::StorageEngine(StorageEngine&& other) noexcept {
    std::unique_lock<std::shared_mutex> lock(other.mutex_);
    store_ = std::move(other.store_);
    wal_ = std::move(other.wal_);
}

StorageEngine& StorageEngine::operator=(StorageEngine&& other) noexcept {
    if (this != &other) {
        std::unique_lock<std::shared_mutex> lhs_lock(mutex_, std::defer_lock);
        std::unique_lock<std::shared_mutex> rhs_lock(other.mutex_, std::defer_lock);
        std::lock(lhs_lock, rhs_lock);
        store_ = std::move(other.store_);
        wal_ = std::move(other.wal_);
    }
    return *this;
}

bool StorageEngine::set(std::string_view key, std::string_view value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (wal_) {
        wal_->append_set(key, value);
    }
    store_[std::string(key)] = std::string(value);
    return true;
}

std::optional<std::string> StorageEngine::get(std::string_view key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = store_.find(std::string(key));
    if (it != store_.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool StorageEngine::del(std::string_view key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (wal_) {
        wal_->append_del(key);
    }
    return store_.erase(std::string(key)) > 0;
}

bool StorageEngine::exists(std::string_view key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return store_.find(std::string(key)) != store_.end();
}

size_t StorageEngine::size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return store_.size();
}

bool StorageEngine::empty() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return store_.empty();
}

void StorageEngine::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (wal_) {
        wal_->append_clear();
    }
    store_.clear();
}

std::unordered_map<std::string, std::string> StorageEngine::snapshot() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return store_;
}

void StorageEngine::restore_snapshot(const std::unordered_map<std::string, std::string>& data) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    store_ = data;
}

void StorageEngine::attach_wal(std::shared_ptr<WAL> wal) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    wal_ = std::move(wal);
}

std::shared_ptr<WAL> StorageEngine::wal() const noexcept {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return wal_;
}

CommandResult StorageEngine::apply_unlocked(const Command& cmd) {
    if (wal_) {
        wal_->append(static_cast<WALRecordType>(cmd.type), cmd.key, cmd.value);
    }

    switch (cmd.type) {
        case CommandType::SET: {
            auto it = store_.find(cmd.key);
            std::optional<std::string> prev = std::nullopt;
            if (it != store_.end()) {
                prev = std::move(it->second);
            }
            store_[cmd.key] = cmd.value;
            return CommandResult::ok(std::move(prev));
        }
        case CommandType::DEL: {
            auto it = store_.find(cmd.key);
            if (it != store_.end()) {
                std::string prev = std::move(it->second);
                store_.erase(it);
                return CommandResult::ok(std::move(prev));
            }
            return CommandResult::failure("Key not found");
        }
        case CommandType::CLEAR: {
            store_.clear();
            return CommandResult::ok(std::nullopt);
        }
        case CommandType::NOOP:
            return CommandResult::ok(std::nullopt);
        default:
            return CommandResult::failure("Unknown command type");
    }
}

CommandResult StorageEngine::apply(const Command& cmd) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return apply_unlocked(cmd);
}

std::vector<CommandResult> StorageEngine::apply_batch(const std::vector<Command>& batch) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::vector<CommandResult> results;
    results.reserve(batch.size());
    for (const auto& cmd : batch) {
        results.push_back(apply_unlocked(cmd));
    }
    return results;
}

} // namespace distributed_kv