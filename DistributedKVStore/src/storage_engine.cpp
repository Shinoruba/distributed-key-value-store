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
        std::unique_lock<std::shared_mutex> lhs(mutex_, std::defer_lock);
        std::unique_lock<std::shared_mutex> rhs(other.mutex_, std::defer_lock);
        std::lock(lhs, rhs);

        store_ = std::move(other.store_);
        wal_ = std::move(other.wal_);
    }
    return *this;
}

bool StorageEngine::set(std::string_view key, std::string_view value, std::optional<uint64_t> ttl_ms) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    if (wal_) {
        wal_->append_set(key, value);
    }

    std::optional<std::chrono::steady_clock::time_point> exp = std::nullopt;
    if (ttl_ms && *ttl_ms > 0) {
        exp = std::chrono::steady_clock::now() + std::chrono::milliseconds(*ttl_ms);
    }

    store_[std::string(key)] = ValueEntry{std::string(value), exp};
    return true;
}

std::optional<std::string> StorageEngine::get(std::string_view key) {
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = store_.find(std::string(key));
        if (it == store_.end()) {
            return std::nullopt;
        }
        if (!it->second.is_expired()) {
            return it->second.value;
        }
    }

    // Passive eviction
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = store_.find(std::string(key));
    if (it != store_.end() && it->second.is_expired()) {
        store_.erase(it);
    }
    return std::nullopt;
}

bool StorageEngine::del(std::string_view key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = store_.find(std::string(key));
    if (it == store_.end()) {
        return false;
    }

    bool was_expired = it->second.is_expired();
    store_.erase(it);

    if (was_expired) {
        return false;
    }

    if (wal_) {
        wal_->append_del(key);
    }
    return true;
}

bool StorageEngine::exists(std::string_view key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = store_.find(std::string(key));
    if (it == store_.end() || it->second.is_expired()) {
        return false;
    }
    return true;
}

int64_t StorageEngine::ttl(std::string_view key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = store_.find(std::string(key));
    if (it == store_.end() || it->second.is_expired()) {
        return -2;
    }
    if (!it->second.expire_at.has_value()) {
        return -1;
    }
    auto now = std::chrono::steady_clock::now();
    if (now >= *it->second.expire_at) {
        return -2;
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(*it->second.expire_at - now).count();
}

size_t StorageEngine::purge_expired(size_t max_keys_to_check) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    size_t purged = 0;
    size_t checked = 0;

    for (auto it = store_.begin(); it != store_.end() && checked < max_keys_to_check;) {
        ++checked;
        if (it->second.is_expired()) {
            it = store_.erase(it);
            ++purged;
        } else {
            ++it;
        }
    }
    return purged;
}

size_t StorageEngine::size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    size_t valid = 0;
    for (const auto& [k, v] : store_) {
        if (!v.is_expired()) ++valid;
    }
    return valid;
}

bool StorageEngine::empty() const {
    return size() == 0;
}

void StorageEngine::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (wal_) {
        wal_->append_clear();
    }
    store_.clear();
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

CommandResult StorageEngine::apply_unlocked(const Command& cmd) {
    switch (cmd.type) {
        case CommandType::SET: {
            if (wal_) {
                wal_->append_set(cmd.key, cmd.value);
            }
            std::optional<std::string> prev = std::nullopt;
            auto it = store_.find(cmd.key);
            if (it != store_.end() && !it->second.is_expired()) {
                prev = it->second.value;
            }

            std::optional<std::chrono::steady_clock::time_point> exp = std::nullopt;
            if (cmd.ttl_ms > 0) {
                exp = std::chrono::steady_clock::now() + std::chrono::milliseconds(cmd.ttl_ms);
            }

            store_[cmd.key] = ValueEntry{cmd.value, exp};
            return CommandResult::ok(std::move(prev));
        }

        case CommandType::DEL: {
            auto it = store_.find(cmd.key);
            if (it == store_.end() || it->second.is_expired()) {
                if (it != store_.end()) store_.erase(it);
                return CommandResult::failure("Key not found");
            }
            std::string prev = it->second.value;
            store_.erase(it);
            if (wal_) {
                wal_->append_del(cmd.key);
            }
            return CommandResult::ok(std::move(prev));
        }

        case CommandType::CLEAR: {
            if (wal_) {
                wal_->append_clear();
            }
            store_.clear();
            return CommandResult::ok();
        }

        case CommandType::NOOP:
            return CommandResult::ok();

        default:
            return CommandResult::failure("Unsupported command type");
    }
}

std::unordered_map<std::string, std::string> StorageEngine::snapshot() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::unordered_map<std::string, std::string> snap;
    snap.reserve(store_.size());
    for (const auto& [k, v] : store_) {
        if (!v.is_expired()) {
            snap.emplace(k, v.value);
        }
    }
    return snap;
}

void StorageEngine::restore_snapshot(const std::unordered_map<std::string, std::string>& data) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    store_.clear();
    for (const auto& [k, v] : data) {
        store_[k] = ValueEntry{v, std::nullopt};
    }
}

void StorageEngine::attach_wal(std::shared_ptr<WAL> wal) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    wal_ = std::move(wal);
}

std::shared_ptr<WAL> StorageEngine::wal() const noexcept {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return wal_;
}

} // namespace distributed_kv