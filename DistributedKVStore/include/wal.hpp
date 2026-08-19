#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace distributed_kv {

class StorageEngine;

enum class WALRecordType : uint8_t {
    SET = 1,
    DEL = 2,
    CLEAR = 3,
    NOOP = 4
};

struct WALRecord {
    WALRecordType type{WALRecordType::NOOP};
    std::string key;
    std::string value;
};

class CRC32 {
public:
    static uint32_t calculate(const uint8_t* data, size_t length);
    static uint32_t calculate(std::string_view data);
};

class WAL {
public:
    explicit WAL(std::filesystem::path log_path, bool auto_flush = true);
    ~WAL();

    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    WAL(WAL&& other) noexcept;
    WAL& operator=(WAL&& other) noexcept;

    bool append_set(std::string_view key, std::string_view value);
    bool append_del(std::string_view key);
    bool append_clear();
    bool append(WALRecordType type, std::string_view key, std::string_view value);
    bool append_batch(const std::vector<WALRecord>& records);

    void flush();
    void sync();
    void truncate();
    void close();

    size_t recover(StorageEngine& engine);

    const std::filesystem::path& path() const noexcept;
    uint64_t size_bytes() const;
    bool is_open() const noexcept;

private:
    void open_writer();
    bool write_record_unlocked(WALRecordType type, std::string_view key, std::string_view value);

    std::filesystem::path path_;
    bool auto_flush_{true};
    std::ofstream writer_;
    mutable std::mutex mutex_;
};

} // namespace distributed_kv