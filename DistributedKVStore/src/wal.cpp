#include "wal.hpp"
#include "storage_engine.hpp"

#include <array>
#include <cstring>

namespace distributed_kv {

static constexpr auto generate_crc32_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc & 1) ? (0xEDB88320u ^ (crc >> 1)) : (crc >> 1);
        }
        table[i] = crc;
    }
    return table;
}

static constexpr auto CRC32_TABLE = generate_crc32_table();

uint32_t CRC32::calculate(const uint8_t* data, size_t length) {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < length; ++i) {
        uint8_t index = static_cast<uint8_t>((crc ^ data[i]) & 0xFF);
        crc = CRC32_TABLE[index] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

uint32_t CRC32::calculate(std::string_view data) {
    return calculate(reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

WAL::WAL(std::filesystem::path log_path, bool auto_flush)
    : path_(std::move(log_path)), auto_flush_(auto_flush) {
    open_writer();
}

WAL::~WAL() {
    close();
}

WAL::WAL(WAL&& other) noexcept {
    std::lock_guard<std::mutex> lock(other.mutex_);
    path_ = std::move(other.path_);
    auto_flush_ = other.auto_flush_;
    writer_ = std::move(other.writer_);
}

WAL& WAL::operator=(WAL&& other) noexcept {
    if (this != &other) {
        std::scoped_lock lock(mutex_, other.mutex_);
        if (writer_.is_open()) {
            writer_.close();
        }
        path_ = std::move(other.path_);
        auto_flush_ = other.auto_flush_;
        writer_ = std::move(other.writer_);
    }
    return *this;
}

void WAL::open_writer() {
    if (path_.has_parent_path()) {
        std::filesystem::create_directories(path_.parent_path());
    }
    writer_.open(path_, std::ios::binary | std::ios::app | std::ios::out);
}

void WAL::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (writer_.is_open()) {
        writer_.flush();
        writer_.close();
    }
}

bool WAL::is_open() const noexcept {
    return writer_.is_open();
}

const std::filesystem::path& WAL::path() const noexcept {
    return path_;
}

uint64_t WAL::size_bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (std::filesystem::exists(path_)) {
        return std::filesystem::file_size(path_);
    }
    return 0;
}

void WAL::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (writer_.is_open()) {
        writer_.flush();
    }
}

void WAL::sync() {
    flush();
}

void WAL::truncate() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (writer_.is_open()) {
        writer_.close();
    }
    std::ofstream truncator(path_, std::ios::binary | std::ios::trunc | std::ios::out);
    truncator.close();
    open_writer();
}

bool WAL::write_record_unlocked(WALRecordType type, std::string_view key, std::string_view value, uint64_t ttl_ms) {
    if (!writer_.is_open()) {
        return false;
    }

    const uint32_t key_len = static_cast<uint32_t>(key.size());
    const uint32_t val_len = static_cast<uint32_t>(value.size());
    const uint32_t payload_len = sizeof(uint8_t) + sizeof(uint32_t) + key_len + sizeof(uint32_t) + val_len + sizeof(uint64_t);

    std::vector<uint8_t> payload;
    payload.reserve(payload_len);

    payload.push_back(static_cast<uint8_t>(type));

    const uint8_t* key_len_bytes = reinterpret_cast<const uint8_t*>(&key_len);
    payload.insert(payload.end(), key_len_bytes, key_len_bytes + sizeof(uint32_t));
    payload.insert(payload.end(), key.begin(), key.end());

    const uint8_t* val_len_bytes = reinterpret_cast<const uint8_t*>(&val_len);
    payload.insert(payload.end(), val_len_bytes, val_len_bytes + sizeof(uint32_t));
    payload.insert(payload.end(), value.begin(), value.end());

    const uint8_t* ttl_bytes = reinterpret_cast<const uint8_t*>(&ttl_ms);
    payload.insert(payload.end(), ttl_bytes, ttl_bytes + sizeof(uint64_t));

    const uint32_t checksum = CRC32::calculate(payload.data(), payload.size());

    writer_.write(reinterpret_cast<const char*>(&payload_len), sizeof(uint32_t));
    writer_.write(reinterpret_cast<const char*>(&checksum), sizeof(uint32_t));
    writer_.write(reinterpret_cast<const char*>(payload.data()), payload.size());

    if (auto_flush_) {
        writer_.flush();
    }

    return writer_.good();
}

bool WAL::append_set(std::string_view key, std::string_view value, uint64_t ttl_ms) {
    return append(WALRecordType::SET, key, value, ttl_ms);
}

bool WAL::append_del(std::string_view key) {
    return append(WALRecordType::DEL, key, "");
}

bool WAL::append_clear() {
    return append(WALRecordType::CLEAR, "", "");
}

bool WAL::append(WALRecordType type, std::string_view key, std::string_view value, uint64_t ttl_ms) {
    std::lock_guard<std::mutex> lock(mutex_);
    return write_record_unlocked(type, key, value, ttl_ms);
}

bool WAL::append_batch(const std::vector<WALRecord>& records) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& rec : records) {
        if (!write_record_unlocked(rec.type, rec.key, rec.value, rec.ttl_ms)) {
            return false;
        }
    }
    return true;
}

size_t WAL::recover(StorageEngine& engine) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (writer_.is_open()) {
        writer_.close();
    }

    if (!std::filesystem::exists(path_)) {
        open_writer();
        return 0;
    }

    std::ifstream reader(path_, std::ios::binary);
    if (!reader.is_open()) {
        open_writer();
        return 0;
    }

    size_t recovered_count = 0;
    std::vector<uint8_t> payload_buffer;

    while (reader.peek() != EOF) {
        uint32_t payload_len = 0;
        if (!reader.read(reinterpret_cast<char*>(&payload_len), sizeof(uint32_t))) {
            break;
        }

        uint32_t expected_crc = 0;
        if (!reader.read(reinterpret_cast<char*>(&expected_crc), sizeof(uint32_t))) {
            break;
        }

        if (payload_len == 0 || payload_len > 64 * 1024 * 1024) {
            break;
        }

        payload_buffer.resize(payload_len);
        if (!reader.read(reinterpret_cast<char*>(payload_buffer.data()), payload_len)) {
            break;
        }

        uint32_t actual_crc = CRC32::calculate(payload_buffer.data(), payload_buffer.size());
        if (actual_crc != expected_crc) {
            break;
        }

        size_t offset = 0;
        WALRecordType type = static_cast<WALRecordType>(payload_buffer[offset]);
        offset += sizeof(uint8_t);

        if (offset + sizeof(uint32_t) > payload_len) break;
        uint32_t key_len = *reinterpret_cast<const uint32_t*>(payload_buffer.data() + offset);
        offset += sizeof(uint32_t);

        if (offset + key_len > payload_len) break;
        std::string key(reinterpret_cast<const char*>(payload_buffer.data() + offset), key_len);
        offset += key_len;

        if (offset + sizeof(uint32_t) > payload_len) break;
        uint32_t val_len = *reinterpret_cast<const uint32_t*>(payload_buffer.data() + offset);
        offset += sizeof(uint32_t);

        if (offset + val_len > payload_len) break;
        std::string value(reinterpret_cast<const char*>(payload_buffer.data() + offset), val_len);
        offset += val_len;

        uint64_t ttl_ms = 0;
        if (offset + sizeof(uint64_t) <= payload_len) {
            ttl_ms = *reinterpret_cast<const uint64_t*>(payload_buffer.data() + offset);
        }

        switch (type) {
            case WALRecordType::SET:
                engine.set(key, value, ttl_ms > 0 ? std::optional<uint64_t>(ttl_ms) : std::nullopt);
                ++recovered_count;
                break;
            case WALRecordType::DEL:
                engine.del(key);
                ++recovered_count;
                break;
            case WALRecordType::CLEAR:
                engine.clear();
                ++recovered_count;
                break;
            case WALRecordType::NOOP:
                ++recovered_count;
                break;
        }
    }

    reader.close();
    open_writer();
    return recovered_count;
}

} // namespace distributed_kv