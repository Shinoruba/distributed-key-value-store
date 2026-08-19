#include "protocol.hpp"

#include <cstring>

namespace distributed_kv {

Request Request::make_ping() {
    Request req;
    req.op = OpCode::PING;
    return req;
}

Request Request::make_set(std::string key, std::string value, uint64_t ttl_ms) {
    Request req;
    req.op = OpCode::SET;
    req.key = std::move(key);
    req.value = std::move(value);
    req.ttl_ms = ttl_ms;
    return req;
}

Request Request::make_get(std::string key) {
    Request req;
    req.op = OpCode::GET;
    req.key = std::move(key);
    return req;
}

Request Request::make_del(std::string key) {
    Request req;
    req.op = OpCode::DEL;
    req.key = std::move(key);
    return req;
}

Request Request::make_stats() {
    Request req;
    req.op = OpCode::STATS;
    return req;
}

Request Request::make_ttl(std::string key) {
    Request req;
    req.op = OpCode::TTL;
    req.key = std::move(key);
    return req;
}

Response Response::ok(std::string value, std::string message) {
    Response res;
    res.status = StatusCode::OK;
    res.value = std::move(value);
    res.message = std::move(message);
    return res;
}

Response Response::not_found(std::string message) {
    Response res;
    res.status = StatusCode::NOT_FOUND;
    res.message = std::move(message);
    return res;
}

Response Response::error(std::string message) {
    Response res;
    res.status = StatusCode::ERR;
    res.message = std::move(message);
    return res;
}

std::vector<uint8_t> Protocol::serialize_request(const Request& req) {
    const uint32_t key_len = static_cast<uint32_t>(req.key.size());
    const uint32_t val_len = static_cast<uint32_t>(req.value.size());

    uint32_t payload_len = sizeof(uint8_t);
    if (req.op == OpCode::SET) {
        payload_len += sizeof(uint32_t) + key_len + sizeof(uint32_t) + val_len + sizeof(uint64_t);
    } else if (req.op == OpCode::GET || req.op == OpCode::DEL || req.op == OpCode::TTL) {
        payload_len += sizeof(uint32_t) + key_len;
    }

    std::vector<uint8_t> buffer;
    buffer.reserve(sizeof(uint32_t) + payload_len);

    const uint8_t* p_len = reinterpret_cast<const uint8_t*>(&payload_len);
    buffer.insert(buffer.end(), p_len, p_len + sizeof(uint32_t));

    buffer.push_back(static_cast<uint8_t>(req.op));

    if (req.op == OpCode::SET) {
        const uint8_t* k_len = reinterpret_cast<const uint8_t*>(&key_len);
        buffer.insert(buffer.end(), k_len, k_len + sizeof(uint32_t));
        buffer.insert(buffer.end(), req.key.begin(), req.key.end());

        const uint8_t* v_len = reinterpret_cast<const uint8_t*>(&val_len);
        buffer.insert(buffer.end(), v_len, v_len + sizeof(uint32_t));
        buffer.insert(buffer.end(), req.value.begin(), req.value.end());

        const uint8_t* ttl_bytes = reinterpret_cast<const uint8_t*>(&req.ttl_ms);
        buffer.insert(buffer.end(), ttl_bytes, ttl_bytes + sizeof(uint64_t));
    } else if (req.op == OpCode::GET || req.op == OpCode::DEL || req.op == OpCode::TTL) {
        const uint8_t* k_len = reinterpret_cast<const uint8_t*>(&key_len);
        buffer.insert(buffer.end(), k_len, k_len + sizeof(uint32_t));
        buffer.insert(buffer.end(), req.key.begin(), req.key.end());
    }

    return buffer;
}

std::optional<Request> Protocol::deserialize_request(const uint8_t* data, size_t size) {
    if (size < sizeof(uint8_t)) {
        return std::nullopt;
    }

    Request req;
    req.op = static_cast<OpCode>(data[0]);

    if (req.op == OpCode::PING || req.op == OpCode::STATS) {
        return req;
    }

    if (req.op == OpCode::GET || req.op == OpCode::DEL || req.op == OpCode::TTL) {
        if (size < sizeof(uint8_t) + sizeof(uint32_t)) {
            return std::nullopt;
        }

        uint32_t key_len = *reinterpret_cast<const uint32_t*>(data + 1);
        if (size < sizeof(uint8_t) + sizeof(uint32_t) + key_len) {
            return std::nullopt;
        }

        req.key.assign(reinterpret_cast<const char*>(data + 1 + sizeof(uint32_t)), key_len);
        return req;
    }

    if (req.op == OpCode::SET) {
        if (size < sizeof(uint8_t) + sizeof(uint32_t)) {
            return std::nullopt;
        }

        size_t offset = 1;
        uint32_t key_len = *reinterpret_cast<const uint32_t*>(data + offset);
        offset += sizeof(uint32_t);

        if (offset + key_len + sizeof(uint32_t) > size) {
            return std::nullopt;
        }

        req.key.assign(reinterpret_cast<const char*>(data + offset), key_len);
        offset += key_len;

        uint32_t val_len = *reinterpret_cast<const uint32_t*>(data + offset);
        offset += sizeof(uint32_t);

        if (offset + val_len > size) {
            return std::nullopt;
        }

        req.value.assign(reinterpret_cast<const char*>(data + offset), val_len);
        offset += val_len;

        if (offset + sizeof(uint64_t) <= size) {
            req.ttl_ms = *reinterpret_cast<const uint64_t*>(data + offset);
        }

        return req;
    }

    return std::nullopt;
}

std::vector<uint8_t> Protocol::serialize_response(const Response& res) {
    const uint32_t val_len = static_cast<uint32_t>(res.value.size());
    const uint32_t msg_len = static_cast<uint32_t>(res.message.size());
    const uint32_t payload_len = sizeof(uint8_t) + sizeof(uint32_t) + val_len + sizeof(uint32_t) + msg_len;

    std::vector<uint8_t> buffer;
    buffer.reserve(sizeof(uint32_t) + payload_len);

    const uint8_t* p_len = reinterpret_cast<const uint8_t*>(&payload_len);
    buffer.insert(buffer.end(), p_len, p_len + sizeof(uint32_t));

    buffer.push_back(static_cast<uint8_t>(res.status));

    const uint8_t* v_len = reinterpret_cast<const uint8_t*>(&val_len);
    buffer.insert(buffer.end(), v_len, v_len + sizeof(uint32_t));
    buffer.insert(buffer.end(), res.value.begin(), res.value.end());

    const uint8_t* m_len = reinterpret_cast<const uint8_t*>(&msg_len);
    buffer.insert(buffer.end(), m_len, m_len + sizeof(uint32_t));
    buffer.insert(buffer.end(), res.message.begin(), res.message.end());

    return buffer;
}

std::optional<Response> Protocol::deserialize_response(const uint8_t* data, size_t size) {
    if (size < sizeof(uint8_t) + sizeof(uint32_t) * 2) {
        return std::nullopt;
    }

    Response res;
    res.status = static_cast<StatusCode>(data[0]);

    size_t offset = 1;
    uint32_t val_len = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += sizeof(uint32_t);

    if (offset + val_len + sizeof(uint32_t) > size) {
        return std::nullopt;
    }

    res.value.assign(reinterpret_cast<const char*>(data + offset), val_len);
    offset += val_len;

    uint32_t msg_len = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += sizeof(uint32_t);

    if (offset + msg_len > size) {
        return std::nullopt;
    }

    res.message.assign(reinterpret_cast<const char*>(data + offset), msg_len);

    return res;
}

} // namespace distributed_kv