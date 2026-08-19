#include "protocol.hpp"

namespace distributed_kv {

Request Request::make_ping() {
    return {OpCode::PING, "", ""};
}

Request Request::make_set(std::string key, std::string value) {
    return {OpCode::SET, std::move(key), std::move(value)};
}

Request Request::make_get(std::string key) {
    return {OpCode::GET, std::move(key), ""};
}

Request Request::make_del(std::string key) {
    return {OpCode::DEL, std::move(key), ""};
}

Request Request::make_stats() {
    return {OpCode::STATS, "", ""};
}

Response Response::ok(std::string value, std::string message) {
    return {StatusCode::OK, std::move(value), std::move(message)};
}

Response Response::not_found(std::string message) {
    return {StatusCode::NOT_FOUND, "", std::move(message)};
}

Response Response::error(std::string message) {
    return {StatusCode::ERR, "", std::move(message)};
}

std::vector<uint8_t> Protocol::serialize_request(const Request& req) {
    const uint32_t key_len = static_cast<uint32_t>(req.key.size());
    const uint32_t val_len = static_cast<uint32_t>(req.value.size());

    uint32_t payload_len = sizeof(uint8_t);
    if (req.op == OpCode::GET || req.op == OpCode::DEL) {
        payload_len += sizeof(uint32_t) + key_len;
    } else if (req.op == OpCode::SET) {
        payload_len += sizeof(uint32_t) + key_len + sizeof(uint32_t) + val_len;
    }

    std::vector<uint8_t> buffer;
    buffer.reserve(sizeof(uint32_t) + payload_len);

    const uint8_t* p_len_bytes = reinterpret_cast<const uint8_t*>(&payload_len);
    buffer.insert(buffer.end(), p_len_bytes, p_len_bytes + sizeof(uint32_t));

    buffer.push_back(static_cast<uint8_t>(req.op));

    if (req.op == OpCode::GET || req.op == OpCode::DEL) {
        const uint8_t* k_len_bytes = reinterpret_cast<const uint8_t*>(&key_len);
        buffer.insert(buffer.end(), k_len_bytes, k_len_bytes + sizeof(uint32_t));
        buffer.insert(buffer.end(), req.key.begin(), req.key.end());
    } else if (req.op == OpCode::SET) {
        const uint8_t* k_len_bytes = reinterpret_cast<const uint8_t*>(&key_len);
        buffer.insert(buffer.end(), k_len_bytes, k_len_bytes + sizeof(uint32_t));
        buffer.insert(buffer.end(), req.key.begin(), req.key.end());

        const uint8_t* v_len_bytes = reinterpret_cast<const uint8_t*>(&val_len);
        buffer.insert(buffer.end(), v_len_bytes, v_len_bytes + sizeof(uint32_t));
        buffer.insert(buffer.end(), req.value.begin(), req.value.end());
    }

    return buffer;
}

std::optional<Request> Protocol::deserialize_request(const uint8_t* data, size_t size) {
    if (size < sizeof(uint8_t)) {
        return std::nullopt;
    }

    Request req;
    req.op = static_cast<OpCode>(data[0]);
    size_t offset = 1;

    switch (req.op) {
        case OpCode::PING:
        case OpCode::STATS:
            return req;

        case OpCode::GET:
        case OpCode::DEL: {
            if (offset + sizeof(uint32_t) > size) return std::nullopt;
            uint32_t key_len = *reinterpret_cast<const uint32_t*>(data + offset);
            offset += sizeof(uint32_t);

            if (offset + key_len > size) return std::nullopt;
            req.key.assign(reinterpret_cast<const char*>(data + offset), key_len);
            return req;
        }

        case OpCode::SET: {
            if (offset + sizeof(uint32_t) > size) return std::nullopt;
            uint32_t key_len = *reinterpret_cast<const uint32_t*>(data + offset);
            offset += sizeof(uint32_t);

            if (offset + key_len > size) return std::nullopt;
            req.key.assign(reinterpret_cast<const char*>(data + offset), key_len);
            offset += key_len;

            if (offset + sizeof(uint32_t) > size) return std::nullopt;
            uint32_t val_len = *reinterpret_cast<const uint32_t*>(data + offset);
            offset += sizeof(uint32_t);

            if (offset + val_len > size) return std::nullopt;
            req.value.assign(reinterpret_cast<const char*>(data + offset), val_len);
            return req;
        }

        default:
            return std::nullopt;
    }
}

std::vector<uint8_t> Protocol::serialize_response(const Response& res) {
    const uint32_t val_len = static_cast<uint32_t>(res.value.size());
    const uint32_t msg_len = static_cast<uint32_t>(res.message.size());
    const uint32_t payload_len = sizeof(uint8_t) + sizeof(uint32_t) + val_len + sizeof(uint32_t) + msg_len;

    std::vector<uint8_t> buffer;
    buffer.reserve(sizeof(uint32_t) + payload_len);

    const uint8_t* p_len_bytes = reinterpret_cast<const uint8_t*>(&payload_len);
    buffer.insert(buffer.end(), p_len_bytes, p_len_bytes + sizeof(uint32_t));

    buffer.push_back(static_cast<uint8_t>(res.status));

    const uint8_t* v_len_bytes = reinterpret_cast<const uint8_t*>(&val_len);
    buffer.insert(buffer.end(), v_len_bytes, v_len_bytes + sizeof(uint32_t));
    buffer.insert(buffer.end(), res.value.begin(), res.value.end());

    const uint8_t* m_len_bytes = reinterpret_cast<const uint8_t*>(&msg_len);
    buffer.insert(buffer.end(), m_len_bytes, m_len_bytes + sizeof(uint32_t));
    buffer.insert(buffer.end(), res.message.begin(), res.message.end());

    return buffer;
}

std::optional<Response> Protocol::deserialize_response(const uint8_t* data, size_t size) {
    if (size < sizeof(uint8_t)) {
        return std::nullopt;
    }

    Response res;
    res.status = static_cast<StatusCode>(data[0]);
    size_t offset = 1;

    if (offset + sizeof(uint32_t) > size) return std::nullopt;
    uint32_t val_len = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += sizeof(uint32_t);

    if (offset + val_len > size) return std::nullopt;
    res.value.assign(reinterpret_cast<const char*>(data + offset), val_len);
    offset += val_len;

    if (offset + sizeof(uint32_t) > size) return std::nullopt;
    uint32_t msg_len = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += sizeof(uint32_t);

    if (offset + msg_len > size) return std::nullopt;
    res.message.assign(reinterpret_cast<const char*>(data + offset), msg_len);

    return res;
}

} // namespace distributed_kv