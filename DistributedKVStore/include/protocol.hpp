#pragma once

#ifdef ERROR
#undef ERROR
#endif

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace distributed_kv {

enum class OpCode : uint8_t {
    PING = 1,
    SET = 2,
    GET = 3,
    DEL = 4,
    STATS = 5,
    TTL = 6
};

enum class StatusCode : uint8_t {
    OK = 0,
    NOT_FOUND = 1,
    ERR = 2
};

struct Request {
    OpCode op{OpCode::PING};
    std::string key;
    std::string value;
    uint64_t ttl_ms{0};

    static Request make_ping();
    static Request make_set(std::string key, std::string value, uint64_t ttl_ms = 0);
    static Request make_get(std::string key);
    static Request make_del(std::string key);
    static Request make_stats();
    static Request make_ttl(std::string key);
};

struct Response {
    StatusCode status{StatusCode::OK};
    std::string value;
    std::string message;

    static Response ok(std::string value = "", std::string message = "OK");
    static Response not_found(std::string message = "Key not found");
    static Response error(std::string message);
};

class Protocol {
public:
    static std::vector<uint8_t> serialize_request(const Request& req);
    static std::optional<Request> deserialize_request(const uint8_t* data, size_t size);

    static std::vector<uint8_t> serialize_response(const Response& res);
    static std::optional<Response> deserialize_response(const uint8_t* data, size_t size);
};

} // namespace distributed_kv