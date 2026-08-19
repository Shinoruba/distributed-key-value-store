# CMakeLists.txt for DistributedKVStore

set(CORE_SOURCES
    "src/storage_engine.cpp"
    "include/storage_engine.hpp"
    "src/wal.cpp"
    "include/wal.hpp"
    "src/protocol.cpp"
    "include/protocol.hpp"
    "src/tcp_session.cpp"
    "include/tcp_session.hpp"
    "src/tcp_server.cpp"
    "include/tcp_server.hpp"
    "src/raft_rpc.cpp"
    "include/raft_rpc.hpp"
    "src/raft_node.cpp"
    "include/raft_node.hpp"
    "include/raft_types.hpp"
)

# Core static library
add_library(kvstore_core STATIC ${CORE_SOURCES})

target_include_directories(kvstore_core PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}"
)

target_link_libraries(kvstore_core PUBLIC asio)
target_compile_features(kvstore_core PUBLIC cxx_std_17)

if (MSVC)
    target_compile_options(kvstore_core PRIVATE /W4 /permissive-)
else()
    target_compile_options(kvstore_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

# 1. Server Daemon Executable
add_executable(kvstore-server "src/server_main.cpp")
target_link_libraries(kvstore-server PRIVATE kvstore_core)
target_compile_features(kvstore-server PRIVATE cxx_std_17)

# 2. Interactive CLI Client Executable
add_executable(kvstore-cli "src/cli_main.cpp")
target_link_libraries(kvstore-cli PRIVATE kvstore_core)
target_compile_features(kvstore-cli PRIVATE cxx_std_17)

# 3. Test Suite Executable
add_executable(kvstore-tests "DistributedKVStore.cpp" "DistributedKVStore.h")
target_link_libraries(kvstore-tests PRIVATE kvstore_core)
target_compile_features(kvstore-tests PRIVATE cxx_std_17)

# Backward-compatibility alias executable
add_executable(DistributedKVStore "DistributedKVStore.cpp" "DistributedKVStore.h")
target_link_libraries(DistributedKVStore PRIVATE kvstore_core)
target_compile_features(DistributedKVStore PRIVATE cxx_std_17)
