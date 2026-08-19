# CMakeLists.txt for DistributedKVStore

add_executable(DistributedKVStore
    "DistributedKVStore.cpp"
    "DistributedKVStore.h"
    "src/storage_engine.cpp"
    "include/storage_engine.hpp"
)

target_include_directories(DistributedKVStore PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}"
)

target_compile_features(DistributedKVStore PUBLIC cxx_std_17)

if (MSVC)
    target_compile_options(DistributedKVStore PRIVATE /W4 /permissive-)
else()
    target_compile_options(DistributedKVStore PRIVATE -Wall -Wextra -Wpedantic)
endif()
