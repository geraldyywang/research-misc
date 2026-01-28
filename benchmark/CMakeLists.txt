cmake_minimum_required(VERSION 3.20)
project(benchmark LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(BUILD_UNITTESTS OFF CACHE INTERNAL "")
set(BUILD_SHELL OFF CACHE INTERNAL "")
set(ENABLE_EXTENSION_AUTOLOADING ON CACHE INTERNAL "")
set(ENABLE_EXTENSION_AUTOINSTALL ON CACHE INTERNAL "")
set(BUILD_EXTENSIONS "tpch;parquet;icu" CACHE INTERNAL "")
set(DUCKDB_EXTENSION_ARROW_LOAD_TYPE "STATIC" CACHE INTERNAL "")
set(BUILD_ARROW_EXTENSION ON CACHE INTERNAL "")
set(DUCKDB_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/duckdb/src/include")
set(DUCKDB_SCRIPTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/duckdb/scripts")

add_definitions(-DDUCKDB_USE_REAL_ZSTD=1)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(CURL REQUIRED)

add_subdirectory(external/tomlplusplus)
add_subdirectory(external/duckdb)
add_subdirectory(external/duckdb-nanoarrow)

add_library(bench_logic STATIC 
    src/converters.cc 
    src/loaders.cc
)

target_include_directories(nanoarrow_extension PRIVATE "${DUCKDB_INCLUDE_DIR}")
target_include_directories(nanoarrow_loadable_extension PRIVATE 
    "${DUCKDB_INCLUDE_DIR}" 
    ${CMAKE_CURRENT_SOURCE_DIR}/external/duckdb/third_party/zstd/include
)
set_target_properties(nanoarrow_loadable_extension PROPERTIES DUCKDB_SCRIPTS_DIR "${DUCKDB_SCRIPTS_DIR}")

target_include_directories(bench_logic PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/external/duckdb/src/include
    ${CMAKE_CURRENT_SOURCE_DIR}/external/duckdb-nanoarrow/src/include
)

target_link_libraries(bench_logic PUBLIC 
    arrow_static 
    parquet_static 
    tomlplusplus::tomlplusplus
    CURL::libcurl
)

add_executable(benchmark src/main.cc)
target_link_libraries(benchmark PRIVATE bench_logic duckdb nanoarrow_extension)

add_executable(gen_data src/generate_data.cc)
target_link_libraries(gen_data PRIVATE bench_logic duckdb nanoarrow_extension)

message(STATUS "Arrow Version: ${Arrow_VERSION}")
