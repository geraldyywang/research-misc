cmake_minimum_required(VERSION 3.20)
project(benchmark LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(BUILD_UNITTESTS OFF CACHE INTERNAL "")
set(BUILD_SHELL OFF CACHE INTERNAL "")
set(ENABLE_EXTENSION_AUTOLOADING ON CACHE INTERNAL "")
set(ENABLE_EXTENSION_AUTOINSTALL ON CACHE INTERNAL "")
set(BUILD_EXTENSIONS "tpch;parquet;arrow" CACHE INTERNAL "")

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(CURL REQUIRED)

add_subdirectory(external/tomlplusplus)
add_subdirectory(external/duckdb)

add_library(bench_logic STATIC 
    src/converters.cc 
    src/loaders.cc
)

target_include_directories(bench_logic PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    external/duckdb/src/include
)

target_link_libraries(bench_logic PUBLIC 
    arrow_static 
    parquet_static 
    tomlplusplus::tomlplusplus
    CURL::libcurl
)

add_executable(benchmark src/main.cc)
target_link_libraries(benchmark PRIVATE bench_logic duckdb)

add_executable(gen_data src/generate_data.cc)
target_link_libraries(gen_data PRIVATE bench_logic duckdb)

message(STATUS "Arrow Version: ${Arrow_VERSION}")
