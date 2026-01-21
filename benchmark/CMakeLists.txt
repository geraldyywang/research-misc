# CMAKE FOR LOCAL TESTING

cmake_minimum_required(VERSION 3.20)
project(benchmark LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

add_subdirectory(external/tomlplusplus)
add_subdirectory(external/duckdb)

add_executable(benchmark src/main.cc)

target_link_libraries(benchmark
  PRIVATE
    Arrow::arrow_static
    Parquet::parquet_static
    tomlplusplus::tomlplusplus
    duckdb
)
