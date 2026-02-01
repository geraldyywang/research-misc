cmake_minimum_required(VERSION 3.20)
project(benchmark LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# --- 1. DuckDB Global Configuration ---
# These MUST be set before add_subdirectory to prevent path resolution errors
set(DUCKDB_PATH "${CMAKE_CURRENT_SOURCE_DIR}/external/duckdb" CACHE PATH "Path to DuckDB" FORCE)
set(DUCKDB_SCRIPTS_DIR "${DUCKDB_PATH}/scripts" CACHE PATH "" FORCE)
set(DUCKDB_EXTENSION_SCRIPTS_DIR "${DUCKDB_SCRIPTS_DIR}" CACHE PATH "" FORCE)

# Standard DuckDB build flags
set(BUILD_UNITTESTS OFF CACHE INTERNAL "")
set(BUILD_SHELL OFF CACHE INTERNAL "")
set(ENABLE_EXTENSION_AUTOLOADING ON CACHE INTERNAL "")
set(ENABLE_EXTENSION_AUTOINSTALL ON CACHE INTERNAL "")
set(BUILD_EXTENSIONS "tpch;parquet;icu" CACHE INTERNAL "")

set(DUCKDB_EXTENSION_ARROW_LOAD_TYPE "STATIC" CACHE INTERNAL "")
set(BUILD_ARROW_EXTENSION ON CACHE INTERNAL "")

# --- 2. Dependencies ---
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(CURL REQUIRED)

# --- 3. Add Subdirectories ---
add_subdirectory(external/tomlplusplus)
add_subdirectory(external/duckdb)

# We set this again right before nanoarrow to ensure it doesn't try to find its own
set(DUCKDB_INCLUDE_DIR "${DUCKDB_PATH}/src/include" CACHE PATH "" FORCE)
add_subdirectory(external/duckdb-nanoarrow)

foreach(target nanoarrow_extension nanoarrow_loadable_extension)
    if(TARGET ${target})
        # Fix missing headers (arrow.hpp, etc)
        target_include_directories(${target} PRIVATE 
            "${DUCKDB_INCLUDE_DIR}"
            "${DUCKDB_PATH}/src"
            "${DUCKDB_PATH}/third_party/zstd/include"
        )
        
        # Fix ZSTD namespace error: 
        # By setting this ONLY for these targets, we match DuckDB's internal expectations
        target_compile_definitions(${target} PRIVATE DUCKDB_USE_REAL_ZSTD=1)
    endif()
endforeach()

# --- 4. Logic Library ---
add_library(bench_logic STATIC 
    src/converters.cc 
    src/loaders.cc
)

target_include_directories(bench_logic PUBLIC 
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${DUCKDB_PATH}/src/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/external/duckdb-nanoarrow/src/include"
)

target_link_libraries(bench_logic PUBLIC 
    arrow_static 
    parquet_static 
    tomlplusplus::tomlplusplus
    CURL::libcurl
    duckdb_static
)

# --- 5. Executables ---
add_executable(benchmark src/main.cc)
target_link_libraries(benchmark PRIVATE 
    bench_logic 
    duckdb_static 
    nanoarrow_extension
)

add_executable(gen_data src/generate_data.cc)
target_link_libraries(gen_data PRIVATE 
    bench_logic 
    duckdb_static 
    nanoarrow_extension
)

# --- 6. Manual Header Fixups ---
target_include_directories(nanoarrow_extension PRIVATE "${DUCKDB_PATH}/src/include")

message(STATUS "Arrow Version: ${Arrow_VERSION}")