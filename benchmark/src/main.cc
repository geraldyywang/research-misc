#include <array>
#include <filesystem>
#include <iostream>
#include <string>

#include "converters.h"
#include "loaders.h"

int main(int argc, char* argv[]) {
  using namespace rmisc::benchmark;
  namespace fs = std::filesystem;

  auto cwd{fs::current_path()};

  auto table_specs{create_tables(cwd / "tpch_data" / "benchmark_config.toml")};
  for (const auto& table_spec : table_specs) {
    auto batch{build_record_batch(table_spec)};

    BatchToParquet(batch, cwd / "tpch_data" / (table_spec.name + ".parquet"));
    std::cout << "Created " + table_spec.name + ".parquet\n";
    BatchToArrow(batch, cwd / "tpch_data" / (table_spec.name + ".arrow"));
    std::cout << "Created " + table_spec.name + ".arrow\n";
    BatchToArrows(batch, cwd / "tpch_data" / (table_spec.name + ".arrows"));
    std::cout << "Created " + table_spec.name + ".arrows\n";
    BatchToCSV(batch, cwd / "tpch_data" / (table_spec.name + ".csv"));
    std::cout << "Created " + table_spec.name + ".csv\n";
  }

  std::cout << "Starting benchmark\n";
  RunBenchmark(table_specs, cwd / "results.csv", 100);
}