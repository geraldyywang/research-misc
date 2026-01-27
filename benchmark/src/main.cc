#include <filesystem>
#include <iostream>
#include <string>

#include "converters.h"
#include "loaders.h"

int main(int Argc, char* Argv[]) {
  (void)Argc;
  (void)Argv;

  using namespace rmisc::benchmark;
  namespace fs = std::filesystem;

  const auto cwd{fs::current_path()};

  const auto tableSpecs{CreateTables(cwd / "tpch_data" / "benchmark_config.toml")};
  for (const auto& tableSpec : tableSpecs) {
    const auto batch{BuildRecordBatch(tableSpec)};

    BatchToParquet(batch, cwd / "tpch_data" / (tableSpec.name + ".parquet"));
    std::cout << "Created " << tableSpec.name << ".parquet\n";

    BatchToArrow(batch, cwd / "tpch_data" / (tableSpec.name + ".arrow"));
    std::cout << "Created " << tableSpec.name << ".arrow\n";

    BatchToArrows(batch, cwd / "tpch_data" / (tableSpec.name + ".arrows"));
    std::cout << "Created " << tableSpec.name << ".arrows\n";

    BatchToCSV(batch, cwd / "tpch_data" / (tableSpec.name + ".csv"));
    std::cout << "Created " << tableSpec.name << ".csv\n";
  }

  std::cout << "Starting benchmark\n";
  RunBenchmark(tableSpecs, cwd / "results.csv", 100);

  return 0;
}