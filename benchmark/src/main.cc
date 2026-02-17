#include <filesystem>
#include <iostream>
#include <string>

#include "converters.h"
#include "loaders.h"

int main(int Argc, char* Argv[]) {
  using namespace rmisc::benchmark;
  namespace fs = std::filesystem;

  bool genOtherFmts {true};
  bool runBenchmark {true};

  if (Argc >= 2) {
    genOtherFmts = false, runBenchmark = false;

    if (Argv[1] == "gen") {
      genOtherFmts = true;
    }
    if (Argv[1] == "benchmark") {
      runBenchmark = true;
    }
  }

  const auto cwd{fs::current_path()};
  const auto tableSpecs{CreateTables(cwd / "tpch_data" / "benchmark_config.toml")};

  if (genOtherFmts) {
    for (const auto& tableSpec : tableSpecs) {
      const auto batch{BuildTable(tableSpec)};

      BatchToParquet(batch, cwd / "tpch_data" / (tableSpec.name + ".parquet"));
      std::cout << "Created " << tableSpec.name << ".parquet\n";

      BatchToArrow(batch, cwd / "tpch_data" / (tableSpec.name + ".arrow"));
      std::cout << "Created " << tableSpec.name << ".arrow\n";

      BatchToArrows(batch, cwd / "tpch_data" / (tableSpec.name + ".arrows"));
      std::cout << "Created " << tableSpec.name << ".arrows\n";

      BatchToCSV(batch, cwd / "tpch_data" / (tableSpec.name + ".csv"));
      std::cout << "Created " << tableSpec.name << ".csv\n";
    }
  }

  if (runBenchmark) {
    std::cout << "Starting benchmark\n";
    RunBenchmark(tableSpecs, cwd / "results.csv", 10);
  }

  return 0;
}