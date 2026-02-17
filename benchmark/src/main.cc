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
    std::string mode(Argv[1]);
    genOtherFmts = (mode == "gen");
    runBenchmark = (mode == "bench");
  }

  const auto cwd{fs::current_path()};
  const auto tableSpecs{CreateTables(cwd / "tpch_data" / "benchmark_config.toml")};

  if (genOtherFmts) {
    for (const auto& tableSpec : tableSpecs) {
      StreamTableToFormats(tableSpec, cwd / "tpch_data");
      std::cout << "Created " << tableSpec.name << " files\n";
    }
  }

  if (runBenchmark) {
    std::cout << "Starting benchmark\n";
    RunBenchmark(tableSpecs, cwd / "results.csv", 10);
  }

  return 0;
}