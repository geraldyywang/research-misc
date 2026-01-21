#include "converters.h"
#include "loaders.h"

#include <array>
#include <string>
#include <filesystem>

int main() {
    using namespace rmisc::benchmark;
    namespace fs = std::filesystem;

    auto cwd {fs::current_path()};

    auto table_specs {create_tables(cwd / "benchmark_config.toml")};
    for (const auto& table_spec : table_specs) {
        auto batch {build_record_batch(table_spec)};

        BatchToParquet(batch, cwd / (table_spec.name + ".parquet"));
        BatchToArrow(batch, cwd / (table_spec.name + ".arrow"));
        BatchToArrows(batch, cwd / (table_spec.name + ".arrows"));
        BatchToCSV(batch, cwd / (table_spec.name + ".csv"));
    }
    
    RunBenchmark(table_specs, cwd / "results.csv");
}