#ifndef RESEARCH_MISC__BENCHMARK_LOADERS_H_
#define RESEARCH_MISC__BENCHMARK_LOADERS_H_

#include <filesystem>
#include <vector>

#include "converters.h"

namespace rmisc::benchmark {

void RunBenchmark(const std::vector<TableSpec>& tables,
                  const std::filesystem::path& summary_csv, int n_trials = 0);

// Parquet

// Feather

// Arrow

// Arrows

// CSV

// tbl

}  // namespace rmisc::benchmark

#endif