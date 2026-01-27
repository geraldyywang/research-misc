#ifndef RESEARCH_MISC__BENCHMARK_LOADERS_H_
#define RESEARCH_MISC__BENCHMARK_LOADERS_H_

#include <filesystem>
#include <vector>

#include "converters.h"

namespace rmisc::benchmark {

void RunBenchmark(const std::vector<TableSpec>& Tables,
                  const std::filesystem::path& SummaryCsv,
                  int NTrials = 0);

}  // namespace rmisc::benchmark

#endif  // RESEARCH_MISC__BENCHMARK_LOADERS_H_