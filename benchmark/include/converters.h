#ifndef RESEARCH_MISC__BENCHMARK_CONVERTERS_H_
#define RESEARCH_MISC__BENCHMARK_CONVERTERS_H_

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

namespace rmisc::benchmark {

enum class ColumnType { Int32, Int64, Double, String, Date32, Decimal128 };

struct ColumnSpec {
  std::string name;
  ColumnType type;

  //
  // Used for Decimal128 type.
  //
  //

  int precision{};
  int scale{};
};

struct TableSpec {
  std::string name;
  std::filesystem::path tblPath;
  std::vector<ColumnSpec> columns;
};

std::vector<TableSpec> CreateTables(const std::filesystem::path& ConfigPath);

void StreamTableToFormats(const TableSpec& tableSpec, const std::filesystem::path& dataDir);

}  // namespace rmisc::benchmark

#endif  // RESEARCH_MISC__BENCHMARK_CONVERTERS_H_