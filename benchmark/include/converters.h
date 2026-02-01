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

std::shared_ptr<arrow::Table> BuildTable(const TableSpec& TableSpec);

void BatchToParquet(const std::shared_ptr<arrow::Table> Table, const std::filesystem::path& OutPath);

void BatchToArrow(const std::shared_ptr<arrow::Table> Table, const std::filesystem::path& OutPath);

void BatchToArrows(const std::shared_ptr<arrow::Table> Table, const std::filesystem::path& OutPath);

void BatchToCSV(const std::shared_ptr<arrow::Table> Table, const std::filesystem::path& OutPath);

}  // namespace rmisc::benchmark

#endif  // RESEARCH_MISC__BENCHMARK_CONVERTERS_H_