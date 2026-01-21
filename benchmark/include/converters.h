#ifndef RESEARCH_MISC__BENCHMARK_CONVERTERS_H_
#define RESEARCH_MISC__BENCHMARK_CONVERTERS_H_

#include <arrow/api.h>

#include <filesystem>
#include <string>
#include <vector>

namespace rmisc::benchmark {

enum class ColumnType { Int32, Int64, Double, String, Date32, Decimal128 };

struct ColumnSpec {
  std::string name;
  ColumnType type;

  //
  // Used for Decimal128 type
  //
  //
  int precision{};
  int scale{};
};

struct TableSpec {
  std::string name;
  std::filesystem::path tbl_path;
  std::vector<ColumnSpec> columns;
};

std::vector<TableSpec> create_tables(const std::filesystem::path& config_path);

std::shared_ptr<arrow::RecordBatch> build_record_batch(const TableSpec& tspec);

// Parquet
void BatchToParquet(const std::shared_ptr<arrow::RecordBatch> batch, const std::filesystem::path& out_path);

// Arrow
void BatchToArrow(const std::shared_ptr<arrow::RecordBatch>, const std::filesystem::path& out_path);

// Arrows
void BatchToArrows(const std::shared_ptr<arrow::RecordBatch>, const std::filesystem::path& out_path);

// CSV
void BatchToCSV(const std::shared_ptr<arrow::RecordBatch>, const std::filesystem::path& out_path);

}  // namespace rmisc::benchmark

#endif