#include "converters.h"

#include <arrow/csv/writer.h>  // For CSV
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>  // For Feather/Arrow/Arrows
#include <arrow/util/logging.h>
#include <parquet/arrow/writer.h>  // For Parquet

#include <cinttypes>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <toml++/toml.hpp>
#include <utility>
#include <vector>

namespace rmisc::benchmark {

namespace fs = std::filesystem;

namespace {

const int64_t ARROW_RECORD_BATCH_MAX_CHUNK_SIZE{122880};

ColumnType ParseColumnType(const std::string& S) {
  if (S == "int32") {
    return ColumnType::Int32;
  }
  if (S == "int64") {
    return ColumnType::Int64;
  }
  if (S == "double") {
    return ColumnType::Double;
  }
  if (S == "string") {
    return ColumnType::String;
  }
  if (S == "date32") {
    return ColumnType::Date32;
  }
  if (S == "decimal128") {
    return ColumnType::Decimal128;
  }
  throw std::runtime_error("Unknown column type: " + S);
}

std::shared_ptr<arrow::DataType> ArrowTypeFromCol(const ColumnSpec& Col) {
  switch (Col.type) {
    case ColumnType::Int32:
      return arrow::int32();
    case ColumnType::Int64:
      return arrow::int64();
    case ColumnType::Double:
      return arrow::float64();
    case ColumnType::String:
      return arrow::utf8();
    case ColumnType::Date32:
      return arrow::date32();
    case ColumnType::Decimal128:
      return arrow::decimal128(Col.precision, Col.scale);
    default:
      throw std::runtime_error("Unknown column type");
  }
}

std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const ColumnSpec& Col) {
  switch (Col.type) {
    case ColumnType::Int32:
      return std::make_unique<arrow::Int32Builder>();
    case ColumnType::Int64:
      return std::make_unique<arrow::Int64Builder>();
    case ColumnType::Double:
      return std::make_unique<arrow::DoubleBuilder>();
    case ColumnType::String:
      return std::make_unique<arrow::StringBuilder>();
    case ColumnType::Date32:
      return std::make_unique<arrow::Date32Builder>();
    case ColumnType::Decimal128:
      return std::make_unique<arrow::Decimal128Builder>(
          arrow::decimal128(Col.precision, Col.scale));
    default:
      throw std::runtime_error("Unsupported column type");
  }
}

std::shared_ptr<arrow::Schema> MakeArrowSchema(const TableSpec& Table) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(Table.columns.size());

  for (const auto& Col : Table.columns) {
    fields.push_back(
        std::make_shared<arrow::Field>(Col.name, ArrowTypeFromCol(Col), false));
  }

  return std::make_shared<arrow::Schema>(fields);
}

std::vector<std::string> SplitPipe(const std::string& Line) {
  std::vector<std::string> fields;
  std::stringstream ss{Line};
  std::string item;

  while (std::getline(ss, item, '|')) {
    fields.push_back(item);
  }

  if (!fields.empty() && fields.back().empty()) {
    fields.pop_back();
  }

  return fields;
}

//
// Convert "YYYY-MM-DD" to days since epoch (1970-01-01).
// Uses a purely arithmetic approach to avoid timezone issues with std::mktime.
//
//
int32_t ParseDateToDays(const std::string& DateStr) {
  int y{};
  int m{};
  int d{};
  if (std::sscanf(DateStr.c_str(), "%d-%d-%d", &y, &m, &d) != 3) {
    throw std::runtime_error("Invalid date format: " + DateStr);
  }

  //
  // Adjust months for the algorithm (Jan/Feb become 13/14 of previous year).
  //
  //

  if (m < 3) {
    m += 12;
    y -= 1;
  }

  //
  // Calculate days (valid for Gregorian calendar).
  //
  //

  const int32_t days{static_cast<int32_t>(365 * y + y / 4 - y / 100 + y / 400 +
                                          (153 * m + 8) / 5 + d - 1)};

  //
  // Subtract days for 1970-01-01 (719468 in this calculation) to get epoch
  // days.
  //
  //

  return days - 719468;
}

arrow::Status AppendFieldToBuilder(arrow::ArrayBuilder* Builder,
                                   const ColumnSpec& ColSpec,
                                   const std::string& Field) {
  if (Field.empty() || Field == "\\N") {
    return Builder->AppendNull();
  }

  switch (ColSpec.type) {
    case ColumnType::Int32:
      return static_cast<arrow::Int32Builder*>(Builder)->Append(
          std::stoi(Field));

    case ColumnType::Int64:
      return static_cast<arrow::Int64Builder*>(Builder)->Append(
          std::stoll(Field));

    case ColumnType::Double:
      return static_cast<arrow::DoubleBuilder*>(Builder)->Append(
          std::stod(Field));

    case ColumnType::String:
      return static_cast<arrow::StringBuilder*>(Builder)->Append(Field);

    case ColumnType::Date32:
      try {
        return static_cast<arrow::Date32Builder*>(Builder)->Append(
            ParseDateToDays(Field));
      } catch (...) {
        return arrow::Status::Invalid("Date parsing failed for: " + Field);
      }

    case ColumnType::Decimal128: {
      arrow::Decimal128 val{};
      int32_t parsedPrecision{};
      int32_t parsedScale{};
      auto st{arrow::Decimal128::FromString(Field, &val, &parsedPrecision,
                                            &parsedScale)};

      auto rescaledVal{val.Rescale(parsedScale, ColSpec.scale)};
      if (!rescaledVal.ok()) {
        return arrow::Status::Invalid("Could not rescale '" + Field +
                                      "' to scale " +
                                      std::to_string(ColSpec.scale));
      }

      return static_cast<arrow::Decimal128Builder*>(Builder)->Append(val);
    }

    default:
      return arrow::Status::Invalid("Unsupported column type");
  }
}

}  // namespace

std::vector<TableSpec> CreateTables(const fs::path& ConfigPath) {
  toml::table root{toml::parse_file(ConfigPath.string())};
  auto* tables{root["tables"].as_table()};
  if (!tables) {
    throw std::runtime_error("Missing [tables]");
  }

  std::vector<TableSpec> tableSpecs;

  for (auto&& [tableName, tableNode] : *tables) {
    auto* table{tableNode.as_table()};
    if (!table) {
      continue;
    }

    TableSpec tableSpec{};
    tableSpec.name = std::string(tableName.str());
    tableSpec.tblPath = table->at("tblPath").value<std::string>().value();

    auto* columns{table->at("columns").as_array()};
    if (!columns) {
      throw std::runtime_error("Missing columns for table: " + tableSpec.name);
    }

    for (auto&& columnNode : *columns) {
      auto* column{columnNode.as_table()};
      if (!column) {
        continue;
      }

      ColumnSpec colSpec{};
      colSpec.name = column->at("name").value<std::string>().value();
      colSpec.type =
          ParseColumnType(column->at("type").value<std::string>().value());

      if (colSpec.type == ColumnType::Decimal128) {
        colSpec.precision = column->at("precision").value<int>().value();
        colSpec.scale = column->at("scale").value<int>().value();
      }

      tableSpec.columns.push_back(std::move(colSpec));
    }

    tableSpecs.push_back(std::move(tableSpec));
  }

  return tableSpecs;
}

std::shared_ptr<arrow::Table> BuildTable(const TableSpec& TableSpec) {
  auto schema{MakeArrowSchema(TableSpec)};
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  builders.reserve(TableSpec.columns.size());

  for (const auto& Col : TableSpec.columns) {
    builders.push_back(MakeBuilder(Col));
  }

  std::ifstream in{TableSpec.tblPath};
  if (!in) {
    throw std::runtime_error("Failed to open tbl file: " +
                             TableSpec.tblPath.string());
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

  std::string line;
  int64_t rowCount{0};

  auto flush{[&]() {
    if (rowCount == 0) {
      return;
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& builder : builders) {
      std::shared_ptr<arrow::Array> arr;
      auto st{builder->Finish(&arr)};
      if (!st.ok()) {
        throw std::runtime_error("Finish() failed: " + st.ToString());
      }
      arrays.push_back(std::move(arr));
    }
    batches.push_back(arrow::RecordBatch::Make(schema, rowCount, arrays));
    rowCount = 0;
  }};

  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }

    if (line.back() == '\r') {
      line.pop_back();
    }

    const auto fields{SplitPipe(line)};
    if (fields.size() != TableSpec.columns.size()) {
      throw std::runtime_error("Field count mismatch in " +
                               TableSpec.tblPath.string() + ": got " +
                               std::to_string(fields.size()) + ", expected " +
                               std::to_string(TableSpec.columns.size()));
    }

    for (size_t i{0}; i < TableSpec.columns.size(); ++i) {
      const auto st{AppendFieldToBuilder(builders[i].get(),
                                         TableSpec.columns[i], fields[i])};
      if (!st.ok()) {
        throw std::runtime_error("Append failed for column " +
                                 TableSpec.columns[i].name + ": " +
                                 st.ToString());
      }
    }

    ++rowCount;
    if (rowCount >= ARROW_RECORD_BATCH_MAX_CHUNK_SIZE) {
      flush();
    }
  }

  return arrow::Table::FromRecordBatches(schema, batches).ValueOrDie();
}

void BatchToParquet(const std::shared_ptr<arrow::Table> Table,
                    const fs::path& OutPath) {
  auto outfile =
      arrow::io::FileOutputStream::Open(OutPath.string()).ValueOrDie();
  //
  // Parquet WriteTable handles chunks automatically.
  // It will create one Parquet RowGroup per Arrow RecordBatch.
  //
  //
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(*Table, arrow::default_memory_pool(), outfile,
                                 ARROW_RECORD_BATCH_MAX_CHUNK_SIZE));
}

void BatchToArrow(const std::shared_ptr<arrow::Table> Table,
                  const fs::path& OutPath) {
  auto outfile =
      arrow::io::FileOutputStream::Open(OutPath.string()).ValueOrDie();
  //
  // The File format (.arrow) supports multiple batches
  //
  //
  auto writer =
      arrow::ipc::MakeFileWriter(outfile, Table->schema()).ValueOrDie();
  ARROW_CHECK_OK(writer->WriteTable(*Table));
  ARROW_CHECK_OK(writer->Close());
}

void BatchToArrows(const std::shared_ptr<arrow::Table> Table,
                   const fs::path& OutPath) {
  auto outfile =
      arrow::io::FileOutputStream::Open(OutPath.string()).ValueOrDie();
  //
  // The Stream format (.arrows) writes batches one after another
  //
  //
  auto writer =
      arrow::ipc::MakeStreamWriter(outfile, Table->schema()).ValueOrDie();
  ARROW_CHECK_OK(writer->WriteTable(*Table));
  ARROW_CHECK_OK(writer->Close());
}

void BatchToCSV(const std::shared_ptr<arrow::Table> Table,
                const fs::path& OutPath) {
  auto outfile =
      arrow::io::FileOutputStream::Open(OutPath.string()).ValueOrDie();
  //
  // CSV writer will iterate through all chunks in the table
  //
  //
  auto status = arrow::csv::WriteCSV(
      *Table, arrow::csv::WriteOptions::Defaults(), outfile.get());
  ARROW_CHECK_OK(status);
}

}  // namespace rmisc::benchmark