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


void StreamTableToFormats(const TableSpec& tableSpec, const fs::path& dataDir) {
    auto schema {MakeArrowSchema(tableSpec)};
    auto pool {arrow::default_memory_pool()};

    
    auto pqFile {arrow::io::FileOutputStream::Open((dataDir / (tableSpec.name + ".parquet")).string()).ValueOrDie()};
    auto pqWriter {parquet::arrow::FileWriter::Open(
        *schema, 
        pool, 
        pqFile, 
        parquet::default_writer_properties(),
        parquet::default_arrow_writer_properties()
    ).ValueOrDie()};

    auto csvFile {arrow::io::FileOutputStream::Open((dataDir / (tableSpec.name + ".csv")).string()).ValueOrDie()};
    
    auto arrowFile {arrow::io::FileOutputStream::Open((dataDir / (tableSpec.name + ".arrow")).string()).ValueOrDie()};
    auto arrowWriter {arrow::ipc::MakeFileWriter(arrowFile, schema).ValueOrDie()};

    auto arrowsFile {arrow::io::FileOutputStream::Open((dataDir / (tableSpec.name + ".arrows")).string()).ValueOrDie()};
    auto arrowsWriter {arrow::ipc::MakeStreamWriter(arrowsFile, schema).ValueOrDie()};


    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    for (const auto& col : tableSpec.columns) {
        builders.push_back(MakeBuilder(col));
    }

    
    std::ifstream in{tableSpec.tblPath};
    std::string line;
    int64_t rowCount {};

    auto flushAndWrite {[&]() {
        if (rowCount == 0) return;

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (auto& builder : builders) {
            std::shared_ptr<arrow::Array> arr;
            ARROW_CHECK_OK(builder->Finish(&arr));
            arrays.push_back(std::move(arr));
        }

        auto batch {arrow::RecordBatch::Make(schema, rowCount, arrays)};
        auto table {arrow::Table::FromRecordBatches(schema, {batch}).ValueOrDie()};

        ARROW_CHECK_OK(pqWriter->WriteTable(*table, ARROW_RECORD_BATCH_MAX_CHUNK_SIZE));
        ARROW_CHECK_OK(arrowWriter->WriteTable(*table));
        ARROW_CHECK_OK(arrow::csv::WriteCSV(*table, arrow::csv::WriteOptions::Defaults(), csvFile.get()));
        ARROW_CHECK_OK(arrowsWriter->WriteTable(*table));

        rowCount = 0;
    }};

    while (std::getline(in, line)) {
        if (line.empty()) continue;
        if (line.back() == '\r') line.pop_back();

        const auto fields = SplitPipe(line);
        for (size_t i = 0; i < tableSpec.columns.size(); ++i) {
            ARROW_CHECK_OK(AppendFieldToBuilder(builders[i].get(), tableSpec.columns[i], fields[i]));
        }

        if (++rowCount >= ARROW_RECORD_BATCH_MAX_CHUNK_SIZE) {
            flushAndWrite();
        }
    }

    flushAndWrite();

    ARROW_CHECK_OK(pqWriter->Close());
    ARROW_CHECK_OK(arrowWriter->Close());
    ARROW_CHECK_OK(arrowsWriter->Close());
}

}  // namespace rmisc::benchmark