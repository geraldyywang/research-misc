#include "converters.h"

#include <arrow/csv/writer.h>  // For CSV
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>  // For Feather/Arrow/Arrows
#include <arrow/util/logging.h>
#include <parquet/arrow/writer.h>  // For Parquet

#include <fstream>
#include <memory>
#include <sstream>
#include <toml++/toml.hpp>

namespace rmisc::benchmark {

namespace fs = std::filesystem;

namespace {

ColumnType ParseColumnType(const std::string& s) {
  if (s == "int32") return ColumnType::Int32;
  if (s == "int64") return ColumnType::Int64;
  if (s == "string") return ColumnType::String;
  if (s == "date32") return ColumnType::Date32;
  if (s == "decimal128") return ColumnType::Decimal128;
  throw std::runtime_error("Unknown column type: " + s);
}

std::shared_ptr<arrow::DataType> ArrowTypeFromCol(const ColumnSpec& col) {
  switch (col.type) {
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
      return arrow::decimal128(col.precision, col.scale);
    default:
      throw std::runtime_error("Unknown column type");
  }
}

std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const ColumnSpec& col) {
  switch (col.type) {
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
          arrow::decimal128(col.precision, col.scale));
    default:
      throw std::runtime_error("Unsupported column type");
  }
}

std::shared_ptr<arrow::Schema> MakeArrowSchema(const TableSpec& table) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(table.columns.size());

  for (const auto& col : table.columns) {
    fields.push_back(
        std::make_shared<arrow::Field>(col.name, ArrowTypeFromCol(col), false));
  }

  return std::make_shared<arrow::Schema>(fields);
}

std::vector<std::string> SplitPipe(const std::string& line) {
  std::vector<std::string> fields;
  std::stringstream ss(line);
  std::string item;

  while (std::getline(ss, item, '|')) {
    fields.push_back(item);
  }

  if (!fields.empty() && fields.back().empty()) {
    fields.pop_back();
  }
  return fields;
}

// HELPER: Convert "YYYY-MM-DD" string to days since Epoch (1970-01-01)
// We use a purely arithmetic approach to avoid Timezone issues with std::mktime
int32_t ParseDateToDays(const std::string& date_str) {
  int y, m, d;
  if (sscanf(date_str.c_str(), "%d-%d-%d", &y, &m, &d) != 3) {
    throw std::runtime_error("Invalid date format: " + date_str);
  }

  // Adjust months for the algorithm (Jan/Feb become 13/14 of previous year)
  if (m < 3) {
    m += 12;
    y -= 1;
  }

  // Calculate days (valid for Gregorian calendar)
  int32_t days{365 * y + y / 4 - y / 100 + y / 400 + (153 * m + 8) / 5 + d - 1};

  // Subtract days for 1970-01-01 (which is 719468 in this calculation) to get
  // Epoch
  return days - 719468;
}

arrow::Status AppendFieldToBuilder(arrow::ArrayBuilder* b,
                                   const ColumnSpec& cspec,
                                   const std::string& field) {
  if (field.empty() || field == "\\N") {
    return b->AppendNull();
  }

  switch (cspec.type) {
    case ColumnType::Int32:
      return static_cast<arrow::Int32Builder*>(b)->Append(std::stoi(field));

    case ColumnType::Int64:
      return static_cast<arrow::Int64Builder*>(b)->Append(std::stoll(field));

    case ColumnType::Double:
      return static_cast<arrow::DoubleBuilder*>(b)->Append(std::stod(field));

    case ColumnType::String:
      return static_cast<arrow::StringBuilder*>(b)->Append(field);

    case ColumnType::Date32:
      try {
        return static_cast<arrow::Date32Builder*>(b)->Append(
            ParseDateToDays(field));
      } catch (...) {
        return arrow::Status::Invalid("Date parsing failed for: " + field);
      }

    case ColumnType::Decimal128: {
      arrow::Decimal128 val{};
      int32_t precision{}, scale{};
      auto st{arrow::Decimal128::FromString(field, &val, &precision, &scale)};

      auto rescaled_val{val.Rescale(scale, cspec.scale)};
      if (!rescaled_val.ok()) {
        return arrow::Status::Invalid("Could not rescale '" + field +
                                      "' to scale " +
                                      std::to_string(cspec.scale));
      }

      return static_cast<arrow::Decimal128Builder*>(b)->Append(val);
    }

    default:
      return arrow::Status::Invalid("Unsupported column type");
  }
}

}  // namespace

std::vector<TableSpec> create_tables(const fs::path& config_path) {
  toml::table root{toml::parse_file(config_path.string())};
  auto* tables{root["tables"].as_table()};
  if (!tables) throw std::runtime_error("Missing [tables]");

  std::vector<TableSpec> table_specs;

  for (auto&& [table_name, table_node] : *tables) {
    auto* table{table_node.as_table()};
    if (!table) continue;

    TableSpec table_spec;
    table_spec.name = std::string(table_name.str());
    table_spec.tbl_path = table->at("tbl_path").value<std::string>().value();

    auto* columns{table->at("columns").as_array()};
    if (!columns) {
      throw std::runtime_error("Missing columns for table: " + table_spec.name);
    }

    for (auto&& column_node : *columns) {
      auto* column{column_node.as_table()};
      if (!column) continue;

      ColumnSpec col_spec;
      col_spec.name = column->at("name").value<std::string>().value();
      col_spec.type =
          ParseColumnType(column->at("type").value<std::string>().value());

      if (col_spec.type == ColumnType::Decimal128) {
        col_spec.precision = column->at("precision").value<int>().value();
        col_spec.scale = column->at("scale").value<int>().value();
      }

      table_spec.columns.push_back(std::move(col_spec));
    }

    table_specs.push_back(std::move(table_spec));
  }

  return table_specs;
}

std::shared_ptr<arrow::RecordBatch> build_record_batch(const TableSpec& tspec) {
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  builders.reserve(tspec.columns.size());
  for (const auto& col : tspec.columns) {
    builders.push_back(MakeBuilder(col));
  }

  std::ifstream in(tspec.tbl_path);
  if (!in) {
    throw std::runtime_error("Failed to open tbl file: " +
                             tspec.tbl_path.string());
  }

  std::string line;
  int64_t row_count{};

  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }

    if (line.back() == '\r') {
      line.pop_back();
    }

    auto fields{SplitPipe(line)};
    if (fields.size() != tspec.columns.size()) {
      throw std::runtime_error("Field count mismatch in " +
                               tspec.tbl_path.string() + ": got " +
                               std::to_string(fields.size()) + ", expected " +
                               std::to_string(tspec.columns.size()));
    }

    for (size_t i{}; i < tspec.columns.size(); ++i) {
      auto st{
          AppendFieldToBuilder(builders[i].get(), tspec.columns[i], fields[i])};
      if (!st.ok()) {
        throw std::runtime_error("Append failed for column " +
                                 tspec.columns[i].name + ": " + st.ToString());
      }
    }

    ++row_count;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(builders.size());

  for (auto& b : builders) {
    std::shared_ptr<arrow::Array> arr;
    auto st{b->Finish(&arr)};
    if (!st.ok()) {
      throw std::runtime_error("Finish() failed: " + st.ToString());
    }
    arrays.push_back(std::move(arr));
  }

  auto result{
      arrow::RecordBatch::Make(MakeArrowSchema(tspec), row_count, arrays)};
  return result;
}

void BatchToParquet(const std::shared_ptr<arrow::RecordBatch> batch,
                    const fs::path& out_path) {
  auto table{arrow::Table::FromRecordBatches({batch}).ValueOrDie()};
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  outfile = arrow::io::FileOutputStream::Open(out_path.string()).ValueOrDie();

  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile, batch->num_rows()));
}

void BatchToArrow(const std::shared_ptr<arrow::RecordBatch> batch,
                  const fs::path& out_path) {
  auto table{arrow::Table::FromRecordBatches({batch}).ValueOrDie()};
  auto outfile{
      arrow::io::FileOutputStream::Open(out_path.string()).ValueOrDie()};

  auto status{arrow::ipc::MakeFileWriter(outfile, batch->schema())};
  auto writer{status.ValueOrDie()};
  ARROW_CHECK_OK(writer->WriteTable(*table));
  ARROW_CHECK_OK(writer->Close());
}

void BatchToArrows(const std::shared_ptr<arrow::RecordBatch> batch,
                   const fs::path& out_path) {
  // IPC Stream Format
  auto outfile{
      arrow::io::FileOutputStream::Open(out_path.string()).ValueOrDie()};
  auto writer{
      arrow::ipc::MakeStreamWriter(outfile, batch->schema()).ValueOrDie()};
  ARROW_CHECK_OK(writer->WriteRecordBatch(*batch));
  ARROW_CHECK_OK(writer->Close());
}

void BatchToCSV(const std::shared_ptr<arrow::RecordBatch> batch,
                const fs::path& out_path) {
  auto outfile{
      arrow::io::FileOutputStream::Open(out_path.string()).ValueOrDie()};
  auto status{arrow::csv::WriteCSV(*batch, arrow::csv::WriteOptions::Defaults(),
                                   outfile.get())};
  ARROW_CHECK_OK(status);
}

}  // namespace rmisc::benchmark