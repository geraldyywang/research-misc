#include "loaders.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <duckdb.hpp>

namespace rmisc::benchmark {

namespace fs = std::filesystem;

namespace {

std::string ToDuckDBType(const ColumnSpec& Col) {
  switch (Col.type) {
    case ColumnType::Int32:
      return "INTEGER";
    case ColumnType::Int64:
      return "BIGINT";
    case ColumnType::Double:
      return "DOUBLE";
    case ColumnType::String:
      return "VARCHAR";
    case ColumnType::Date32:
      return "DATE";
    case ColumnType::Decimal128:
      return "DECIMAL(" + std::to_string(Col.precision) + "," + std::to_string(Col.scale) + ")";
    default:
      return "VARCHAR";
  }
}

std::string BuildCreateTableSql(const TableSpec& Table) {
  std::string createSql{"CREATE TABLE " + Table.name + " ("};
  for (size_t i{0}; i < Table.columns.size(); ++i) {
    createSql += Table.columns[i].name + " " + ToDuckDBType(Table.columns[i]);
    if (i + 1 < Table.columns.size()) {
      createSql += ", ";
    }
  }
  createSql += ");";
  return createSql;
}

std::string BuildLoadSql(const TableSpec& Table, const std::string& Format, const fs::path& FilePath) {
  const std::string pathStr{FilePath.string()};

  if (Format == "parquet") {
    return "COPY " + Table.name + " FROM '" + pathStr + "' (FORMAT PARQUET)";
  }
  if (Format == "csv") {
    return "COPY " + Table.name + " FROM '" + pathStr +
           "' (FORMAT CSV, DELIMITER ',', HEADER TRUE)";
  }
  if (Format == "tbl") {
    return "COPY " + Table.name + " FROM '" + pathStr +
           "' (FORMAT CSV, DELIMITER '|', HEADER FALSE)";
  }
  if (Format == "arrow" || Format == "arrows" || Format == "feather") {
    return "INSERT INTO " + Table.name + " SELECT * FROM '" + pathStr + "'";;
  }

  return {};
}

}  // namespace

void RunBenchmark(const std::vector<TableSpec>& Tables, const fs::path& SummaryCsv, int NTrials) {
  duckdb::DuckDB Db{nullptr};
  duckdb::Connection Con{Db};

  // Db.LoadExtension<duckdb::ArrowExtension>();

  // auto arrowRes{Con.Query("LOAD arrow;")};
  // if (arrowRes->HasError()) {
  //   std::cerr << "Warning: Could not load arrow extension: " << arrowRes->GetError() << std::endl;
  // }

  Con.Query("LOAD parquet;");

  const std::array<std::string, 5> Formats{
      "parquet",
      "arrow",
      "arrows",
      "csv",
      "tbl",
  };

  std::unordered_map<std::string, std::unordered_map<std::string, double>> timeResults;
  std::unordered_map<std::string, std::unordered_map<std::string, int>> numDataPoints;

  for (const auto& Table : Tables) {
    const std::string createSql{BuildCreateTableSql(Table)};

    for (const auto& Format : Formats) {
      const fs::path filePath{"tpch_data/" + Table.name + "." + Format};
      const std::string loadSql{BuildLoadSql(Table, Format, filePath)};
      if (loadSql.empty()) {
        continue;
      }

      for (int trial{0}; trial < NTrials; ++trial) {
        try {
          Con.Query("DROP TABLE IF EXISTS " + Table.name);
          Con.Query(createSql);

          const auto start{std::chrono::high_resolution_clock::now()};

          auto loadRes{Con.Query(loadSql)};
          if (loadRes->HasError()) {
            throw std::runtime_error(loadRes->GetError());
          }

          const auto end{std::chrono::high_resolution_clock::now()};
          const std::chrono::duration<double, std::milli> elapsed{end - start};

          const double prevAvg{timeResults[Table.name][Format]};
          const int prevCount{numDataPoints[Table.name][Format]};

          if (prevAvg >= 0.0) {
            timeResults[Table.name][Format] =
                (prevAvg * prevCount + elapsed.count()) / (prevCount + 1);
            numDataPoints[Table.name][Format] = prevCount + 1;
          }
        } catch (const std::exception& E) {
          std::cerr << "Error benchmarking " << Format << " for " << Table.name << ": " << E.what()
                    << std::endl;
          timeResults[Table.name][Format] = -1.0;
        }
      }
    }
  }

  std::ofstream out{SummaryCsv};
  out << "table_name,parquet,arrow,arrows,csv,tbl\n";

  for (const auto& Table : Tables) {
    out << Table.name;
    for (const auto& Format : Formats) {
      out << "," << std::fixed << std::setprecision(4) << timeResults[Table.name][Format];
    }
    out << "\n";
  }
}

}  // namespace rmisc::benchmark