#include "loaders.h"

#include <duckdb.hpp>
#include <fstream>
#include <iostream>
#include <unordered_map>

namespace rmisc::benchmark {

namespace fs = std::filesystem;

namespace {

std::string ToDuckDBType(const ColumnSpec& col) {
  switch (col.type) {
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
      return "DECIMAL(" + std::to_string(col.precision) + "," +
             std::to_string(col.scale) + ")";
    default:
      return "VARCHAR";
  }
}

}  // namespace

void RunBenchmark(const std::vector<TableSpec>& tables,
                  const fs::path& summary_csv) {
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  const std::array<std::string, 5> formats{"parquet", "arrow", "arrows", "csv",
                                           "tbl"};

  std::unordered_map<std::string, std::unordered_map<std::string, double>>
      time_results;

  for (const auto& tspec : tables) {
    std::string create_sql{"CREATE TABLE " + tspec.name + " ("};
    for (size_t i = 0; i < tspec.columns.size(); ++i) {
      create_sql +=
          tspec.columns[i].name + " " + ToDuckDBType(tspec.columns[i]);
      if (i < tspec.columns.size() - 1) create_sql += ", ";
    }
    create_sql += ");";

    for (const auto& fmt : formats) {
      fs::path file_path{tspec.name + "." + fmt};

      std::string load_sql;
      if (fmt == "parquet") {
        load_sql = "COPY " + tspec.name + " FROM '" + file_path.string() +
                   "' (FORMAT PARQUET)";
      } else if (fmt == "csv") {
        load_sql = "COPY " + tspec.name + " FROM '" + file_path.string() +
                   "' (FORMAT CSV, DELIMITER '|')";
      } else {
        load_sql = "INSERT INTO " + tspec.name + " SELECT * FROM read_ipc('" +
                   file_path.string() + "')";
      }

      try {
        con.Query("DROP TABLE IF EXISTS " + tspec.name);
        con.Query(create_sql);

        auto start{std::chrono::high_resolution_clock::now()};

        auto res{con.Query(load_sql)};
        if (res->HasError()) {
          throw std::runtime_error(res->GetError());
        }

        auto end{std::chrono::high_resolution_clock::now()};
        std::chrono::duration<double, std::milli> elapsed{end - start};

        time_results[tspec.name][fmt] = elapsed.count();
      } catch (const std::exception& e) {
        std::cerr << "Error benchmarking " << fmt << " for " << tspec.name
                  << ": " << e.what() << std::endl;
        time_results[tspec.name][fmt] = -1.0;
      }
    }
  }

  std::ofstream out(summary_csv);
  out << "table_name,parquet,arrow,arrows,csv,tbl\n";
  for (const auto& tspec : tables) {
    out << tspec.name;
    for (const auto& f : formats) {
      out << "," << std::fixed << std::setprecision(4)
          << time_results[tspec.name][f];
    }
    out << "\n";
  }
}

}  // namespace rmisc::benchmark