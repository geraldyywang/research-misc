#include <duckdb.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

void MustQuery(duckdb::Connection& Con, const std::string& Sql) {
  auto result{Con.Query(Sql)};
  if (result->HasError()) {
    throw std::runtime_error("Query Failed: " + Sql + "\nError: " + result->GetError());
  }
}

int main() {
  try {
    std::cout << "Generating TPC-H data (SF 1)..." << std::endl;

    fs::create_directories("tpch_data");

    duckdb::DuckDB Db{nullptr};
    duckdb::Connection Con{Db};

    std::cout << "Loading TPC-H extension..." << std::endl;
    MustQuery(Con, "LOAD tpch;");

    std::cout << "Generating data (SF 1)..." << std::endl;
    MustQuery(Con, "CALL dbgen(sf=1);");

    const std::vector<std::string> Tables{
        "lineitem",
        "orders",
        "customer",
        "part",
        "partsupp",
        "supplier",
        "nation",
        "region",
    };

    for (const auto& Table : Tables) {
      std::cout << "Exporting " << Table << "..." << std::endl;

      const std::string path{"tpch_data/" + Table + ".tbl"};

      //
      // Using CSV format with a specific delimiter to mimic .tbl files.
      //
      //

      const std::string sql{"COPY " + Table + " TO '" + path + "' (DELIMITER '|', HEADER FALSE);"};
      MustQuery(Con, sql);
    }

    std::cout << "Generating benchmark_config.toml..." << std::endl;

    std::string tomlContent{"[tables]\n"};

    for (const auto& Table : Tables) {
      auto result{Con.Query("PRAGMA table_info('" + Table + "');")};

      tomlContent += "\n[tables." + Table + "]\n";
      tomlContent += "tblPath = 'tpch_data/" + Table + ".tbl'\n";
      tomlContent += "columns = [\n";

      for (size_t rowIdx{0}; rowIdx < result->RowCount(); ++rowIdx) {
        const std::string name{result->GetValue(1, rowIdx).ToString()};
        const std::string dtype{result->GetValue(2, rowIdx).ToString()};

        std::string ctype{"string"};
        const int precision{12};
        const int scale{2};

        if (dtype.find("INT") != std::string::npos) {
          ctype = "int64";
        } else if (dtype.find("DOUBLE") != std::string::npos) {
          ctype = "double";
        } else if (dtype.find("DECIMAL") != std::string::npos) {
          ctype = "decimal128";
        } else if (dtype.find("DATE") != std::string::npos) {
          ctype = "date32";
        }

        tomlContent += "  { name = '" + name + "', type = '" + ctype + "'";
        if (ctype == "decimal128") {
          tomlContent += ", precision = " + std::to_string(precision) + ", scale = " +
                         std::to_string(scale);
        }
        tomlContent += " },\n";
      }

      tomlContent += "]\n";
    }

    std::ofstream out{"tpch_data/benchmark_config.toml"};
    out << tomlContent;

    std::cout << "Success! Files created in tpch_data/" << std::endl;
  } catch (const std::exception& E) {
    std::cerr << "CRITICAL ERROR: " << E.what() << std::endl;
    return 1;
  }

  return 0;
}