#include <duckdb.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>

namespace fs = std::filesystem;

int main() {
    try {
        std::cout << "Generating TPC-H data (SF 1)..." << std::endl;
        
        // 1. Setup DuckDB Connection
        duckdb::DuckDB db(nullptr); // in-memory
        duckdb::Connection con(db);

        // 2. Install and Load TPC-H
        con.Query("INSTALL tpch; LOAD tpch;");
        con.Query("CALL dbgen(sf=1);");

        std::vector<std::string> tables = {
            "lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"
        };

        fs::create_directories("tpch_data");

        // 3. Export Tables to .tbl (CSV format)
        for (const auto& table : tables) {
            std::string path = "tpch_data/" + table + ".tbl";
            std::string sql = "COPY " + table + " TO '" + path + "' (DELIMITER '|', HEADER FALSE);";
            con.Query(sql);
        }

        std::cout << "Generating benchmark_config.toml..." << std::endl;
        std::string toml_content = "[tables]\n";

        // 4. Introspect Schema for each table
        for (const auto& table : tables) {
            auto result = con.Query("PRAGMA table_info('" + table + "');");
            
            toml_content += "\n[tables." + table + "]\ntbl_path = 'tpch_data/" + table + ".tbl'\ncolumns = [\n";

            // PRAGMA table_info columns: 0:cid, 1:name, 2:type, 3:notnull, 4:dflt_value, 5:pk
            for (size_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
                std::string name = result->GetValue(1, row_idx).ToString();
                std::string dtype = result->GetValue(2, row_idx).ToString();

                std::string ctype = "string";
                int precision = 12;
                int scale = 2;

                if (dtype.find("INT") != std::string::npos) {
                    ctype = "int64";
                } else if (dtype.find("DOUBLE") != std::string::npos) {
                    ctype = "double";
                } else if (dtype.find("DECIMAL") != std::string::npos) {
                    ctype = "decimal128";
                } else if (dtype.find("DATE") != std::string::npos) {
                    ctype = "date32";
                }

                toml_content += "  { name = '" + name + "', type = '" + ctype + "'";
                if (ctype == "decimal128") {
                    toml_content += ", precision = " + std::to_string(precision) + 
                                    ", scale = " + std::to_string(scale);
                }
                toml_content += " },\n";
            }
            toml_content += "]\n";
        }

        // 5. Write TOML to file
        std::ofstream out("tpch_data/benchmark_config.toml");
        out << toml_content;
        out.close();

        std::cout << "Done!" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}