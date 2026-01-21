import duckdb
import os

print("Generating TPC-H data (SF 1)...")
con = duckdb.connect(':memory:')
con.execute("INSTALL tpch; LOAD tpch;")
con.execute("CALL dbgen(sf=1);")

tables = ["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"]
os.makedirs("tpch_data", exist_ok=True)

for table in tables:
    path = f"tpch_data/{table}.tbl"
    con.execute(f"COPY {table} TO '{path}' (DELIMITER '|', HEADER FALSE);")

print("Generating benchmark_config.toml...")
toml_content = "[tables]\n"

for table in tables:
    cols = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    toml_content += f"\n[tables.{table}]\ntbl_path = 'tpch_data/{table}.tbl'\ncolumns = [\n"
    
    for _, name, dtype, _, _, _ in cols:
        ctype = "string"
        precision, scale = 12, 2
        if "INT" in dtype: 
            ctype = "int64"
        elif "DOUBLE" in dtype: 
            ctype = "double"
        elif "DECIMAL" in dtype: 
            ctype = "decimal128"
        elif "DATE" in dtype: 
            ctype = "date32"    
        
        col_entry = f"  {{ name = '{name}', type = '{ctype}'"
        
        if ctype == "decimal128":
            col_entry += f", precision = {precision}, scale = {scale}"
        col_entry += " },"
        toml_content += col_entry + "\n"
    
    toml_content += "]\n"

with open("tpch_data/benchmark_config.toml", "w") as f:
    f.write(toml_content)

print("Done!")