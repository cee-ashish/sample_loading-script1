import pandas as pd
import sqlite3
import duckdb

def load_csv_to_dataframe(csv_path):
    """Reads a CSV file into a pandas DataFrame."""
    df = pd.read_csv(csv_path)
    return df

# def write_to_sqlite(df, db_path="sample.sqlite", table_name="ship_data"):
#     """Writes DataFrame to an SQLite database."""
#     conn = sqlite3.connect(db_path)
#     df.to_sql(table_name, conn, if_exists="replace", index=False)
#     conn.close()

# def write_to_duckdb(df, db_path="my_database.duckdb", table_name="ship_data"):
#     """Writes DataFrame to a DuckDB database."""
#     conn = duckdb.connect(db_path)
#     conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
#     conn.close()


# csv_file = "ship_data.csv"  # Replace with your CSV file path
# df = load_csv_to_dataframe(csv_file)

# # Write to databases
# write_to_sqlite(df)
# write_to_duckdb(df)

import pandas as pd

# Load Parquet file
df = pd.read_parquet("data.parquet")
print(df)
# Count rows
row_count = len(df)
print("Total rows:", row_count)


