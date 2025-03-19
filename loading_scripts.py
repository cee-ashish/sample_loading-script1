import pandas as pd
from parquet_resoures import ParquetLoader
from postgres_reource import PostgresLoader  
from Duckdb_resourcers import DuckDBLoader
from Sqlite_resource import SQLiteLoader
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from sqlalchemy import MetaData, Table


def main():
    # Define the CSV file path
    csv_file = "sample_ais_data_with_duplicates (1).csv"  # Update this with your actual file path
    DB_URL = "postgresql://postgres:postgres@localhost:5432/test_db"
    SQLITE_DB_PATH = "sample.sqlite"
    DUCKDB_PATH = "my_database.duckdb"

    engine = sa.create_engine(DB_URL)

    metadata = MetaData()
    # users_table = Table("ship_data", metadata, autoload_with=engine)

    metadata.reflect(bind=engine)
    users_table = metadata.tables.get("ship_tracks")
    # # if users_table is None:
    # #     raise ValueError("Table 'ship_data' does not exist in the database")

   
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    df = pd.read_csv(csv_file)
    
    sqlite_resource = SQLiteLoader(db_path=SQLITE_DB_PATH)
    duckdb_resource = DuckDBLoader(db_path=DUCKDB_PATH)
    postgres_resource = PostgresLoader(session=session)
    parquet_resource = ParquetLoader(storage_path="data.parquet")
    
    data = parquet_resource.load_data(
            model="data.parquet",  # Update with the actual SQLAlchemy model or table reference
            selected_columns_or_path= None,  # Modify as needed
            time_bucket=None,  # Modify as needed
            area_scope=None,  # Modify as needed
            filters=  {'mmsi_no': '854636086'},  # Modify as needed
            limit=None,
            offset=None,
            order_by=None,
            order=None,
            distinct=None,
            only_latest=None,
            convert_decimals=False,
            log_statement=False,
            log_sample_values=False,
            pretty_print=False,
            # logger=None
        )
    print(f"Total records loaded from PostgreSQL/ Parquet: {len(data)}")

if __name__ == "__main__":
    main()
