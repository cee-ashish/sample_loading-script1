import pandas as pd
from parquet_resoures import ParquetLoader
from postgres_reource import PostgresLoader  
from Duckdb_resourcers import DuckDBLoader
from Sqlite_resource import SQLiteLoader
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from sqlalchemy import MetaData, Table


def main():
   
    csv_file = "ship_data.csv"  
    DB_URL = "postgresql://postgres:postgres@localhost:5432/test_db"
    SQLITE_DB_PATH = "sqlite:///sample.sqlite"
    DUCKDB_PATH = "my_database.duckdb"

    engine = sa.create_engine(DB_URL)

    metadata = MetaData()
   

    metadata.reflect(bind=engine)
    users_table = metadata.tables.get("ship_tracks")
    if users_table is None:
        raise ValueError("Table 'ship_data' does not exist in the database")

   
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    df = pd.read_csv(csv_file)
    
    sqlite_resource = SQLiteLoader(db_path=SQLITE_DB_PATH)
    duckdb_resource = DuckDBLoader(db_path=DUCKDB_PATH)
    postgres_resource = PostgresLoader(session=session)
    parquet_resource = ParquetLoader(storage_path="data.parquet")
    
    data = duckdb_resource.load_data(
            model=users_table,  # Update with the actual SQLAlchemy model or table reference
            selected_columns_or_path= None,  
            time_bucket= None, 
            group_by = None,
            area_scope=None,
            filters= None,  
            limit= None,
            offset=None,
            order_by=None,
            order=None,
            distinct=None,
            only_latest=None,
            log_statement=False,
            log_sample_values=False,
            pretty_print=False,
            
        )
    print(f"Total records loaded from database: {len(data)}{data}")

    data = [
        { "id": 811,
    "trackname": "Vessel A",
    "latitude": 9.017601117281243,
    "longitude": -164.20747226465468,
    "course": 355.3257932654318,
    "speed": 3.5168679594234726,
    "height_depth": 14.86946835309863,
    "mmsi_no": 564383546,
    "imo": 6878518,
    "cargo_type": "Bulk",
    "length": 326,
    "width": 18,
    "name": "Explorer",
    "timestamp_updated": "2024-01-01 00:00:00"}
    ]

    
    result = duckdb_resource.upsert_data("ship_data", data, id_fields=["id"], unique_fields=["mmsi_no"], no_update_cols = ["trackname"], return_counts=True)
    print(result)




if __name__ == "__main__":
    main()
