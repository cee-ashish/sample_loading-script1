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

    engine = sa.create_engine(SQLITE_DB_PATH)

    metadata = MetaData()
   

    metadata.reflect(bind=engine)
    users_table = metadata.tables.get("ship_data")
    if users_table is None:
        raise ValueError("Table 'ship_data' does not exist in the database")

   
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    df = pd.read_csv(csv_file)
    
    sqlite_resource = SQLiteLoader(db_path=SQLITE_DB_PATH)
    duckdb_resource = DuckDBLoader(db_path=DUCKDB_PATH)
    postgres_resource = PostgresLoader(session=session)
    parquet_resource = ParquetLoader(storage_path="data.parquet")
    
    # data = duckdb_resource.load_data(
    #         model=users_table,  # Update with the actual SQLAlchemy model or table reference
    #         selected_columns_or_path= None,  
    #         time_bucket= None, 
    #         group_by = None,
    #         area_scope=None,
    #         filters= None,  
    #         limit= None,
    #         offset=None,
    #         order_by=None,
    #         order=None,
    #         distinct=None,
    #         only_latest=None,
    #         log_statement=False,
    #         log_sample_values=False,
    #         pretty_print=False,
            
    #     )
    # print(f"Total records loaded from database: {len(data)}{data}")

    data = [
        { "id": 1221,
    "trackname": "Vessel djA",
    "latitude": 9.017601117281212243,
    "longitude": -164.2074722646541268,
    "course": 355.325793265431812,
    "speed": 3.516867959423472612,
    "height_depth": 14.869468353124309863,
    "mmsi_no": 1234512,
    "imo": 687851821,
    "cargo_type": "Bulk_",
    "length": 32612,
    "width": 181,
    "name": "Expl23orer",
    "timestamp_updated": "2025-01-01 00:00:00"}
    ]

    
    result = parquet_resource.upsert_data("/home/root1/AshishSherawat/sample_scripts/data.parquet", data, id_fields=["id"], unique_fields=None, no_update_cols = None, return_counts=True)
    print(result)




if __name__ == "__main__":
    main()
