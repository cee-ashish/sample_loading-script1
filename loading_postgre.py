import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from sqlalchemy import MetaData, Table
import pandas as pd


# data = pd.read_csv("sample_ais_data_with_duplicates (1).csv")

# database_url = f"postgresql://postgres:postgres@localhost:5432/test_db"

# engine = sa.create_engine(database_url)
# data.to_sql("ship_tracks", engine, schema="public", index=False, if_exists='append')




csv_file = "sample_ais_data_with_duplicates (1).csv"        
parquet_file = "data.parquet"  


df = pd.read_csv(csv_file)


df.to_parquet(parquet_file, engine="pyarrow", index=False)

print(f"CSV successfully converted to Parquet: {parquet_file}")
  