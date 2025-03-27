import os
import pandas as pd
import pyarrow.parquet as pq
import sqlalchemy as sa
from sqlalchemy.orm import Session
from typing import Any, List, Dict
from typing import Any, List, Dict, Optional, Type, Union, Tuple
import maya
import pyarrow as pa
import pyarrow.parquet as pq

import pandas as pd


class ParquetLoader:
    def __init__(self, storage_path: str):
        self.storage_path = storage_path
        self.logger = None  
    

    def load_data(
    self,
    model: Any,
    selected_columns_or_path: Any,
    time_bucket: Any = None,
    area_scope: Any = None,
    group_by: List[str] = None,
    filters: dict = None,
    limit: int = None,
    offset: int = None,
    order_by: str = None,
    order: str = "asc",
    distinct: bool = False,
    only_latest: dict = None,
    log_statement: bool = False,
    log_sample_values: bool = False,
    pretty_print: bool = True,
    logger=None,
) -> List[Dict[str, Any]]:
    
   
        table_path = (
            self.storage_path if os.path.isfile(self.storage_path) 
            else os.path.join(self.storage_path, selected_columns_or_path)
        )
        print("parquet")
       
        if not os.path.exists(table_path):
            print(f"❌ Error: Parquet file '{table_path}' does not exist!")
            return []

       
        try:
            df = pq.read_table(table_path).to_pandas()
             

            if offset:
                raise ValueError("OFFSET will not work with parquet system")
           
            if filters:
                for column, value in filters.items():
                    if column not in df.columns:
                        raise ValueError(f"Column '{column}' not found in DataFrame")

                    if isinstance(value, list):  
                        df = df[df[column].isin(value)]

                    elif isinstance(value, dict):  
                        for op, val in value.items():
                            if op == "==":
                                df = df[df[column] == val]
                            elif op == "!=":
                                df = df[df[column] != val]
                            elif op == ">":
                                df = df[df[column] > val]
                            elif op == ">=":
                                df = df[df[column] >= val]
                            elif op == "<":
                                df = df[df[column] < val]
                            elif op == "<=":
                                df = df[df[column] <= val]
                            else:
                                raise ValueError(f"Unsupported operator: {op}")
                        return df
                    else: 
                        df = df[df[column] == value]

         
            if time_bucket and "timestamp_updated" in df.columns:
    
                df["timestamp_updated"] = pd.to_datetime(df["timestamp_updated"])

                bucket_interval = time_bucket.get("bucket_interval")  
                bucket_timestamp = time_bucket.get("bucket_timestamp")
                distinct_column = time_bucket.get("distinct_column")

                df["time_bucket"] = df[bucket_timestamp].dt.floor(bucket_interval)
                df = df.drop_duplicates(subset=["time_bucket", distinct_column])[[distinct_column]]

           
            if only_latest:
                timestamp_column = only_latest["timestamp_column"]
                latest_on = only_latest["latest_on"]
                if timestamp_column in df.columns and latest_on in df.columns:
                    df[timestamp_column] = pd.to_datetime(df[timestamp_column])
                    df = df.sort_values(by=[latest_on, timestamp_column], ascending=[True, False])
                    df = df.drop_duplicates(subset=[latest_on], keep="first")
                else:
                    print(f"⚠ Warning: Columns '{timestamp_column}' or '{latest_on}' not found.")

           
            if distinct:
                df = df.drop_duplicates()

            if group_by:
                df = df.groupby(group_by, as_index=False).agg(lambda x: x.iloc[0]) 

            if order_by:
                df = df.sort_values(by=order_by, ascending=(order == "asc"))

           
            if limit:
                df = df.head(limit)

            return df.to_dict("records")

        except Exception as e:
            print(f"❌ Error loading Parquet file: {e}")
            return []

    def _get_table_path(self, table_name: str) -> str:
        """Construct the path to the Parquet file for a given table name."""
        return os.path.join(self.base_path, f"{table_name}.parquet")

    def upsert_data(self, model, data, id_fields, unique_fields, no_update_cols, return_counts):
        """
        Upserts data into a Parquet file.
        
        - Updates existing records if they match `id_fields` or `unique_fields`
        - Inserts new records if they don’t exist
        - Prevents updating columns listed in `no_update_cols`
        
        :param model: Path to the Parquet file (acts as the table)
        :param data: List of dictionaries (records to upsert)
        :param id_fields: List of primary key columns
        :param unique_fields: List of unique identifier columns
        :param no_update_cols: List of columns that should NOT be updated
        :param return_counts: Boolean, whether to return inserted/updated row counts
        :return: Dictionary with success status and row counts (if return_counts is True)
        """
        
        table_path = model  
        new_data_df = pd.DataFrame(data)

       
        unique_fields = unique_fields if unique_fields else []
        subset_keys = id_fields + unique_fields 
        if os.path.exists(table_path):
           
            existing_data_df = pq.read_table(table_path).to_pandas()

          
            existing_row_count = len(existing_data_df)

           
            merged_df = pd.concat([existing_data_df, new_data_df]).drop_duplicates(
                subset=subset_keys, keep="last"
            )

           
            if "timestamp_updated" in merged_df.columns:
                merged_df = merged_df.sort_values(by="timestamp_updated").drop_duplicates(
                    subset=subset_keys, keep="last"
                )

           
            if no_update_cols:
                for col in no_update_cols:
                    if col in existing_data_df.columns:
                        merged_df[col] = existing_data_df[col]

           
            new_row_count = len(merged_df)
            inserted_rows = new_row_count - existing_row_count
            updated_rows = existing_row_count - len(existing_data_df)

        else:
           
            merged_df = new_data_df
            inserted_rows = len(new_data_df)
            updated_rows = 0

       
        merged_df.to_parquet(table_path, index=False)

        
        if return_counts:
            return {"success": True, "inserted_rows": inserted_rows, "updated_rows": updated_rows}
        
        return {"success": True}