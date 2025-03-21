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

class ParquetLoader:
    def __init__(self, storage_path: str):
        self.storage_path = storage_path
        self.logger = None  
    def _jsonlogic_to_pyarrow_filters(self, logic: Dict) -> List[Tuple[str, str, Any]]:
        """Convert jsonlogic rules to pyarrow filter tuples."""
        filters = []

        def parse_logic(logic):
            for operator, operands in logic.items():
                if operator in ["and", "or"]:
                    for operand in operands:
                        parse_logic(operand)
                else:
                    column = operands[0]["var"]
                    value = operands[1]
                    # Convert string to pyarrow timestamp if necessary
                    if isinstance(value, str) and column == "timestamp_updated":
                        value = maya.parse(value).datetime()
                        value = pa.scalar(value, type=pa.timestamp("us", tz="UTC"))
                    filters.append((column, operator, value))

        parse_logic(logic)
        return filters

    def load_data(
    self,
    model: Any,
    selected_columns_or_path: Any,
    time_bucket: Any = None,
    area_scope: Any = None,
    filters: Any = None,
    limit: int = None,
    offset: int = None,
    order_by: str = None,
    order: str = "asc",
    distinct: bool = False,
    only_latest: dict = None,
    convert_decimals: bool = True,
    log_statement: bool = False,
    log_sample_values: bool = False,
    pretty_print: bool = True,
    logger=None,
) -> List[Dict[str, Any]]:
    
    # Determine correct file path
        table_path = (
            self.storage_path if os.path.isfile(self.storage_path) 
            else os.path.join(self.storage_path, selected_columns_or_path)
        )

        # Ensure file exists
        if not os.path.exists(table_path):
            print(f"❌ Error: Parquet file '{table_path}' does not exist!")
            return []

        # Load Parquet file
        try:
            df = pq.read_table(table_path).to_pandas()
             # Debug first few rows

            # Apply filters
            if filters:
                for column, value in filters.items():
                    if column not in df.columns:
                        raise ValueError(f"Column '{column}' not found in DataFrame")

                    if isinstance(value, list):  # Handle 'IN' queries
                        df = df[df[column].isin(value)]

                    elif isinstance(value, dict):  # Handle comparison operators
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
                    else:  # Default equality check
                        df = df[df[column] == value]

            # Apply time bucketing
            if time_bucket and "timestamp_updated" in df.columns:
                df["timestamp_updated"] = pd.to_datetime(df["timestamp_updated"])
                df["time_bucket"] = df["timestamp_updated"].dt.floor(time_bucket)
                df = df.drop_duplicates(subset=["time_bucket"])

            # Keep only the latest records
            if only_latest:
                timestamp_column = only_latest["timestamp_column"]
                latest_on = only_latest["latest_on"]
                if timestamp_column in df.columns and latest_on in df.columns:
                    df[timestamp_column] = pd.to_datetime(df[timestamp_column])
                    df = df.sort_values(by=[latest_on, timestamp_column], ascending=[True, False])
                    df = df.drop_duplicates(subset=[latest_on], keep="first")
                else:
                    print(f"⚠ Warning: Columns '{timestamp_column}' or '{latest_on}' not found.")

            # Remove duplicates if distinct=True
            if distinct:
                df = df.drop_duplicates()

            # Apply sorting
            if order_by:
                df = df.sort_values(by=order_by, ascending=(order == "asc"))

            # Apply limit
            if limit:
                df = df.head(limit)

            return df.to_dict("records")

        except Exception as e:
            print(f"❌ Error loading Parquet file: {e}")
            return []

