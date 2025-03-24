import duckdb
import pandas as pd
from typing import Any, List, Dict

class DuckDBLoader:
    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize DuckDBLoader with an in-memory or file-based DuckDB instance.
        """
        self.conn = duckdb.connect(database=db_path)
        self.logger = None  
    
    def load_data(
        self,
        model: str,
        selected_columns_or_path: list[Any] = None,
        time_bucket: Dict[str, Any] = None,
        area_scope: Any = None,
        filters: Any = None,
        limit: int = None,
        offset: int = None,
        order_by: str = None,
        order: str = "asc",
        distinct: bool = False,
        only_latest: Dict[str, str] = None,
        convert_decimals: bool = True,
        log_statement: bool = False,
        log_sample_values: bool = False,
        pretty_print: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Load data from a DuckDB database with filtering, sorting, and grouping.
        """
        query = f"SELECT * FROM {model}"
        
        if selected_columns_or_path:
            columns = []
            for item in selected_columns_or_path:
                if isinstance(item, str):
                    columns.append(item)
                elif isinstance(item, tuple):
                    col_name, func = item
                    columns.append(f"{func}({col_name})")
            query = f"SELECT {', '.join(columns)} FROM {model}"
        
        if filters:
            conditions = []
            for column, value in filters.items():
                if isinstance(value, list):  
                    conditions.append(f"{column} IN ({', '.join(map(str, value))})")
                elif isinstance(value, dict):  
                    for op, val in value.items():
                        if op == "==":
                            conditions.append(f"{column} = {val}")
                        elif op == "!=":
                            conditions.append(f"{column} != {val}")
                        elif op == ">":
                            conditions.append(f"{column} > {val}")
                        elif op == ">=":
                            conditions.append(f"{column} >= {val}")
                        elif op == "<":
                            conditions.append(f"{column} < {val}")
                        elif op == "<=":
                            conditions.append(f"{column} <= {val}")
                else: 
                    conditions.append(f"{column} = {value}")

            query += " WHERE " + " AND ".join(conditions)
        
        if time_bucket:
            bucket_interval = time_bucket.get("bucket_interval")  # Example: "1 hour"
            bucket_timestamp = time_bucket.get("bucket_timestamp")  # Example: "timestamp_updated"
            distinct_column = time_bucket.get("distinct_column")  # Example: "mmsi_no"

            query = f"""
                SELECT DISTINCT 
                    time_bucket(INTERVAL '{bucket_interval}', CAST({bucket_timestamp} AS TIMESTAMP)) AS time_bucket,
                    {distinct_column}
                FROM {model}
            """
        
        if only_latest:
            timestamp_column = only_latest.get("timestamp_column")
            latest_on_column = only_latest.get("latest_on")

            query = f"""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {latest_on_column} ORDER BY {timestamp_column} DESC) AS rn
                    FROM {model}
                ) WHERE rn = 1
            """
        
        if distinct:
            query = query.replace("SELECT *", "SELECT DISTINCT *")
        
        if order_by:
            order_clause = "DESC" if order.lower() == "desc" else "ASC"
            query += f" ORDER BY {order_by} {order_clause}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        if offset:
            query += f" OFFSET {offset}"
        
        if log_statement:
            print(f"Executing Query: {query}")
        
        try:
            result_df = self.conn.execute(query).fetchdf()
            return result_df.to_dict("records")
        except Exception as e:
            print(f"Error executing query: {query}, Error: {str(e)}")
            raise
