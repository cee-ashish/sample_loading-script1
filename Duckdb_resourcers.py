import duckdb
import pandas as pd
from typing import Any, List, Dict
from datetime import datetime
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
        filters: dict = None,
        limit: int = None,
        offset: int = None,
        group_by: List[str] = None,
        order_by: str = None,
        order: str = "asc",
        distinct: bool = False,
        only_latest: Dict[str, str] = None,
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
            bucket_interval = time_bucket.get("bucket_interval")  
            bucket_timestamp = time_bucket.get("bucket_timestamp")  
            distinct_column = time_bucket.get("distinct_column") 

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
        
        if group_by:
            group_by_clause = ", ".join(group_by)

           
            result = self.conn.execute(f"PRAGMA table_info('ship_data')").fetchall()
            all_columns = [row[1] for row in result]  
            updated_columns = [
                col if col in group_by else f"ANY_VALUE({col}) AS {col}"
                for col in all_columns
            ]
            
            query = f"SELECT {', '.join(updated_columns)} FROM ship_data GROUP BY {group_by_clause}"



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
    

    def upsert_data(self, table_name, data, id_fields, unique_fields, no_update_cols=None, return_counts=False):
        """
        Upserts data into DuckDB table with an additional check for unique fields.

        :param table_name: Name of the table
        :param data: List of dictionaries containing the data to insert or update
        :param id_fields: List of column names that act as unique identifiers
        :param unique_fields: Additional unique constraints
        :param no_update_cols: List of column names that should not be updated
        :param return_counts: Boolean, whether to return the number of inserted and updated rows
        :return: Dictionary with success status and counts of inserted/updated rows
        """
        if not data:
            return {"success": False, "message": "No data provided", "inserted_rows": 0, "updated_rows": 0}

        if no_update_cols is None:
            no_update_cols = []

        try:
           
            for record in data:
                if isinstance(record["timestamp_updated"], str):
                    record["timestamp_updated"] = datetime.fromisoformat(record["timestamp_updated"])

          
            existing_records = {}
            cursor = self.conn.cursor()

            for record in data:
                unique_filter = " AND ".join([f"{col} = ?" for col in unique_fields])
                query = f"SELECT *, timestamp_updated FROM {table_name} WHERE {unique_filter}"
                result = cursor.execute(query, [record[col] for col in unique_fields]).fetchall()

                if result:
                    existing_timestamp = result[0][-1]  

                    if isinstance(existing_timestamp, str):
                        existing_timestamp = datetime.fromisoformat(existing_timestamp)

                    existing_records[tuple(record[col] for col in unique_fields)] = (result[0], existing_timestamp)

            final_data = []
            for record in data:
                existing_record = existing_records.get(tuple(record[col] for col in unique_fields))

                if existing_record:
                    existing_timestamp = existing_record[1]

                    
                    if record["timestamp_updated"] > existing_timestamp:
                        final_data.append(record)
                else:
                    final_data.append(record)  

            if not final_data:
                return {"success": True, "message": "No updates needed", "inserted_rows": 0, "updated_rows": 0}

            update_cols = [col for col in data[0].keys() if col not in id_fields and col not in no_update_cols]

           
            placeholders = ", ".join(["?"] * len(final_data[0]))
            columns = ", ".join(final_data[0].keys())
            query = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"

            with self.conn:
                cursor.executemany(query, [tuple(record.values()) for record in final_data])

            return {"success": True, "inserted_rows": len(final_data), "updated_rows": len(final_data)}

        except Exception as e:
            return {"success": False, "message": str(e), "inserted_rows": 0, "updated_rows": 0}