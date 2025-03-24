import sqlite3
import pandas as pd
from typing import Any, List, Dict
from decimal import Decimal
from sqlalchemy import create_engine, desc, asc
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table
from sqlalchemy.sql import func, select
from sqlalchemy.sql.expression import over
import sqlalchemy as sa
import re

class SQLiteLoader:
    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize SQLiteLoader with an in-memory or file-based SQLite database.
        """
        self.engine = create_engine(db_path)
        self.Session = sessionmaker(bind=self.engine)
    
    def load_data(
    self,
    model: Any,
    filters: dict = None,
    area_scope: Any = None,
    selected_columns_or_path: list[Any] = None,
    limit: int = None,
    group_by: List[str] = None,
    order_by: str = None,
    order: str = "asc",
    convert_decimals: bool = True,
    offset: int = None,
    distinct: bool = False,
    time_bucket: dict = None,
    only_latest: dict = None,
    log_statement: bool = False,
    log_sample_values: bool = False,
    pretty_print: bool = True,
) -> List[Dict[str, Any]]:
        """
        Load data from an SQLite database with filtering, sorting, and grouping.
        """
        session = self.Session()
        query = session.query(model)  

        # Apply selected columns
        if selected_columns_or_path:
    # Ensure selected_columns_or_path contains SQLAlchemy column objects
            selected_columns = [
                getattr(model.c, col) if isinstance(model, Table) else getattr(model, col)
                for col in selected_columns_or_path
            ]
            query = query.with_entities(*selected_columns)

        # Apply filters
        if filters:
            query = query.filter(*[model.c[key] == value for key, value in filters.items()])
        
        # Apply group by
        if group_by:
            query = query.group_by(*group_by)

        # Apply ordering
        if order_by:
            order_clause = desc(order_by) if order.lower() == "desc" else asc(order_by)
            query = query.order_by(order_clause)

        # Apply limit and offset
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)

        # ✅ Apply "only_latest" logic (Latest record per unique `latest_on`)
        if only_latest:
            timestamp_column = only_latest.get("timestamp_column")  # e.g., "timestamp_updated"
            latest_on_column = only_latest.get("latest_on")  # e.g., "mmsi_no"

            if timestamp_column and latest_on_column:
                subquery = (
                    select(
                        model,  # Select all columns
                        func.row_number().over(
                            partition_by=model.c[latest_on_column], 
                            order_by=model.c[timestamp_column].desc()
                        ).label("rn")
                    ).alias("subq")
                )

                query = session.query(subquery).filter(subquery.c.rn == 1)  

        if time_bucket is not None and isinstance(time_bucket, dict):
            bucket_interval = time_bucket.get("bucket_interval")  
            bucket_timestamp = time_bucket.get("bucket_timestamp")
            distinct_column = time_bucket.get("distinct_column")

           
            bucket_timestamp_column = model.c[bucket_timestamp]
            distinct_column_ref = model.c[distinct_column]

        
            match = re.match(r"(\d+) hour", bucket_interval)
            hours = int(match.group(1)) if match else 1 

           
            bucket_expr = sa.func.strftime(
                "%Y-%m-%d %H:00:00",
                sa.func.datetime(bucket_timestamp_column, f"-{hours - 1} hours")
            ).label("time_bucket")

           
            query = session.query(distinct_column_ref).distinct(distinct_column_ref)

        # Execute query
        results = query.all()
        session.close()

        # Convert results to dictionary
        def model_to_dict(row):
            """Converts SQLAlchemy ORM objects and Table row results into dictionaries."""
            if hasattr(row, "__dict__"):  
                return {k: v for k, v in row.__dict__.items() if k != "_sa_instance_state"}
            elif hasattr(row, "_mapping"): 
                return dict(row._mapping)
            else: 
                return dict(row)

        data = [model_to_dict(row) for row in results]

        # Convert Decimal to float
        if convert_decimals:
            for row in data:
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)

        # Apply distinct manually
        if distinct:
            seen = set()
            unique_data = []
            for item in data:
                tuple_item = tuple(item.items())
                if tuple_item not in seen:
                    seen.add(tuple_item)
                    unique_data.append(item)
            return unique_data

        return data
