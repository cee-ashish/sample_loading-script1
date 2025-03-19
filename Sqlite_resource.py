import sqlite3
import pandas as pd
from typing import Any, List, Dict
from decimal import Decimal
from sqlalchemy import create_engine, desc, asc
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table

class SQLiteLoader:
    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize SQLiteLoader with an in-memory or file-based SQLite database.
        """
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.Session = sessionmaker(bind=self.engine)
    
    def load_data(
    self,
    model: Any,
    filters: Any = None,
    area_scope: Any = None,
    selected_columns_or_path: Any = None,
    limit: int = None,
    group_by: List[str] = None,
    order_by: str = None,
    order: str = "asc",
    convert_decimals: bool = True,
    offset: int = None,
    interval: str = None,
    uniq_trk_id: str = None,
    distinct: bool = False,
    count: bool = False,
    time_bucket: dict = None,
    only_latest: dict = None,
    distinct_on: List[str] = None,
    log_statement: bool = False,
    log_sample_values: bool = False,
    pretty_print: bool = True,
    logger: bool = False,
) -> List[Dict[str, Any]]:
        """
        Load data from an SQLite database with filtering, sorting, and grouping.
        """
        session = self.Session()

        # Check if model is a Table object (raw table) or ORM model
        if isinstance(model, Table):
            query = session.query(*model.columns)  # Query Table columns directly
        else:
            query = session.query(model)  # Query ORM model instances

        # Apply column selection only if model is not a Table object
        if selected_columns_or_path and not isinstance(model, Table):
            query = query.with_entities(*selected_columns_or_path)

        # Apply filters if provided
        if filters:
            query = query.filter(*filters)

        # Apply group by if provided
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

        # Return count if requested
        if count:
            return [{"count": query.count()}]

        # Execute query and fetch results
        results = query.all()
        session.close()

        # Convert results to dictionary format
        def model_to_dict(row):
            """Converts SQLAlchemy ORM objects and Table row results into dictionaries."""
            if hasattr(row, "__dict__"):  # ORM model
                return {k: v for k, v in row.__dict__.items() if k != "_sa_instance_state"}
            elif hasattr(row, "_mapping"):  # Raw Table query row
                return dict(row._mapping)
            else:  # Unexpected format (fallback)
                return dict(row)

        data = [model_to_dict(row) for row in results]

        # Convert Decimal values to float if enabled
        if convert_decimals:
            for row in data:
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)

        # Remove duplicates if distinct=True
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
