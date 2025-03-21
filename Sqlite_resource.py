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
        self.engine = create_engine(db_path)
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
     
       
        query = session.query(model)  
        


       
        if selected_columns_or_path and not isinstance(model, Table):
            query = query.with_entities(*selected_columns_or_path)

       
        if filters:
            query = query.filter(*[model.c[key] == value for key, value in filters.items()])
        
        if group_by:
            query = query.group_by(*group_by)

       
        if order_by:
            order_clause = desc(order_by) if order.lower() == "desc" else asc(order_by)
            query = query.order_by(order_clause)

        
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)

      
        if count:
            return [{"count": query.count()}]

       
        results = query.all()

        session.close()

      
        def model_to_dict(row):
            """Converts SQLAlchemy ORM objects and Table row results into dictionaries."""
            if hasattr(row, "__dict__"):  
                return {k: v for k, v in row.__dict__.items() if k != "_sa_instance_state"}
            elif hasattr(row, "_mapping"): 
                return dict(row._mapping)
            else: 
                return dict(row)

        data = [model_to_dict(row) for row in results]
      
       
        if convert_decimals:
            for row in data:
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)

      
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
