import os
import pandas as pd
import pyarrow.parquet as pq
import sqlalchemy as sa
from sqlalchemy.orm import Session
from typing import Any, List, Dict

class PostgresLoader:
    def __init__(self, session: Session):
        self.session = session
        self.logger = None  

    def load_data(
        self,
        model: Any,
        selected_columns_or_path: list[Any],
        time_bucket: Any,
        area_scope: Any,
        filters: Any,
        limit: int = None,
        offset: int = None,  
        order_by: str = None,
        order: str = "asc",  
        distinct: bool = False,
        only_latest: dict = None,
        log_statement: bool = (
            os.getenv("LOG_SQL_STATEMENTS", "False").lower() == "true"
        ),
        log_sample_values: bool = False,
        pretty_print: bool = True,
        logger=None,
    ) -> List[Dict[str, Any]]:
        """
        Load data from a PostgreSQL database with filtering, sorting, and grouping.
        """
        query = sa.select(model)

        if selected_columns_or_path:
            query_columns = []
            for item in selected_columns_or_path:
                if isinstance(item, str):
                   
                    query_columns.append(model.c[item])
                elif isinstance(item, tuple):
                    col_name, func = item
                    column_attr = model.c[col_name]  
                    query_columns.append(self.apply_function(column_attr, func))
            
            # logger.debug(f"Selected columns: {query_columns}")
            query = sa.select(*query_columns)
        else:
            query = sa.select(model)

        if filters:
            conditions = []
            for key, value in filters.items():
                conditions.append(model.c[key] == value) 
            if area_scope:
                conditions.append(model.c.area == area_scope)
            query = query.where(*conditions)

        if time_bucket is not None and isinstance(time_bucket, dict):
                bucket_interval = time_bucket.get("bucket_interval")
                bucket_timestamp = time_bucket.get("bucket_timestamp")
                bucket_timestamp_column = getattr(model, bucket_timestamp)
                distinct_on = time_bucket.get("distinct_column")
                distinct_on_column = getattr(model, distinct_on)
                query = query.distinct(
                    sa.func.time_bucket(bucket_interval, bucket_timestamp_column),
                    distinct_on_column,
                )
        if distinct:
            query = query.distinct()

        if only_latest:
            time_col = model.c[only_latest["timestamp_column"]]
            latest_col = model.c[only_latest["latest_on"]]
            query = query.distinct(latest_col).order_by(latest_col, sa.desc(time_col))

        if order_by:
            order_func = sa.asc if order == "asc" else sa.desc
            query = query.order_by(order_func(model.c[order_by]))

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        result_set = self.session.execute(query).mappings().all()
        return [dict(row) for row in result_set]


