import os
import pandas as pd
import pyarrow.parquet as pq
import sqlalchemy as sa
from sqlalchemy.orm import Session
from typing import Any, List, Dict

class PostgresLoader:
    def __init__(self, session: Session):
        self.session = session
        self.logger = None  # Define a logger if needed

    def load_data(
        self,
        model: Any,
        selected_columns_or_path: Any,
        time_bucket: Any,
        area_scope: Any,
        filters: Any,
        limit: int = None,
        offset: int = None,  
        order_by: str = None,
        order: str = "asc",  
        distinct: bool = False,
        only_latest: dict = None,
        convert_decimals: bool = True,
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

        if filters:
            conditions = []
            for key, value in filters.items():
                conditions.append(model.c[key] == value)  # Use `model.c[key]`
            if area_scope:
                conditions.append(model.c.area == area_scope)
            query = query.where(*conditions)

        if time_bucket:
            query = query.distinct(
                sa.func.time_bucket(time_bucket, model.timestamp_updated),
                getattr(model, "mmsi"),
            )

        if distinct:
            query = query.distinct()

        if only_latest:
            time_col = getattr(model, only_latest["timestamp_column"])
            latest_col = getattr(model, only_latest["latest_on"])
            query = query.distinct(latest_col).order_by(latest_col, sa.desc(time_col))

        if order_by:
            order_func = sa.asc if order == "asc" else sa.desc
            query = query.order_by(order_func(getattr(model, order_by)))

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        result_set = self.session.execute(query).mappings().all()
        return [dict(row) for row in result_set]


