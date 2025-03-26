import os
import pandas as pd
import pyarrow.parquet as pq
import sqlalchemy as sa
from sqlalchemy.orm import Session
from typing import Any, List, Dict
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from datetime import datetime


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
        group_by: List[str] = None,
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

        if group_by:
            query = query.group_by(*[model.c[col] for col in group_by])

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        result_set = self.session.execute(query).mappings().all()
        return [dict(row) for row in result_set]

    def upsert_data(self, model, data, id_fields, unique_fields, no_update_cols=None, return_counts=False):
        """
        Upserts data into PostgreSQL table with an additional check for unique fields.

        """
        if not data:
            return {"success": False, "message": "No data provided", "inserted_rows": 0, "updated_rows": 0}

        if no_update_cols is None:
            no_update_cols = []

        try:
            
            for record in data:
                if isinstance(record["timestamp_updated"], str):
                    record["timestamp_updated"] = datetime.fromisoformat(record["timestamp_updated"])

           
            index_cols = [model.c[field] for field in id_fields + unique_fields]

            latest_records = {}
            for record in data:
                record_key = tuple(str(record[col]) for col in id_fields + unique_fields)
                if (
                    record_key not in latest_records or
                    record["timestamp_updated"] > latest_records[record_key]["timestamp_updated"]
                ):
                    latest_records[record_key] = record

            data = list(latest_records.values())

            update_cols = [
                c.name for c in model.columns
                if c.name not in id_fields and c.name not in no_update_cols
            ]

           
            existing_records = {
                tuple(getattr(row, col) for col in unique_fields): row
                for row in self.session.query(model).filter(
                    or_(*[and_(*(model.c[col] == record[col] for col in unique_fields)) for record in data])
                ).all()
            }

            final_data = []
            for record in data:
                existing_record = existing_records.get(tuple(record[col] for col in unique_fields))

                if existing_record:
                 
                    if record["timestamp_updated"] > existing_record.timestamp_updated:
                        final_data.append(record)
                else:
                   
                    final_data.append(record)

            if not final_data:
                return {"success": True, "message": "No updates needed", "inserted_rows": 0, "updated_rows": 0}

            stmt = insert(model).on_conflict_do_update(
                index_elements=id_fields + unique_fields,  
                set_={col: getattr(insert(model).excluded, col) for col in update_cols},
                where=(model.c.timestamp_updated < getattr(insert(model).excluded, "timestamp_updated")),
            )

            inserted_rows, updated_rows = 0, 0

            with self.session.begin():
                if return_counts:
                    existing_rows_before = self.session.query(model).filter(
                        or_(*[
                            and_(*(model.c[id_field] == str(record[id_field]) for id_field in id_fields))
                            for record in final_data
                        ])
                    ).count()

                self.session.execute(stmt, final_data)

                if return_counts:
                    existing_rows_after = self.session.query(model).filter(
                        or_(*[
                            and_(*(model.c[id_field] == str(record[id_field]) for id_field in id_fields))
                            for record in final_data
                        ])
                    ).count()

                    inserted_rows = existing_rows_after - existing_rows_before
                    updated_rows = len(final_data) - inserted_rows

            return {"success": True, "inserted_rows": inserted_rows, "updated_rows": updated_rows}
        except Exception as e:
            return {"success": False, "message": str(e), "inserted_rows": 0, "updated_rows": 0}

