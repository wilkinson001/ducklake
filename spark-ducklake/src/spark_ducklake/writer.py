import logging

import pyarrow as pa
from pyspark.sql.datasource import (
    DataSourceWriter,
    DataSourceStreamWriter,
    WriterCommitMessage,
)
from pyspark.sql.types import StructType

from spark_ducklake.connection import DuckLakeConfig
from spark_ducklake.pool import get_connection

logger = logging.getLogger(__name__)


def _insert_rows(conn, table: str, rows: list, schema: StructType):
    if not rows:
        return

    columns = [field.name for field in schema.fields]
    cols = list(zip(*rows))
    _batch = pa.table({name: list(col) for name, col in zip(columns, cols)})
    conn.execute(f"INSERT INTO my_lake.{table} SELECT * FROM _batch")


def _merge_rows(conn, table: str, rows: list, schema: StructType, merge_keys: str):
    if not rows:
        return

    columns = [field.name for field in schema.fields]
    cols = list(zip(*rows))
    _batch = pa.table({name: list(col) for name, col in zip(columns, cols)})

    keys = [k.strip() for k in merge_keys.split(",")]
    on_clause = " AND ".join(f"target.{k} = source.{k}" for k in keys)

    conn.execute(
        f"""MERGE INTO my_lake.{table} AS target
            USING _batch AS source
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *"""
    )


class DuckLakeWriter(DataSourceWriter):
    def __init__(
        self,
        config: DuckLakeConfig,
        table: str,
        schema: StructType,
        write_mode: str = "append",
        merge_keys: str = "",
    ):
        self.config = config
        self.table = table
        self.schema = schema
        self.write_mode = write_mode
        self.merge_keys = merge_keys

    def write(self, iterator):
        conn = get_connection(self.config)
        rows = list(iterator)
        if self.write_mode == "merge":
            _merge_rows(conn, self.table, rows, self.schema, self.merge_keys)
        else:
            _insert_rows(conn, self.table, rows, self.schema)
        return WriterCommitMessage()

    def commit(self, messages):
        pass

    def abort(self, messages):
        logger.warning(
            "DuckLakeWriter.abort called — writes already committed to DuckLake"
        )


class DuckLakeStreamWriter(DataSourceStreamWriter):
    def __init__(
        self,
        config: DuckLakeConfig,
        table: str,
        schema: StructType,
        write_mode: str,
        merge_keys: str,
        overwrite: bool,
    ):
        self.config = config
        self.table = table
        self.schema = schema
        self.write_mode = write_mode
        self.merge_keys = merge_keys
        self.overwrite = overwrite

    def write(self, iterator):
        conn = get_connection(self.config)
        rows = list(iterator)
        if self.overwrite:
            conn.execute(f"DELETE FROM my_lake.{self.table}")
        if self.write_mode == "merge":
            _merge_rows(conn, self.table, rows, self.schema, self.merge_keys)
        else:
            _insert_rows(conn, self.table, rows, self.schema)
        return WriterCommitMessage()

    def commit(self, messages, batchId):
        pass

    def abort(self, messages, batchId):
        logger.warning(
            f"DuckLakeStreamWriter.abort called for batch {batchId} "
            "— successful executor writes cannot be rolled back"
        )
