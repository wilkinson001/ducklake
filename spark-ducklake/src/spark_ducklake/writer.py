import itertools
import logging
from collections.abc import Generator

import pyarrow as pa
from pyspark.sql.datasource import (
    DataSourceWriter,
    DataSourceStreamWriter,
    WriterCommitMessage,
)
from pyspark.sql.types import StructType

from spark_ducklake.connection import DuckLakeConfig, quote_identifier
from spark_ducklake.pool import get_connection

logger = logging.getLogger(__name__)


def _iter_batches(iterator, batch_size: int) -> Generator[list, None, None]:
    while True:
        chunk = list(itertools.islice(iterator, batch_size))
        if not chunk:
            return
        yield chunk


def _insert_rows(conn, qualified_table: str, rows: list, schema: StructType):
    if not rows:
        return

    columns = [field.name for field in schema.fields]
    cols = list(zip(*rows))
    _batch = pa.table({name: list(col) for name, col in zip(columns, cols)})
    conn.execute(f"INSERT INTO {qualified_table} SELECT * FROM _batch")


def _merge_rows(
    conn, qualified_table: str, rows: list, schema: StructType, merge_keys: str
):
    if not rows:
        return

    columns = [field.name for field in schema.fields]
    cols = list(zip(*rows))
    _batch = pa.table({name: list(col) for name, col in zip(columns, cols)})

    keys = [k.strip() for k in merge_keys.split(",")]
    on_clause = " AND ".join(
        f"target.{quote_identifier(k)} = source.{quote_identifier(k)}" for k in keys
    )

    conn.execute(
        f"""MERGE INTO {qualified_table} AS target
            USING _batch AS source
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *"""
    )


class DuckLakeWriter(DataSourceWriter):
    def __init__(
        self,
        config: DuckLakeConfig,
        schema: str,
        table: str,
        spark_schema: StructType,
        write_mode: str = "append",
        merge_keys: str = "",
        batch_size: int = 10000,
    ):
        self.config = config
        self.schema = schema
        self.table = table
        self.spark_schema = spark_schema
        self.write_mode = write_mode
        self.merge_keys = merge_keys
        self.batch_size = batch_size

    @property
    def qualified_table(self) -> str:
        return f"my_lake.{quote_identifier(self.schema)}.{quote_identifier(self.table)}"

    def write(self, iterator):
        conn = get_connection(self.config)
        for rows in _iter_batches(iterator, self.batch_size):
            if self.write_mode == "merge":
                _merge_rows(
                    conn,
                    self.qualified_table,
                    rows,
                    self.spark_schema,
                    self.merge_keys,
                )
            else:
                _insert_rows(conn, self.qualified_table, rows, self.spark_schema)
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
        schema: str,
        table: str,
        spark_schema: StructType,
        write_mode: str,
        merge_keys: str,
        overwrite: bool,
        batch_size: int = 10000,
    ):
        self.config = config
        self.schema = schema
        self.table = table
        self.spark_schema = spark_schema
        self.write_mode = write_mode
        self.merge_keys = merge_keys
        self.overwrite = overwrite
        self.batch_size = batch_size

    @property
    def qualified_table(self) -> str:
        return f"my_lake.{quote_identifier(self.schema)}.{quote_identifier(self.table)}"

    def write(self, iterator):
        conn = get_connection(self.config)
        if self.overwrite:
            conn.execute(f"DELETE FROM {self.qualified_table}")
        for rows in _iter_batches(iterator, self.batch_size):
            if self.write_mode == "merge":
                _merge_rows(
                    conn,
                    self.qualified_table,
                    rows,
                    self.spark_schema,
                    self.merge_keys,
                )
            else:
                _insert_rows(conn, self.qualified_table, rows, self.spark_schema)
        return WriterCommitMessage()

    def commit(self, messages, batchId):
        pass

    def abort(self, messages, batchId):
        logger.warning(
            f"DuckLakeStreamWriter.abort called for batch {batchId} "
            "— successful executor writes cannot be rolled back"
        )
