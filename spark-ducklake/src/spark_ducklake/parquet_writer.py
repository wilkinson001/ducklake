import logging
import uuid
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructType,
    TimestampType,
)

from spark_ducklake.connection import DuckLakeConfig, quote_identifier
from spark_ducklake.pool import get_connection
from spark_ducklake.writer import _iter_batches

logger = logging.getLogger(__name__)

_SPARK_TO_ARROW: dict[type, pa.DataType] = {
    IntegerType: pa.int32(),
    LongType: pa.int64(),
    ShortType: pa.int16(),
    FloatType: pa.float32(),
    DoubleType: pa.float64(),
    StringType: pa.string(),
    BooleanType: pa.bool_(),
    DateType: pa.date32(),
    TimestampType: pa.timestamp("us"),
}


def _spark_to_pyarrow_type(spark_type):
    mapped = _SPARK_TO_ARROW.get(type(spark_type))
    if mapped is not None:
        return mapped
    if isinstance(spark_type, DecimalType):
        return pa.decimal128(spark_type.precision, spark_type.scale)
    return pa.string()


def _spark_schema_to_pyarrow(spark_schema: StructType) -> pa.Schema:
    fields = []
    for f in spark_schema.fields:
        pa_type = _spark_to_pyarrow_type(f.dataType)
        fields.append(pa.field(f.name, pa_type, nullable=f.nullable))
    return pa.schema(fields)


@dataclass
class DuckLakeCommitMessage(WriterCommitMessage):
    files: list[str] = field(default_factory=list)


class DuckLakeParquetWriter(DataSourceWriter):
    def __init__(
        self,
        config: DuckLakeConfig,
        dl_schema: str,
        table: str,
        spark_schema: StructType,
        data_path: str,
        job_id: str,
        overwrite: bool = False,
        max_records_per_file: int = 1_000_000,
    ):
        self.config = config
        self.dl_schema = dl_schema
        self.table = table
        self.spark_schema = spark_schema
        self.data_path = data_path
        self.job_id = job_id
        self.overwrite = overwrite
        self.max_records_per_file = max_records_per_file

    @property
    def qualified_table(self) -> str:
        return (
            f"my_lake.{quote_identifier(self.dl_schema)}.{quote_identifier(self.table)}"
        )

    def _build_s3_filesystem(self) -> pafs.S3FileSystem:
        kwargs = {}
        if self.config.s3_access_key and self.config.s3_secret_key:
            kwargs["access_key"] = self.config.s3_access_key
            kwargs["secret_key"] = self.config.s3_secret_key
        if self.config.s3_endpoint:
            kwargs["endpoint_override"] = self.config.s3_endpoint
        kwargs["scheme"] = "https" if self.config.s3_use_ssl else "http"
        return pafs.S3FileSystem(**kwargs)

    def _file_prefix(self) -> str:
        prefix = self.data_path
        if prefix.startswith("s3://"):
            prefix = prefix[len("s3://") :]
        if not prefix.endswith("/"):
            prefix += "/"
        return f"{prefix}{self.dl_schema}/{self.table}/"

    def write(self, iterator):
        fs = self._build_s3_filesystem()
        arrow_schema = _spark_schema_to_pyarrow(self.spark_schema)
        columns = [f.name for f in self.spark_schema.fields]
        file_prefix = self._file_prefix()
        written_files = []

        for rows in _iter_batches(iterator, self.max_records_per_file):
            cols = list(zip(*rows))
            arrow_table = pa.table(
                {name: list(col) for name, col in zip(columns, cols)},
                schema=arrow_schema,
            )
            file_id = uuid.uuid4()
            s3_key = f"{file_prefix}ducklake-{file_id}.parquet"
            pq.write_table(arrow_table, s3_key, filesystem=fs)
            written_files.append(f"s3://{s3_key}")

        return DuckLakeCommitMessage(files=written_files)

    def commit(self, messages):
        all_files = []
        for msg in messages:
            if msg is not None and hasattr(msg, "files"):
                all_files.extend(msg.files)

        if not all_files:
            if self.overwrite:
                conn = get_connection(self.config)
                conn.execute(f"DELETE FROM {self.qualified_table}")
            return

        conn = get_connection(self.config)
        conn.execute("BEGIN TRANSACTION")
        try:
            if self.overwrite:
                conn.execute(f"DELETE FROM {self.qualified_table}")

            file_list_sql = ", ".join(f"'{f}'" for f in all_files)
            conn.execute(
                f"CALL ducklake_add_data_files("
                f"'my_lake', "
                f"'{self.table}', "
                f"[{file_list_sql}], "
                f"schema => '{self.dl_schema}'"
                f")"
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def abort(self, messages):
        all_files = []
        for msg in messages:
            if msg is not None and hasattr(msg, "files"):
                all_files.extend(msg.files)

        if not all_files:
            return

        logger.warning(
            "DuckLakeParquetWriter.abort called — attempting to clean up %d orphaned files",
            len(all_files),
        )
        try:
            fs = self._build_s3_filesystem()
            for f in all_files:
                s3_key = f
                if s3_key.startswith("s3://"):
                    s3_key = s3_key[len("s3://") :]
                try:
                    fs.delete_file(s3_key)
                except Exception:
                    logger.warning("Failed to delete orphaned file: %s", f)
        except Exception:
            logger.warning("Failed to connect to S3 for orphan cleanup")
