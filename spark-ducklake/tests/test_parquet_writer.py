import logging
import pickle

import pyarrow as pa
import pytest
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_ducklake.connection import DuckLakeConfig
from spark_ducklake.parquet_writer import (
    DuckLakeCommitMessage,
    DuckLakeParquetWriter,
    _spark_schema_to_pyarrow,
    _spark_to_pyarrow_type,
)


SPARK_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]
)

CONFIG = DuckLakeConfig(
    postgres_conn="dbname=test",
    s3_endpoint="localhost:9000",
    s3_access_key="key",
    s3_secret_key="secret",
    s3_bucket="bucket",
)

CONFIG_NO_CREDS = DuckLakeConfig(
    postgres_conn="dbname=test",
    s3_bucket="bucket",
)


def test_parquet_writer_without_credentials_is_picklable():
    w = DuckLakeParquetWriter(
        config=CONFIG_NO_CREDS,
        dl_schema="main",
        table="test_table",
        spark_schema=SPARK_SCHEMA,
        data_path="s3://bucket/",
        job_id="test-job-id",
    )
    restored = pickle.loads(pickle.dumps(w))
    assert restored.config.s3_access_key is None
    assert restored.config.s3_secret_key is None
    assert restored.config.s3_endpoint is None


# Type mapping tests


@pytest.mark.parametrize(
    "spark_type, expected",
    [
        (IntegerType(), pa.int32()),
        (LongType(), pa.int64()),
        (ShortType(), pa.int16()),
        (FloatType(), pa.float32()),
        (DoubleType(), pa.float64()),
        (StringType(), pa.string()),
        (BooleanType(), pa.bool_()),
        (DateType(), pa.date32()),
        (TimestampType(), pa.timestamp("us")),
    ],
)
def test_spark_to_pyarrow_type(spark_type, expected):
    assert _spark_to_pyarrow_type(spark_type) == expected


def test_decimal_type_maps_to_decimal128():
    result = _spark_to_pyarrow_type(DecimalType(10, 2))
    assert result == pa.decimal128(10, 2)


def test_unknown_type_defaults_to_string():
    assert _spark_to_pyarrow_type(BinaryType()) == pa.string()


# Schema conversion tests


def test_spark_schema_to_pyarrow_basic():
    result = _spark_schema_to_pyarrow(SPARK_SCHEMA)
    assert result == pa.schema(
        [
            pa.field("id", pa.int32(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
        ]
    )


def test_spark_schema_to_pyarrow_preserves_nullable():
    schema = StructType([StructField("x", IntegerType(), nullable=False)])
    result = _spark_schema_to_pyarrow(schema)
    assert result.field("x").nullable is False


# CommitMessage tests


def test_commit_message_is_picklable():
    msg = DuckLakeCommitMessage(files=["s3://bucket/path/file.parquet"])
    restored = pickle.loads(pickle.dumps(msg))
    assert restored.files == ["s3://bucket/path/file.parquet"]


def test_commit_message_default_empty_files():
    msg = DuckLakeCommitMessage()
    assert msg.files == []


def test_commit_message_with_multiple_files():
    msg = DuckLakeCommitMessage(files=["a.parquet", "b.parquet", "c.parquet"])
    assert len(msg.files) == 3


# DuckLakeParquetWriter tests


def test_parquet_writer_is_picklable():
    w = DuckLakeParquetWriter(
        config=CONFIG,
        dl_schema="main",
        table="test_table",
        spark_schema=SPARK_SCHEMA,
        data_path="s3://bucket/",
        job_id="test-job-id",
    )
    restored = pickle.loads(pickle.dumps(w))
    assert restored.dl_schema == "main"
    assert restored.table == "test_table"
    assert restored.data_path == "s3://bucket/"
    assert restored.job_id == "test-job-id"
    assert restored.overwrite is False
    assert restored.max_records_per_file == 1_000_000


def test_parquet_writer_overwrite_is_picklable():
    w = DuckLakeParquetWriter(
        config=CONFIG,
        dl_schema="main",
        table="test_table",
        spark_schema=SPARK_SCHEMA,
        data_path="s3://bucket/",
        job_id="test-job-id",
        overwrite=True,
    )
    restored = pickle.loads(pickle.dumps(w))
    assert restored.overwrite is True


def test_parquet_writer_custom_max_records_is_picklable():
    w = DuckLakeParquetWriter(
        config=CONFIG,
        dl_schema="main",
        table="test_table",
        spark_schema=SPARK_SCHEMA,
        data_path="s3://bucket/",
        job_id="test-job-id",
        max_records_per_file=500,
    )
    restored = pickle.loads(pickle.dumps(w))
    assert restored.max_records_per_file == 500


def test_parquet_writer_qualified_table():
    w = DuckLakeParquetWriter(
        config=CONFIG,
        dl_schema="main",
        table="test_table",
        spark_schema=SPARK_SCHEMA,
        data_path="s3://bucket/",
        job_id="test-job-id",
    )
    assert w.qualified_table == 'my_lake."main"."test_table"'


def test_parquet_writer_file_prefix_strips_s3():
    w = DuckLakeParquetWriter(
        config=CONFIG,
        dl_schema="main",
        table="test_table",
        spark_schema=SPARK_SCHEMA,
        data_path="s3://bucket/",
        job_id="test-job-id",
    )
    assert w._file_prefix() == "bucket/main/test_table/"


def test_parquet_writer_file_prefix_handles_no_trailing_slash():
    w = DuckLakeParquetWriter(
        config=CONFIG,
        dl_schema="main",
        table="test_table",
        spark_schema=SPARK_SCHEMA,
        data_path="s3://bucket",
        job_id="test-job-id",
    )
    assert w._file_prefix() == "bucket/main/test_table/"


def test_parquet_writer_abort_with_no_files(caplog):
    w = DuckLakeParquetWriter(
        config=CONFIG,
        dl_schema="main",
        table="test_table",
        spark_schema=SPARK_SCHEMA,
        data_path="s3://bucket/",
        job_id="test-job-id",
    )
    with caplog.at_level(logging.WARNING):
        w.abort([])
    assert "orphaned" not in caplog.text


def test_parquet_writer_abort_with_none_messages(caplog):
    w = DuckLakeParquetWriter(
        config=CONFIG,
        dl_schema="main",
        table="test_table",
        spark_schema=SPARK_SCHEMA,
        data_path="s3://bucket/",
        job_id="test-job-id",
    )
    with caplog.at_level(logging.WARNING):
        w.abort([None, None])
    assert "orphaned" not in caplog.text
