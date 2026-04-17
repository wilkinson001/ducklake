import logging
import pickle

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_ducklake.connection import DuckLakeConfig
from spark_ducklake.writer import DuckLakeWriter, DuckLakeStreamWriter, _iter_batches


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


def test_writer_is_picklable():
    w = DuckLakeWriter(CONFIG, "main", "test_table", SPARK_SCHEMA)
    restored = pickle.loads(pickle.dumps(w))
    assert restored.schema == "main"
    assert restored.table == "test_table"
    assert restored.write_mode == "append"


def test_writer_merge_mode_is_picklable():
    w = DuckLakeWriter(CONFIG, "main", "test_table", SPARK_SCHEMA, "merge", "id")
    restored = pickle.loads(pickle.dumps(w))
    assert restored.write_mode == "merge"
    assert restored.merge_keys == "id"


def test_stream_writer_is_picklable():
    w = DuckLakeStreamWriter(
        CONFIG, "main", "test_table", SPARK_SCHEMA, "append", "", False
    )
    restored = pickle.loads(pickle.dumps(w))
    assert restored.schema == "main"
    assert restored.table == "test_table"
    assert restored.write_mode == "append"
    assert restored.overwrite is False


def test_stream_writer_merge_mode_is_picklable():
    w = DuckLakeStreamWriter(
        CONFIG, "main", "test_table", SPARK_SCHEMA, "merge", "id,tenant_id", False
    )
    restored = pickle.loads(pickle.dumps(w))
    assert restored.write_mode == "merge"
    assert restored.merge_keys == "id,tenant_id"


def test_stream_writer_overwrite_mode_is_picklable():
    w = DuckLakeStreamWriter(
        CONFIG, "main", "test_table", SPARK_SCHEMA, "append", "", True
    )
    restored = pickle.loads(pickle.dumps(w))
    assert restored.overwrite is True


def test_stream_writer_abort_logs_warning(caplog):
    w = DuckLakeStreamWriter(
        CONFIG, "main", "test_table", SPARK_SCHEMA, "append", "", False
    )
    with caplog.at_level(logging.WARNING):
        w.abort([], 42)
    assert "batch 42" in caplog.text


def test_batch_writer_abort_logs_warning(caplog):
    w = DuckLakeWriter(CONFIG, "main", "test_table", SPARK_SCHEMA)
    with caplog.at_level(logging.WARNING):
        w.abort([])
    assert "DuckLakeWriter.abort" in caplog.text


def test_writer_default_batch_size():
    w = DuckLakeWriter(CONFIG, "main", "test_table", SPARK_SCHEMA)
    assert w.batch_size == 10000


def test_writer_custom_batch_size_is_picklable():
    w = DuckLakeWriter(CONFIG, "main", "test_table", SPARK_SCHEMA, batch_size=500)
    restored = pickle.loads(pickle.dumps(w))
    assert restored.batch_size == 500


def test_stream_writer_default_batch_size():
    w = DuckLakeStreamWriter(
        CONFIG, "main", "test_table", SPARK_SCHEMA, "append", "", False
    )
    assert w.batch_size == 10000


def test_stream_writer_custom_batch_size_is_picklable():
    w = DuckLakeStreamWriter(
        CONFIG, "main", "test_table", SPARK_SCHEMA, "append", "", False, batch_size=500
    )
    restored = pickle.loads(pickle.dumps(w))
    assert restored.batch_size == 500


def test_iter_batches_empty_iterator():
    assert list(_iter_batches(iter([]), 10)) == []


def test_iter_batches_smaller_than_batch_size():
    result = list(_iter_batches(iter([1, 2, 3]), 10))
    assert result == [[1, 2, 3]]


def test_iter_batches_exact_batch_size():
    result = list(_iter_batches(iter([1, 2, 3]), 3))
    assert result == [[1, 2, 3]]


def test_iter_batches_multiple_chunks():
    result = list(_iter_batches(iter(range(7)), 3))
    assert result == [[0, 1, 2], [3, 4, 5], [6]]
