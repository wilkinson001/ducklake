import logging
import pickle

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_ducklake.connection import DuckLakeConfig
from spark_ducklake.writer import DuckLakeWriter, DuckLakeStreamWriter


SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])

CONFIG = DuckLakeConfig(
    postgres_conn="dbname=test",
    s3_endpoint="localhost:9000",
    s3_access_key="key",
    s3_secret_key="secret",
    s3_bucket="bucket",
)


class TestDuckLakeWriter:
    def test_is_picklable(self):
        w = DuckLakeWriter(CONFIG, "test_table", SCHEMA)
        restored = pickle.loads(pickle.dumps(w))
        assert restored.table == "test_table"
        assert restored.write_mode == "append"

    def test_merge_mode_is_picklable(self):
        w = DuckLakeWriter(CONFIG, "test_table", SCHEMA, "merge", "id")
        restored = pickle.loads(pickle.dumps(w))
        assert restored.write_mode == "merge"
        assert restored.merge_keys == "id"


class TestDuckLakeStreamWriter:
    def test_is_picklable(self):
        w = DuckLakeStreamWriter(CONFIG, "test_table", SCHEMA, "append", "", False)
        restored = pickle.loads(pickle.dumps(w))
        assert restored.table == "test_table"
        assert restored.write_mode == "append"
        assert restored.overwrite is False

    def test_merge_mode_is_picklable(self):
        w = DuckLakeStreamWriter(CONFIG, "test_table", SCHEMA, "merge", "id,tenant_id", False)
        restored = pickle.loads(pickle.dumps(w))
        assert restored.write_mode == "merge"
        assert restored.merge_keys == "id,tenant_id"

    def test_overwrite_mode_is_picklable(self):
        w = DuckLakeStreamWriter(CONFIG, "test_table", SCHEMA, "append", "", True)
        restored = pickle.loads(pickle.dumps(w))
        assert restored.overwrite is True

    def test_abort_logs_warning(self, caplog):
        w = DuckLakeStreamWriter(CONFIG, "test_table", SCHEMA, "append", "", False)
        with caplog.at_level(logging.WARNING):
            w.abort([], 42)
        assert "batch 42" in caplog.text

    def test_batch_writer_abort_logs_warning(self, caplog):
        w = DuckLakeWriter(CONFIG, "test_table", SCHEMA)
        with caplog.at_level(logging.WARNING):
            w.abort([])
        assert "DuckLakeWriter.abort" in caplog.text
