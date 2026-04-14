"""Integration tests for spark-ducklake connector.

Run inside the Spark Docker container with docker-compose services running:
    docker compose exec spark python3 -m pytest /opt/spark-ducklake/tests/test_integration.py -v
"""

import duckdb
import pytest
from pyspark.sql import SparkSession

from spark_ducklake.connection import DuckLakeConfig
from spark_ducklake.datasource import DuckLakeDataSource

DUCKLAKE_OPTS = {
    "table": "test_table",
    "postgres_conn": "dbname=ducklake user=ducklake password=ducklake host=postgres port=5432",
    "s3_endpoint": "minio:9000",
    "s3_access_key": "minioadmin",
    "s3_secret_key": "minioadmin",
    "s3_bucket": "ducklake",
    "s3_use_ssl": "false",
}


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("spark-ducklake-tests")
        .getOrCreate()
    )
    session.dataSource.register(DuckLakeDataSource)
    yield session
    session.stop()


@pytest.fixture(scope="module")
def ducklake_config():
    return DuckLakeConfig.from_options(DUCKLAKE_OPTS)


@pytest.fixture(autouse=True)
def reset_table(ducklake_config):
    """Reset test_table before each test."""
    conn = ducklake_config.connect()
    try:
        conn.execute("DROP TABLE IF EXISTS my_lake.test_table")
        conn.execute("CREATE TABLE my_lake.test_table (id INTEGER, name VARCHAR)")
        conn.execute("INSERT INTO my_lake.test_table VALUES (1, 'alice'), (2, 'bob')")
    finally:
        conn.close()


def _read_df(spark):
    reader = spark.read.format("ducklake")
    for k, v in DUCKLAKE_OPTS.items():
        reader = reader.option(k, v)
    return reader.load()


def _write_df(df, extra_opts=None, mode="append"):
    writer = df.write.format("ducklake").mode(mode)
    for k, v in DUCKLAKE_OPTS.items():
        writer = writer.option(k, v)
    if extra_opts:
        for k, v in extra_opts.items():
            writer = writer.option(k, v)
    writer.save()


class TestBatchRead:
    def test_reads_all_rows(self, spark):
        df = _read_df(spark)
        rows = df.orderBy("id").collect()
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "alice"
        assert rows[1]["id"] == 2
        assert rows[1]["name"] == "bob"

    def test_schema_inference(self, spark):
        df = _read_df(spark)
        field_names = [f.name for f in df.schema.fields]
        assert "id" in field_names
        assert "name" in field_names


class TestBatchWrite:
    def test_append_inserts_new_rows(self, spark):
        new_data = spark.createDataFrame([(3, "charlie"), (4, "diana")], ["id", "name"])
        _write_df(new_data)

        df = _read_df(spark)
        assert df.count() == 4

    def test_append_preserves_existing_rows(self, spark):
        new_data = spark.createDataFrame([(3, "charlie")], ["id", "name"])
        _write_df(new_data)

        rows = _read_df(spark).orderBy("id").collect()
        assert rows[0]["name"] == "alice"
        assert rows[1]["name"] == "bob"
        assert rows[2]["name"] == "charlie"


class TestBatchMerge:
    def test_merge_updates_existing_row(self, spark):
        merge_data = spark.createDataFrame(
            [(1, "alice_updated")], ["id", "name"]
        ).coalesce(1)
        _write_df(merge_data, {"writeMode": "merge", "mergeKeys": "id"})

        rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
        assert rows[1] == "alice_updated"
        assert rows[2] == "bob"

    def test_merge_inserts_new_row(self, spark):
        merge_data = spark.createDataFrame(
            [(5, "eve")], ["id", "name"]
        ).coalesce(1)
        _write_df(merge_data, {"writeMode": "merge", "mergeKeys": "id"})

        rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
        assert rows[5] == "eve"
        assert len(rows) == 3

    def test_merge_updates_and_inserts(self, spark):
        merge_data = spark.createDataFrame(
            [(1, "alice_updated"), (5, "eve")], ["id", "name"]
        ).coalesce(1)
        _write_df(merge_data, {"writeMode": "merge", "mergeKeys": "id"})

        rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
        assert rows[1] == "alice_updated"
        assert rows[2] == "bob"
        assert rows[5] == "eve"
        assert len(rows) == 3


class TestBatchOverwrite:
    def test_overwrite_replaces_all_rows(self, spark):
        new_data = spark.createDataFrame([(10, "zara")], ["id", "name"])
        _write_df(new_data, mode="overwrite")

        rows = _read_df(spark).collect()
        assert len(rows) == 1
        assert rows[0]["id"] == 10
        assert rows[0]["name"] == "zara"

    def test_overwrite_then_read_shows_only_new_data(self, spark):
        new_data = spark.createDataFrame(
            [(10, "zara"), (11, "yuki")], ["id", "name"]
        )
        _write_df(new_data, mode="overwrite")

        rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
        assert rows == {10: "zara", 11: "yuki"}


class TestColumnProjection:
    def test_select_single_column(self, spark):
        df = _read_df(spark).select("name")
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["name"] in ("alice", "bob")
        assert "id" not in rows[0].asDict()

    def test_select_reordered_columns(self, spark):
        df = _read_df(spark).select("name", "id")
        rows = df.orderBy("id").collect()
        assert rows[0]["name"] == "alice"
        assert rows[0]["id"] == 1
