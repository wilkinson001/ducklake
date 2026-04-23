"""Integration tests for spark-ducklake connector.

Run inside the Spark Docker container with docker-compose services running:
    docker compose exec spark python3 -m pytest /opt/spark-ducklake/tests/test_integration.py -v
"""

import tempfile
import time

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


# Batch read


def test_reads_all_rows(spark):
    df = _read_df(spark)
    rows = df.orderBy("id").collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "alice"
    assert rows[1]["id"] == 2
    assert rows[1]["name"] == "bob"


def test_schema_inference(spark):
    df = _read_df(spark)
    fields = {f.name: type(f.dataType).__name__ for f in df.schema.fields}
    assert fields["id"] == "IntegerType"
    assert fields["name"] == "StringType"


# Batch write


def test_append_inserts_new_rows(spark):
    new_data = spark.createDataFrame([(3, "charlie"), (4, "diana")], ["id", "name"])
    _write_df(new_data)

    rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
    assert len(rows) == 4
    assert rows[3] == "charlie"
    assert rows[4] == "diana"


def test_append_preserves_existing_rows(spark):
    new_data = spark.createDataFrame([(3, "charlie")], ["id", "name"])
    _write_df(new_data)

    rows = _read_df(spark).orderBy("id").collect()
    assert rows[0]["name"] == "alice"
    assert rows[1]["name"] == "bob"
    assert rows[2]["name"] == "charlie"


# Batch merge


def test_merge_updates_existing_row(spark):
    merge_data = spark.createDataFrame([(1, "alice_updated")], ["id", "name"]).coalesce(
        1
    )
    _write_df(merge_data, {"writeMode": "merge", "mergeKeys": "id"})

    rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
    assert rows[1] == "alice_updated"
    assert rows[2] == "bob"


def test_merge_inserts_new_row(spark):
    merge_data = spark.createDataFrame([(5, "eve")], ["id", "name"]).coalesce(1)
    _write_df(merge_data, {"writeMode": "merge", "mergeKeys": "id"})

    rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
    assert rows[5] == "eve"
    assert len(rows) == 3


def test_merge_updates_and_inserts(spark):
    merge_data = spark.createDataFrame(
        [(1, "alice_updated"), (5, "eve")], ["id", "name"]
    ).coalesce(1)
    _write_df(merge_data, {"writeMode": "merge", "mergeKeys": "id"})

    rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
    assert rows[1] == "alice_updated"
    assert rows[2] == "bob"
    assert rows[5] == "eve"
    assert len(rows) == 3


# Batch overwrite


def test_overwrite_replaces_all_rows(spark):
    new_data = spark.createDataFrame([(10, "zara")], ["id", "name"])
    _write_df(new_data, mode="overwrite")

    rows = _read_df(spark).collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 10
    assert rows[0]["name"] == "zara"


def test_overwrite_then_read_shows_only_new_data(spark):
    new_data = spark.createDataFrame([(10, "zara"), (11, "yuki")], ["id", "name"])
    _write_df(new_data, mode="overwrite")

    rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
    assert rows == {10: "zara", 11: "yuki"}


# Partitioned read


def test_read_with_multiple_partitions(spark):
    """Reading with numPartitions > 1 returns same data as single partition."""
    reader = spark.read.format("ducklake")
    for k, v in DUCKLAKE_OPTS.items():
        reader = reader.option(k, v)
    df = reader.option("numPartitions", "2").load()

    rows = df.orderBy("id").collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "alice"
    assert rows[1]["id"] == 2
    assert rows[1]["name"] == "bob"


def test_read_with_more_partitions_than_rows(spark):
    """numPartitions > row count still returns all rows with correct data."""
    reader = spark.read.format("ducklake")
    for k, v in DUCKLAKE_OPTS.items():
        reader = reader.option(k, v)
    df = reader.option("numPartitions", "10").load()

    rows = df.orderBy("id").collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[1]["id"] == 2


def test_partitioned_read_with_column_projection(spark):
    """Column projection works with multiple partitions."""
    reader = spark.read.format("ducklake")
    for k, v in DUCKLAKE_OPTS.items():
        reader = reader.option(k, v)
    df = reader.option("numPartitions", "2").load().select("name")

    rows = df.collect()
    assert len(rows) == 2
    assert "id" not in rows[0].asDict()


# Column projection


def test_select_single_column(spark):
    df = _read_df(spark).select("name")
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["name"] in ("alice", "bob")
    assert "id" not in rows[0].asDict()


def test_select_reordered_columns(spark):
    df = _read_df(spark).select("name", "id")
    rows = df.orderBy("id").collect()
    assert rows[0]["name"] == "alice"
    assert rows[0]["id"] == 1


# Write batching


def test_batched_write_inserts_all_rows(spark):
    """Writing more rows than writeBatchSize still inserts everything with correct data."""
    data = [(i, f"user_{i}") for i in range(100)]
    new_data = spark.createDataFrame(data, ["id", "name"])

    writer = new_data.write.format("ducklake").mode("overwrite")
    for k, v in DUCKLAKE_OPTS.items():
        writer = writer.option(k, v)
    writer.option("writeBatchSize", "10").save()

    rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
    assert len(rows) == 100
    assert rows[0] == "user_0"
    assert rows[99] == "user_99"


def test_batched_merge_across_multiple_batches(spark):
    """Merge with more rows than writeBatchSize produces correct upsert results."""
    data = [(i, f"new_{i}") for i in range(1, 21)]
    merge_data = spark.createDataFrame(data, ["id", "name"]).coalesce(1)

    _write_df(
        merge_data,
        {"writeMode": "merge", "mergeKeys": "id", "writeBatchSize": "5"},
    )

    rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
    assert rows[1] == "new_1"
    assert rows[2] == "new_2"
    assert len(rows) == 20


# Parquet write path (append/overwrite via ducklake_add_data_files)


def test_parquet_write_with_max_records_per_file(spark, ducklake_config):
    """Writing more rows than maxRecordsPerFile produces multiple files, all registered."""
    data = [(i, f"user_{i}") for i in range(100)]
    new_data = spark.createDataFrame(data, ["id", "name"])
    _write_df(new_data, extra_opts={"maxRecordsPerFile": "25"}, mode="overwrite")

    df = _read_df(spark)
    assert df.count() == 100

    conn = ducklake_config.connect()
    try:
        file_count = conn.execute(
            "SELECT file_count FROM ducklake_table_info('my_lake') "
            "WHERE table_name = 'test_table'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert file_count >= 4


def test_parquet_append_with_multiple_partitions(spark):
    """Parallel append from multiple partitions succeeds without conflicts."""
    data = [(i, f"user_{i}") for i in range(3, 23)]
    new_data = spark.createDataFrame(data, ["id", "name"]).repartition(4)
    _write_df(new_data)

    rows = {r["id"]: r["name"] for r in _read_df(spark).collect()}
    assert len(rows) == 22
    assert rows[1] == "alice"
    assert rows[3] == "user_3"
    assert rows[22] == "user_22"


# Stream read


def _stream_reader(spark, extra_opts=None):
    reader = spark.readStream.format("ducklake")
    for k, v in DUCKLAKE_OPTS.items():
        reader = reader.option(k, v)
    if extra_opts:
        for k, v in extra_opts.items():
            reader = reader.option(k, v)
    return reader.load()


def _wait_for_condition(query, condition_fn, timeout_sec=30):
    start = time.time()
    while not condition_fn(query):
        if time.time() - start >= timeout_sec:
            raise TimeoutError(
                f"Timeout after {timeout_sec}s. Query exception: {query.exception()}"
            )
        time.sleep(0.2)


def test_stream_read_picks_up_insertions(spark):
    """Stream reader emits rows inserted since last offset."""
    df = _stream_reader(spark)
    results = []

    def collect_batch(batch_df, batch_id):
        results.extend(batch_df.collect())

    q = df.writeStream.trigger(availableNow=True).foreachBatch(collect_batch).start()
    q.awaitTermination(timeout=30)
    assert q.exception() is None
    assert len(results) == 2
    names = sorted(r["name"] for r in results)
    assert names == ["alice", "bob"]


def test_stream_read_starting_version(spark, ducklake_config):
    """startingVersion with a non-zero value produces a working stream that includes new data."""
    # Get the current latest snapshot after fixture setup
    conn = ducklake_config.connect()
    try:
        latest = conn.execute(
            "SELECT MAX(snapshot_id) FROM ducklake_snapshots('my_lake')"
        ).fetchone()[0]
    finally:
        conn.close()

    # Insert new data (creates a new snapshot)
    conn = ducklake_config.connect()
    try:
        conn.execute("INSERT INTO my_lake.test_table VALUES (3, 'charlie')")
    finally:
        conn.close()

    # Stream from the latest snapshot — should include charlie
    df = _stream_reader(spark, {"startingVersion": str(latest)})
    results = []

    def collect_batch(batch_df, batch_id):
        results.extend(batch_df.collect())

    q = df.writeStream.trigger(availableNow=True).foreachBatch(collect_batch).start()
    q.awaitTermination(timeout=30)
    assert q.exception() is None
    assert len(results) > 0
    names = [r["name"] for r in results]
    assert "charlie" in names


@pytest.mark.skip(
    reason="CDC returns metadata columns not in schema() — known limitation"
)
def test_stream_read_cdc_change_feed(spark, ducklake_config):
    """readChangeFeed=true emits change records with metadata columns."""
    conn = ducklake_config.connect()
    try:
        latest = conn.execute(
            "SELECT MAX(snapshot_id) FROM ducklake_snapshots('my_lake')"
        ).fetchone()[0]
    finally:
        conn.close()

    conn = ducklake_config.connect()
    try:
        conn.execute("INSERT INTO my_lake.test_table VALUES (3, 'charlie')")
    finally:
        conn.close()

    df = _stream_reader(
        spark,
        {"readChangeFeed": "true", "startingVersion": str(latest)},
    )
    results = []

    def collect_batch(batch_df, batch_id):
        results.extend(batch_df.collect())

    q = df.writeStream.trigger(availableNow=True).foreachBatch(collect_batch).start()
    q.awaitTermination(timeout=30)
    assert q.exception() is None
    assert len(results) >= 1


# Stream write


def test_stream_write_appends_rows(spark, ducklake_config):
    """Stream writer appends rows to a DuckLake table."""
    source_data = spark.createDataFrame([(3, "charlie"), (4, "diana")], ["id", "name"])
    input_dir = tempfile.mkdtemp()
    checkpoint_dir = tempfile.mkdtemp()
    source_data.write.format("json").mode("overwrite").save(input_dir)

    stream_df = spark.readStream.schema("id int, name string").json(input_dir)

    writer = stream_df.writeStream.format("ducklake")
    for k, v in DUCKLAKE_OPTS.items():
        writer = writer.option(k, v)
    q = (
        writer.option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination(timeout=30)
    assert q.exception() is None

    conn = ducklake_config.connect()
    try:
        rows = conn.execute(
            "SELECT id, name FROM my_lake.test_table ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 4
    assert rows[2] == (3, "charlie")
    assert rows[3] == (4, "diana")


def test_stream_write_merge(spark, ducklake_config):
    """Stream writer with merge mode upserts rows correctly."""
    source_data = spark.createDataFrame(
        [(1, "alice_updated"), (3, "charlie")], ["id", "name"]
    )
    input_dir = tempfile.mkdtemp()
    checkpoint_dir = tempfile.mkdtemp()
    source_data.coalesce(1).write.format("json").mode("overwrite").save(input_dir)

    stream_df = spark.readStream.schema("id int, name string").json(input_dir)

    writer = stream_df.writeStream.format("ducklake")
    for k, v in DUCKLAKE_OPTS.items():
        writer = writer.option(k, v)
    q = (
        writer.option("checkpointLocation", checkpoint_dir)
        .option("writeMode", "merge")
        .option("mergeKeys", "id")
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination(timeout=30)
    assert q.exception() is None

    conn = ducklake_config.connect()
    try:
        rows = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT id, name FROM my_lake.test_table ORDER BY id"
            ).fetchall()
        }
    finally:
        conn.close()
    assert rows[1] == "alice_updated"
    assert rows[2] == "bob"
    assert rows[3] == "charlie"
    assert len(rows) == 3


@pytest.mark.skip(
    reason="Stream overwrite uses executor-side DELETE+INSERT which conflicts with DuckLake transactions — needs staging table architecture"
)
def test_stream_write_overwrite(spark, ducklake_config):
    """Stream writer with complete output mode replaces all rows."""
    source_data = spark.createDataFrame(
        [(10, "zara"), (10, "zara"), (11, "yuki")], ["id", "name"]
    )
    input_dir = tempfile.mkdtemp()
    checkpoint_dir = tempfile.mkdtemp()
    source_data.write.format("json").mode("overwrite").save(input_dir)

    # complete output mode requires an aggregation
    stream_df = (
        spark.readStream.schema("id int, name string")
        .json(input_dir)
        .groupBy("id", "name")
        .count()
    )

    writer = stream_df.writeStream.format("ducklake").outputMode("complete")
    for k, v in DUCKLAKE_OPTS.items():
        writer = writer.option(k, v)

    # Write to a separate table since schema differs (has count column)
    conn = ducklake_config.connect()
    try:
        conn.execute("DROP TABLE IF EXISTS my_lake.test_overwrite_stream")
        conn.execute(
            "CREATE TABLE my_lake.test_overwrite_stream "
            "(id INTEGER, name VARCHAR, count BIGINT)"
        )
    finally:
        conn.close()

    q = (
        writer.option("table", "test_overwrite_stream")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination(timeout=30)
    assert q.exception() is None

    conn = ducklake_config.connect()
    try:
        rows = {
            r[0]: r[2]
            for r in conn.execute(
                "SELECT id, name, count FROM my_lake.test_overwrite_stream ORDER BY id"
            ).fetchall()
        }
    finally:
        conn.close()
    assert rows[10] == 2
    assert rows[11] == 1


# Custom schema


def test_read_write_with_custom_schema(spark, ducklake_config):
    """Tables in a non-default schema can be read and written."""
    conn = ducklake_config.connect()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS my_lake.custom")
        conn.execute("DROP TABLE IF EXISTS my_lake.custom.schema_test")
        conn.execute(
            "CREATE TABLE my_lake.custom.schema_test (id INTEGER, name VARCHAR)"
        )
        conn.execute("INSERT INTO my_lake.custom.schema_test VALUES (1, 'alice')")
    finally:
        conn.close()

    reader = spark.read.format("ducklake")
    for k, v in DUCKLAKE_OPTS.items():
        reader = reader.option(k, v)
    df = reader.option("table", "custom.schema_test").load()

    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "alice"

    # Write back to the custom schema table
    new_data = spark.createDataFrame([(2, "bob")], ["id", "name"])
    writer = new_data.write.format("ducklake").mode("append")
    for k, v in DUCKLAKE_OPTS.items():
        writer = writer.option(k, v)
    writer.option("table", "custom.schema_test").save()

    df2 = reader.option("table", "custom.schema_test").load()
    assert df2.count() == 2
