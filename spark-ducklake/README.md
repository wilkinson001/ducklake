# spark-ducklake

A PySpark 4 Python DataSource V2 connector for [DuckLake](https://ducklake.select/). Provides batch and streaming read/write access to DuckLake tables, including merge/upsert support on writes.

Uses DuckDB with the DuckLake extension on both driver and executors. DuckLake stores metadata in PostgreSQL and data as Parquet files in S3-compatible object storage.

## Quickstart

Register the data source and read a table:

```python
from pyspark.sql import SparkSession
from spark_ducklake.datasource import DuckLakeDataSource

spark = SparkSession.builder.master("local[2]").getOrCreate()
spark.dataSource.register(DuckLakeDataSource)

df = spark.read.format("ducklake") \
    .option("table", "my_table") \
    .option("postgres_conn", "dbname=ducklake user=ducklake password=ducklake host=postgres port=5432") \
    .option("s3_endpoint", "minio:9000") \
    .option("s3_access_key", "minioadmin") \
    .option("s3_secret_key", "minioadmin") \
    .option("s3_bucket", "ducklake") \
    .option("s3_use_ssl", "false") \
    .load()

df.show()
```

## Configuration Options

| Option | Required | Description |
|--------|----------|-------------|
| `table` | Yes | DuckLake table name |
| `postgres_conn` | Yes | PostgreSQL connection string for DuckLake metadata catalog |
| `s3_endpoint` | Yes | S3-compatible endpoint (e.g. `minio:9000`) |
| `s3_access_key` | Yes | S3 access key |
| `s3_secret_key` | Yes | S3 secret key |
| `s3_bucket` | Yes | S3 bucket for DuckLake data |
| `s3_use_ssl` | No | `"true"` or `"false"` (default: `"false"`) |
| `readChangeFeed` | No | `"true"` to enable CDC stream reads (default: `"false"`) |
| `writeMode` | No | `"append"` or `"merge"` (default: `"append"`) |
| `mergeKeys` | No | Comma-separated merge key columns (required when `writeMode=merge`) |
| `numPartitions` | No | Number of partitions for batch reads (default: `"1"`) |
| `startingVersion` | No | Snapshot ID to start streaming from (default: `"0"`) |
| `maxSnapshotsPerBatch` | No | Max snapshots per micro-batch, `"0"` for unlimited (default: `"0"`) |

## Usage

### Batch Read

```python
df = spark.read.format("ducklake") \
    .option("table", "my_table") \
    .option(...) \
    .load()
```

Supports column projection — Spark will push selected columns down to DuckDB:

```python
df.select("id", "name").show()  # only reads id and name columns
```

### Batch Write (append)

```python
df.write.format("ducklake") \
    .option("table", "my_table") \
    .option(...) \
    .mode("append").save()
```

### Batch Write (overwrite)

Deletes all existing rows before writing, following the Delta Lake overwrite pattern. Recovery from a failed overwrite relies on DuckLake's time travel.

```python
df.write.format("ducklake") \
    .option("table", "my_table") \
    .option(...) \
    .mode("overwrite").save()
```

### Batch Write (merge/upsert)

Merge requires `.coalesce(1)` to avoid concurrent transaction conflicts between executors.

```python
df.coalesce(1).write.format("ducklake") \
    .option("table", "my_table") \
    .option("writeMode", "merge") \
    .option("mergeKeys", "id") \
    .option(...) \
    .mode("append").save()
```

### Stream Read (new rows)

Uses DuckLake snapshot IDs as streaming offsets. Default mode emits newly inserted rows.

```python
spark.readStream.format("ducklake") \
    .option("table", "my_table") \
    .option(...) \
    .load()
```

### Stream Read (CDC change feed)

Emits full change records including insert/update/delete markers.

```python
spark.readStream.format("ducklake") \
    .option("table", "my_table") \
    .option("readChangeFeed", "true") \
    .option(...) \
    .load()
```

### Stream Write (append)

```python
df.writeStream.format("ducklake") \
    .option("table", "my_table") \
    .option(...) \
    .start()
```

### Stream Write (merge)

```python
df.writeStream.format("ducklake") \
    .option("table", "my_table") \
    .option("writeMode", "merge") \
    .option("mergeKeys", "id") \
    .option(...) \
    .start()
```

### Stream Write (complete/overwrite)

For aggregation queries using `outputMode("complete")`, each micro-batch replaces the entire table.

```python
df.writeStream.format("ducklake") \
    .option("table", "my_table") \
    .option(...) \
    .outputMode("complete") \
    .start()
```

## Local Development

The project runs inside a Docker container alongside PostgreSQL and MinIO:

```bash
# Start all services
docker compose up -d

# Run tests
docker compose exec spark python3 -m pytest /opt/spark-ducklake/tests/ -v

# Interactive PySpark shell
docker compose exec spark pyspark
```

The Dockerfile uses a multistage build:
- `base` stage: production image with pyspark, duckdb, pyarrow, pandas
- `test` stage: extends base with pytest and pytest-cov

Build the production image:
```bash
docker build --target base -t spark-ducklake:prod ./spark-ducklake
```

## Architecture

DuckDB runs on both the Spark driver and executors. A pickle-serializable `DuckLakeConfig` is sent to executors, where `connect()` creates a fresh DuckDB connection with extensions loaded and the DuckLake catalog attached.

| Method | Runs on | Operation |
|--------|---------|-----------|
| `schema()` | Driver | `DESCRIBE my_lake.<table>` |
| `latestOffset()` | Driver | `ducklake_snapshots` |
| `read()` | Executor | `SELECT` / `ducklake_table_insertions` / `ducklake_table_changes` |
| `write()` | Executor | `INSERT INTO` / `MERGE INTO` |
| `commit()` | Driver | No-op (writes committed at executor level) |
| `abort()` | Driver | Log warning |

## Known Limitations and Future Work

### Readers

- **No predicate pushdown** — PySpark's Python DataSource V2 API does not support filter pushdown. All filters are applied by Spark after reading. This is a platform limitation, not a connector limitation.
- **No column projection in CDC mode** — CDC stream reads use `SELECT *` because the change feed includes metadata columns. This matches Delta Lake's change feed behavior. Users can `.drop()` unwanted columns after reading.
- **No schema evolution handling** — if the DuckLake table schema changes between snapshots, the stream reader doesn't adapt.

### Writers

- **Concurrent merge requires coalesce(1)** — merge operations from multiple executors cause DuckLake transaction conflicts. Should either enforce single-partition writes automatically or implement retry with backoff.
- **No abort rollback** — partial writes from successful executors can't be rolled back on abort. Should track pre-batch snapshot and restore on failure.
- **No partition-level overwrite** — overwrite deletes the entire table. Should support `replaceWhere` for partition-scoped overwrites.
- **No schema validation** — mismatches between DataFrame and DuckLake table schemas fail with raw DuckDB errors at the executor level.
- **No write batching** — `write()` materialises the entire partition into a pandas DataFrame in memory. Should batch large partitions to avoid OOM.
- **No idempotent writes** — stream writer doesn't use `batchId` for deduplication. Retried micro-batches produce duplicates.
- **No delete support** — no way to delete rows from Spark.

### Both

- **No connection pooling** — every `read()` and `write()` call creates a new DuckDB connection, installs extensions, and attaches the catalog.
- **No SQL injection protection** — table names and column names are interpolated directly into SQL. Should validate identifiers or use DuckDB's quoting.
- **No Spark metrics** — no custom task metrics reported (rows read/written, bytes scanned).
- **Plaintext credentials** — S3 credentials passed as Spark options. Should support IAM roles, instance profiles, and credential providers.
