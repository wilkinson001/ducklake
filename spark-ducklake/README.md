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
| `table` | Yes | DuckLake table name, optionally prefixed with schema (e.g. `"my_table"` or `"custom_schema.my_table"`) |
| `postgres_conn` | Yes | PostgreSQL connection string for DuckLake metadata catalog |
| `s3_endpoint` | No | S3-compatible endpoint (e.g. `minio:9000`). Omit for AWS S3. |
| `s3_access_key` | No | S3 access key. Omit to use IAM roles, instance profiles, or environment variables. |
| `s3_secret_key` | No | S3 secret key. Omit to use IAM roles, instance profiles, or environment variables. |
| `s3_bucket` | Yes | S3 bucket for DuckLake data |
| `s3_use_ssl` | No | `"true"` or `"false"` (default: `"false"`) |
| `readChangeFeed` | No | `"true"` to enable CDC stream reads (default: `"false"`) |
| `writeMode` | No | `"append"` or `"merge"` (default: `"append"`) |
| `mergeKeys` | No | Comma-separated merge key columns (required when `writeMode=merge`) |
| `numPartitions` | No | Number of partitions for batch reads (default: `"1"`) |
| `startingVersion` | No | Snapshot ID to start streaming from (default: `"0"`) |
| `maxSnapshotsPerBatch` | No | Max snapshots per micro-batch, `"0"` for unlimited (default: `"0"`) |
| `writeBatchSize` | No | Maximum rows per DuckDB merge statement (default: `"10000"`) |
| `maxRecordsPerFile` | No | Maximum rows per Parquet file for append/overwrite writes (default: `"1000000"`) |

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

Supports predicate pushdown when enabled — filters are pushed to DuckDB to reduce data scanned:

```python
spark.conf.set("spark.sql.python.filterPushdown.enabled", "true")

df = spark.read.format("ducklake") \
    .option("table", "my_table") \
    .option(...) \
    .load()

df.filter("id > 100").filter("name IS NOT NULL").show()  # filters pushed to DuckDB
```

Only AND-combined filters are pushed. OR predicates (e.g., `id = 1 OR name = 'alice'`) are applied by Spark after reading. See the [PySpark filter pushdown documentation](https://spark.apache.org/docs/4.1.0/api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSourceReader.pushFilters.html) for details.

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

Merge operations require a partitioning strategy to avoid DuckLake transaction conflicts between executors. Two options:

**Option 1: `coalesce(1)`** — serialise all data through one executor. Simplest, guaranteed no conflicts. Best for small merge batches.

```python
df.coalesce(1).write.format("ducklake") \
    .option("table", "my_table") \
    .option("writeMode", "merge") \
    .option("mergeKeys", "id") \
    .option(...) \
    .mode("append").save()
```

**Option 2: `repartition(N, "key")`** — partition by merge key so each executor handles a disjoint set of keys. Parallel, no conflicts. Best for large merges with a good distribution key.

```python
df.repartition(4, "id").write.format("ducklake") \
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

- `base` stage: production image with pyspark, duckdb, pyarrow
- `test` stage: extends base with pytest and pytest-cov

Build the production image:

```bash
docker build --target base -t spark-ducklake:prod ./spark-ducklake
```

## Architecture

DuckDB runs on both the Spark driver and executors. A pickle-serializable `DuckLakeConfig` is sent to executors, where `connect()` creates a fresh DuckDB connection with extensions loaded and the DuckLake catalog attached.

DuckLake uses optimistic concurrency with automatic retry for non-conflicting operations. Concurrent appends to the same table are automatically resolved. Concurrent merges on overlapping data are detected as logical conflicts and will fail — partition your data to avoid this (see [Batch Write (merge/upsert)](#batch-write-mergeupsert)).

| Method | Runs on | Operation |
|--------|---------|-----------|
| `schema()` | Driver | `DESCRIBE my_lake.<table>` |
| `latestOffset()` | Driver | `ducklake_snapshots` |
| `read()` | Executor | `SELECT` / `ducklake_table_insertions` / `ducklake_table_changes` |
| `write()` (append/overwrite) | Executor | Write Parquet files to S3 via PyArrow |
| `write()` (merge) | Executor | `MERGE INTO` via DuckDB |
| `commit()` (append/overwrite) | Driver | `ducklake_add_data_files` (atomic file registration) |
| `commit()` (merge) | Driver | No-op (writes committed at executor level) |
| `abort()` | Driver | Log warning |

## Known Limitations and Future Work

### Readers

- **Predicate pushdown limited to AND-combined filters** — filter pushdown is supported for batch reads (requires `spark.sql.python.filterPushdown.enabled=true`). Supported filters: `=`, `>`, `>=`, `<`, `<=`, `IS NULL`, `IS NOT NULL`, `IN`, `LIKE` (starts with, ends with, contains), `NOT`. OR predicates are not pushed down — Spark applies them post-read. See [PySpark Filter Pushdown API](https://spark.apache.org/docs/4.1.0/api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSourceReader.pushFilters.html).
- **No column projection in CDC mode** — CDC stream reads use `SELECT *` because the change feed includes metadata columns. This matches Delta Lake's change feed behavior. Users can `.drop()` unwanted columns after reading.
- **No schema evolution handling** — if the DuckLake table schema changes between snapshots, the stream reader doesn't adapt.

### Writers

- **Merge requires partitioning strategy** — concurrent merge operations from multiple executors on overlapping keys cause DuckLake transaction conflicts. Use `.coalesce(1)` or `.repartition(N, "merge_key")` to avoid conflicts. This is inherent to row-level SQL merge and matches the JDBC connector pattern. Append writes are unaffected — DuckLake automatically resolves concurrent appends.
- **No abort rollback for merge writes** — merge writes execute directly on executors and can't be rolled back on abort. Append/overwrite writes use atomic file registration and are safe.
- **No partition-level overwrite** — overwrite deletes the entire table. Should support `replaceWhere` for partition-scoped overwrites.
- **No schema validation** — mismatches between DataFrame and DuckLake table schemas fail with raw DuckDB errors at the executor level.
- **No idempotent writes for merge/streaming** — batch append/overwrite writes are idempotent (atomic file registration). Merge writes and streaming writes don't use `batchId` for deduplication; retried micro-batches may produce duplicates.
- **No delete support** — no way to delete rows from Spark.

### Both

- **No Spark metrics** — no custom task metrics reported (rows read/written, bytes scanned).
- **No custom credential providers** — S3 credentials are resolved via explicit options, environment variables, config files, or IAM roles. Custom credential provider plugins (e.g., HashiCorp Vault) are not supported.
