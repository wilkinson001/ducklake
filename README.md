# ducklake

A monorepo for experimenting with [DuckLake](https://ducklake.select/) — a lakehouse format that stores metadata in PostgreSQL and data as Parquet files in S3-compatible object storage.

## Getting Started

```bash
cp .env.example .env
docker compose up -d
```

This starts PostgreSQL, MinIO (S3-compatible object store), and a Spark container. MinIO console is available at `http://localhost:9001` (minioadmin/minioadmin).

To tear down and reset all data:

```bash
docker compose down -v
```

## Projects

### ducklake-setup

Python package for connecting to the DuckLake instance from the host machine. Uses `pydantic-settings` for configuration and provides helper functions to install DuckDB extensions and attach the DuckLake catalog.

### spark-ducklake

PySpark 4 Python DataSource V2 connector for DuckLake. Supports batch and streaming read/write with merge/upsert and overwrite modes. Runs inside the Spark Docker container.

See [spark-ducklake/README.md](spark-ducklake/README.md) for usage, configuration options, and known limitations.

### cdc-lambda

Rust AWS Lambda function that polls DuckLake for CDC changes. Reads a checkpoint from PostgreSQL, queries DuckLake for new snapshots via `ducklake_snapshots` and `ducklake_table_changes`, logs changes to stdout, and updates the checkpoint. Designed for deployment as a standalone Lambda binary.

Run locally with `cargo run` from the `cdc-lambda/` directory (requires Docker Compose services running).

## Infrastructure

### docker-compose.yml

| Service | Image | Purpose | Ports |
|---------|-------|---------|-------|
| postgres | postgres:18-alpine | DuckLake metadata catalog + checkpoints table | 5432 |
| minio | minio/minio | S3-compatible object storage for Parquet data | 9000 (API), 9001 (console) |
| minio-init | minio/mc | Creates the S3 bucket on first startup | — |
| spark | Custom (apache/spark:4.0.2-python3 + DuckDB) | PySpark environment for spark-ducklake | — |

### postgres/init

SQL scripts run on first PostgreSQL initialization. Creates the `checkpoints` table used by the CDC lambda.
