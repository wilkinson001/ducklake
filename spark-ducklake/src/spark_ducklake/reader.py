from pyspark.sql.datasource import (
    DataSourceReader,
    DataSourceStreamReader,
    InputPartition,
)

from spark_ducklake.connection import DuckLakeConfig, quote_identifier
from spark_ducklake.pool import get_connection


def _column_list(columns: list[str]) -> str:
    if columns:
        return ", ".join(quote_identifier(c) for c in columns)
    return "*"


class DuckLakePartition(InputPartition):
    def __init__(self, start_snapshot: int, end_snapshot: int):
        self.start_snapshot = start_snapshot
        self.end_snapshot = end_snapshot


class DuckLakeBatchPartition(InputPartition):
    def __init__(self, limit: int, offset: int):
        self.limit = limit
        self.offset = offset


class DuckLakeReader(DataSourceReader):
    def __init__(
        self,
        config: DuckLakeConfig,
        schema: str,
        table: str,
        columns: list[str],
        num_partitions: int = 1,
    ):
        self.config = config
        self.schema = schema
        self.table = table
        self.columns = columns
        self.num_partitions = num_partitions

    def partitions(self):
        conn = get_connection(self.config)
        count = conn.execute(
            f"SELECT COUNT(*) FROM my_lake.{quote_identifier(self.schema)}.{quote_identifier(self.table)}"
        ).fetchone()[0]

        if self.num_partitions <= 1 or count == 0:
            return [DuckLakeBatchPartition(limit=count or 1, offset=0)]

        partition_size = (count + self.num_partitions - 1) // self.num_partitions
        return [
            DuckLakeBatchPartition(limit=partition_size, offset=i * partition_size)
            for i in range(self.num_partitions)
        ]

    def read(self, partition: InputPartition):
        assert isinstance(partition, DuckLakeBatchPartition)
        conn = get_connection(self.config)
        cols = _column_list(self.columns)
        result = conn.execute(
            f"SELECT {cols} FROM my_lake.{quote_identifier(self.schema)}.{quote_identifier(self.table)}"
            f" LIMIT {partition.limit} OFFSET {partition.offset}"
        ).fetchall()
        yield from result


class DuckLakeStreamReader(DataSourceStreamReader):
    def __init__(
        self,
        config: DuckLakeConfig,
        schema: str,
        table: str,
        read_change_feed: bool,
        columns: list[str],
        starting_version: int = 0,
        max_snapshots_per_batch: int = 0,
    ):
        self.config = config
        self.schema = schema
        self.table = table
        self.read_change_feed = read_change_feed
        self.columns = columns
        self.starting_version = starting_version
        self.max_snapshots_per_batch = max_snapshots_per_batch
        self._committed_offset = starting_version

    def initialOffset(self):
        return {"snapshot_id": self.starting_version}

    def latestOffset(self):
        conn = get_connection(self.config)
        row = conn.execute(
            "SELECT MAX(snapshot_id) FROM ducklake_snapshots('my_lake')"
        ).fetchone()
        latest = row[0] if row and row[0] is not None else 0

        if self.max_snapshots_per_batch > 0:
            latest = min(latest, self._committed_offset + self.max_snapshots_per_batch)

        return {"snapshot_id": latest}

    def partitions(self, start: dict, end: dict):
        return [DuckLakePartition(start["snapshot_id"], end["snapshot_id"])]

    def read(self, partition: InputPartition):
        assert isinstance(partition, DuckLakePartition)
        conn = get_connection(self.config)
        if self.read_change_feed:
            sql = (
                f"SELECT * FROM ducklake_table_changes("
                f"'my_lake', '{self.schema}', '{self.table}', "
                f"{partition.start_snapshot}, {partition.end_snapshot})"
            )
        else:
            cols = _column_list(self.columns)
            sql = (
                f"SELECT {cols} FROM ducklake_table_insertions("
                f"'my_lake', '{self.schema}', '{self.table}', "
                f"{partition.start_snapshot}, {partition.end_snapshot})"
            )
        result = conn.execute(sql).fetchall()
        yield from result

    def commit(self, end: dict):
        self._committed_offset = end["snapshot_id"]
