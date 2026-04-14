from pyspark.sql.datasource import (
    DataSourceReader,
    DataSourceStreamReader,
    InputPartition,
)

from spark_ducklake.connection import DuckLakeConfig


def _column_list(columns: list[str]) -> str:
    if columns:
        return ", ".join(columns)
    return "*"


class DuckLakePartition(InputPartition):
    def __init__(self, start_snapshot: int, end_snapshot: int):
        self.start_snapshot = start_snapshot
        self.end_snapshot = end_snapshot


class DuckLakeReader(DataSourceReader):
    def __init__(self, config: DuckLakeConfig, table: str, columns: list[str]):
        self.config = config
        self.table = table
        self.columns = columns

    def read(self, partition: InputPartition):
        conn = self.config.connect()
        try:
            cols = _column_list(self.columns)
            result = conn.execute(
                f"SELECT {cols} FROM my_lake.{self.table}"
            ).fetchall()
            yield from result
        finally:
            conn.close()


class DuckLakeStreamReader(DataSourceStreamReader):
    def __init__(
        self,
        config: DuckLakeConfig,
        table: str,
        read_change_feed: bool,
        columns: list[str],
    ):
        self.config = config
        self.table = table
        self.read_change_feed = read_change_feed
        self.columns = columns

    def initialOffset(self):
        return {"snapshot_id": 0}

    def latestOffset(self):
        conn = self.config.connect()
        try:
            row = conn.execute(
                "SELECT MAX(snapshot_id) FROM ducklake_snapshots('my_lake')"
            ).fetchone()
            return {"snapshot_id": row[0]} if row else {}
        finally:
            conn.close()

    def partitions(self, start: dict, end: dict):
        return [DuckLakePartition(start["snapshot_id"], end["snapshot_id"])]

    def read(self, partition: InputPartition):
        assert isinstance(partition, DuckLakePartition)
        conn = self.config.connect()
        try:
            if self.read_change_feed:
                # CDC functions return table columns + metadata; select all
                sql = (
                    f"SELECT * FROM ducklake_table_changes("
                    f"'my_lake', '{self.table}', "
                    f"{partition.start_snapshot}, {partition.end_snapshot})"
                )
            else:
                cols = _column_list(self.columns)
                sql = (
                    f"SELECT {cols} FROM ducklake_table_insertions("
                    f"'my_lake', '{self.table}', "
                    f"{partition.start_snapshot}, {partition.end_snapshot})"
                )
            result = conn.execute(sql).fetchall()
            yield from result
        finally:
            conn.close()

    def commit(self, end: dict):
        pass
