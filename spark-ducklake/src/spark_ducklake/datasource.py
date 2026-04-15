from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    DataSourceStreamWriter,
)
from pyspark.sql.types import (
    BooleanType,
    DataType,
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
from spark_ducklake.pool import get_connection
from spark_ducklake.reader import DuckLakeReader, DuckLakeStreamReader
from spark_ducklake.writer import DuckLakeWriter, DuckLakeStreamWriter


DUCKDB_TO_SPARK_TYPES = {
    "BOOLEAN": BooleanType(),
    "TINYINT": ShortType(),
    "SMALLINT": ShortType(),
    "INTEGER": IntegerType(),
    "BIGINT": LongType(),
    "FLOAT": FloatType(),
    "DOUBLE": DoubleType(),
    "VARCHAR": StringType(),
    "DATE": DateType(),
    "TIMESTAMP": TimestampType(),
    "TIMESTAMP WITH TIME ZONE": TimestampType(),
}


def duckdb_type_to_spark(duckdb_type: str) -> DataType:
    upper = duckdb_type.upper()
    if upper in DUCKDB_TO_SPARK_TYPES:
        return DUCKDB_TO_SPARK_TYPES[upper]
    if upper.startswith("DECIMAL"):
        return DecimalType()
    return StringType()


class DuckLakeDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "ducklake"

    def schema(self) -> StructType:
        config = DuckLakeConfig.from_options(self.options)
        table = self.options["table"]
        conn = get_connection(config)
        rows = conn.execute(f"DESCRIBE my_lake.{table}").fetchall()
        fields = [
            StructField(row[0], duckdb_type_to_spark(row[1]), nullable=True)
            for row in rows
        ]
        return StructType(fields)

    def reader(self, schema: StructType) -> DataSourceReader:
        config = DuckLakeConfig.from_options(self.options)
        table = self.options["table"]
        columns = [field.name for field in schema.fields]
        num_partitions = int(self.options.get("numPartitions", "1"))
        return DuckLakeReader(config, table, columns, num_partitions)

    def writer(self, schema: StructType, overwrite: bool) -> DuckLakeWriter:
        config = DuckLakeConfig.from_options(self.options)
        table = self.options["table"]
        if overwrite:
            conn = get_connection(config)
            conn.execute(f"DELETE FROM my_lake.{table}")
        write_mode = self.options.get("writeMode", "append")
        merge_keys = self.options.get("mergeKeys", "")
        return DuckLakeWriter(config, table, schema, write_mode, merge_keys)

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        config = DuckLakeConfig.from_options(self.options)
        table = self.options["table"]
        columns = [field.name for field in schema.fields]
        read_change_feed = self.options.get("readChangeFeed", "false").lower() == "true"
        starting_version = int(self.options.get("startingVersion", "0"))
        max_snapshots_per_batch = int(self.options.get("maxSnapshotsPerBatch", "0"))
        return DuckLakeStreamReader(
            config,
            table,
            read_change_feed,
            columns,
            starting_version,
            max_snapshots_per_batch,
        )

    def streamWriter(
        self, schema: StructType, overwrite: bool
    ) -> DataSourceStreamWriter:
        config = DuckLakeConfig.from_options(self.options)
        table = self.options["table"]
        write_mode = self.options.get("writeMode", "append")
        merge_keys = self.options.get("mergeKeys", "")
        return DuckLakeStreamWriter(
            config, table, schema, write_mode, merge_keys, overwrite
        )
