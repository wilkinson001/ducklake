import uuid

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    DataSourceStreamWriter,
    DataSourceWriter,
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

from spark_ducklake.connection import DuckLakeConfig, parse_table_name, quote_identifier
from spark_ducklake.pool import get_connection
from spark_ducklake.reader import DuckLakeReader, DuckLakeStreamReader
from spark_ducklake.parquet_writer import DuckLakeParquetWriter
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
        dl_schema, table = parse_table_name(self.options["table"])
        conn = get_connection(config)
        rows = conn.execute(
            f"DESCRIBE my_lake.{quote_identifier(dl_schema)}.{quote_identifier(table)}"
        ).fetchall()
        fields = [
            StructField(row[0], duckdb_type_to_spark(row[1]), nullable=True)
            for row in rows
        ]
        return StructType(fields)

    def reader(self, schema: StructType) -> DataSourceReader:
        config = DuckLakeConfig.from_options(self.options)
        dl_schema, table = parse_table_name(self.options["table"])
        columns = [field.name for field in schema.fields]
        num_partitions = int(self.options.get("numPartitions", "1"))
        return DuckLakeReader(config, dl_schema, table, columns, num_partitions)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        config = DuckLakeConfig.from_options(self.options)
        dl_schema, table = parse_table_name(self.options["table"])
        write_mode = self.options.get("writeMode", "append")

        if write_mode == "merge":
            merge_keys = self.options.get("mergeKeys", "")
            batch_size = int(self.options.get("writeBatchSize", "10000"))
            if overwrite:
                conn = get_connection(config)
                conn.execute(
                    f"DELETE FROM my_lake.{quote_identifier(dl_schema)}.{quote_identifier(table)}"
                )
            return DuckLakeWriter(
                config, dl_schema, table, schema, write_mode, merge_keys, batch_size
            )

        conn = get_connection(config)
        settings = conn.execute(
            "SELECT data_path FROM ducklake_settings('my_lake')"
        ).fetchone()
        data_path = settings[0]

        # Use the DuckLake table schema (not the DataFrame schema) for Parquet
        # type mapping — ducklake_add_data_files requires exact type matches.
        table_schema = self.schema()

        job_id = str(uuid.uuid4())
        max_records = int(self.options.get("maxRecordsPerFile", "1000000"))
        return DuckLakeParquetWriter(
            config=config,
            dl_schema=dl_schema,
            table=table,
            spark_schema=table_schema,
            data_path=data_path,
            job_id=job_id,
            overwrite=overwrite,
            max_records_per_file=max_records,
        )

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        config = DuckLakeConfig.from_options(self.options)
        dl_schema, table = parse_table_name(self.options["table"])
        columns = [field.name for field in schema.fields]
        read_change_feed = self.options.get("readChangeFeed", "false").lower() == "true"
        starting_version = int(self.options.get("startingVersion", "0"))
        max_snapshots_per_batch = int(self.options.get("maxSnapshotsPerBatch", "0"))
        return DuckLakeStreamReader(
            config,
            dl_schema,
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
        dl_schema, table = parse_table_name(self.options["table"])
        write_mode = self.options.get("writeMode", "append")
        merge_keys = self.options.get("mergeKeys", "")
        batch_size = int(self.options.get("writeBatchSize", "10000"))
        return DuckLakeStreamWriter(
            config,
            dl_schema,
            table,
            schema,
            write_mode,
            merge_keys,
            overwrite,
            batch_size,
        )
