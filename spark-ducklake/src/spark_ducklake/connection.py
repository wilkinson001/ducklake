from dataclasses import dataclass

import duckdb


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def parse_table_name(name: str) -> tuple[str, str]:
    parts = name.split(".")
    if len(parts) == 1:
        return ("main", parts[0])
    if len(parts) == 2:
        return (parts[0], parts[1])
    raise ValueError(f"Invalid table name '{name}': expected 'table' or 'schema.table'")


@dataclass(frozen=True)
class DuckLakeConfig:
    postgres_conn: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    s3_use_ssl: bool = False

    @staticmethod
    def from_options(options: dict) -> "DuckLakeConfig":
        return DuckLakeConfig(
            postgres_conn=options["postgres_conn"],
            s3_endpoint=options["s3_endpoint"],
            s3_access_key=options["s3_access_key"],
            s3_secret_key=options["s3_secret_key"],
            s3_bucket=options["s3_bucket"],
            s3_use_ssl=options.get("s3_use_ssl", "false").lower() == "true",
        )

    def connect(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()
        conn.execute("INSTALL ducklake; INSTALL httpfs; INSTALL postgres;")
        conn.execute("LOAD ducklake; LOAD httpfs; LOAD postgres;")
        conn.execute(
            f"""CREATE SECRET ducklake_s3 (
                TYPE S3,
                KEY_ID '{self.s3_access_key}',
                SECRET '{self.s3_secret_key}',
                ENDPOINT '{self.s3_endpoint}',
                URL_STYLE 'path',
                USE_SSL {self.s3_use_ssl}
            )"""
        )
        conn.execute(
            f"""ATTACH 'ducklake:postgres:{self.postgres_conn}'
                AS my_lake (DATA_PATH 's3://{self.s3_bucket}/')"""
        )
        return conn
