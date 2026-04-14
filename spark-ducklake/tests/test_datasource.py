from spark_ducklake.datasource import duckdb_type_to_spark

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    FloatType,
)


def test_basic_type_mapping():
    assert isinstance(duckdb_type_to_spark("INTEGER"), IntegerType)
    assert isinstance(duckdb_type_to_spark("BIGINT"), LongType)
    assert isinstance(duckdb_type_to_spark("VARCHAR"), StringType)
    assert isinstance(duckdb_type_to_spark("DOUBLE"), DoubleType)
    assert isinstance(duckdb_type_to_spark("BOOLEAN"), BooleanType)
    assert isinstance(duckdb_type_to_spark("DATE"), DateType)
    assert isinstance(duckdb_type_to_spark("TIMESTAMP"), TimestampType)
    assert isinstance(duckdb_type_to_spark("FLOAT"), FloatType)


def test_decimal_type_mapping():
    assert isinstance(duckdb_type_to_spark("DECIMAL(10,2)"), DecimalType)
    assert isinstance(duckdb_type_to_spark("DECIMAL"), DecimalType)


def test_unknown_type_defaults_to_string():
    assert isinstance(duckdb_type_to_spark("BLOB"), StringType)
    assert isinstance(duckdb_type_to_spark("UNKNOWN_TYPE"), StringType)


def test_case_insensitive():
    assert isinstance(duckdb_type_to_spark("integer"), IntegerType)
    assert isinstance(duckdb_type_to_spark("Varchar"), StringType)
