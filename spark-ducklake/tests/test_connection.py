import pickle

import pytest

from spark_ducklake.connection import DuckLakeConfig, parse_table_name, quote_identifier


def test_config_from_options():
    options = {
        "postgres_conn": "dbname=ducklake user=ducklake password=ducklake host=postgres port=5432",
        "s3_endpoint": "minio:9000",
        "s3_access_key": "minioadmin",
        "s3_secret_key": "minioadmin",
        "s3_bucket": "ducklake",
    }
    config = DuckLakeConfig.from_options(options)
    assert config.postgres_conn == options["postgres_conn"]
    assert config.s3_endpoint == "minio:9000"
    assert config.s3_use_ssl is False


def test_config_from_options_with_ssl():
    options = {
        "postgres_conn": "dbname=test",
        "s3_endpoint": "s3.amazonaws.com",
        "s3_access_key": "key",
        "s3_secret_key": "secret",
        "s3_bucket": "bucket",
        "s3_use_ssl": "true",
    }
    config = DuckLakeConfig.from_options(options)
    assert config.s3_use_ssl is True


def test_config_is_picklable():
    config = DuckLakeConfig(
        postgres_conn="dbname=test",
        s3_endpoint="localhost:9000",
        s3_access_key="key",
        s3_secret_key="secret",
        s3_bucket="bucket",
    )
    restored = pickle.loads(pickle.dumps(config))
    assert restored.postgres_conn == config.postgres_conn
    assert restored.s3_endpoint == config.s3_endpoint
    assert restored.s3_access_key == config.s3_access_key
    assert restored.s3_secret_key == config.s3_secret_key
    assert restored.s3_bucket == config.s3_bucket
    assert restored.s3_use_ssl == config.s3_use_ssl


@pytest.mark.parametrize(
    "name, expected",
    [
        ("test_table", '"test_table"'),
        ('my"table', '"my""table"'),
        ("my table", '"my table"'),
        ("select", '"select"'),
    ],
)
def test_quote_identifier(name, expected):
    assert quote_identifier(name) == expected


@pytest.mark.parametrize(
    "name, expected",
    [
        ("my_table", ("main", "my_table")),
        ("custom.my_table", ("custom", "my_table")),
    ],
)
def test_parse_table_name(name, expected):
    assert parse_table_name(name) == expected


def test_config_from_options_without_s3_credentials():
    options = {
        "postgres_conn": "dbname=ducklake host=postgres",
        "s3_bucket": "ducklake",
    }
    config = DuckLakeConfig.from_options(options)
    assert config.postgres_conn == options["postgres_conn"]
    assert config.s3_bucket == "ducklake"
    assert config.s3_endpoint is None
    assert config.s3_access_key is None
    assert config.s3_secret_key is None
    assert config.s3_use_ssl is False


def test_config_without_credentials_is_picklable():
    config = DuckLakeConfig(
        postgres_conn="dbname=test",
        s3_bucket="bucket",
    )
    restored = pickle.loads(pickle.dumps(config))
    assert restored.postgres_conn == "dbname=test"
    assert restored.s3_bucket == "bucket"
    assert restored.s3_endpoint is None
    assert restored.s3_access_key is None
    assert restored.s3_secret_key is None


def test_parse_table_name_rejects_three_parts():
    with pytest.raises(ValueError, match="Invalid table name"):
        parse_table_name("a.b.c")
