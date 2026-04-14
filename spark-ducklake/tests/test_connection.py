import pickle

from spark_ducklake.connection import DuckLakeConfig


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
