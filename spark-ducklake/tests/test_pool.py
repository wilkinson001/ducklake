import threading
from unittest.mock import patch, MagicMock

import duckdb

from spark_ducklake.connection import DuckLakeConfig
from spark_ducklake.pool import _get_cache, close_connections, get_connection


CONFIG = DuckLakeConfig(
    postgres_conn="dbname=test",
    s3_endpoint="localhost:9000",
    s3_access_key="key",
    s3_secret_key="secret",
    s3_bucket="bucket",
)

CONFIG_OTHER = DuckLakeConfig(
    postgres_conn="dbname=other",
    s3_endpoint="localhost:9000",
    s3_access_key="key",
    s3_secret_key="secret",
    s3_bucket="other-bucket",
)


@patch.object(DuckLakeConfig, "connect", side_effect=lambda: duckdb.connect())
def test_get_connection_returns_duckdb_connection(mock_connect):
    try:
        conn = get_connection(CONFIG)
        result = conn.execute("SELECT 1").fetchone()
        assert result == (1,)
    finally:
        close_connections()


@patch.object(DuckLakeConfig, "connect", side_effect=lambda: duckdb.connect())
def test_get_connection_returns_same_object_for_same_config(mock_connect):
    try:
        conn1 = get_connection(CONFIG)
        conn2 = get_connection(CONFIG)
        assert conn1 is conn2
    finally:
        close_connections()


@patch.object(DuckLakeConfig, "connect", side_effect=lambda: duckdb.connect())
def test_get_connection_returns_different_objects_for_different_configs(mock_connect):
    try:
        conn1 = get_connection(CONFIG)
        conn2 = get_connection(CONFIG_OTHER)
        assert conn1 is not conn2
    finally:
        close_connections()


@patch.object(DuckLakeConfig, "connect", side_effect=lambda: duckdb.connect())
def test_close_connections_clears_cache(mock_connect):
    conn1 = get_connection(CONFIG)
    close_connections()
    conn2 = get_connection(CONFIG)
    assert conn1 is not conn2
    close_connections()


@patch.object(DuckLakeConfig, "connect", side_effect=lambda: duckdb.connect())
def test_connections_are_isolated_across_threads(mock_connect):
    main_conn = get_connection(CONFIG)
    thread_conn = None

    def worker():
        nonlocal thread_conn
        thread_conn = get_connection(CONFIG)
        close_connections()

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert thread_conn is not main_conn
    close_connections()


@patch.object(DuckLakeConfig, "connect", side_effect=lambda: duckdb.connect())
def test_close_connections_calls_close_on_cached_connections(mock_connect):
    mock_conn = MagicMock()
    cache = _get_cache()
    cache[("key",)] = mock_conn
    close_connections()
    mock_conn.close.assert_called_once()
