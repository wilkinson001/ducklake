import threading
from dataclasses import astuple

import duckdb

from spark_ducklake.connection import DuckLakeConfig

_local = threading.local()


def _get_cache() -> dict[tuple, duckdb.DuckDBPyConnection]:
    if not hasattr(_local, "connections"):
        _local.connections = {}
    return _local.connections


def get_connection(config: DuckLakeConfig) -> duckdb.DuckDBPyConnection:
    cache = _get_cache()
    key = astuple(config)
    if key not in cache:
        cache[key] = config.connect()
    return cache[key]


def close_connections() -> None:
    cache = _get_cache()
    for conn in cache.values():
        try:
            conn.close()
        except duckdb.Error:
            pass
    cache.clear()
