import pickle

from spark_ducklake.reader import (
    DuckLakeBatchPartition,
    DuckLakePartition,
    _column_list,
)


def test_column_list_with_columns():
    assert _column_list(["id", "name"]) == "id, name"


def test_column_list_single_column():
    assert _column_list(["id"]) == "id"


def test_column_list_empty_returns_star():
    assert _column_list([]) == "*"


def test_batch_partition_stores_limit_offset():
    p = DuckLakeBatchPartition(limit=100, offset=200)
    assert p.limit == 100
    assert p.offset == 200


def test_batch_partition_is_picklable():
    p = DuckLakeBatchPartition(limit=50, offset=0)
    restored = pickle.loads(pickle.dumps(p))
    assert restored.limit == 50
    assert restored.offset == 0


def test_partition_stores_snapshot_range():
    p = DuckLakePartition(start_snapshot=3, end_snapshot=7)
    assert p.start_snapshot == 3
    assert p.end_snapshot == 7


def test_partition_is_picklable():
    p = DuckLakePartition(start_snapshot=1, end_snapshot=5)
    restored = pickle.loads(pickle.dumps(p))
    assert restored.start_snapshot == 1
    assert restored.end_snapshot == 5
