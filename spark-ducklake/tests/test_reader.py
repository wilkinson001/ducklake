import datetime
import pickle

import pytest

from pyspark.sql.datasource import (
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNotNull,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    StringContains,
    StringEndsWith,
    StringStartsWith,
)

from spark_ducklake.reader import (
    DuckLakeBatchPartition,
    DuckLakePartition,
    _column_list,
    _filter_to_sql,
    _format_value,
)


@pytest.mark.parametrize(
    "columns, expected",
    [
        (["id", "name"], '"id", "name"'),
        (["id"], '"id"'),
        ([], "*"),
    ],
)
def test_column_list(columns, expected):
    assert _column_list(columns) == expected


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


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, "NULL"),
        (42, "42"),
        (3.14, "3.14"),
        (True, "true"),
        (False, "false"),
        ("hello", "'hello'"),
        ("it's", "'it''s'"),
        (datetime.date(2026, 1, 15), "'2026-01-15'"),
        (datetime.datetime(2026, 1, 15, 10, 30, 0), "'2026-01-15 10:30:00'"),
    ],
)
def test_format_value(value, expected):
    assert _format_value(value) == expected


@pytest.mark.parametrize(
    "filter_obj, expected",
    [
        (EqualTo(("id",), 5), '"id" = 5'),
        (EqualTo(("name",), "alice"), "\"name\" = 'alice'"),
        (EqualTo(("name",), None), '"name" = NULL'),
        (EqualTo(("name",), "it's"), "\"name\" = 'it''s'"),
        (GreaterThan(("id",), 3), '"id" > 3'),
        (GreaterThanOrEqual(("id",), 3), '"id" >= 3'),
        (LessThan(("id",), 10), '"id" < 10'),
        (LessThanOrEqual(("id",), 10), '"id" <= 10'),
        (IsNull(("name",)), '"name" IS NULL'),
        (IsNotNull(("name",)), '"name" IS NOT NULL'),
        (In(("id",), [1, 2, 3]), '"id" IN (1, 2, 3)'),
        (In(("name",), ["alice", "bob"]), "\"name\" IN ('alice', 'bob')"),
        (StringStartsWith(("name",), "al"), "\"name\" LIKE 'al%'"),
        (StringEndsWith(("name",), "ce"), "\"name\" LIKE '%ce'"),
        (StringContains(("name",), "li"), "\"name\" LIKE '%li%'"),
        (Not(EqualTo(("id",), 5)), 'NOT ("id" = 5)'),
        (Not(IsNull(("name",))), 'NOT ("name" IS NULL)'),
    ],
)
def test_filter_to_sql(filter_obj, expected):
    assert _filter_to_sql(filter_obj) == expected


@pytest.mark.parametrize(
    "filter_obj",
    [
        EqualTo(("a", "b"), 5),  # nested column path
        "not a filter",  # unknown type
    ],
)
def test_filter_to_sql_unsupported(filter_obj):
    assert _filter_to_sql(filter_obj) is None
