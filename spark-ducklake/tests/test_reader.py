import pickle

from spark_ducklake.reader import DuckLakePartition, _column_list


class TestColumnList:
    def test_with_columns(self):
        assert _column_list(["id", "name"]) == "id, name"

    def test_single_column(self):
        assert _column_list(["id"]) == "id"

    def test_empty_columns_returns_star(self):
        assert _column_list([]) == "*"


class TestDuckLakePartition:
    def test_stores_snapshot_range(self):
        p = DuckLakePartition(start_snapshot=3, end_snapshot=7)
        assert p.start_snapshot == 3
        assert p.end_snapshot == 7

    def test_is_picklable(self):
        p = DuckLakePartition(start_snapshot=1, end_snapshot=5)
        restored = pickle.loads(pickle.dumps(p))
        assert restored.start_snapshot == 1
        assert restored.end_snapshot == 5
