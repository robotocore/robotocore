"""Unit tests for the RDS SQLite database engine."""

import threading

import pytest

from robotocore.services.rds.engine import SQLiteEngine, create_engine


class TestSQLiteEngine:
    """Tests for SQLiteEngine SQL execution."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-db")

    def teardown_method(self):
        self.engine.close()

    def test_create_table_and_insert(self):
        result = self.engine.execute_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"
        )
        assert result[0]["rowsAffected"] == -1  # DDL returns -1

        result = self.engine.execute_sql(
            "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
            [1, "Alice", "alice@example.com"],
        )
        assert result[0]["rowsAffected"] == 1

    def test_select(self):
        self.engine.execute_sql("CREATE TABLE items (id INTEGER, name TEXT, price REAL)")
        self.engine.execute_sql("INSERT INTO items VALUES (?, ?, ?)", [1, "Widget", 9.99])
        self.engine.execute_sql("INSERT INTO items VALUES (?, ?, ?)", [2, "Gadget", 19.99])

        rows = self.engine.execute_sql("SELECT * FROM items ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Widget"
        assert rows[0]["price"] == 9.99
        assert rows[1]["id"] == 2

    def test_update(self):
        self.engine.execute_sql("CREATE TABLE kv (key TEXT, value TEXT)")
        self.engine.execute_sql("INSERT INTO kv VALUES (?, ?)", ["foo", "bar"])

        result = self.engine.execute_sql("UPDATE kv SET value = ? WHERE key = ?", ["baz", "foo"])
        assert result[0]["rowsAffected"] == 1

        rows = self.engine.execute_sql("SELECT value FROM kv WHERE key = ?", ["foo"])
        assert rows[0]["value"] == "baz"

    def test_delete(self):
        self.engine.execute_sql("CREATE TABLE tmp (id INTEGER)")
        self.engine.execute_sql("INSERT INTO tmp VALUES (?)", [1])
        self.engine.execute_sql("INSERT INTO tmp VALUES (?)", [2])

        result = self.engine.execute_sql("DELETE FROM tmp WHERE id = ?", [1])
        assert result[0]["rowsAffected"] == 1

        rows = self.engine.execute_sql("SELECT * FROM tmp")
        assert len(rows) == 1
        assert rows[0]["id"] == 2

    def test_type_mapping_int(self):
        self.engine.execute_sql("CREATE TABLE t (val INTEGER)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [42])
        rows = self.engine.execute_sql("SELECT val FROM t")
        assert isinstance(rows[0]["val"], int)
        assert rows[0]["val"] == 42

    def test_type_mapping_float(self):
        self.engine.execute_sql("CREATE TABLE t (val REAL)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [3.14])
        rows = self.engine.execute_sql("SELECT val FROM t")
        assert isinstance(rows[0]["val"], float)
        assert abs(rows[0]["val"] - 3.14) < 0.001

    def test_type_mapping_str(self):
        self.engine.execute_sql("CREATE TABLE t (val TEXT)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", ["hello"])
        rows = self.engine.execute_sql("SELECT val FROM t")
        assert isinstance(rows[0]["val"], str)
        assert rows[0]["val"] == "hello"

    def test_type_mapping_none(self):
        self.engine.execute_sql("CREATE TABLE t (val TEXT)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [None])
        rows = self.engine.execute_sql("SELECT val FROM t")
        assert rows[0]["val"] is None

    def test_type_mapping_bool(self):
        self.engine.execute_sql("CREATE TABLE t (val INTEGER)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [True])
        rows = self.engine.execute_sql("SELECT val FROM t")
        # SQLite stores bools as integers
        assert rows[0]["val"] == 1


class TestTransactions:
    """Tests for transaction support."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-tx-db")
        self.engine.execute_sql("CREATE TABLE t (id INTEGER, val TEXT)")

    def teardown_method(self):
        self.engine.close()

    def test_begin_and_commit(self):
        tx_id = self.engine.begin_transaction()
        assert tx_id  # non-empty string

        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [1, "committed"])
        self.engine.commit_transaction(tx_id)

        rows = self.engine.execute_sql("SELECT * FROM t")
        assert len(rows) == 1
        assert rows[0]["val"] == "committed"

    def test_begin_and_rollback(self):
        # Insert baseline data
        self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [0, "baseline"])

        tx_id = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [1, "rolled_back"])
        self.engine.rollback_transaction(tx_id)

        rows = self.engine.execute_sql("SELECT * FROM t")
        assert len(rows) == 1
        assert rows[0]["val"] == "baseline"

    def test_invalid_transaction_id(self):
        with pytest.raises(ValueError, match="not found"):
            self.engine.execute_in_transaction(
                "nonexistent-tx", "INSERT INTO t VALUES (?, ?)", [1, "x"]
            )


class TestConcurrency:
    """Tests for thread-safe access."""

    def test_concurrent_writes(self):
        engine = SQLiteEngine("test-concurrent")
        engine.execute_sql("CREATE TABLE counter (val INTEGER)")
        engine.execute_sql("INSERT INTO counter VALUES (?)", [0])

        errors = []

        def increment():
            try:
                for _ in range(100):
                    engine.execute_sql("UPDATE counter SET val = val + 1")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=increment) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        rows = engine.execute_sql("SELECT val FROM counter")
        assert rows[0]["val"] == 400
        engine.close()


class TestFactory:
    """Tests for the create_engine factory."""

    def test_create_mysql_engine(self):
        engine = create_engine("mysql", "my-mysql-db")
        assert engine is not None
        engine.close()

    def test_create_postgres_engine(self):
        engine = create_engine("postgres", "my-pg-db")
        assert engine is not None
        engine.close()

    def test_create_aurora_engine(self):
        engine = create_engine("aurora-mysql", "my-aurora-db")
        assert engine is not None
        engine.close()
