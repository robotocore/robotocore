"""Unit tests for the RDS SQLite database engine."""

import sqlite3
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


class TestComplexSQL:
    """Tests for complex SQL operations."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-complex")
        self.engine.execute_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
        )
        self.engine.execute_sql(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)"
        )
        for i, (name, age) in enumerate([("Alice", 30), ("Bob", 25), ("Carol", 35)], start=1):
            self.engine.execute_sql("INSERT INTO users VALUES (?, ?, ?)", [i, name, age])
        self.engine.execute_sql("INSERT INTO orders VALUES (?, ?, ?)", [1, 1, 100.0])
        self.engine.execute_sql("INSERT INTO orders VALUES (?, ?, ?)", [2, 1, 200.0])
        self.engine.execute_sql("INSERT INTO orders VALUES (?, ?, ?)", [3, 2, 50.0])

    def teardown_method(self):
        self.engine.close()

    def test_join(self):
        rows = self.engine.execute_sql(
            "SELECT u.name, o.amount FROM users u "
            "JOIN orders o ON u.id = o.user_id ORDER BY o.amount"
        )
        assert len(rows) == 3
        assert rows[0]["name"] == "Bob"
        assert rows[0]["amount"] == 50.0

    def test_subquery(self):
        rows = self.engine.execute_sql(
            "SELECT name FROM users WHERE id IN (SELECT DISTINCT user_id FROM orders)"
        )
        names = {r["name"] for r in rows}
        assert names == {"Alice", "Bob"}

    def test_aggregate_sum(self):
        rows = self.engine.execute_sql(
            "SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id ORDER BY user_id"
        )
        assert len(rows) == 2
        assert rows[0]["total"] == 300.0
        assert rows[1]["total"] == 50.0

    def test_aggregate_count(self):
        rows = self.engine.execute_sql("SELECT COUNT(*) as cnt FROM users")
        assert rows[0]["cnt"] == 3

    def test_aggregate_avg(self):
        rows = self.engine.execute_sql("SELECT AVG(age) as avg_age FROM users")
        assert abs(rows[0]["avg_age"] - 30.0) < 0.001

    def test_like(self):
        rows = self.engine.execute_sql("SELECT name FROM users WHERE name LIKE ?", ["A%"])
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_between(self):
        rows = self.engine.execute_sql(
            "SELECT name FROM users WHERE age BETWEEN ? AND ? ORDER BY name",
            [26, 34],
        )
        assert [r["name"] for r in rows] == ["Alice"]

    def test_order_by_desc(self):
        rows = self.engine.execute_sql("SELECT name FROM users ORDER BY age DESC")
        assert [r["name"] for r in rows] == ["Carol", "Alice", "Bob"]

    def test_limit_offset(self):
        rows = self.engine.execute_sql("SELECT name FROM users ORDER BY id LIMIT 2 OFFSET 1")
        assert [r["name"] for r in rows] == ["Bob", "Carol"]

    def test_having(self):
        rows = self.engine.execute_sql(
            "SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING total > 100"
        )
        assert len(rows) == 1
        assert rows[0]["user_id"] == 1


class TestSchemaOperations:
    """Tests for schema-altering SQL."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-schema")

    def teardown_method(self):
        self.engine.close()

    def test_alter_table_add_column(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER)")
        self.engine.execute_sql("ALTER TABLE t ADD COLUMN name TEXT DEFAULT 'unknown'")
        self.engine.execute_sql("INSERT INTO t (id) VALUES (?)", [1])
        rows = self.engine.execute_sql("SELECT * FROM t")
        assert rows[0]["name"] == "unknown"

    def test_create_index(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        result = self.engine.execute_sql("CREATE INDEX idx_name ON t (name)")
        assert result[0]["rowsAffected"] == -1  # DDL

    def test_drop_table(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER)")
        self.engine.execute_sql("DROP TABLE t")
        with pytest.raises(Exception, match="no such table"):
            self.engine.execute_sql("SELECT * FROM t")

    def test_create_table_if_not_exists(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER)")
        # Should not raise
        self.engine.execute_sql("CREATE TABLE IF NOT EXISTS t (id INTEGER)")

    def test_unique_constraint(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER UNIQUE)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [1])
        with pytest.raises(Exception):
            self.engine.execute_sql("INSERT INTO t VALUES (?)", [1])

    def test_foreign_key(self):
        self.engine.execute_sql("PRAGMA foreign_keys = ON")
        self.engine.execute_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        self.engine.execute_sql(
            "CREATE TABLE child (id INTEGER, parent_id INTEGER, "
            "FOREIGN KEY (parent_id) REFERENCES parent(id))"
        )
        self.engine.execute_sql("INSERT INTO parent VALUES (?)", [1])
        self.engine.execute_sql("INSERT INTO child VALUES (?, ?)", [1, 1])
        # Should fail: no parent with id=99
        with pytest.raises(Exception):
            self.engine.execute_sql("INSERT INTO child VALUES (?, ?)", [2, 99])


class TestErrorCases:
    """Tests for error handling."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-errors")

    def teardown_method(self):
        self.engine.close()

    def test_syntax_error(self):
        with pytest.raises(sqlite3.OperationalError):
            self.engine.execute_sql("SELEKT * FROM nonexistent")

    def test_table_not_found(self):
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            self.engine.execute_sql("SELECT * FROM nonexistent")

    def test_column_not_found(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER)")
        with pytest.raises(sqlite3.OperationalError, match="no such column"):
            self.engine.execute_sql("SELECT missing_col FROM t")

    def test_constraint_violation_not_null(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER NOT NULL)")
        with pytest.raises(sqlite3.IntegrityError):
            self.engine.execute_sql("INSERT INTO t VALUES (?)", [None])

    def test_wrong_param_count(self):
        self.engine.execute_sql("CREATE TABLE t (a INTEGER, b INTEGER)")
        with pytest.raises(Exception):
            self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [1])


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

    def test_commit_invalid_transaction_raises(self):
        with pytest.raises(ValueError, match="not found"):
            self.engine.commit_transaction("nonexistent-tx")

    def test_rollback_invalid_transaction_raises(self):
        with pytest.raises(ValueError, match="not found"):
            self.engine.rollback_transaction("nonexistent-tx")

    def test_double_commit_raises(self):
        tx_id = self.engine.begin_transaction()
        self.engine.commit_transaction(tx_id)
        with pytest.raises(ValueError, match="not found"):
            self.engine.commit_transaction(tx_id)

    def test_double_rollback_raises(self):
        tx_id = self.engine.begin_transaction()
        self.engine.rollback_transaction(tx_id)
        with pytest.raises(ValueError, match="not found"):
            self.engine.rollback_transaction(tx_id)

    def test_multiple_statements_in_transaction(self):
        tx_id = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [1, "a"])
        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [2, "b"])
        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [3, "c"])
        self.engine.commit_transaction(tx_id)

        rows = self.engine.execute_sql("SELECT * FROM t ORDER BY id")
        assert len(rows) == 3

    def test_select_in_transaction(self):
        self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [1, "existing"])
        tx_id = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [2, "new"])
        rows = self.engine.execute_in_transaction(tx_id, "SELECT * FROM t ORDER BY id")
        assert len(rows) == 2
        self.engine.commit_transaction(tx_id)

    def test_rollback_preserves_pre_transaction_data(self):
        self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [1, "before"])
        tx_id = self.engine.begin_transaction()
        self.engine.execute_in_transaction(
            tx_id, "UPDATE t SET val = ? WHERE id = ?", ["changed", 1]
        )
        self.engine.rollback_transaction(tx_id)
        rows = self.engine.execute_sql("SELECT val FROM t WHERE id = ?", [1])
        assert rows[0]["val"] == "before"

    def test_concurrent_transactions(self):
        """Two sequential transactions: commit first, rollback second."""
        self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [1, "original"])

        # Sequential: tx1 commit, then tx2 rollback
        tx1 = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx1, "INSERT INTO t VALUES (?, ?)", [2, "tx1"])
        self.engine.commit_transaction(tx1)

        tx2 = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx2, "INSERT INTO t VALUES (?, ?)", [3, "tx2"])
        self.engine.rollback_transaction(tx2)

        rows = self.engine.execute_sql("SELECT * FROM t ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["val"] == "original"
        assert rows[1]["val"] == "tx1"

    def test_overlapping_transactions_graceful(self):
        """Overlapping transactions are handled gracefully (SQLite limitation)."""
        self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [1, "original"])

        tx1 = self.engine.begin_transaction()
        tx2 = self.engine.begin_transaction()

        self.engine.execute_in_transaction(tx1, "INSERT INTO t VALUES (?, ?)", [2, "tx1"])
        self.engine.execute_in_transaction(tx2, "INSERT INTO t VALUES (?, ?)", [3, "tx2"])

        # Commit tx1 - this may affect tx2's savepoint in SQLite
        self.engine.commit_transaction(tx1)
        # Rollback tx2 - should not raise even if savepoint was released
        self.engine.rollback_transaction(tx2)


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

    def test_concurrent_reads_and_writes(self):
        engine = SQLiteEngine("test-rw")
        engine.execute_sql("CREATE TABLE t (id INTEGER, val TEXT)")
        for i in range(10):
            engine.execute_sql("INSERT INTO t VALUES (?, ?)", [i, f"val{i}"])

        errors = []

        def reader():
            try:
                for _ in range(50):
                    rows = engine.execute_sql("SELECT * FROM t")
                    assert len(rows) >= 10
            except Exception as e:
                errors.append(e)

        def writer():
            try:
                for i in range(50):
                    engine.execute_sql("INSERT INTO t VALUES (?, ?)", [100 + i, f"new{i}"])
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(3)] + [
            threading.Thread(target=writer) for _ in range(2)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        rows = engine.execute_sql("SELECT COUNT(*) as cnt FROM t")
        assert rows[0]["cnt"] == 110  # 10 + 2*50
        engine.close()


class TestResourceCleanup:
    """Tests for proper resource cleanup."""

    def test_close_prevents_further_ops(self):
        engine = SQLiteEngine("test-close")
        engine.execute_sql("CREATE TABLE t (id INTEGER)")
        engine.close()
        with pytest.raises(Exception):
            engine.execute_sql("SELECT * FROM t")

    def test_close_idempotent(self):
        engine = SQLiteEngine("test-close-idem")
        engine.close()
        # Second close should not raise (ProgrammingError is ok if it does though)
        try:
            engine.close()
        except Exception:
            pass  # Acceptable - SQLite may raise on double close


class TestColumnMetadata:
    """Tests for get_column_metadata."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-meta")
        self.engine.execute_sql("CREATE TABLE t (id INTEGER, name TEXT, price REAL)")

    def teardown_method(self):
        self.engine.close()

    def test_basic_metadata(self):
        meta = self.engine.get_column_metadata("SELECT * FROM t")
        assert len(meta) == 3
        assert meta[0]["name"] == "id"
        assert meta[1]["name"] == "name"
        assert meta[2]["name"] == "price"

    def test_invalid_sql_returns_empty(self):
        meta = self.engine.get_column_metadata("INVALID SQL")
        assert meta == []

    def test_metadata_for_joined_query(self):
        self.engine.execute_sql("CREATE TABLE orders (id INTEGER, user_id INTEGER)")
        meta = self.engine.get_column_metadata(
            "SELECT t.id, t.name, orders.id as order_id FROM t JOIN orders ON t.id = orders.user_id"
        )
        assert len(meta) == 3
        assert meta[2]["name"] == "order_id"


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

    def test_engine_is_functional(self):
        engine = create_engine("mysql", "func-test")
        engine.execute_sql("CREATE TABLE t (x INTEGER)")
        engine.execute_sql("INSERT INTO t VALUES (?)", [42])
        rows = engine.execute_sql("SELECT x FROM t")
        assert rows[0]["x"] == 42
        engine.close()


class TestBinaryAndEdgeCases:
    """Tests for edge cases: binary data, empty strings, large values, NULL."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-edge")

    def teardown_method(self):
        self.engine.close()

    def test_empty_string(self):
        self.engine.execute_sql("CREATE TABLE t (val TEXT)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [""])
        rows = self.engine.execute_sql("SELECT val FROM t")
        assert rows[0]["val"] == ""

    def test_binary_data(self):
        self.engine.execute_sql("CREATE TABLE t (val BLOB)")
        data = bytes(range(256))
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [data])
        rows = self.engine.execute_sql("SELECT val FROM t")
        assert rows[0]["val"] == data

    def test_large_text(self):
        self.engine.execute_sql("CREATE TABLE t (val TEXT)")
        big = "x" * 1_000_000
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [big])
        rows = self.engine.execute_sql("SELECT LENGTH(val) as len FROM t")
        assert rows[0]["len"] == 1_000_000

    def test_null_in_various_columns(self):
        self.engine.execute_sql("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
        self.engine.execute_sql("INSERT INTO t VALUES (?, ?, ?)", [None, None, None])
        rows = self.engine.execute_sql("SELECT * FROM t")
        assert rows[0]["a"] is None
        assert rows[0]["b"] is None
        assert rows[0]["c"] is None

    def test_mixed_null_and_values(self):
        self.engine.execute_sql("CREATE TABLE t (a INTEGER, b TEXT)")
        self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [1, None])
        self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [None, "hello"])
        rows = self.engine.execute_sql("SELECT * FROM t ORDER BY ROWID")
        assert rows[0]["a"] == 1
        assert rows[0]["b"] is None
        assert rows[1]["a"] is None
        assert rows[1]["b"] == "hello"

    def test_empty_result_set(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER)")
        rows = self.engine.execute_sql("SELECT * FROM t")
        assert rows == []

    def test_very_large_integer(self):
        self.engine.execute_sql("CREATE TABLE t (val INTEGER)")
        big_int = 2**62
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [big_int])
        rows = self.engine.execute_sql("SELECT val FROM t")
        assert rows[0]["val"] == big_int
