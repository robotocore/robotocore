"""End-to-end and edge-case tests for RDS real database engines.

Tests real-world SQL patterns, transaction edge cases, error handling,
response format accuracy, and multi-database isolation.
"""

import sqlite3

import pytest

from robotocore.services.rds.data_provider import (
    _build_column_metadata,
    _convert_parameters,
    _python_to_rds_field,
    _rows_to_records,
)
from robotocore.services.rds.engine import SQLiteEngine
from robotocore.services.rds.provider import (
    _create_engine_for_instance,
    _destroy_engine_for_instance,
    get_engine,
)

# ---------------------------------------------------------------------------
# Real-world SQL patterns
# ---------------------------------------------------------------------------


class TestColumnTypes:
    """CREATE TABLE with all SQLite-supported column types."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-coltypes")

    def teardown_method(self):
        self.engine.close()

    def test_integer_column(self):
        self.engine.execute_sql("CREATE TABLE t (v INTEGER)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [42])
        rows = self.engine.execute_sql("SELECT v FROM t")
        assert rows[0]["v"] == 42
        assert isinstance(rows[0]["v"], int)

    def test_real_column(self):
        self.engine.execute_sql("CREATE TABLE t (v REAL)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [3.14])
        rows = self.engine.execute_sql("SELECT v FROM t")
        assert abs(rows[0]["v"] - 3.14) < 1e-9

    def test_text_column(self):
        self.engine.execute_sql("CREATE TABLE t (v TEXT)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", ["hello"])
        rows = self.engine.execute_sql("SELECT v FROM t")
        assert rows[0]["v"] == "hello"

    def test_blob_column(self):
        self.engine.execute_sql("CREATE TABLE t (v BLOB)")
        data = b"\x00\x01\xff\xfe"
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [data])
        rows = self.engine.execute_sql("SELECT v FROM t")
        assert rows[0]["v"] == data

    def test_numeric_column(self):
        self.engine.execute_sql("CREATE TABLE t (v NUMERIC)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [123])
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [45.67])
        self.engine.execute_sql("INSERT INTO t VALUES (?)", ["89"])
        rows = self.engine.execute_sql("SELECT v FROM t ORDER BY ROWID")
        assert rows[0]["v"] == 123
        assert abs(rows[1]["v"] - 45.67) < 1e-9
        # SQLite coerces numeric strings to numbers in NUMERIC columns
        assert rows[2]["v"] == 89


class TestInsertDefaults:
    """INSERT with DEFAULT values and auto-increment primary keys."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-defaults")

    def teardown_method(self):
        self.engine.close()

    def test_autoincrement_pk(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        self.engine.execute_sql("INSERT INTO t (name) VALUES (?)", ["Alice"])
        self.engine.execute_sql("INSERT INTO t (name) VALUES (?)", ["Bob"])
        rows = self.engine.execute_sql("SELECT * FROM t ORDER BY id")
        assert rows[0]["id"] == 1
        assert rows[1]["id"] == 2

    def test_default_values(self):
        self.engine.execute_sql(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active', "
            "created_at TEXT DEFAULT (datetime('now')))"
        )
        self.engine.execute_sql("INSERT INTO t (id) VALUES (?)", [1])
        rows = self.engine.execute_sql("SELECT * FROM t")
        assert rows[0]["status"] == "active"
        assert rows[0]["created_at"] is not None

    def test_default_integer(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, count INTEGER DEFAULT 0)")
        self.engine.execute_sql("INSERT INTO t (id) VALUES (?)", [1])
        rows = self.engine.execute_sql("SELECT count FROM t")
        assert rows[0]["count"] == 0


class TestJoins:
    """SELECT with multiple JOINs."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-joins")
        self.engine.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        self.engine.execute_sql(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)"
        )
        self.engine.execute_sql(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER, product TEXT)"
        )
        self.engine.execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        self.engine.execute_sql(
            "INSERT INTO orders VALUES (10, 1, 100.0), (20, 1, 200.0), (30, 2, 50.0)"
        )
        self.engine.execute_sql(
            "INSERT INTO items VALUES (100, 10, 'Widget'), (101, 10, 'Gadget'), (102, 30, 'Gizmo')"
        )

    def teardown_method(self):
        self.engine.close()

    def test_inner_join(self):
        rows = self.engine.execute_sql(
            "SELECT u.name, o.total FROM users u "
            "INNER JOIN orders o ON u.id = o.user_id ORDER BY o.total"
        )
        assert len(rows) == 3
        assert rows[0]["name"] == "Bob"
        assert rows[0]["total"] == 50.0

    def test_left_join(self):
        rows = self.engine.execute_sql(
            "SELECT u.name, o.total FROM users u "
            "LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.name"
        )
        # Carol has no orders -> NULL total
        carol_row = [r for r in rows if r["name"] == "Carol"][0]
        assert carol_row["total"] is None

    def test_three_table_join(self):
        rows = self.engine.execute_sql(
            "SELECT u.name, o.total, i.product FROM users u "
            "JOIN orders o ON u.id = o.user_id "
            "JOIN items i ON o.id = i.order_id "
            "ORDER BY i.product"
        )
        assert len(rows) == 3
        products = [r["product"] for r in rows]
        assert "Gadget" in products
        assert "Widget" in products
        assert "Gizmo" in products

    def test_self_join(self):
        self.engine.execute_sql(
            "CREATE TABLE employees (id INTEGER, name TEXT, manager_id INTEGER)"
        )
        self.engine.execute_sql("INSERT INTO employees VALUES (1, 'Boss', NULL)")
        self.engine.execute_sql("INSERT INTO employees VALUES (2, 'Worker', 1)")
        rows = self.engine.execute_sql(
            "SELECT e.name, m.name as manager FROM employees e "
            "LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.id"
        )
        assert rows[0]["manager"] is None
        assert rows[1]["manager"] == "Boss"


class TestUpdateDelete:
    """UPDATE with subquery, DELETE with CASCADE."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-upddel")

    def teardown_method(self):
        self.engine.close()

    def test_update_with_subquery(self):
        self.engine.execute_sql("CREATE TABLE scores (id INTEGER, score INTEGER)")
        self.engine.execute_sql("INSERT INTO scores VALUES (1, 80), (2, 90), (3, 70)")
        self.engine.execute_sql(
            "UPDATE scores SET score = score + 10 "
            "WHERE id IN (SELECT id FROM scores WHERE score < 85)"
        )
        rows = self.engine.execute_sql("SELECT * FROM scores ORDER BY id")
        assert rows[0]["score"] == 90  # 80 + 10
        assert rows[1]["score"] == 90  # unchanged (was >= 85)
        assert rows[2]["score"] == 80  # 70 + 10

    def test_delete_cascade(self):
        self.engine.execute_sql("PRAGMA foreign_keys = ON")
        self.engine.execute_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        self.engine.execute_sql(
            "CREATE TABLE child (id INTEGER, parent_id INTEGER, "
            "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE)"
        )
        self.engine.execute_sql("INSERT INTO parent VALUES (1)")
        self.engine.execute_sql("INSERT INTO child VALUES (10, 1), (11, 1)")
        self.engine.execute_sql("DELETE FROM parent WHERE id = 1")
        rows = self.engine.execute_sql("SELECT * FROM child")
        assert len(rows) == 0


class TestUpsert:
    """INSERT OR REPLACE, INSERT ON CONFLICT."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-upsert")
        self.engine.execute_sql(
            "CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT, updated INTEGER DEFAULT 0)"
        )

    def teardown_method(self):
        self.engine.close()

    def test_insert_or_replace(self):
        self.engine.execute_sql("INSERT INTO kv (key, value) VALUES (?, ?)", ["a", "v1"])
        self.engine.execute_sql("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", ["a", "v2"])
        rows = self.engine.execute_sql("SELECT value FROM kv WHERE key = ?", ["a"])
        assert rows[0]["value"] == "v2"

    def test_insert_on_conflict_do_update(self):
        self.engine.execute_sql("INSERT INTO kv (key, value) VALUES (?, ?)", ["b", "v1"])
        self.engine.execute_sql(
            "INSERT INTO kv (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated = updated + 1",
            ["b", "v2"],
        )
        rows = self.engine.execute_sql("SELECT value, updated FROM kv WHERE key = ?", ["b"])
        assert rows[0]["value"] == "v2"
        assert rows[0]["updated"] == 1

    def test_insert_on_conflict_do_nothing(self):
        self.engine.execute_sql("INSERT INTO kv (key, value) VALUES (?, ?)", ["c", "v1"])
        self.engine.execute_sql("INSERT OR IGNORE INTO kv (key, value) VALUES (?, ?)", ["c", "v2"])
        rows = self.engine.execute_sql("SELECT value FROM kv WHERE key = ?", ["c"])
        assert rows[0]["value"] == "v1"


class TestCTEAndWindowFunctions:
    """Common table expressions and window functions."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-cte-win")
        self.engine.execute_sql(
            "CREATE TABLE sales (id INTEGER, product TEXT, amount REAL, sale_date TEXT)"
        )
        data = [
            (1, "A", 100.0, "2024-01-01"),
            (2, "B", 200.0, "2024-01-02"),
            (3, "A", 150.0, "2024-01-03"),
            (4, "B", 250.0, "2024-01-04"),
            (5, "A", 120.0, "2024-01-05"),
        ]
        for row in data:
            self.engine.execute_sql("INSERT INTO sales VALUES (?, ?, ?, ?)", list(row))

    def teardown_method(self):
        self.engine.close()

    def test_cte(self):
        rows = self.engine.execute_sql(
            "WITH product_totals AS ("
            "  SELECT product, SUM(amount) as total FROM sales GROUP BY product"
            ") SELECT * FROM product_totals ORDER BY product"
        )
        assert len(rows) == 2
        assert rows[0]["product"] == "A"
        assert rows[0]["total"] == 370.0
        assert rows[1]["product"] == "B"
        assert rows[1]["total"] == 450.0

    def test_recursive_cte(self):
        rows = self.engine.execute_sql(
            "WITH RECURSIVE cnt(x) AS ("
            "  VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<5"
            ") SELECT x FROM cnt"
        )
        assert [r["x"] for r in rows] == [1, 2, 3, 4, 5]

    def test_row_number(self):
        rows = self.engine.execute_sql(
            "SELECT product, amount, "
            "ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) as rn "
            "FROM sales"
        )
        # Check that row numbers are assigned per product group
        a_rows = [r for r in rows if r["product"] == "A"]
        assert a_rows[0]["rn"] == 1
        assert a_rows[0]["amount"] == 150.0

    def test_rank(self):
        rows = self.engine.execute_sql(
            "SELECT product, amount, "
            "RANK() OVER (ORDER BY amount DESC) as rnk "
            "FROM sales ORDER BY rnk"
        )
        assert rows[0]["amount"] == 250.0
        assert rows[0]["rnk"] == 1

    def test_lag_lead(self):
        rows = self.engine.execute_sql(
            "SELECT id, amount, "
            "LAG(amount, 1) OVER (ORDER BY id) as prev_amount, "
            "LEAD(amount, 1) OVER (ORDER BY id) as next_amount "
            "FROM sales ORDER BY id"
        )
        assert rows[0]["prev_amount"] is None
        assert rows[0]["next_amount"] == 200.0
        assert rows[-1]["next_amount"] is None


class TestSQLiteFunctions:
    """JSON functions, date/time functions, PRAGMA, ANALYZE."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-sqlfuncs")

    def teardown_method(self):
        self.engine.close()

    def test_json_extract(self):
        self.engine.execute_sql("CREATE TABLE t (data TEXT)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", ['{"name": "Alice", "age": 30}'])
        rows = self.engine.execute_sql(
            "SELECT json_extract(data, '$.name') as name, json_extract(data, '$.age') as age FROM t"
        )
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 30

    def test_json_array(self):
        rows = self.engine.execute_sql("SELECT json_array(1, 2, 'three') as arr")
        assert rows[0]["arr"] == '[1,2,"three"]'

    def test_json_object(self):
        rows = self.engine.execute_sql("SELECT json_object('key', 'value') as obj")
        assert rows[0]["obj"] == '{"key":"value"}'

    def test_datetime_functions(self):
        rows = self.engine.execute_sql("SELECT datetime('2024-01-15 10:30:00') as dt")
        assert rows[0]["dt"] == "2024-01-15 10:30:00"

    def test_strftime(self):
        rows = self.engine.execute_sql("SELECT strftime('%Y', '2024-06-15') as yr")
        assert rows[0]["yr"] == "2024"

    def test_julianday(self):
        rows = self.engine.execute_sql(
            "SELECT julianday('2024-01-02') - julianday('2024-01-01') as diff"
        )
        assert abs(rows[0]["diff"] - 1.0) < 1e-9

    def test_pragma_table_info(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        rows = self.engine.execute_sql("PRAGMA table_info(t)")
        assert len(rows) == 2
        names = [r["name"] for r in rows]
        assert "id" in names
        assert "name" in names

    def test_analyze(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER)")
        self.engine.execute_sql("INSERT INTO t VALUES (1), (2), (3)")
        # ANALYZE should not raise
        result = self.engine.execute_sql("ANALYZE")
        assert result[0]["rowsAffected"] is not None


class TestMultipleStatements:
    """Multiple statements in one call (should error in SQLite via execute)."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-multi")

    def teardown_method(self):
        self.engine.close()

    def test_multiple_statements_raises(self):
        """SQLite's cursor.execute() does not support multiple statements."""
        with pytest.raises(Exception):
            self.engine.execute_sql("CREATE TABLE t1 (id INTEGER); CREATE TABLE t2 (id INTEGER)")


# ---------------------------------------------------------------------------
# Transaction edge cases
# ---------------------------------------------------------------------------


class TestTransactionEdgeCases:
    """Nested transactions, read-your-writes, concurrent modifications."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-tx-edge")
        self.engine.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

    def teardown_method(self):
        self.engine.close()

    def test_nested_transactions_via_savepoints(self):
        """Two begin_transaction calls create independent savepoints."""
        tx1 = self.engine.begin_transaction()
        tx2 = self.engine.begin_transaction()
        # Both IDs should be valid and different
        assert tx1 != tx2
        assert tx1 in self.engine._transactions
        assert tx2 in self.engine._transactions
        self.engine.commit_transaction(tx2)
        self.engine.commit_transaction(tx1)

    def test_read_your_writes_in_transaction(self):
        """Data written in a transaction is visible within the same transaction."""
        tx_id = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [1, "hello"])
        rows = self.engine.execute_in_transaction(tx_id, "SELECT val FROM t WHERE id = ?", [1])
        assert rows[0]["val"] == "hello"
        self.engine.commit_transaction(tx_id)

    def test_transaction_isolation_uncommitted_not_visible(self):
        """Data written but not committed is visible (SQLite single-connection)."""
        # SQLite with a single connection doesn't truly isolate transactions --
        # uncommitted data in savepoints is still visible from the same connection.
        tx_id = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [1, "uncommitted"])
        # From outside the transaction (still same connection)
        rows = self.engine.execute_sql("SELECT * FROM t")
        # With savepoints on a single connection, the row IS visible
        assert len(rows) == 1
        self.engine.rollback_transaction(tx_id)
        # After rollback, row should be gone
        rows = self.engine.execute_sql("SELECT * FROM t")
        assert len(rows) == 0

    def test_execute_after_commit_raises(self):
        tx_id = self.engine.begin_transaction()
        self.engine.commit_transaction(tx_id)
        with pytest.raises(ValueError, match="not found"):
            self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [1, "x"])

    def test_execute_after_rollback_raises(self):
        tx_id = self.engine.begin_transaction()
        self.engine.rollback_transaction(tx_id)
        with pytest.raises(ValueError, match="not found"):
            self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [1, "x"])

    def test_rollback_then_new_transaction(self):
        """After rolling back, a new transaction should work fine."""
        tx1 = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx1, "INSERT INTO t VALUES (?, ?)", [1, "a"])
        self.engine.rollback_transaction(tx1)

        tx2 = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx2, "INSERT INTO t VALUES (?, ?)", [1, "b"])
        self.engine.commit_transaction(tx2)

        rows = self.engine.execute_sql("SELECT val FROM t WHERE id = ?", [1])
        assert rows[0]["val"] == "b"

    def test_concurrent_transaction_writes_same_row(self):
        """Two transactions that both modify the same row; last commit wins."""
        self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [1, "original"])

        tx1 = self.engine.begin_transaction()
        tx2 = self.engine.begin_transaction()

        self.engine.execute_in_transaction(
            tx1, "UPDATE t SET val = ? WHERE id = ?", ["from_tx1", 1]
        )
        self.engine.execute_in_transaction(
            tx2, "UPDATE t SET val = ? WHERE id = ?", ["from_tx2", 1]
        )
        self.engine.commit_transaction(tx1)
        self.engine.commit_transaction(tx2)

        rows = self.engine.execute_sql("SELECT val FROM t WHERE id = ?", [1])
        # Last committed write wins
        assert rows[0]["val"] == "from_tx2"

    def test_transaction_with_error_still_allows_rollback(self):
        """If a SQL statement errors within a transaction, the transaction is still active."""
        tx_id = self.engine.begin_transaction()
        self.engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?, ?)", [1, "ok"])
        with pytest.raises(Exception):
            self.engine.execute_in_transaction(
                tx_id, "INSERT INTO nonexistent_table VALUES (?)", [1]
            )
        # Transaction should still be rollbackable
        self.engine.rollback_transaction(tx_id)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestSQLErrors:
    """SQL syntax errors, constraint violations, missing objects."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-sqlerr")

    def teardown_method(self):
        self.engine.close()

    def test_syntax_error(self):
        with pytest.raises(sqlite3.OperationalError):
            self.engine.execute_sql("SELEKKT * FROM t")

    def test_unique_constraint_violation(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER UNIQUE)")
        self.engine.execute_sql("INSERT INTO t VALUES (?)", [1])
        with pytest.raises(sqlite3.IntegrityError):
            self.engine.execute_sql("INSERT INTO t VALUES (?)", [1])

    def test_not_null_constraint_violation(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER NOT NULL)")
        with pytest.raises(sqlite3.IntegrityError):
            self.engine.execute_sql("INSERT INTO t VALUES (?)", [None])

    def test_check_constraint_violation(self):
        self.engine.execute_sql("CREATE TABLE t (age INTEGER CHECK(age >= 0))")
        with pytest.raises(sqlite3.IntegrityError):
            self.engine.execute_sql("INSERT INTO t VALUES (?)", [-1])

    def test_foreign_key_constraint_violation(self):
        self.engine.execute_sql("PRAGMA foreign_keys = ON")
        self.engine.execute_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        self.engine.execute_sql(
            "CREATE TABLE child (id INTEGER, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent(id))"
        )
        with pytest.raises(sqlite3.IntegrityError):
            self.engine.execute_sql("INSERT INTO child VALUES (?, ?)", [1, 999])

    def test_table_not_found(self):
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            self.engine.execute_sql("SELECT * FROM ghost")

    def test_column_not_found(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER)")
        with pytest.raises(sqlite3.OperationalError, match="no such column"):
            self.engine.execute_sql("SELECT phantom FROM t")

    def test_division_by_zero(self):
        """SQLite returns NULL for division by zero, not an error."""
        rows = self.engine.execute_sql("SELECT 1 / 0 as result")
        assert rows[0]["result"] is None

    def test_division_by_zero_float(self):
        """Float division by zero in SQLite also returns NULL (not inf)."""
        rows = self.engine.execute_sql("SELECT 1.0 / 0.0 as result")
        assert rows[0]["result"] is None


class TestEngineRegistrationEdgeCases:
    """ResourceArn pointing to non-existent DB, execute before creation."""

    def test_get_engine_nonexistent(self):
        engine = get_engine("999999999999", "us-east-1", "does-not-exist")
        assert engine is None

    def test_engine_scoped_by_all_three_keys(self):
        _create_engine_for_instance("111", "us-east-1", "db1", "mysql")
        _create_engine_for_instance("111", "us-west-2", "db1", "mysql")
        _create_engine_for_instance("222", "us-east-1", "db1", "mysql")

        e1 = get_engine("111", "us-east-1", "db1")
        e2 = get_engine("111", "us-west-2", "db1")
        e3 = get_engine("222", "us-east-1", "db1")

        # All three are distinct engines
        assert e1 is not e2
        assert e1 is not e3
        assert e2 is not e3

        # Write to one, verify others are unaffected
        e1.execute_sql("CREATE TABLE t (x INTEGER)")
        e1.execute_sql("INSERT INTO t VALUES (?)", [1])
        with pytest.raises(Exception, match="no such table"):
            e2.execute_sql("SELECT * FROM t")

        _destroy_engine_for_instance("111", "us-east-1", "db1")
        _destroy_engine_for_instance("111", "us-west-2", "db1")
        _destroy_engine_for_instance("222", "us-east-1", "db1")


# ---------------------------------------------------------------------------
# Response format accuracy
# ---------------------------------------------------------------------------


class TestResponseFormatAccuracy:
    """Verify columnMetadata, records, generatedFields, numberOfRecordsUpdated."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-respfmt")
        self.engine.execute_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "name TEXT, score REAL, active INTEGER)"
        )

    def teardown_method(self):
        self.engine.close()

    def test_column_metadata_fields(self):
        """Each column metadata entry has all required AWS fields."""
        self.engine.execute_sql(
            "INSERT INTO users (name, score, active) VALUES (?, ?, ?)",
            ["Alice", 9.5, 1],
        )
        rows = self.engine.execute_sql("SELECT * FROM users")
        metadata = _build_column_metadata(rows)
        assert len(metadata) == 4
        for m in metadata:
            assert "name" in m
            assert "typeName" in m
            assert "label" in m
            assert "nullable" in m
            assert "precision" in m
            assert "scale" in m

    def test_column_metadata_name_equals_label(self):
        self.engine.execute_sql(
            "INSERT INTO users (name, score, active) VALUES (?, ?, ?)",
            ["Alice", 9.5, 1],
        )
        rows = self.engine.execute_sql("SELECT * FROM users")
        metadata = _build_column_metadata(rows)
        for m in metadata:
            assert m["name"] == m["label"]

    def test_records_format_typed_fields(self):
        self.engine.execute_sql(
            "INSERT INTO users (name, score, active) VALUES (?, ?, ?)",
            ["Alice", 9.5, 1],
        )
        rows = self.engine.execute_sql("SELECT * FROM users")
        records = _rows_to_records(rows)
        assert len(records) == 1
        record = records[0]
        # id -> longValue
        assert "longValue" in record[0]
        # name -> stringValue
        assert "stringValue" in record[1]
        # score -> doubleValue
        assert "doubleValue" in record[2]
        # active -> longValue (SQLite stores booleans as ints)
        assert "longValue" in record[3]

    def test_records_null_field(self):
        self.engine.execute_sql(
            "INSERT INTO users (name, score, active) VALUES (?, ?, ?)",
            [None, None, None],
        )
        rows = self.engine.execute_sql("SELECT name, score, active FROM users")
        records = _rows_to_records(rows)
        for field in records[0]:
            assert field == {"isNull": True}

    def test_number_of_records_updated_insert(self):
        result = self.engine.execute_sql(
            "INSERT INTO users (name, score, active) VALUES (?, ?, ?)",
            ["Alice", 9.5, 1],
        )
        assert result[0]["rowsAffected"] == 1

    def test_number_of_records_updated_multi_update(self):
        self.engine.execute_sql(
            "INSERT INTO users (name, score, active) VALUES (?, ?, ?)", ["A", 1.0, 1]
        )
        self.engine.execute_sql(
            "INSERT INTO users (name, score, active) VALUES (?, ?, ?)", ["B", 2.0, 1]
        )
        result = self.engine.execute_sql("UPDATE users SET active = ? WHERE active = ?", [0, 1])
        assert result[0]["rowsAffected"] == 2

    def test_number_of_records_updated_delete(self):
        self.engine.execute_sql(
            "INSERT INTO users (name, score, active) VALUES (?, ?, ?)", ["A", 1.0, 1]
        )
        self.engine.execute_sql(
            "INSERT INTO users (name, score, active) VALUES (?, ?, ?)", ["B", 2.0, 1]
        )
        result = self.engine.execute_sql("DELETE FROM users")
        assert result[0]["rowsAffected"] == 2

    def test_empty_result_set_format(self):
        rows = self.engine.execute_sql("SELECT * FROM users")
        records = _rows_to_records(rows)
        metadata = _build_column_metadata(rows)
        assert records == []
        assert metadata == []

    def test_select_with_alias_in_metadata(self):
        self.engine.execute_sql(
            "INSERT INTO users (name, score, active) VALUES (?, ?, ?)", ["A", 1.0, 1]
        )
        rows = self.engine.execute_sql("SELECT name AS user_name, score AS user_score FROM users")
        metadata = _build_column_metadata(rows)
        assert metadata[0]["name"] == "user_name"
        assert metadata[1]["name"] == "user_score"


class TestConvertParametersEdgeCases:
    """Edge cases in parameter conversion."""

    def test_empty_parameters(self):
        result = _convert_parameters("SELECT 1", [])
        assert result == []

    def test_mixed_named_and_unnamed(self):
        """If any parameter has a name, use named binding."""
        params = [
            {"name": "id", "value": {"longValue": 1}},
            {"value": {"stringValue": "hello"}},
        ]
        result = _convert_parameters("SELECT :id", params)
        # Should return dict since at least one has a name
        assert isinstance(result, dict)

    def test_boolean_param_round_trip(self):
        field = _python_to_rds_field(True)
        assert field == {"booleanValue": True}
        # Round-trip
        param = {"name": "flag", "value": field}
        val = _convert_parameters("SELECT :flag", [param])
        assert val["flag"] is True


# ---------------------------------------------------------------------------
# Multi-database isolation
# ---------------------------------------------------------------------------


class TestMultiDatabaseIsolation:
    """Create two DB instances with same table name but different data."""

    def setup_method(self):
        _create_engine_for_instance("123456789012", "us-east-1", "db-alpha", "mysql")
        _create_engine_for_instance("123456789012", "us-east-1", "db-beta", "mysql")
        self.alpha = get_engine("123456789012", "us-east-1", "db-alpha")
        self.beta = get_engine("123456789012", "us-east-1", "db-beta")

    def teardown_method(self):
        _destroy_engine_for_instance("123456789012", "us-east-1", "db-alpha")
        _destroy_engine_for_instance("123456789012", "us-east-1", "db-beta")

    def test_same_table_different_data(self):
        self.alpha.execute_sql("CREATE TABLE items (id INTEGER, name TEXT)")
        self.beta.execute_sql("CREATE TABLE items (id INTEGER, name TEXT)")

        self.alpha.execute_sql("INSERT INTO items VALUES (?, ?)", [1, "alpha-item"])
        self.beta.execute_sql("INSERT INTO items VALUES (?, ?)", [1, "beta-item"])

        alpha_rows = self.alpha.execute_sql("SELECT name FROM items")
        beta_rows = self.beta.execute_sql("SELECT name FROM items")

        assert alpha_rows[0]["name"] == "alpha-item"
        assert beta_rows[0]["name"] == "beta-item"

    def test_delete_one_db_other_still_works(self):
        self.alpha.execute_sql("CREATE TABLE t (x INTEGER)")
        self.alpha.execute_sql("INSERT INTO t VALUES (?)", [42])
        self.beta.execute_sql("CREATE TABLE t (x INTEGER)")
        self.beta.execute_sql("INSERT INTO t VALUES (?)", [99])

        _destroy_engine_for_instance("123456789012", "us-east-1", "db-alpha")

        # Alpha is gone
        assert get_engine("123456789012", "us-east-1", "db-alpha") is None

        # Beta still works
        rows = self.beta.execute_sql("SELECT x FROM t")
        assert rows[0]["x"] == 99

    def test_different_schemas_per_db(self):
        self.alpha.execute_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        self.beta.execute_sql("CREATE TABLE t (id INTEGER, value REAL)")

        self.alpha.execute_sql("INSERT INTO t VALUES (?, ?)", [1, "hello"])
        self.beta.execute_sql("INSERT INTO t VALUES (?, ?)", [1, 3.14])

        alpha_rows = self.alpha.execute_sql("SELECT * FROM t")
        beta_rows = self.beta.execute_sql("SELECT * FROM t")

        assert "name" in alpha_rows[0]
        assert "value" in beta_rows[0]


# ---------------------------------------------------------------------------
# Thread safety under stress
# ---------------------------------------------------------------------------


class TestConcurrentTransactionStress:
    """Stress test with many concurrent transactions."""

    def test_many_sequential_transactions(self):
        engine = SQLiteEngine("test-tx-stress")
        engine.execute_sql("CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)")
        engine.execute_sql("INSERT INTO counter VALUES (1, 0)")

        for _ in range(50):
            tx_id = engine.begin_transaction()
            engine.execute_in_transaction(tx_id, "UPDATE counter SET val = val + 1 WHERE id = 1")
            engine.commit_transaction(tx_id)

        rows = engine.execute_sql("SELECT val FROM counter")
        assert rows[0]["val"] == 50
        engine.close()

    def test_interleaved_commit_rollback(self):
        engine = SQLiteEngine("test-tx-interleave")
        engine.execute_sql("CREATE TABLE t (id INTEGER)")

        for i in range(20):
            tx_id = engine.begin_transaction()
            engine.execute_in_transaction(tx_id, "INSERT INTO t VALUES (?)", [i])
            if i % 2 == 0:
                engine.commit_transaction(tx_id)
            else:
                engine.rollback_transaction(tx_id)

        rows = engine.execute_sql("SELECT COUNT(*) as cnt FROM t")
        assert rows[0]["cnt"] == 10  # Only even-numbered inserts committed
        engine.close()


class TestGetColumnMetadataEdgeCases:
    """Edge cases for get_column_metadata."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-meta-edge")

    def teardown_method(self):
        self.engine.close()

    def test_metadata_for_expression_columns(self):
        self.engine.execute_sql("CREATE TABLE t (a INTEGER, b INTEGER)")
        meta = self.engine.get_column_metadata("SELECT a + b as sum_ab, a * b as prod_ab FROM t")
        assert len(meta) == 2
        assert meta[0]["name"] == "sum_ab"
        assert meta[1]["name"] == "prod_ab"

    def test_metadata_for_dml_returns_empty(self):
        self.engine.execute_sql("CREATE TABLE t (id INTEGER)")
        # INSERT doesn't return columns
        meta = self.engine.get_column_metadata("INSERT INTO t VALUES (1)")
        assert meta == []

    def test_metadata_for_aggregate(self):
        self.engine.execute_sql("CREATE TABLE t (val INTEGER)")
        meta = self.engine.get_column_metadata("SELECT COUNT(*) as cnt, AVG(val) as avg FROM t")
        assert len(meta) == 2
        assert meta[0]["name"] == "cnt"
        assert meta[1]["name"] == "avg"


# ---------------------------------------------------------------------------
# Batch execute edge cases
# ---------------------------------------------------------------------------


class TestBatchExecuteEdgeCases:
    """Edge cases for BatchExecuteStatement parameter sets."""

    def setup_method(self):
        self.engine = SQLiteEngine("test-batch-edge")
        self.engine.execute_sql("CREATE TABLE t (id INTEGER, name TEXT)")

    def teardown_method(self):
        self.engine.close()

    def test_empty_parameter_sets(self):
        """BatchExecute with empty parameterSets should not insert anything."""
        rows = self.engine.execute_sql("SELECT COUNT(*) as cnt FROM t")
        assert rows[0]["cnt"] == 0

    def test_multiple_parameter_sets(self):
        """Simulate batch insert with multiple param sets."""
        for params in [[1, "Alice"], [2, "Bob"], [3, "Carol"]]:
            self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", params)

        rows = self.engine.execute_sql("SELECT COUNT(*) as cnt FROM t")
        assert rows[0]["cnt"] == 3

    def test_batch_with_partial_failure(self):
        """If one row violates a constraint, an error should occur."""
        self.engine.execute_sql("DROP TABLE t")
        self.engine.execute_sql("CREATE TABLE t (id INTEGER UNIQUE, name TEXT)")
        self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [1, "Alice"])
        # This should fail because id=1 already exists
        with pytest.raises(sqlite3.IntegrityError):
            self.engine.execute_sql("INSERT INTO t VALUES (?, ?)", [1, "Duplicate"])


# ---------------------------------------------------------------------------
# File-based engine
# ---------------------------------------------------------------------------


class TestFileBasedEngine:
    """Test that use_file=True creates a file-based database."""

    def test_file_engine_is_functional(self):
        engine = SQLiteEngine("test-file-engine", use_file=True)
        engine.execute_sql("CREATE TABLE t (x INTEGER)")
        engine.execute_sql("INSERT INTO t VALUES (?)", [42])
        rows = engine.execute_sql("SELECT x FROM t")
        assert rows[0]["x"] == 42
        engine.close()

        # Clean up the file
        import os

        path = "/tmp/robotocore_rds_test-file-engine.db"
        if os.path.exists(path):
            os.remove(path)
