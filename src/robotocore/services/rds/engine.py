"""Database engine abstraction for RDS instances.

Provides a SQLite-backed engine that executes real SQL queries in-memory,
giving behavioral fidelity beyond Moto's metadata-only mock.
"""

import sqlite3
import threading
import uuid
from typing import Protocol


class DatabaseEngine(Protocol):
    """Protocol for database engines backing RDS instances."""

    def execute_sql(self, sql: str, params: list | None = None) -> list[dict]:
        """Execute a SQL statement and return rows as list of dicts."""
        ...

    def close(self) -> None:
        """Close the database connection."""
        ...

    def begin_transaction(self) -> str:
        """Begin a transaction and return a transaction ID."""
        ...

    def commit_transaction(self, tx_id: str) -> None:
        """Commit a transaction by ID."""
        ...

    def rollback_transaction(self, tx_id: str) -> None:
        """Roll back a transaction by ID."""
        ...


class SQLiteEngine:
    """SQLite-backed database engine for RDS emulation.

    Each instance gets its own in-memory SQLite database with full SQL support.
    Thread-safe via a per-engine lock.
    """

    def __init__(self, db_identifier: str, use_file: bool = False):
        self.db_identifier = db_identifier
        self._lock = threading.Lock()
        if use_file:
            self._conn = sqlite3.connect(
                f"/tmp/robotocore_rds_{db_identifier}.db",
                check_same_thread=False,
            )
        else:
            self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        # Active transactions: tx_id -> savepoint name
        self._transactions: dict[str, str] = {}

    def execute_sql(self, sql: str, params: list | None = None) -> list[dict]:
        """Execute SQL and return results as list of dicts."""
        with self._lock:
            return self._execute_sql_unlocked(sql, params, commit=True)

    def _execute_sql_unlocked(
        self, sql: str, params: list | None, commit: bool = True
    ) -> list[dict]:
        """Execute SQL without acquiring the lock (caller must hold it)."""
        cursor = self._conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        # For statements that don't return rows (INSERT, UPDATE, DELETE, CREATE, etc.)
        if cursor.description is None:
            if commit:
                self._conn.commit()
            return [{"rowsAffected": cursor.rowcount}]
        columns = [desc[0] for desc in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(dict(zip(columns, row)))
        return rows

    def close(self) -> None:
        """Close the database connection."""
        with self._lock:
            self._conn.close()

    def begin_transaction(self) -> str:
        """Begin a transaction, return a transaction ID."""
        tx_id = str(uuid.uuid4())
        savepoint = f"sp_{tx_id.replace('-', '_')}"
        with self._lock:
            self._conn.execute(f"SAVEPOINT {savepoint}")
            self._transactions[tx_id] = savepoint
        return tx_id

    def commit_transaction(self, tx_id: str) -> None:
        """Commit a transaction by releasing its savepoint."""
        with self._lock:
            savepoint = self._transactions.pop(tx_id, None)
            if savepoint is None:
                raise ValueError(f"Transaction {tx_id} not found or already completed")
            try:
                self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            except sqlite3.OperationalError:
                # Savepoint may have been released by a parent commit
                self._conn.commit()

    def rollback_transaction(self, tx_id: str) -> None:
        """Roll back a transaction to its savepoint."""
        with self._lock:
            savepoint = self._transactions.pop(tx_id, None)
            if savepoint is None:
                raise ValueError(f"Transaction {tx_id} not found or already completed")
            try:
                self._conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            except sqlite3.OperationalError:
                # Savepoint may have been released by a parent commit
                pass

    def execute_in_transaction(
        self, tx_id: str, sql: str, params: list | None = None
    ) -> list[dict]:
        """Execute SQL within an existing transaction."""
        with self._lock:
            if tx_id not in self._transactions:
                raise ValueError(f"Transaction {tx_id} not found or already completed")
            return self._execute_sql_unlocked(sql, params, commit=False)

    def get_column_metadata(self, sql: str) -> list[dict]:
        """Get column metadata for a SELECT query without fetching all rows."""
        with self._lock:
            cursor = self._conn.cursor()
            # Use LIMIT 0 trick to get column info without data
            try:
                wrapped = f"SELECT * FROM ({sql}) LIMIT 0"
                cursor.execute(wrapped)
            except sqlite3.Error:
                try:
                    cursor.execute(sql)
                except sqlite3.Error:
                    return []
            if cursor.description is None:
                return []
            metadata = []
            for desc in cursor.description:
                metadata.append(
                    {
                        "name": desc[0],
                        "typeName": "VARCHAR",  # SQLite is dynamically typed
                        "label": desc[0],
                        "nullable": 1,
                        "precision": 0,
                        "scale": 0,
                    }
                )
            return metadata


def create_engine(engine_type: str, db_identifier: str) -> DatabaseEngine:
    """Factory to create a database engine.

    Args:
        engine_type: The RDS engine type (e.g., 'mysql', 'postgres', 'aurora-mysql').
            All types are backed by SQLite for local emulation.
        db_identifier: The DB instance identifier.

    Returns:
        A DatabaseEngine instance.
    """
    # All engine types use SQLite under the hood for local emulation
    return SQLiteEngine(db_identifier)
