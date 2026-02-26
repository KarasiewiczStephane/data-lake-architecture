"""SQLite-backed metadata catalog for the data lake.

Provides table registration, column-level metadata, statistics
tracking, schema versioning, data lineage, and search capabilities.
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator

logger = logging.getLogger(__name__)


class MetadataStore:
    """Metadata catalog backed by SQLite.

    Stores table registry, column metadata, statistics, schema
    versions, and lineage relationships for all data lake layers.

    Args:
        db_path: Path to SQLite database file. Use ':memory:' for testing.
    """

    def __init__(self, db_path: str = "catalog.db") -> None:
        self.db_path = db_path
        self._persistent_conn: sqlite3.Connection | None = None
        if db_path == ":memory:":
            self._persistent_conn = sqlite3.connect(":memory:")
            self._persistent_conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with self._connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tables (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    layer TEXT NOT NULL,
                    location TEXT NOT NULL,
                    partition_keys TEXT,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name, layer)
                );

                CREATE TABLE IF NOT EXISTS columns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    data_type TEXT NOT NULL,
                    description TEXT,
                    is_nullable BOOLEAN DEFAULT 1,
                    is_partition_key BOOLEAN DEFAULT 0,
                    FOREIGN KEY (table_id) REFERENCES tables(id),
                    UNIQUE(table_id, name)
                );

                CREATE TABLE IF NOT EXISTS column_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    column_id INTEGER NOT NULL,
                    row_count INTEGER,
                    null_count INTEGER,
                    distinct_count INTEGER,
                    min_value TEXT,
                    max_value TEXT,
                    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (column_id) REFERENCES columns(id)
                );

                CREATE TABLE IF NOT EXISTS schema_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_id INTEGER NOT NULL,
                    version TEXT NOT NULL,
                    schema_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (table_id) REFERENCES tables(id)
                );

                CREATE TABLE IF NOT EXISTS lineage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_table_id INTEGER NOT NULL,
                    target_table_id INTEGER NOT NULL,
                    transformation TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (source_table_id) REFERENCES tables(id),
                    FOREIGN KEY (target_table_id) REFERENCES tables(id)
                );

                CREATE INDEX IF NOT EXISTS idx_tables_name ON tables(name);
                CREATE INDEX IF NOT EXISTS idx_columns_name ON columns(name);
            """)
        logger.debug("Initialized catalog database at %s", self.db_path)

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Context manager for database connections.

        For in-memory databases, reuses a persistent connection.
        For file-based databases, creates a new connection each time.

        Yields:
            SQLite connection with Row factory enabled.
        """
        if self._persistent_conn is not None:
            yield self._persistent_conn
            self._persistent_conn.commit()
        else:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            finally:
                conn.close()

    def register_table(
        self,
        name: str,
        layer: str,
        location: str,
        partition_keys: list[str] | None = None,
        description: str = "",
    ) -> int:
        """Register or update a table in the catalog.

        Args:
            name: Table name.
            layer: Data layer (bronze, silver, gold).
            location: S3/storage location.
            partition_keys: List of partition key columns.
            description: Human-readable description.

        Returns:
            Row ID of the registered table.
        """
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO tables (name, layer, location, partition_keys, description)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(name, layer) DO UPDATE SET
                    location = excluded.location,
                    partition_keys = excluded.partition_keys,
                    description = excluded.description,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    name,
                    layer,
                    location,
                    json.dumps(partition_keys) if partition_keys else None,
                    description,
                ),
            )
            logger.info("Registered table %s.%s", layer, name)
            return cursor.lastrowid

    def register_column(
        self,
        table_name: str,
        layer: str,
        column_name: str,
        data_type: str,
        description: str = "",
        is_nullable: bool = True,
        is_partition_key: bool = False,
    ) -> int:
        """Register a column for a table.

        Args:
            table_name: Parent table name.
            layer: Parent table layer.
            column_name: Column name.
            data_type: Column data type string.
            description: Column description.
            is_nullable: Whether the column allows nulls.
            is_partition_key: Whether this column is a partition key.

        Returns:
            Row ID of the registered column.

        Raises:
            ValueError: If the parent table is not found.
        """
        with self._connection() as conn:
            table = conn.execute(
                "SELECT id FROM tables WHERE name = ? AND layer = ?",
                (table_name, layer),
            ).fetchone()
            if not table:
                raise ValueError(f"Table {layer}.{table_name} not found")

            cursor = conn.execute(
                """
                INSERT INTO columns (table_id, name, data_type, description,
                                     is_nullable, is_partition_key)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(table_id, name) DO UPDATE SET
                    data_type = excluded.data_type,
                    description = excluded.description
                """,
                (
                    table["id"],
                    column_name,
                    data_type,
                    description,
                    is_nullable,
                    is_partition_key,
                ),
            )
            return cursor.lastrowid

    def update_column_stats(
        self,
        table_name: str,
        layer: str,
        column_name: str,
        row_count: int,
        null_count: int,
        distinct_count: int,
        min_value: str | None = None,
        max_value: str | None = None,
    ) -> None:
        """Record statistics for a column.

        Args:
            table_name: Parent table name.
            layer: Parent table layer.
            column_name: Column name.
            row_count: Total number of rows.
            null_count: Number of null values.
            distinct_count: Number of distinct values.
            min_value: Minimum value as string.
            max_value: Maximum value as string.

        Raises:
            ValueError: If the column is not found.
        """
        with self._connection() as conn:
            col = conn.execute(
                """
                SELECT c.id FROM columns c
                JOIN tables t ON c.table_id = t.id
                WHERE t.name = ? AND t.layer = ? AND c.name = ?
                """,
                (table_name, layer, column_name),
            ).fetchone()
            if not col:
                raise ValueError(
                    f"Column {column_name} not found in {layer}.{table_name}"
                )

            conn.execute(
                """
                INSERT INTO column_stats
                    (column_id, row_count, null_count, distinct_count,
                     min_value, max_value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    col["id"],
                    row_count,
                    null_count,
                    distinct_count,
                    min_value,
                    max_value,
                ),
            )

    def add_schema_version(
        self, table_name: str, layer: str, version: str, schema: dict
    ) -> None:
        """Record a new schema version for a table.

        Args:
            table_name: Table name.
            layer: Table layer.
            version: Version string (e.g., '1.0', '2.0').
            schema: Schema definition as a dictionary.

        Raises:
            ValueError: If the table is not found.
        """
        with self._connection() as conn:
            table = conn.execute(
                "SELECT id FROM tables WHERE name = ? AND layer = ?",
                (table_name, layer),
            ).fetchone()
            if not table:
                raise ValueError(f"Table {layer}.{table_name} not found")

            conn.execute(
                """
                INSERT INTO schema_versions (table_id, version, schema_json)
                VALUES (?, ?, ?)
                """,
                (table["id"], version, json.dumps(schema)),
            )
            logger.info("Added schema version %s for %s.%s", version, layer, table_name)

    def get_schema_versions(self, table_name: str, layer: str) -> list[dict[str, Any]]:
        """Get all schema versions for a table.

        Args:
            table_name: Table name.
            layer: Table layer.

        Returns:
            List of dicts with version, schema, and created_at.
        """
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT sv.version, sv.schema_json, sv.created_at
                FROM schema_versions sv
                JOIN tables t ON sv.table_id = t.id
                WHERE t.name = ? AND t.layer = ?
                ORDER BY sv.created_at
                """,
                (table_name, layer),
            ).fetchall()
            return [
                {
                    "version": r["version"],
                    "schema": json.loads(r["schema_json"]),
                    "created_at": r["created_at"],
                }
                for r in rows
            ]

    def add_lineage(
        self,
        source_table: str,
        source_layer: str,
        target_table: str,
        target_layer: str,
        transformation: str = "",
    ) -> None:
        """Record a data lineage relationship.

        Args:
            source_table: Source table name.
            source_layer: Source layer.
            target_table: Target table name.
            target_layer: Target layer.
            transformation: Description of the transformation.

        Raises:
            ValueError: If source or target table is not found.
        """
        with self._connection() as conn:
            source = conn.execute(
                "SELECT id FROM tables WHERE name = ? AND layer = ?",
                (source_table, source_layer),
            ).fetchone()
            target = conn.execute(
                "SELECT id FROM tables WHERE name = ? AND layer = ?",
                (target_table, target_layer),
            ).fetchone()
            if not source or not target:
                raise ValueError("Source or target table not found in catalog")

            conn.execute(
                """
                INSERT INTO lineage (source_table_id, target_table_id, transformation)
                VALUES (?, ?, ?)
                """,
                (source["id"], target["id"], transformation),
            )
            logger.info(
                "Added lineage: %s.%s -> %s.%s",
                source_layer,
                source_table,
                target_layer,
                target_table,
            )

    def search(self, term: str) -> list[dict[str, Any]]:
        """Search tables and columns by name or description.

        Args:
            term: Search term (matched with LIKE %term%).

        Returns:
            List of matching results with match_type field.
        """
        with self._connection() as conn:
            results = []

            tables = conn.execute(
                """
                SELECT name, layer, description, 'table' as match_type
                FROM tables
                WHERE name LIKE ? OR description LIKE ?
                """,
                (f"%{term}%", f"%{term}%"),
            ).fetchall()
            results.extend(dict(r) for r in tables)

            columns = conn.execute(
                """
                SELECT c.name, t.name as table_name, t.layer,
                       c.description, 'column' as match_type
                FROM columns c
                JOIN tables t ON c.table_id = t.id
                WHERE c.name LIKE ? OR c.description LIKE ?
                """,
                (f"%{term}%", f"%{term}%"),
            ).fetchall()
            results.extend(dict(r) for r in columns)

            return results

    def get_lineage(self, table_name: str, layer: str) -> dict[str, list[dict]]:
        """Get upstream and downstream lineage for a table.

        Args:
            table_name: Table name.
            layer: Table layer.

        Returns:
            Dict with 'upstream' and 'downstream' lists of
            {name, layer} dicts.
        """
        with self._connection() as conn:
            table = conn.execute(
                "SELECT id FROM tables WHERE name = ? AND layer = ?",
                (table_name, layer),
            ).fetchone()
            if not table:
                return {"upstream": [], "downstream": []}

            upstream = conn.execute(
                """
                SELECT t.name, t.layer FROM lineage l
                JOIN tables t ON l.source_table_id = t.id
                WHERE l.target_table_id = ?
                """,
                (table["id"],),
            ).fetchall()

            downstream = conn.execute(
                """
                SELECT t.name, t.layer FROM lineage l
                JOIN tables t ON l.target_table_id = t.id
                WHERE l.source_table_id = ?
                """,
                (table["id"],),
            ).fetchall()

            return {
                "upstream": [dict(r) for r in upstream],
                "downstream": [dict(r) for r in downstream],
            }
