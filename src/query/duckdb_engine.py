"""DuckDB query engine for Parquet files in MinIO.

Provides SQL access to data stored across all medallion layers
with automatic table registration and multiple output formats.
"""

import io
import logging

import duckdb
import pandas as pd
import pyarrow as pa

from src.storage.minio_client import MinIOClient
from src.utils.config import load_config

logger = logging.getLogger(__name__)


class DuckDBEngine:
    """Query engine for Parquet files using DuckDB.

    Downloads Parquet files from MinIO and registers them as DuckDB
    views for SQL querying. Supports multiple output formats.

    Args:
        client: Storage client instance. Auto-created from config if None.
        config: Configuration dictionary. Auto-loaded if None.
    """

    def __init__(
        self,
        client: MinIOClient | None = None,
        config: dict | None = None,
    ) -> None:
        if client is None:
            from src.storage import get_storage_client

            client = get_storage_client()
        self.client = client
        self.config = config or load_config()
        self.conn = duckdb.connect(":memory:")
        self._registered_tables: set[str] = set()

    def register_table(
        self,
        layer: str,
        table_name: str,
        alias: str | None = None,
    ) -> str:
        """Register a Parquet table from MinIO as a DuckDB view.

        Downloads the latest Parquet file and creates an in-memory view.

        Args:
            layer: Data layer (bronze, silver, or gold).
            table_name: Table name within the layer.
            alias: Optional alias for the view. Defaults to 'layer_table'.

        Returns:
            The registered view name.

        Raises:
            ValueError: If no Parquet files found for the table.
        """
        bucket = self.config["buckets"][layer]
        prefix = f"{layer}/{table_name}/"

        objects = list(self.client.list_objects(bucket, prefix))
        parquet_files = [o for o in objects if o["key"].endswith(".parquet")]

        if not parquet_files:
            raise ValueError(f"No Parquet files found for {layer}.{table_name}")

        latest = max(parquet_files, key=lambda x: x["modified"])
        content = self.client.download_file(bucket, latest["key"])

        df = pd.read_parquet(io.BytesIO(content))

        view_name = alias or f"{layer}_{table_name}"
        safe_name = view_name.replace(".", "_").replace("-", "_")

        self.conn.register(safe_name, df)

        self._registered_tables.add(safe_name)
        logger.info("Registered table %s (%d rows)", safe_name, len(df))
        return safe_name

    def register_all_tables(self) -> list[str]:
        """Auto-register all Parquet tables from all layers.

        Returns:
            List of registered view names.
        """
        registered = []
        for layer in ["bronze", "silver", "gold"]:
            bucket = self.config["buckets"][layer]
            try:
                objects = list(self.client.list_objects(bucket, f"{layer}/"))
            except Exception:
                continue

            tables: set[str] = set()
            for obj in objects:
                parts = obj["key"].split("/")
                if len(parts) >= 2:
                    tables.add(parts[1])

            for table in tables:
                try:
                    name = self.register_table(layer, table)
                    registered.append(name)
                except Exception as e:
                    logger.warning("Could not register %s.%s: %s", layer, table, e)

        return registered

    def query(self, sql: str) -> list[dict]:
        """Execute a SQL query and return results as a list of dicts.

        Args:
            sql: SQL query string.

        Returns:
            Query results as a list of row dictionaries.
        """
        result = self.conn.execute(sql).fetchdf()
        return result.to_dict(orient="records")

    def query_df(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return a pandas DataFrame.

        Args:
            sql: SQL query string.

        Returns:
            Query results as a DataFrame.
        """
        return self.conn.execute(sql).fetchdf()

    def query_arrow(self, sql: str) -> pa.Table:
        """Execute a SQL query and return a PyArrow Table.

        Args:
            sql: SQL query string.

        Returns:
            Query results as a PyArrow Table.
        """
        return self.conn.execute(sql).fetch_arrow_table()

    def explain(self, sql: str) -> str:
        """Get the query execution plan.

        Args:
            sql: SQL query to explain.

        Returns:
            Execution plan as a string.
        """
        return self.conn.execute(f"EXPLAIN {sql}").fetchone()[0]

    def list_tables(self) -> list[str]:
        """List all registered table names.

        Returns:
            Sorted list of registered view names.
        """
        return sorted(self._registered_tables)

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()
        logger.debug("Closed DuckDB connection")
