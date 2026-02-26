"""Tests for DuckDB query engine."""

import io
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.query.duckdb_engine import DuckDBEngine


def _make_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to Parquet bytes."""
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@pytest.fixture
def sample_df():
    """Sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "amount": [100.0, 200.0, 300.0],
        }
    )


@pytest.fixture
def mock_config():
    """Test configuration."""
    return {
        "minio": {
            "endpoint": "localhost:9000",
            "access_key": "test",
            "secret_key": "test",
            "secure": False,
        },
        "buckets": {
            "bronze": "test-bronze",
            "silver": "test-silver",
            "gold": "test-gold",
        },
    }


@pytest.fixture
def mock_client():
    """Mock storage client."""
    return MagicMock()


@pytest.fixture
def engine(mock_client, mock_config):
    """DuckDBEngine with mocked dependencies."""
    eng = DuckDBEngine(client=mock_client, config=mock_config)
    yield eng
    eng.close()


class TestRegisterTable:
    """Tests for table registration."""

    def test_register_table(self, engine, mock_client, sample_df):
        """Registering a table makes it queryable."""
        now = datetime.now(UTC)
        mock_client.list_objects.return_value = iter(
            [{"key": "silver/orders/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = _make_parquet_bytes(sample_df)

        name = engine.register_table("silver", "orders")
        assert name == "silver_orders"
        assert "silver_orders" in engine.list_tables()

    def test_register_with_alias(self, engine, mock_client, sample_df):
        """Custom alias is used as view name."""
        now = datetime.now(UTC)
        mock_client.list_objects.return_value = iter(
            [{"key": "gold/sales/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = _make_parquet_bytes(sample_df)

        name = engine.register_table("gold", "sales", alias="my_sales")
        assert name == "my_sales"

    def test_no_parquet_raises(self, engine, mock_client):
        """Missing Parquet files raise ValueError."""
        mock_client.list_objects.return_value = iter(
            [
                {
                    "key": "silver/orders/data.json",
                    "size": 100,
                    "modified": datetime.now(UTC),
                }
            ]
        )
        with pytest.raises(ValueError, match="No Parquet files"):
            engine.register_table("silver", "orders")


class TestQuery:
    """Tests for query execution."""

    def test_query_returns_dicts(self, engine, mock_client, sample_df):
        """query() returns list of dictionaries."""
        now = datetime.now(UTC)
        mock_client.list_objects.return_value = iter(
            [{"key": "silver/t/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = _make_parquet_bytes(sample_df)
        engine.register_table("silver", "t")

        results = engine.query("SELECT * FROM silver_t WHERE amount > 150")
        assert len(results) == 2
        assert all(isinstance(r, dict) for r in results)

    def test_query_df_returns_dataframe(self, engine, mock_client, sample_df):
        """query_df() returns a pandas DataFrame."""
        now = datetime.now(UTC)
        mock_client.list_objects.return_value = iter(
            [{"key": "silver/t/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = _make_parquet_bytes(sample_df)
        engine.register_table("silver", "t")

        result = engine.query_df("SELECT count(*) as cnt FROM silver_t")
        assert isinstance(result, pd.DataFrame)
        assert result["cnt"].iloc[0] == 3

    def test_query_arrow_returns_table(self, engine, mock_client, sample_df):
        """query_arrow() returns a PyArrow Table."""
        now = datetime.now(UTC)
        mock_client.list_objects.return_value = iter(
            [{"key": "silver/t/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = _make_parquet_bytes(sample_df)
        engine.register_table("silver", "t")

        result = engine.query_arrow("SELECT * FROM silver_t")
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    def test_explain(self, engine, mock_client, sample_df):
        """explain() returns a plan string."""
        now = datetime.now(UTC)
        mock_client.list_objects.return_value = iter(
            [{"key": "silver/t/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = _make_parquet_bytes(sample_df)
        engine.register_table("silver", "t")

        plan = engine.explain("SELECT * FROM silver_t")
        assert isinstance(plan, str)
        assert len(plan) > 0


class TestRegisterAllTables:
    """Tests for auto-registration."""

    def test_registers_across_layers(self, engine, mock_client, sample_df):
        """register_all_tables discovers tables in all layers."""
        now = datetime.now(UTC)
        parquet_bytes = _make_parquet_bytes(sample_df)

        def list_objects_side_effect(bucket, prefix):
            if "silver" in prefix:
                return iter(
                    [
                        {
                            "key": "silver/orders/data.parquet",
                            "size": 100,
                            "modified": now,
                        }
                    ]
                )
            return iter([])

        mock_client.list_objects.side_effect = list_objects_side_effect
        mock_client.download_file.return_value = parquet_bytes

        registered = engine.register_all_tables()
        assert "silver_orders" in registered

    def test_skips_failed_registration(self, engine, mock_client):
        """Failed registrations are skipped with warning."""
        now = datetime.now(UTC)

        def list_objects_side_effect(bucket, prefix):
            if "silver" in prefix:
                return iter(
                    [{"key": "silver/bad/data.txt", "size": 100, "modified": now}]
                )
            return iter([])

        mock_client.list_objects.side_effect = list_objects_side_effect

        registered = engine.register_all_tables()
        assert registered == []


class TestListTables:
    """Tests for table listing."""

    def test_empty_initially(self, engine):
        """No tables registered initially."""
        assert engine.list_tables() == []

    def test_sorted_output(self, engine, mock_client, sample_df):
        """Tables are returned sorted."""
        now = datetime.now(UTC)
        parquet_bytes = _make_parquet_bytes(sample_df)

        mock_client.list_objects.return_value = iter(
            [{"key": "silver/b_table/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = parquet_bytes
        engine.register_table("silver", "b_table")

        mock_client.list_objects.return_value = iter(
            [{"key": "silver/a_table/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = parquet_bytes
        engine.register_table("silver", "a_table")

        assert engine.list_tables() == ["silver_a_table", "silver_b_table"]
