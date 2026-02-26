"""Tests for gold layer aggregation with star schema."""

import io
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.processing.gold_aggregator import GoldAggregator


def _make_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Helper to convert DataFrame to Parquet bytes."""
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@pytest.fixture
def mock_config():
    """Test configuration dict."""
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
def aggregator(mock_client, mock_config):
    """GoldAggregator with mocked dependencies."""
    return GoldAggregator(client=mock_client, config=mock_config)


@pytest.fixture
def silver_orders():
    """Sample silver orders DataFrame."""
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "customer_id": ["C1", "C2", "C1", "C3"],
            "product_id": ["P1", "P2", "P1", "P3"],
            "amount": [100.0, 200.0, 150.0, 300.0],
            "quantity": [1, 2, 1, 3],
        }
    )


class TestCreateDimensionTable:
    """Tests for dimension table creation."""

    def test_extracts_unique_values(self, aggregator, mock_client, silver_orders):
        """Dimension table has unique rows."""
        parquet_data = _make_parquet_bytes(silver_orders)
        now = datetime.now(UTC)
        mock_client.list_objects.return_value = iter(
            [{"key": "silver/orders/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = parquet_data
        mock_client.upload_file.return_value = "s3://test-gold/dim"

        result = aggregator.create_dimension_table(
            "dim_customer", "orders", ["customer_id"]
        )
        assert result["row_count"] == 3  # C1, C2, C3
        assert result["table_type"] == "dimension"

    def test_adds_surrogate_key(self, aggregator, mock_client, silver_orders):
        """Surrogate key column is added when specified."""
        parquet_data = _make_parquet_bytes(silver_orders)
        now = datetime.now(UTC)
        mock_client.list_objects.return_value = iter(
            [{"key": "silver/orders/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = parquet_data
        mock_client.upload_file.return_value = "s3://test-gold/dim"

        aggregator.create_dimension_table(
            "dim_customer", "orders", ["customer_id"], surrogate_key="customer_sk"
        )

        upload_call = mock_client.upload_file.call_args
        written_bytes = upload_call[0][2]
        df = pq.read_table(io.BytesIO(written_bytes)).to_pandas()
        assert "customer_sk" in df.columns
        assert df.columns[0] == "customer_sk"
        assert list(df["customer_sk"]) == [1, 2, 3]

    def test_no_silver_data_raises(self, aggregator, mock_client):
        """Missing silver data raises ValueError."""
        mock_client.list_objects.return_value = iter([])
        with pytest.raises(ValueError, match="No silver data"):
            aggregator.create_dimension_table("dim_x", "missing_table", ["col"])


class TestCreateFactTable:
    """Tests for fact table creation."""

    def test_creates_fact_from_silver(self, aggregator, mock_client, silver_orders):
        """Fact table is created from silver data."""
        parquet_data = _make_parquet_bytes(silver_orders)
        now = datetime.now(UTC)
        mock_client.list_objects.return_value = iter(
            [{"key": "silver/orders/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = parquet_data
        mock_client.upload_file.return_value = "s3://test-gold/fact"

        result = aggregator.create_fact_table(
            "fact_sales", "orders", measures=["amount", "quantity"]
        )
        assert result["row_count"] == 4
        assert result["table_type"] == "fact"

    def test_aggregation(self, aggregator, mock_client, silver_orders):
        """Aggregation groups and summarizes correctly."""
        parquet_data = _make_parquet_bytes(silver_orders)
        now = datetime.now(UTC)
        mock_client.list_objects.return_value = iter(
            [{"key": "silver/orders/data.parquet", "size": 100, "modified": now}]
        )
        mock_client.download_file.return_value = parquet_data
        mock_client.upload_file.return_value = "s3://test-gold/fact"

        result = aggregator.create_fact_table(
            "fact_sales",
            "orders",
            measures=["amount"],
            aggregations={"amount": "sum"},
            group_by=["customer_id"],
        )
        assert result["row_count"] == 3  # C1, C2, C3

        upload_call = mock_client.upload_file.call_args
        written_bytes = upload_call[0][2]
        df = pq.read_table(io.BytesIO(written_bytes)).to_pandas()
        c1_amount = df[df["customer_id"] == "C1"]["amount"].iloc[0]
        assert c1_amount == 250.0  # 100 + 150


class TestWriteGoldTable:
    """Tests for Parquet output."""

    def test_parquet_has_snappy_compression(self, aggregator, mock_client):
        """Output Parquet uses snappy compression."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        mock_client.upload_file.return_value = "s3://test-gold/output"

        aggregator._write_gold_table(df, "test_table", "dimension")

        upload_call = mock_client.upload_file.call_args
        written_bytes = upload_call[0][2]
        pf = pq.ParquetFile(io.BytesIO(written_bytes))
        assert pf.metadata.row_group(0).column(0).compression == "SNAPPY"

    def test_metadata_includes_table_type(self, aggregator, mock_client):
        """Upload metadata includes table type."""
        df = pd.DataFrame({"a": [1]})
        mock_client.upload_file.return_value = "s3://test-gold/output"

        aggregator._write_gold_table(df, "test", "fact")

        upload_call = mock_client.upload_file.call_args
        metadata = (
            upload_call[1]["metadata"]
            if "metadata" in upload_call[1]
            else upload_call[0][3]
        )
        assert metadata["table_type"] == "fact"
