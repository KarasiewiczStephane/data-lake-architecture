"""Tests for silver layer processing pipeline."""

import io
import json
from unittest.mock import MagicMock

import pandas as pd
import pyarrow.parquet as pq
import pytest

from src.processing.silver_processor import SilverProcessor


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
        "partitioning": {"date_format": "%Y/%m/%d"},
    }


@pytest.fixture
def mock_client():
    """Mock storage client."""
    return MagicMock()


@pytest.fixture
def processor(mock_client, mock_config):
    """SilverProcessor with mocked dependencies."""
    return SilverProcessor(client=mock_client, config=mock_config)


class TestParseContent:
    """Tests for _parse_content."""

    def test_parse_json_array(self, processor):
        """Parses JSON array to DataFrame."""
        data = json.dumps([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]).encode()
        df = processor._parse_content(data, "bronze/t/file.json")
        assert len(df) == 2
        assert "id" in df.columns

    def test_parse_json_single(self, processor):
        """Parses single JSON object to single-row DataFrame."""
        data = json.dumps({"id": 1, "name": "a"}).encode()
        df = processor._parse_content(data, "bronze/t/file.json")
        assert len(df) == 1

    def test_parse_jsonl(self, processor):
        """Parses JSONL to DataFrame."""
        data = b'{"id": 1}\n{"id": 2}\n'
        df = processor._parse_content(data, "bronze/t/file.jsonl")
        assert len(df) == 2

    def test_parse_csv(self, processor):
        """Parses CSV to DataFrame."""
        data = b"id,name\n1,a\n2,b\n"
        df = processor._parse_content(data, "bronze/t/file.csv")
        assert len(df) == 2

    def test_unknown_format_raises(self, processor):
        """Unknown format raises ValueError."""
        with pytest.raises(ValueError, match="Unknown file type"):
            processor._parse_content(b"data", "bronze/t/file.xml")


class TestCleanData:
    """Tests for _clean_data."""

    def test_fills_null_strings(self, processor):
        """Null string values are filled with empty string."""
        df = pd.DataFrame({"name": ["a", None, "c"]})
        cleaned = processor._clean_data(df, None, None, None)
        assert cleaned["name"].iloc[1] == ""

    def test_fills_null_numbers(self, processor):
        """Null numeric values are filled with 0."""
        df = pd.DataFrame({"value": [1.0, None, 3.0]})
        cleaned = processor._clean_data(df, None, None, None)
        assert cleaned["value"].iloc[1] == 0.0

    def test_deduplication(self, processor):
        """Deduplication removes duplicate rows."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 1],
                "val": ["a", "b", "c"],
                "_source_file": ["f1", "f2", "f3"],
            }
        )
        cleaned = processor._clean_data(df, None, ["id"], None)
        assert len(cleaned) == 2
        # keep='last' means id=1 should have val='c'
        row = cleaned[cleaned["id"] == 1].iloc[0]
        assert row["val"] == "c"

    def test_schema_enforcement(self, processor):
        """Schema enforcement converts column types."""
        df = pd.DataFrame({"price": ["10", "20", "30"]})
        cleaned = processor._clean_data(df, {"price": "float"}, None, None)
        assert cleaned["price"].dtype == float

    def test_transformations(self, processor):
        """Custom transformations are applied."""
        df = pd.DataFrame({"name": ["  Alice  ", "  Bob  "]})
        cleaned = processor._clean_data(
            df, None, None, {"name": lambda x: x.strip().upper()}
        )
        assert cleaned["name"].iloc[0] == "ALICE"
        assert cleaned["name"].iloc[1] == "BOB"


class TestProcessTable:
    """Tests for end-to-end table processing."""

    def test_no_data_returns_status(self, processor, mock_client):
        """Returns no_data status when bronze is empty."""
        mock_client.list_objects.return_value = iter([])
        result = processor.process_table("orders")
        assert result["status"] == "no_data"
        assert result["records_in"] == 0

    def test_processes_json_files(self, processor, mock_client):
        """Processes JSON files from bronze to silver Parquet."""
        data = json.dumps([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]).encode()
        mock_client.list_objects.return_value = iter(
            [{"key": "bronze/orders/year=2024/month=01/day=01/data.json", "size": 100}]
        )
        mock_client.download_file.return_value = data
        mock_client.upload_file.return_value = "s3://test-silver/silver/orders/..."

        result = processor.process_table("orders")
        assert result["records_in"] == 2
        assert result["records_out"] == 2
        assert result["duplicates_removed"] == 0

        # Verify Parquet was uploaded
        upload_call = mock_client.upload_file.call_args
        assert upload_call[0][0] == "test-silver"
        parquet_bytes = upload_call[0][2]
        df = pq.read_table(io.BytesIO(parquet_bytes)).to_pandas()
        assert len(df) == 2

    def test_deduplication_reflected_in_stats(self, processor, mock_client):
        """Deduplication stats are reflected in result."""
        data = json.dumps(
            [
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b"},
                {"id": 1, "name": "c"},
            ]
        ).encode()
        mock_client.list_objects.return_value = iter(
            [{"key": "bronze/orders/year=2024/month=01/day=01/data.json", "size": 100}]
        )
        mock_client.download_file.return_value = data
        mock_client.upload_file.return_value = "s3://test-silver/output"

        result = processor.process_table("orders", dedup_columns=["id"])
        assert result["records_in"] == 3
        assert result["records_out"] == 2
        assert result["duplicates_removed"] == 1
