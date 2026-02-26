"""Tests for bronze layer ingestion pipeline."""

import json
from unittest.mock import MagicMock

import pytest
import yaml

from src.processing.bronze_loader import BronzeLoader, _count_records


@pytest.fixture
def mock_config(tmp_path):
    """Create a test config and return path + dict."""
    config = {
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
        "catalog": {"db_path": ":memory:"},
    }
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    return config


@pytest.fixture
def mock_client():
    """Create a mock storage client."""
    client = MagicMock()
    client.upload_file.return_value = "s3://test-bronze/some/key"
    client.list_objects.return_value = iter([])
    return client


@pytest.fixture
def loader(mock_client, mock_config):
    """Create a BronzeLoader with mock dependencies."""
    return BronzeLoader(client=mock_client, config=mock_config)


class TestCountRecords:
    """Tests for record counting utility."""

    def test_json_array(self):
        """JSON array counts all elements."""
        data = json.dumps([{"a": 1}, {"a": 2}, {"a": 3}]).encode()
        assert _count_records(data, "json") == 3

    def test_json_single_object(self):
        """Single JSON object counts as 1."""
        data = json.dumps({"a": 1}).encode()
        assert _count_records(data, "json") == 1

    def test_jsonl(self):
        """JSONL counts lines."""
        data = b'{"a": 1}\n{"a": 2}\n{"a": 3}\n'
        assert _count_records(data, "jsonl") == 3

    def test_jsonl_empty_lines_ignored(self):
        """JSONL ignores blank lines."""
        data = b'{"a": 1}\n\n{"a": 2}\n'
        assert _count_records(data, "jsonl") == 2

    def test_csv(self):
        """CSV counts rows excluding header."""
        data = b"col1,col2\na,1\nb,2\n"
        assert _count_records(data, "csv") == 2

    def test_csv_header_only(self):
        """CSV with only header counts as 0."""
        data = b"col1,col2\n"
        assert _count_records(data, "csv") == 0

    def test_unknown_format(self):
        """Unknown format returns 0."""
        assert _count_records(b"data", "xml") == 0


class TestIngestBytes:
    """Tests for raw bytes ingestion."""

    def test_returns_expected_fields(self, loader):
        """Ingestion result contains all expected fields."""
        data = json.dumps([{"id": 1}]).encode()
        result = loader.ingest_bytes(data, "orders", "pos", "json")
        assert "s3_uri" in result
        assert "key" in result
        assert "record_count" in result
        assert "file_hash" in result
        assert "ingestion_timestamp" in result
        assert "metadata" in result

    def test_record_count_set(self, loader):
        """Record count matches data."""
        data = json.dumps([{"id": 1}, {"id": 2}]).encode()
        result = loader.ingest_bytes(data, "orders", "pos", "json")
        assert result["record_count"] == 2

    def test_metadata_contains_source(self, loader):
        """Metadata includes source identifier."""
        data = b'{"id": 1}'
        result = loader.ingest_bytes(data, "events", "web", "json")
        assert result["metadata"]["source"] == "web"

    def test_metadata_contains_schema_version(self, loader):
        """Metadata includes schema version."""
        data = b'{"id": 1}'
        result = loader.ingest_bytes(
            data, "events", "web", "json", schema_version="2.0"
        )
        assert result["metadata"]["schema_version"] == "2.0"

    def test_auto_generates_filename(self, loader):
        """Without filename, one is auto-generated."""
        data = b'{"id": 1}'
        result = loader.ingest_bytes(data, "test", "src", "json")
        assert "test" in result["key"]
        assert result["key"].endswith(".json")

    def test_hash_appended_to_filename(self, loader):
        """Provided filename gets hash appended for uniqueness."""
        data = b'{"id": 1}'
        result = loader.ingest_bytes(data, "test", "src", "json", filename="data.json")
        assert "data_" in result["key"]
        assert result["key"].endswith(".json")

    def test_upload_called_with_correct_bucket(self, loader, mock_client):
        """Upload uses the configured bronze bucket."""
        data = b'{"id": 1}'
        loader.ingest_bytes(data, "orders", "pos", "json")
        call_args = mock_client.upload_file.call_args
        assert call_args[0][0] == "test-bronze"


class TestIngestFile:
    """Tests for file-based ingestion."""

    def test_json_file(self, loader, tmp_path):
        """JSON file ingestion works."""
        f = tmp_path / "data.json"
        f.write_text(json.dumps([{"a": 1}]))
        result = loader.ingest_file(f, "test", "src")
        assert result["record_count"] == 1

    def test_csv_file(self, loader, tmp_path):
        """CSV file ingestion works."""
        f = tmp_path / "data.csv"
        f.write_text("col1,col2\na,1\nb,2\n")
        result = loader.ingest_file(f, "test", "src")
        assert result["record_count"] == 2

    def test_jsonl_file(self, loader, tmp_path):
        """JSONL file ingestion works."""
        f = tmp_path / "data.jsonl"
        f.write_text('{"a":1}\n{"a":2}\n')
        result = loader.ingest_file(f, "test", "src")
        assert result["record_count"] == 2

    def test_unsupported_format_raises(self, loader, tmp_path):
        """Unsupported file format raises ValueError."""
        f = tmp_path / "data.xml"
        f.write_text("<root/>")
        with pytest.raises(ValueError, match="Unsupported format"):
            loader.ingest_file(f, "test", "src")


class TestListIngested:
    """Tests for listing ingested files."""

    def test_returns_list(self, loader, mock_client):
        """list_ingested returns a list."""
        mock_client.list_objects.return_value = iter([{"key": "a"}, {"key": "b"}])
        result = loader.list_ingested("orders")
        assert len(result) == 2

    def test_passes_source_filter(self, loader, mock_client):
        """Source filter is passed to partition filter."""
        mock_client.list_objects.return_value = iter([])
        loader.list_ingested("orders", source="pos")
        call_args = mock_client.list_objects.call_args
        prefix = call_args[0][1]
        assert "source=pos" in prefix
