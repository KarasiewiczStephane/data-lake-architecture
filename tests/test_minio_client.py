"""Tests for MinIO storage client."""

import pytest
from moto import mock_aws

from src.storage.minio_client import MinIOClient

ENDPOINT = "localhost:9000"
ACCESS_KEY = "testing"
SECRET_KEY = "testing"
BUCKET = "test-bucket"


@pytest.fixture
def mock_client():
    """Create a MinIOClient backed by moto's mock S3."""
    with mock_aws():
        client = MinIOClient(ENDPOINT, ACCESS_KEY, SECRET_KEY)
        # Override endpoint to use moto's mock (no actual network call needed)
        import boto3

        client.s3 = boto3.client("s3", region_name="us-east-1")
        yield client


class TestCreateBucket:
    """Tests for bucket creation."""

    def test_create_new_bucket(self, mock_client):
        """Creating a new bucket returns True."""
        assert mock_client.create_bucket(BUCKET) is True

    def test_create_existing_bucket(self, mock_client):
        """Creating an existing bucket returns False."""
        mock_client.create_bucket(BUCKET)
        assert mock_client.create_bucket(BUCKET) is False


class TestUploadDownload:
    """Tests for file upload and download."""

    def test_upload_returns_s3_uri(self, mock_client):
        """Upload returns correct S3 URI."""
        mock_client.create_bucket(BUCKET)
        uri = mock_client.upload_file(BUCKET, "test/file.txt", b"hello world")
        assert uri == f"s3://{BUCKET}/test/file.txt"

    def test_download_returns_content(self, mock_client):
        """Download returns the uploaded content."""
        mock_client.create_bucket(BUCKET)
        content = b"test content"
        mock_client.upload_file(BUCKET, "data.txt", content)
        result = mock_client.download_file(BUCKET, "data.txt")
        assert result == content

    def test_upload_with_metadata(self, mock_client):
        """Upload stores metadata correctly."""
        mock_client.create_bucket(BUCKET)
        metadata = {"source": "test", "version": "1.0"}
        mock_client.upload_file(BUCKET, "meta.txt", b"data", metadata=metadata)
        result = mock_client.get_object_metadata(BUCKET, "meta.txt")
        assert result["source"] == "test"
        assert result["version"] == "1.0"

    def test_upload_file_like_object(self, mock_client):
        """Upload accepts file-like objects."""
        import io

        mock_client.create_bucket(BUCKET)
        data = io.BytesIO(b"file-like content")
        mock_client.upload_file(BUCKET, "stream.txt", data)
        result = mock_client.download_file(BUCKET, "stream.txt")
        assert result == b"file-like content"


class TestListObjects:
    """Tests for object listing."""

    def test_list_empty_bucket(self, mock_client):
        """Listing an empty bucket returns no objects."""
        mock_client.create_bucket(BUCKET)
        objects = list(mock_client.list_objects(BUCKET))
        assert objects == []

    def test_list_with_prefix(self, mock_client):
        """Listing with prefix filters correctly."""
        mock_client.create_bucket(BUCKET)
        mock_client.upload_file(BUCKET, "bronze/a.txt", b"a")
        mock_client.upload_file(BUCKET, "silver/b.txt", b"b")
        objects = list(mock_client.list_objects(BUCKET, prefix="bronze/"))
        assert len(objects) == 1
        assert objects[0]["key"] == "bronze/a.txt"

    def test_list_returns_correct_fields(self, mock_client):
        """Listed objects have key, size, and modified fields."""
        mock_client.create_bucket(BUCKET)
        mock_client.upload_file(BUCKET, "test.txt", b"content")
        objects = list(mock_client.list_objects(BUCKET))
        assert len(objects) == 1
        assert "key" in objects[0]
        assert "size" in objects[0]
        assert "modified" in objects[0]


class TestDeleteObject:
    """Tests for object deletion."""

    def test_delete_removes_object(self, mock_client):
        """Deleting an object removes it from the bucket."""
        mock_client.create_bucket(BUCKET)
        mock_client.upload_file(BUCKET, "delete-me.txt", b"temp")
        mock_client.delete_object(BUCKET, "delete-me.txt")
        assert mock_client.object_exists(BUCKET, "delete-me.txt") is False


class TestObjectExists:
    """Tests for object existence check."""

    def test_existing_object(self, mock_client):
        """Existing object returns True."""
        mock_client.create_bucket(BUCKET)
        mock_client.upload_file(BUCKET, "exists.txt", b"data")
        assert mock_client.object_exists(BUCKET, "exists.txt") is True

    def test_nonexistent_object(self, mock_client):
        """Nonexistent object returns False."""
        mock_client.create_bucket(BUCKET)
        assert mock_client.object_exists(BUCKET, "nope.txt") is False
