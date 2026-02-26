"""Bronze layer ingestion pipeline.

Loads raw data into the bronze layer with automatic partitioning,
metadata tagging, and append-only semantics. Supports JSON, CSV,
and JSONL file formats.
"""

import csv
import hashlib
import io
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from src.storage.minio_client import MinIOClient
from src.storage.partitioner import Partitioner
from src.utils.config import load_config

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = ("json", "csv", "jsonl")


class BronzeLoader:
    """Loads raw data into the bronze layer of the data lake.

    Data is stored as-is (append-only, immutable) with metadata tags
    for source tracking, schema versioning, and data lineage.

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
        self.partitioner = Partitioner()
        self.bucket = self.config["buckets"]["bronze"]

    def ingest_file(
        self,
        file_path: str | Path,
        table_name: str,
        source: str,
        schema_version: str = "1.0",
    ) -> dict:
        """Ingest a local file into the bronze layer.

        Args:
            file_path: Path to local file (JSON, CSV, or JSONL).
            table_name: Logical table name for partitioning.
            source: Data source identifier for lineage.
            schema_version: Version string for schema tracking.

        Returns:
            Dict with s3_uri, key, record_count, file_hash,
            ingestion_timestamp, and metadata.

        Raises:
            ValueError: If file format is not supported.
            FileNotFoundError: If file does not exist.
        """
        file_path = Path(file_path)
        file_format = file_path.suffix.lstrip(".").lower()

        if file_format not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format: {file_format}. "
                f"Supported: {', '.join(SUPPORTED_FORMATS)}"
            )

        with open(file_path, "rb") as f:
            content = f.read()

        return self.ingest_bytes(
            content, table_name, source, file_format, schema_version, file_path.name
        )

    def ingest_bytes(
        self,
        data: bytes,
        table_name: str,
        source: str,
        file_format: str,
        schema_version: str = "1.0",
        filename: str | None = None,
    ) -> dict:
        """Ingest raw bytes into the bronze layer.

        Args:
            data: Raw file content as bytes.
            table_name: Logical table name.
            source: Data source identifier.
            file_format: File format (json, csv, jsonl).
            schema_version: Schema version tag.
            filename: Original filename. Auto-generated if None.

        Returns:
            Dict with ingestion results and metadata.
        """
        timestamp = datetime.now(UTC)
        file_hash = hashlib.md5(data).hexdigest()[:8]  # noqa: S324

        if filename is None:
            filename = (
                f"{table_name}_{timestamp.strftime('%Y%m%d%H%M%S')}"
                f"_{file_hash}.{file_format}"
            )
        else:
            stem = Path(filename).stem
            filename = f"{stem}_{file_hash}.{file_format}"

        key = self.partitioner.generate_key(
            layer="bronze",
            table_name=table_name,
            filename=filename,
            timestamp=timestamp,
            source=source,
        )

        record_count = _count_records(data, file_format)

        metadata = {
            "source": source,
            "ingestion_timestamp": timestamp.isoformat(),
            "schema_version": schema_version,
            "file_hash": file_hash,
            "record_count": str(record_count),
            "original_format": file_format,
        }

        s3_uri = self.client.upload_file(self.bucket, key, data, metadata=metadata)

        logger.info(
            "Ingested %d records to %s (hash=%s)", record_count, s3_uri, file_hash
        )

        return {
            "s3_uri": s3_uri,
            "key": key,
            "record_count": record_count,
            "file_hash": file_hash,
            "ingestion_timestamp": timestamp,
            "metadata": metadata,
        }

    def list_ingested(
        self,
        table_name: str,
        source: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[dict]:
        """List ingested files in the bronze layer.

        Args:
            table_name: Table name to list.
            source: Optional source filter.
            start_date: Optional start date filter.
            end_date: Optional end date filter.

        Returns:
            List of object metadata dicts from storage.
        """
        prefix = self.partitioner.generate_partition_filter(
            "bronze", table_name, start_date, end_date, source
        )
        return list(self.client.list_objects(self.bucket, prefix))


def _count_records(data: bytes, file_format: str) -> int:
    """Count the number of records in raw data.

    Args:
        data: Raw file content.
        file_format: File format identifier.

    Returns:
        Number of records found.
    """
    text = data.decode("utf-8")
    if file_format == "json":
        parsed = json.loads(text)
        return len(parsed) if isinstance(parsed, list) else 1
    elif file_format == "jsonl":
        return sum(1 for line in text.strip().split("\n") if line.strip())
    elif file_format == "csv":
        reader = csv.reader(io.StringIO(text))
        return max(sum(1 for _ in reader) - 1, 0)  # exclude header
    return 0
