"""Silver layer processing pipeline.

Transforms bronze (raw) data into cleaned, validated, typed Parquet
files with deduplication, null handling, and schema enforcement.
"""

import io
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.minio_client import MinIOClient
from src.storage.partitioner import Partitioner
from src.utils.config import load_config

logger = logging.getLogger(__name__)


class SilverProcessor:
    """Processes bronze data into cleaned silver layer Parquet files.

    Reads raw data from bronze, applies cleaning transformations
    (deduplication, null handling, type enforcement), and writes
    validated Parquet to silver.

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
        self.bronze_bucket = self.config["buckets"]["bronze"]
        self.silver_bucket = self.config["buckets"]["silver"]

    def process_table(
        self,
        table_name: str,
        schema: dict[str, str] | None = None,
        dedup_columns: list[str] | None = None,
        transformations: dict[str, Callable] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict:
        """Process bronze data into silver layer.

        Args:
            table_name: Logical table name to process.
            schema: Expected column types {column: dtype} for enforcement.
            dedup_columns: Columns to use for deduplication.
            transformations: {column: transform_func} for cleaning.
            start_date: Process data from this date.
            end_date: Process data until this date.

        Returns:
            Dict with s3_uri, key, records_in, records_out,
            duplicates_removed, and processing_timestamp.
        """
        prefix = self.partitioner.generate_partition_filter(
            "bronze", table_name, start_date, end_date
        )
        bronze_files = list(self.client.list_objects(self.bronze_bucket, prefix))

        if not bronze_files:
            logger.warning("No bronze data found for %s", table_name)
            return {"status": "no_data", "records_in": 0, "records_out": 0}

        dfs = []
        for obj in bronze_files:
            content = self.client.download_file(self.bronze_bucket, obj["key"])
            df = self._parse_content(content, obj["key"])
            df["_source_file"] = obj["key"]
            dfs.append(df)

        combined = pd.concat(dfs, ignore_index=True)
        records_in = len(combined)

        cleaned = self._clean_data(combined, schema, dedup_columns, transformations)
        records_out = len(cleaned)

        timestamp = datetime.now(UTC)
        output_key = self.partitioner.generate_key(
            layer="silver",
            table_name=table_name,
            filename=f"{table_name}.parquet",
            timestamp=timestamp,
        )

        output_df = cleaned.drop(columns=["_source_file"])
        table = pa.Table.from_pandas(output_df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        parquet_bytes = buffer.getvalue()

        metadata = {
            "records_in": str(records_in),
            "records_out": str(records_out),
            "processing_timestamp": timestamp.isoformat(),
            "schema_version": "1.0",
        }

        s3_uri = self.client.upload_file(
            self.silver_bucket, output_key, parquet_bytes, metadata=metadata
        )

        logger.info(
            "Processed %s: %d -> %d records (-%d duplicates)",
            table_name,
            records_in,
            records_out,
            records_in - records_out,
        )

        return {
            "s3_uri": s3_uri,
            "key": output_key,
            "records_in": records_in,
            "records_out": records_out,
            "duplicates_removed": records_in - records_out,
            "processing_timestamp": timestamp,
        }

    def _parse_content(self, content: bytes, key: str) -> pd.DataFrame:
        """Parse bronze file content based on extension.

        Args:
            content: Raw file bytes.
            key: S3 key (used to determine format from extension).

        Returns:
            Parsed DataFrame.

        Raises:
            ValueError: If file type is not recognized.
        """
        text = content.decode("utf-8")
        if key.endswith(".json"):
            data = json.loads(text)
            return pd.DataFrame(data if isinstance(data, list) else [data])
        elif key.endswith(".jsonl"):
            records = [json.loads(line) for line in text.strip().split("\n") if line]
            return pd.DataFrame(records)
        elif key.endswith(".csv"):
            return pd.read_csv(io.StringIO(text))
        raise ValueError(f"Unknown file type: {key}")

    def _clean_data(
        self,
        df: pd.DataFrame,
        schema: dict[str, str] | None,
        dedup_columns: list[str] | None,
        transformations: dict[str, Callable] | None,
    ) -> pd.DataFrame:
        """Apply cleaning operations to a DataFrame.

        Args:
            df: Input DataFrame.
            schema: Column type mapping for enforcement.
            dedup_columns: Columns for deduplication.
            transformations: Column transformation functions.

        Returns:
            Cleaned DataFrame.
        """
        result = df.copy()

        if transformations:
            for col, func in transformations.items():
                if col in result.columns:
                    result[col] = result[col].apply(func)

        for col in result.columns:
            if col == "_source_file":
                continue
            if result[col].dtype == "object":
                result[col] = result[col].fillna("")
            else:
                result[col] = result[col].fillna(0)

        if schema:
            for col, dtype in schema.items():
                if col in result.columns:
                    result[col] = result[col].astype(dtype)

        if dedup_columns:
            result = result.drop_duplicates(subset=dedup_columns, keep="last")

        return result
