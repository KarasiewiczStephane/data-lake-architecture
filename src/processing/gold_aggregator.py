"""Gold layer aggregation with star schema support.

Creates business-level aggregations organized as fact and dimension
tables in optimized Parquet format for analytical queries.
"""

import io
import logging
from datetime import UTC, datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.minio_client import MinIOClient
from src.storage.partitioner import Partitioner
from src.utils.config import load_config

logger = logging.getLogger(__name__)


class GoldAggregator:
    """Creates star schema aggregations in the gold layer.

    Reads cleaned data from silver, builds dimension tables with
    surrogate keys and fact tables with foreign key lookups and
    aggregations.

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
        self.silver_bucket = self.config["buckets"]["silver"]
        self.gold_bucket = self.config["buckets"]["gold"]

    def create_dimension_table(
        self,
        dim_name: str,
        source_table: str,
        columns: list[str],
        surrogate_key: str | None = None,
    ) -> dict:
        """Create a dimension table from silver data.

        Extracts unique values for the specified columns, optionally
        adds a surrogate key, and writes to gold as Parquet.

        Args:
            dim_name: Dimension table name (e.g., 'dim_customer').
            source_table: Silver layer source table name.
            columns: Columns to include in the dimension.
            surrogate_key: Column name for surrogate key. Added if set.

        Returns:
            Dict with s3_uri, key, row_count, and table_type.
        """
        df = self._read_silver_table(source_table)

        available_cols = [c for c in columns if c in df.columns]
        dim_df = df[available_cols].drop_duplicates().reset_index(drop=True)

        if surrogate_key:
            dim_df[surrogate_key] = range(1, len(dim_df) + 1)
            cols = [surrogate_key] + [c for c in dim_df.columns if c != surrogate_key]
            dim_df = dim_df[cols]

        logger.info(
            "Created dimension %s with %d rows from %s",
            dim_name,
            len(dim_df),
            source_table,
        )
        return self._write_gold_table(dim_df, dim_name, table_type="dimension")

    def create_fact_table(
        self,
        fact_name: str,
        source_table: str,
        measures: list[str],
        dimension_keys: dict[str, tuple[str, str]] | None = None,
        aggregations: dict[str, str] | None = None,
        group_by: list[str] | None = None,
    ) -> dict:
        """Create a fact table with optional dimension key lookups.

        Args:
            fact_name: Fact table name (e.g., 'fact_sales').
            source_table: Silver layer source table name.
            measures: Columns containing numeric measures.
            dimension_keys: {dim_table: (lookup_col, fk_col)} mappings.
            aggregations: {measure: 'sum'|'avg'|'count'} for aggregation.
            group_by: Columns to group by for aggregation.

        Returns:
            Dict with s3_uri, key, row_count, and table_type.
        """
        fact_df = self._read_silver_table(source_table)

        if dimension_keys:
            for dim_table, (lookup_col, fk_col) in dimension_keys.items():
                dim_df = self._read_gold_table(dim_table)
                sk_col = dim_df.columns[0]
                lookup = dim_df.set_index(lookup_col)[sk_col].to_dict()
                fact_df[fk_col] = fact_df[lookup_col].map(lookup)

        if aggregations and group_by:
            agg_dict = {m: a for m, a in aggregations.items() if m in fact_df.columns}
            if agg_dict:
                fact_df = fact_df.groupby(group_by, as_index=False).agg(agg_dict)

        logger.info(
            "Created fact %s with %d rows from %s",
            fact_name,
            len(fact_df),
            source_table,
        )
        return self._write_gold_table(fact_df, fact_name, table_type="fact")

    def _read_silver_table(self, table_name: str) -> pd.DataFrame:
        """Read the latest silver table data.

        Args:
            table_name: Silver table name.

        Returns:
            DataFrame with the latest silver data.

        Raises:
            ValueError: If no silver data exists for the table.
        """
        prefix = f"silver/{table_name}/"
        objects = list(self.client.list_objects(self.silver_bucket, prefix))
        if not objects:
            raise ValueError(f"No silver data found for {table_name}")

        latest = max(objects, key=lambda x: x["modified"])
        content = self.client.download_file(self.silver_bucket, latest["key"])
        return pd.read_parquet(io.BytesIO(content))

    def _read_gold_table(self, table_name: str) -> pd.DataFrame:
        """Read existing gold table data.

        Args:
            table_name: Gold table name.

        Returns:
            DataFrame with gold table data.

        Raises:
            ValueError: If no gold data exists for the table.
        """
        prefix = f"gold/{table_name}/"
        objects = list(self.client.list_objects(self.gold_bucket, prefix))
        if not objects:
            raise ValueError(f"No gold data found for {table_name}")

        latest = max(objects, key=lambda x: x["modified"])
        content = self.client.download_file(self.gold_bucket, latest["key"])
        return pd.read_parquet(io.BytesIO(content))

    def _write_gold_table(
        self, df: pd.DataFrame, table_name: str, table_type: str
    ) -> dict:
        """Write a DataFrame to the gold layer as optimized Parquet.

        Args:
            df: Data to write.
            table_name: Gold table name.
            table_type: 'fact' or 'dimension'.

        Returns:
            Dict with s3_uri, key, row_count, and table_type.
        """
        timestamp = datetime.now(UTC)
        key = self.partitioner.generate_key(
            layer="gold",
            table_name=table_name,
            filename=f"{table_name}.parquet",
            timestamp=timestamp,
        )

        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy", row_group_size=50000)

        metadata = {
            "table_type": table_type,
            "row_count": str(len(df)),
            "created_at": timestamp.isoformat(),
        }

        s3_uri = self.client.upload_file(
            self.gold_bucket, key, buffer.getvalue(), metadata=metadata
        )

        return {
            "s3_uri": s3_uri,
            "key": key,
            "row_count": len(df),
            "table_type": table_type,
        }
