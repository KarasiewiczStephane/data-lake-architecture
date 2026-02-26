"""Data partitioning strategy for the data lake.

Generates and parses S3 key paths using Hive-style partitioning
(key=value) organized by date and optional data source.
"""

import logging
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import NamedTuple

logger = logging.getLogger(__name__)


class PartitionKey(NamedTuple):
    """Represents a date-based partition path.

    Attributes:
        year: Partition year.
        month: Partition month.
        day: Partition day.
        source: Optional data source identifier.
    """

    year: int
    month: int
    day: int
    source: str | None = None

    def to_path(self) -> str:
        """Convert partition key to a Hive-style path string.

        Returns:
            Path like 'source=pos/year=2024/month=01/day=15'
            or 'year=2024/month=01/day=15' if no source.
        """
        parts = [f"year={self.year}", f"month={self.month:02d}", f"day={self.day:02d}"]
        if self.source:
            parts.insert(0, f"source={self.source}")
        return "/".join(parts)


class Partitioner:
    """Generates partition paths for data lake objects.

    Uses Hive-style partitioning with year/month/day hierarchy
    and optional source-level partitioning.

    Args:
        date_format: strftime format for date representation.
    """

    def __init__(self, date_format: str = "%Y/%m/%d") -> None:
        self.date_format = date_format

    def generate_key(
        self,
        layer: str,
        table_name: str,
        filename: str,
        timestamp: datetime | None = None,
        source: str | None = None,
    ) -> str:
        """Generate a full S3 key with partition path.

        Args:
            layer: Data layer (bronze, silver, or gold).
            table_name: Logical table name.
            filename: File name with extension.
            timestamp: Partition timestamp. Defaults to now.
            source: Optional data source identifier.

        Returns:
            Full S3 key path, e.g.:
            'bronze/orders/source=pos/year=2024/month=01/day=15/orders_001.json'
        """
        if timestamp is None:
            timestamp = datetime.now(UTC)

        partition = PartitionKey(
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
            source=source,
        )

        path = PurePosixPath(layer) / table_name / partition.to_path() / filename
        return str(path)

    def parse_key(self, key: str) -> dict[str, str | int]:
        """Parse partition information from an S3 key.

        Args:
            key: S3 object key to parse.

        Returns:
            Dict with layer, table, year, month, day, source, filename.
        """
        parts = PurePosixPath(key).parts
        result: dict[str, str | int] = {"layer": parts[0], "table": parts[1]}

        for part in parts[2:-1]:
            if "=" in part:
                k, v = part.split("=", 1)
                result[k] = int(v) if k in ("year", "month", "day") else v

        result["filename"] = parts[-1]
        return result

    def generate_partition_filter(
        self,
        layer: str,
        table_name: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        source: str | None = None,
    ) -> str:
        """Generate a prefix for listing partitioned data.

        Creates the most specific prefix possible for the given
        date range to minimize the number of objects listed.

        Args:
            layer: Data layer name.
            table_name: Logical table name.
            start_date: Start of date range filter.
            end_date: End of date range filter.
            source: Optional source filter.

        Returns:
            S3 prefix string for filtered listing.
        """
        prefix = f"{layer}/{table_name}/"

        if source:
            prefix += f"source={source}/"

        if start_date and end_date and start_date.year == end_date.year:
            prefix += f"year={start_date.year}/"
            if start_date.month == end_date.month:
                prefix += f"month={start_date.month:02d}/"
                if start_date.day == end_date.day:
                    prefix += f"day={start_date.day:02d}/"

        return prefix
