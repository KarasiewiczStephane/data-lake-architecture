"""Tests for data partitioning strategy."""

from datetime import UTC, datetime

from src.storage.partitioner import PartitionKey, Partitioner


class TestPartitionKey:
    """Tests for PartitionKey named tuple."""

    def test_to_path_without_source(self):
        """Path without source has year/month/day only."""
        pk = PartitionKey(year=2024, month=1, day=15)
        assert pk.to_path() == "year=2024/month=01/day=15"

    def test_to_path_with_source(self):
        """Path with source prepends source partition."""
        pk = PartitionKey(year=2024, month=3, day=5, source="pos")
        assert pk.to_path() == "source=pos/year=2024/month=03/day=05"

    def test_month_and_day_zero_padded(self):
        """Month and day values are zero-padded to two digits."""
        pk = PartitionKey(year=2024, month=1, day=2)
        path = pk.to_path()
        assert "month=01" in path
        assert "day=02" in path


class TestGenerateKey:
    """Tests for key generation."""

    def test_generates_expected_path(self):
        """Key path matches expected structure."""
        p = Partitioner()
        ts = datetime(2024, 6, 15, 10, 30)
        key = p.generate_key("bronze", "orders", "data.json", timestamp=ts)
        assert key == "bronze/orders/year=2024/month=06/day=15/data.json"

    def test_with_source(self):
        """Key includes source partition when specified."""
        p = Partitioner()
        ts = datetime(2024, 1, 1)
        key = p.generate_key(
            "bronze", "orders", "data.json", timestamp=ts, source="pos"
        )
        assert key == "bronze/orders/source=pos/year=2024/month=01/day=01/data.json"

    def test_defaults_to_current_time(self):
        """Without timestamp, uses current time."""
        p = Partitioner()
        key = p.generate_key("silver", "customers", "file.parquet")
        now = datetime.now(UTC)
        assert f"year={now.year}" in key

    def test_different_layers(self):
        """Key correctly uses the specified layer."""
        p = Partitioner()
        ts = datetime(2024, 1, 1)
        for layer in ["bronze", "silver", "gold"]:
            key = p.generate_key(layer, "t", "f.txt", timestamp=ts)
            assert key.startswith(f"{layer}/")


class TestParseKey:
    """Tests for key parsing."""

    def test_parse_basic_key(self):
        """Parsing extracts layer, table, partition fields, and filename."""
        p = Partitioner()
        key = "bronze/orders/year=2024/month=06/day=15/data.json"
        result = p.parse_key(key)
        assert result["layer"] == "bronze"
        assert result["table"] == "orders"
        assert result["year"] == 2024
        assert result["month"] == 6
        assert result["day"] == 15
        assert result["filename"] == "data.json"

    def test_parse_key_with_source(self):
        """Parsing extracts source partition."""
        p = Partitioner()
        key = "silver/events/source=web/year=2024/month=01/day=01/data.parquet"
        result = p.parse_key(key)
        assert result["source"] == "web"
        assert result["layer"] == "silver"

    def test_roundtrip(self):
        """generate_key followed by parse_key returns original values."""
        p = Partitioner()
        ts = datetime(2024, 3, 10)
        key = p.generate_key("gold", "sales", "agg.parquet", timestamp=ts, source="api")
        parsed = p.parse_key(key)
        assert parsed["layer"] == "gold"
        assert parsed["table"] == "sales"
        assert parsed["year"] == 2024
        assert parsed["month"] == 3
        assert parsed["day"] == 10
        assert parsed["source"] == "api"
        assert parsed["filename"] == "agg.parquet"


class TestGeneratePartitionFilter:
    """Tests for partition filter prefix generation."""

    def test_no_filters(self):
        """Without date/source filters, returns layer/table prefix."""
        p = Partitioner()
        prefix = p.generate_partition_filter("bronze", "orders")
        assert prefix == "bronze/orders/"

    def test_source_filter(self):
        """Source filter adds source partition to prefix."""
        p = Partitioner()
        prefix = p.generate_partition_filter("bronze", "orders", source="pos")
        assert prefix == "bronze/orders/source=pos/"

    def test_single_day_filter(self):
        """Same start and end date narrows to specific day."""
        p = Partitioner()
        d = datetime(2024, 6, 15)
        prefix = p.generate_partition_filter("silver", "t", start_date=d, end_date=d)
        assert prefix == "silver/t/year=2024/month=06/day=15/"

    def test_same_month_filter(self):
        """Same month with different days narrows to month."""
        p = Partitioner()
        start = datetime(2024, 6, 1)
        end = datetime(2024, 6, 30)
        prefix = p.generate_partition_filter(
            "silver", "t", start_date=start, end_date=end
        )
        assert prefix == "silver/t/year=2024/month=06/"

    def test_same_year_different_month(self):
        """Same year with different months narrows to year."""
        p = Partitioner()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)
        prefix = p.generate_partition_filter(
            "gold", "t", start_date=start, end_date=end
        )
        assert prefix == "gold/t/year=2024/"

    def test_cross_year_filter(self):
        """Different years result in no date filter."""
        p = Partitioner()
        start = datetime(2023, 1, 1)
        end = datetime(2024, 12, 31)
        prefix = p.generate_partition_filter(
            "gold", "t", start_date=start, end_date=end
        )
        assert prefix == "gold/t/"
