"""Tests for data catalog and schema management."""

import pytest

from src.catalog.metadata_store import MetadataStore
from src.catalog.schema_manager import SchemaManager


@pytest.fixture
def catalog():
    """In-memory catalog for testing."""
    return MetadataStore(":memory:")


@pytest.fixture
def schema_mgr(catalog):
    """SchemaManager with in-memory catalog."""
    return SchemaManager(catalog)


class TestTableRegistration:
    """Tests for table registration."""

    def test_register_new_table(self, catalog):
        """Registering a new table returns a row ID."""
        row_id = catalog.register_table(
            "orders", "bronze", "s3://bronze/orders", description="Order data"
        )
        assert row_id is not None

    def test_register_table_with_partitions(self, catalog):
        """Partition keys are stored."""
        catalog.register_table(
            "orders",
            "bronze",
            "s3://bronze/orders",
            partition_keys=["year", "month"],
        )
        results = catalog.search("orders")
        assert len(results) == 1

    def test_update_existing_table(self, catalog):
        """Re-registering updates instead of duplicating."""
        catalog.register_table("orders", "bronze", "s3://old")
        catalog.register_table("orders", "bronze", "s3://new")
        results = catalog.search("orders")
        assert len(results) == 1


class TestColumnRegistration:
    """Tests for column registration."""

    def test_register_column(self, catalog):
        """Column is registered for a table."""
        catalog.register_table("orders", "silver", "s3://silver/orders")
        col_id = catalog.register_column("orders", "silver", "order_id", "int64")
        assert col_id is not None

    def test_column_for_missing_table_raises(self, catalog):
        """Registering column for nonexistent table raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            catalog.register_column("missing", "silver", "col", "string")

    def test_update_column_type(self, catalog):
        """Re-registering column updates the data type."""
        catalog.register_table("orders", "silver", "s3://silver/orders")
        catalog.register_column("orders", "silver", "amount", "int")
        catalog.register_column("orders", "silver", "amount", "float")
        results = catalog.search("amount")
        assert len(results) == 1


class TestColumnStats:
    """Tests for column statistics."""

    def test_update_stats(self, catalog):
        """Stats are recorded successfully."""
        catalog.register_table("orders", "silver", "s3://silver/orders")
        catalog.register_column("orders", "silver", "amount", "float")
        catalog.update_column_stats(
            "orders",
            "silver",
            "amount",
            row_count=1000,
            null_count=5,
            distinct_count=500,
            min_value="0.0",
            max_value="999.99",
        )

    def test_stats_for_missing_column_raises(self, catalog):
        """Stats for nonexistent column raises ValueError."""
        catalog.register_table("orders", "silver", "s3://silver/orders")
        with pytest.raises(ValueError, match="not found"):
            catalog.update_column_stats("orders", "silver", "missing", 100, 0, 100)


class TestSchemaVersioning:
    """Tests for schema version tracking."""

    def test_add_schema_version(self, catalog):
        """Schema version is recorded."""
        catalog.register_table("orders", "silver", "s3://silver/orders")
        catalog.add_schema_version(
            "orders", "silver", "1.0", {"order_id": "int", "amount": "float"}
        )
        versions = catalog.get_schema_versions("orders", "silver")
        assert len(versions) == 1
        assert versions[0]["version"] == "1.0"

    def test_multiple_versions(self, catalog):
        """Multiple schema versions are tracked."""
        catalog.register_table("orders", "silver", "s3://silver/orders")
        catalog.add_schema_version("orders", "silver", "1.0", {"id": "int"})
        catalog.add_schema_version(
            "orders", "silver", "2.0", {"id": "int", "name": "string"}
        )
        versions = catalog.get_schema_versions("orders", "silver")
        assert len(versions) == 2

    def test_schema_for_missing_table_raises(self, catalog):
        """Adding schema to nonexistent table raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            catalog.add_schema_version("missing", "silver", "1.0", {})


class TestLineage:
    """Tests for data lineage."""

    def test_add_lineage(self, catalog):
        """Lineage relationship is recorded."""
        catalog.register_table("orders", "bronze", "s3://bronze/orders")
        catalog.register_table("orders", "silver", "s3://silver/orders")
        catalog.add_lineage("orders", "bronze", "orders", "silver", "clean + dedupe")

    def test_get_upstream(self, catalog):
        """Upstream sources are returned."""
        catalog.register_table("orders", "bronze", "s3://bronze/orders")
        catalog.register_table("orders", "silver", "s3://silver/orders")
        catalog.add_lineage("orders", "bronze", "orders", "silver")

        lineage = catalog.get_lineage("orders", "silver")
        assert len(lineage["upstream"]) == 1
        assert lineage["upstream"][0]["layer"] == "bronze"

    def test_get_downstream(self, catalog):
        """Downstream dependents are returned."""
        catalog.register_table("orders", "bronze", "s3://bronze/orders")
        catalog.register_table("orders", "silver", "s3://silver/orders")
        catalog.add_lineage("orders", "bronze", "orders", "silver")

        lineage = catalog.get_lineage("orders", "bronze")
        assert len(lineage["downstream"]) == 1
        assert lineage["downstream"][0]["layer"] == "silver"

    def test_lineage_missing_table(self, catalog):
        """Lineage for nonexistent table returns empty lists."""
        lineage = catalog.get_lineage("missing", "bronze")
        assert lineage == {"upstream": [], "downstream": []}

    def test_lineage_missing_source_raises(self, catalog):
        """Adding lineage with missing source/target raises ValueError."""
        catalog.register_table("orders", "bronze", "s3://bronze/orders")
        with pytest.raises(ValueError, match="not found"):
            catalog.add_lineage("orders", "bronze", "missing", "silver")


class TestSearch:
    """Tests for catalog search."""

    def test_search_by_table_name(self, catalog):
        """Search finds tables by name."""
        catalog.register_table("orders", "silver", "s3://silver/orders")
        results = catalog.search("order")
        assert any(r["match_type"] == "table" for r in results)

    def test_search_by_column_name(self, catalog):
        """Search finds columns by name."""
        catalog.register_table("orders", "silver", "s3://silver/orders")
        catalog.register_column(
            "orders", "silver", "revenue", "float", description="Total revenue"
        )
        results = catalog.search("revenue")
        assert any(r["match_type"] == "column" for r in results)

    def test_search_by_description(self, catalog):
        """Search matches against descriptions."""
        catalog.register_table(
            "metrics", "gold", "s3://gold/metrics", description="Business KPIs"
        )
        results = catalog.search("KPI")
        assert len(results) == 1

    def test_search_no_results(self, catalog):
        """Empty search returns empty list."""
        results = catalog.search("nonexistent")
        assert results == []


class TestSchemaManager:
    """Tests for schema evolution management."""

    def test_register_schema_auto_version(self, catalog, schema_mgr):
        """Auto-versioning increments correctly."""
        catalog.register_table("orders", "silver", "s3://silver/orders")
        v1 = schema_mgr.register_schema("orders", "silver", {"id": "int"})
        assert v1 == "1.0"
        v2 = schema_mgr.register_schema(
            "orders", "silver", {"id": "int", "name": "str"}
        )
        assert v2 == "2.0"

    def test_compare_schemas_added(self, catalog, schema_mgr):
        """Comparison detects added columns."""
        catalog.register_table("orders", "silver", "s3://silver/orders")
        schema_mgr.register_schema("orders", "silver", {"id": "int"}, version="1.0")
        schema_mgr.register_schema(
            "orders", "silver", {"id": "int", "name": "str"}, version="2.0"
        )
        diff = schema_mgr.compare_schemas("orders", "silver", "1.0", "2.0")
        assert "name" in diff["added"]
        assert diff["is_compatible"] is True

    def test_compare_schemas_removed(self, catalog, schema_mgr):
        """Comparison detects removed columns as incompatible."""
        catalog.register_table("t", "silver", "s3://silver/t")
        schema_mgr.register_schema(
            "t", "silver", {"id": "int", "name": "str"}, version="1.0"
        )
        schema_mgr.register_schema("t", "silver", {"id": "int"}, version="2.0")
        diff = schema_mgr.compare_schemas("t", "silver", "1.0", "2.0")
        assert "name" in diff["removed"]
        assert diff["is_compatible"] is False

    def test_compare_schemas_type_changed(self, catalog, schema_mgr):
        """Comparison detects type changes as incompatible."""
        catalog.register_table("t", "silver", "s3://silver/t")
        schema_mgr.register_schema("t", "silver", {"id": "int"}, version="1.0")
        schema_mgr.register_schema("t", "silver", {"id": "str"}, version="2.0")
        diff = schema_mgr.compare_schemas("t", "silver", "1.0", "2.0")
        assert "id" in diff["type_changed"]
        assert diff["is_compatible"] is False

    def test_get_latest_schema(self, catalog, schema_mgr):
        """Latest schema returns the most recent version."""
        catalog.register_table("t", "silver", "s3://silver/t")
        schema_mgr.register_schema("t", "silver", {"id": "int"}, version="1.0")
        schema_mgr.register_schema(
            "t", "silver", {"id": "int", "name": "str"}, version="2.0"
        )
        latest = schema_mgr.get_latest_schema("t", "silver")
        assert "name" in latest

    def test_get_latest_schema_empty(self, catalog, schema_mgr):
        """Latest schema returns None when no versions exist."""
        catalog.register_table("t", "silver", "s3://silver/t")
        assert schema_mgr.get_latest_schema("t", "silver") is None

    def test_compare_missing_version_raises(self, catalog, schema_mgr):
        """Comparing nonexistent version raises ValueError."""
        catalog.register_table("t", "silver", "s3://silver/t")
        schema_mgr.register_schema("t", "silver", {"id": "int"}, version="1.0")
        with pytest.raises(ValueError, match="not found"):
            schema_mgr.compare_schemas("t", "silver", "1.0", "9.0")
