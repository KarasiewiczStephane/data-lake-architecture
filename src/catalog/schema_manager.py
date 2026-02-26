"""Schema evolution management for the data catalog.

Tracks schema changes across versions and provides comparison
utilities for detecting breaking changes.
"""

import logging
from typing import Any

from src.catalog.metadata_store import MetadataStore

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages schema evolution for data lake tables.

    Provides methods to register schemas, detect changes between
    versions, and validate compatibility.

    Args:
        catalog: MetadataStore instance for persistence.
    """

    def __init__(self, catalog: MetadataStore) -> None:
        self.catalog = catalog

    def register_schema(
        self,
        table_name: str,
        layer: str,
        schema: dict[str, str],
        version: str | None = None,
    ) -> str:
        """Register a new schema version for a table.

        Automatically increments version if not specified.

        Args:
            table_name: Table name.
            layer: Table layer.
            schema: Column name to type mapping.
            version: Explicit version string. Auto-incremented if None.

        Returns:
            The version string that was registered.
        """
        if version is None:
            existing = self.catalog.get_schema_versions(table_name, layer)
            version = f"{len(existing) + 1}.0"

        self.catalog.add_schema_version(table_name, layer, version, schema)

        for col_name, col_type in schema.items():
            self.catalog.register_column(table_name, layer, col_name, col_type)

        logger.info(
            "Registered schema v%s for %s.%s with %d columns",
            version,
            layer,
            table_name,
            len(schema),
        )
        return version

    def compare_schemas(
        self,
        table_name: str,
        layer: str,
        version_a: str,
        version_b: str,
    ) -> dict[str, Any]:
        """Compare two schema versions and report differences.

        Args:
            table_name: Table name.
            layer: Table layer.
            version_a: First version to compare.
            version_b: Second version to compare.

        Returns:
            Dict with added, removed, and type_changed fields.

        Raises:
            ValueError: If either version is not found.
        """
        versions = self.catalog.get_schema_versions(table_name, layer)
        schema_map = {v["version"]: v["schema"] for v in versions}

        if version_a not in schema_map:
            raise ValueError(f"Version {version_a} not found")
        if version_b not in schema_map:
            raise ValueError(f"Version {version_b} not found")

        schema_a = schema_map[version_a]
        schema_b = schema_map[version_b]

        cols_a = set(schema_a.keys())
        cols_b = set(schema_b.keys())

        added = cols_b - cols_a
        removed = cols_a - cols_b
        type_changed = {
            col: {"from": schema_a[col], "to": schema_b[col]}
            for col in cols_a & cols_b
            if schema_a[col] != schema_b[col]
        }

        return {
            "added": sorted(added),
            "removed": sorted(removed),
            "type_changed": type_changed,
            "is_compatible": len(removed) == 0 and len(type_changed) == 0,
        }

    def get_latest_schema(self, table_name: str, layer: str) -> dict[str, str] | None:
        """Get the latest schema version for a table.

        Args:
            table_name: Table name.
            layer: Table layer.

        Returns:
            Schema dict or None if no versions exist.
        """
        versions = self.catalog.get_schema_versions(table_name, layer)
        if not versions:
            return None
        return versions[-1]["schema"]
