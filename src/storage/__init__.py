"""Storage module for MinIO/S3 operations and data partitioning."""

from src.storage.minio_client import MinIOClient
from src.utils.config import get_minio_config


def get_storage_client() -> MinIOClient:
    """Create a storage client from the current configuration.

    Returns:
        Configured MinIOClient instance.
    """
    config = get_minio_config()
    return MinIOClient(
        endpoint=config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=config["secure"],
    )


__all__ = ["MinIOClient", "get_storage_client"]
