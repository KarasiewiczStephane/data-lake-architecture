"""Configuration management for the data lake.

Loads settings from YAML config files with environment variable overrides
for sensitive values like credentials and endpoints.
"""

import logging
import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def load_config(config_path: str | None = None) -> dict[str, Any]:
    """Load configuration from a YAML file.

    Args:
        config_path: Path to config YAML file. Falls back to
            DATALAKE_CONFIG env var, then 'configs/config.yaml'.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If config file does not exist.
    """
    if config_path is None:
        config_path = os.getenv("DATALAKE_CONFIG", "configs/config.yaml")

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    logger.debug("Loading config from %s", config_path)
    with open(path) as f:
        return yaml.safe_load(f)


def get_minio_config() -> dict[str, Any]:
    """Get MinIO connection configuration with environment variable overrides.

    Environment variables take precedence over config file values:
        - MINIO_ENDPOINT
        - MINIO_ACCESS_KEY
        - MINIO_SECRET_KEY

    Returns:
        Dictionary with endpoint, access_key, secret_key, and secure fields.
    """
    config = load_config()
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", config["minio"]["endpoint"]),
        "access_key": os.getenv("MINIO_ACCESS_KEY", config["minio"]["access_key"]),
        "secret_key": os.getenv("MINIO_SECRET_KEY", config["minio"]["secret_key"]),
        "secure": config["minio"].get("secure", False),
    }
