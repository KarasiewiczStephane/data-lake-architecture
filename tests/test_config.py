"""Tests for configuration management."""

import pytest
import yaml

from src.utils.config import get_minio_config, load_config


@pytest.fixture
def sample_config(tmp_path):
    """Create a temporary config file for testing."""
    config = {
        "minio": {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "secure": False,
        },
        "buckets": {
            "bronze": "datalake-bronze",
            "silver": "datalake-silver",
            "gold": "datalake-gold",
        },
        "partitioning": {"date_format": "%Y/%m/%d"},
        "catalog": {"db_path": "./catalog.db"},
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config, f)
    return str(config_file)


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_config_returns_dict(self, sample_config):
        """Config loading returns a dictionary."""
        result = load_config(sample_config)
        assert isinstance(result, dict)

    def test_load_config_has_minio_section(self, sample_config):
        """Config contains minio section."""
        result = load_config(sample_config)
        assert "minio" in result
        assert result["minio"]["endpoint"] == "localhost:9000"

    def test_load_config_has_buckets_section(self, sample_config):
        """Config contains buckets section."""
        result = load_config(sample_config)
        assert "buckets" in result
        assert result["buckets"]["bronze"] == "datalake-bronze"
        assert result["buckets"]["silver"] == "datalake-silver"
        assert result["buckets"]["gold"] == "datalake-gold"

    def test_load_config_missing_file_raises(self):
        """Loading a nonexistent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.yaml")

    def test_load_config_from_env(self, sample_config, monkeypatch):
        """Config path can be set via DATALAKE_CONFIG env var."""
        monkeypatch.setenv("DATALAKE_CONFIG", sample_config)
        result = load_config()
        assert "minio" in result


class TestGetMinioConfig:
    """Tests for get_minio_config function."""

    def test_returns_expected_keys(self, sample_config, monkeypatch):
        """MinIO config returns all expected keys."""
        monkeypatch.setenv("DATALAKE_CONFIG", sample_config)
        monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
        monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
        monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)
        result = get_minio_config()
        assert "endpoint" in result
        assert "access_key" in result
        assert "secret_key" in result
        assert "secure" in result

    def test_env_overrides_config(self, sample_config, monkeypatch):
        """Environment variables override config file values."""
        monkeypatch.setenv("DATALAKE_CONFIG", sample_config)
        monkeypatch.setenv("MINIO_ENDPOINT", "custom-host:9000")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "custom-key")
        monkeypatch.setenv("MINIO_SECRET_KEY", "custom-secret")
        result = get_minio_config()
        assert result["endpoint"] == "custom-host:9000"
        assert result["access_key"] == "custom-key"
        assert result["secret_key"] == "custom-secret"

    def test_defaults_from_config_file(self, sample_config, monkeypatch):
        """Without env vars, values come from config file."""
        monkeypatch.setenv("DATALAKE_CONFIG", sample_config)
        monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
        monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
        monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)
        result = get_minio_config()
        assert result["endpoint"] == "localhost:9000"
        assert result["access_key"] == "minioadmin"
