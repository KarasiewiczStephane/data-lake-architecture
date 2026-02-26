"""Tests for logging setup."""

import logging

from src.utils.logger import get_logger, setup_logging


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_returns_logger(self):
        """setup_logging returns a logger instance."""
        logger = setup_logging("WARNING")
        assert isinstance(logger, logging.Logger)

    def test_sets_log_level(self):
        """Log level is set correctly."""
        setup_logging("DEBUG")
        root = logging.getLogger()
        assert root.level == logging.DEBUG

    def test_default_level_is_info(self, monkeypatch):
        """Default log level is INFO when no env var is set."""
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        setup_logging()
        root = logging.getLogger()
        assert root.level == logging.INFO


class TestGetLogger:
    """Tests for get_logger function."""

    def test_returns_named_logger(self):
        """get_logger returns a logger with the specified name."""
        logger = get_logger("test.module")
        assert logger.name == "test.module"

    def test_returns_logger_instance(self):
        """get_logger returns a logging.Logger instance."""
        logger = get_logger("test")
        assert isinstance(logger, logging.Logger)
