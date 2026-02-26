"""Structured logging setup for the data lake application.

Provides consistent log formatting across all modules with
configurable log levels via environment variable.
"""

import logging
import sys


def setup_logging(level: str | None = None) -> logging.Logger:
    """Configure structured logging for the application.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
            Defaults to LOG_LEVEL env var or INFO.

    Returns:
        Root logger instance configured with the specified level.
    """
    import os

    log_level = level or os.getenv("LOG_LEVEL", "INFO")

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Get a named logger instance.

    Args:
        name: Logger name, typically __name__ from the calling module.

    Returns:
        Logger instance with the given name.
    """
    return logging.getLogger(name)
