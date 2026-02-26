"""Main entry point for the data lake application."""

from src.utils.logger import setup_logging


def main() -> None:
    """Initialize and run the data lake CLI."""
    setup_logging()

    from src.cli import cli

    cli(obj={})


if __name__ == "__main__":
    main()
