"""Tests for CLI entry point."""

from click.testing import CliRunner

from src.cli import cli


class TestCLI:
    """Tests for the CLI group."""

    def test_cli_help(self):
        """CLI --help runs successfully."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Data Lake CLI" in result.output

    def test_cli_no_command(self):
        """CLI with no command shows help."""
        runner = CliRunner()
        result = runner.invoke(cli, [])
        assert result.exit_code == 0
