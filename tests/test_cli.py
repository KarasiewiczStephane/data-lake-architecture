"""Tests for Click CLI commands."""

import json

import pytest
import yaml
from click.testing import CliRunner

from src.cli import cli


@pytest.fixture
def runner():
    """Click test runner."""
    return CliRunner()


@pytest.fixture
def cost_config(tmp_path):
    """Temporary cost config file."""
    config = {
        "data_volume": {"bronze_gb": 100, "silver_gb": 50, "gold_gb": 10},
        "query_pattern": {"queries_per_month": 100, "avg_data_scanned_gb": 0.5},
    }
    path = tmp_path / "cost.yaml"
    with open(path, "w") as f:
        yaml.dump(config, f)
    return str(path)


class TestCLIGroup:
    """Tests for the CLI group."""

    def test_help(self, runner):
        """CLI --help runs successfully."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Data Lake CLI" in result.output

    def test_no_command_shows_help(self, runner):
        """CLI with no command shows help."""
        result = runner.invoke(cli, [])
        assert result.exit_code == 0
        assert "Data Lake CLI" in result.output

    def test_verbose_flag(self, runner):
        """Verbose flag is accepted."""
        result = runner.invoke(cli, ["-v", "--help"])
        assert result.exit_code == 0


class TestIngestCommand:
    """Tests for the ingest command."""

    def test_help(self, runner):
        """Ingest help works."""
        result = runner.invoke(cli, ["ingest", "--help"])
        assert result.exit_code == 0
        assert "Ingest data" in result.output

    def test_missing_source_fails(self, runner):
        """Missing required --source flag fails."""
        result = runner.invoke(cli, ["ingest", "-t", "test", "--source-name", "src"])
        assert result.exit_code != 0


class TestProcessCommand:
    """Tests for the process command."""

    def test_help(self, runner):
        """Process help works."""
        result = runner.invoke(cli, ["process", "--help"])
        assert result.exit_code == 0
        assert "Process data" in result.output


class TestQueryCommand:
    """Tests for the query command."""

    def test_help(self, runner):
        """Query help works."""
        result = runner.invoke(cli, ["query", "--help"])
        assert result.exit_code == 0
        assert "Execute SQL" in result.output


class TestCatalogCommand:
    """Tests for the catalog subcommand group."""

    def test_search_help(self, runner):
        """Catalog search help works."""
        result = runner.invoke(cli, ["catalog", "search", "--help"])
        assert result.exit_code == 0
        assert "Search" in result.output

    def test_lineage_help(self, runner):
        """Catalog lineage help works."""
        result = runner.invoke(cli, ["catalog", "lineage", "--help"])
        assert result.exit_code == 0
        assert "lineage" in result.output.lower()


class TestCostEstimateCommand:
    """Tests for the cost-estimate command."""

    def test_help(self, runner):
        """Cost-estimate help works."""
        result = runner.invoke(cli, ["cost-estimate", "--help"])
        assert result.exit_code == 0
        assert "Estimate AWS costs" in result.output

    def test_json_output(self, runner, cost_config):
        """Cost estimate outputs valid JSON."""
        result = runner.invoke(cli, ["cost-estimate", "-c", cost_config, "-f", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "total_monthly" in data

    def test_table_output(self, runner, cost_config):
        """Cost estimate outputs table format."""
        result = runner.invoke(cli, ["cost-estimate", "-c", cost_config, "-f", "table"])
        assert result.exit_code == 0
        assert "TOTAL:" in result.output


class TestInitCommand:
    """Tests for the init command."""

    def test_help(self, runner):
        """Init help works."""
        result = runner.invoke(cli, ["init", "--help"])
        assert result.exit_code == 0
        assert "Initialize" in result.output
