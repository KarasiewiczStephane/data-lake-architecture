"""Tests for AWS cost estimator."""

import pytest
import yaml

from src.cost.estimator import (
    CostBreakdown,
    CostEstimator,
    DataVolume,
    ETLPattern,
    LambdaPattern,
    QueryPattern,
)


@pytest.fixture
def estimator():
    """Default cost estimator."""
    return CostEstimator()


class TestDataVolume:
    """Tests for DataVolume dataclass."""

    def test_total_gb(self):
        """Total is sum of all layers."""
        vol = DataVolume(bronze_gb=100, silver_gb=50, gold_gb=10)
        assert vol.total_gb == 160

    def test_defaults_to_zero(self):
        """Default values are zero."""
        vol = DataVolume()
        assert vol.total_gb == 0.0


class TestQueryPattern:
    """Tests for QueryPattern dataclass."""

    def test_total_scanned_tb(self):
        """Total scanned converts to TB."""
        qp = QueryPattern(queries_per_month=1000, avg_data_scanned_gb=0.5)
        expected = (1000 * 0.5) / 1024
        assert abs(qp.total_scanned_tb - expected) < 0.001


class TestETLPattern:
    """Tests for ETLPattern dataclass."""

    def test_total_dpu_hours(self):
        """DPU hours are calculated correctly."""
        etl = ETLPattern(runs_per_month=30, avg_duration_minutes=60, dpu_count=2)
        assert etl.total_dpu_hours == 60.0  # 30 * 60/60 * 2


class TestLambdaPattern:
    """Tests for LambdaPattern dataclass."""

    def test_total_gb_seconds(self):
        """GB-seconds are calculated correctly."""
        lp = LambdaPattern(
            invocations_per_month=1000000, avg_duration_ms=1000, memory_mb=1024
        )
        assert lp.total_gb_seconds == 1000000.0  # 1M * 1s * 1GB


class TestCostBreakdown:
    """Tests for CostBreakdown dataclass."""

    def test_total(self):
        """Total sums all components."""
        cb = CostBreakdown(
            s3_storage=10.0,
            s3_requests=1.0,
            s3_transfer=5.0,
            glue_etl=20.0,
            athena_queries=15.0,
            lambda_compute=2.0,
        )
        assert cb.total == 53.0

    def test_to_dict(self):
        """to_dict produces expected structure."""
        cb = CostBreakdown(s3_storage=2.30)
        d = cb.to_dict()
        assert "s3" in d
        assert "total_monthly" in d
        assert d["s3"]["storage"] == 2.30


class TestEstimate:
    """Tests for cost estimation calculations."""

    def test_s3_storage_100gb(self, estimator):
        """100GB S3 storage costs $2.30/month."""
        vol = DataVolume(bronze_gb=100)
        result = estimator.estimate(vol)
        assert abs(result.s3_storage - 2.30) < 0.01

    def test_athena_1tb_scanned(self, estimator):
        """1TB scanned costs $5.00."""
        vol = DataVolume()
        qp = QueryPattern(queries_per_month=1024, avg_data_scanned_gb=1.0)
        result = estimator.estimate(vol, query_pattern=qp)
        assert abs(result.athena_queries - 5.00) < 0.01

    def test_glue_etl_cost(self, estimator):
        """Glue ETL cost matches expected value."""
        vol = DataVolume()
        etl = ETLPattern(runs_per_month=10, avg_duration_minutes=60, dpu_count=2)
        result = estimator.estimate(vol, etl_pattern=etl)
        expected = 10 * 1 * 2 * 0.44  # 10 runs * 1hr * 2 DPU * $0.44
        assert abs(result.glue_etl - expected) < 0.01

    def test_lambda_cost(self, estimator):
        """Lambda cost includes requests and compute."""
        vol = DataVolume()
        lp = LambdaPattern(
            invocations_per_month=1000000, avg_duration_ms=100, memory_mb=128
        )
        result = estimator.estimate(vol, lambda_pattern=lp)
        assert result.lambda_compute > 0

    def test_s3_requests_cost(self, estimator):
        """S3 request costs calculated correctly."""
        vol = DataVolume()
        result = estimator.estimate(
            vol, put_requests_per_month=10000, get_requests_per_month=100000
        )
        expected_puts = (10000 / 1000) * 0.005
        expected_gets = (100000 / 1000) * 0.0004
        assert abs(result.s3_requests - (expected_puts + expected_gets)) < 0.01

    def test_data_transfer_cost(self, estimator):
        """Data transfer cost matches rate."""
        vol = DataVolume()
        result = estimator.estimate(vol, data_transfer_out_gb=100)
        assert abs(result.s3_transfer - 9.0) < 0.01

    def test_no_patterns_zero_cost(self, estimator):
        """Empty patterns result in zero cost."""
        vol = DataVolume()
        result = estimator.estimate(vol)
        assert result.total == 0.0


class TestEstimateFromConfig:
    """Tests for config-based estimation."""

    def test_loads_and_calculates(self, estimator, tmp_path):
        """Config file is loaded and costs calculated."""
        config = {
            "data_volume": {"bronze_gb": 100, "silver_gb": 50, "gold_gb": 10},
            "query_pattern": {"queries_per_month": 100, "avg_data_scanned_gb": 0.1},
            "s3_requests": {"put": 1000, "get": 5000},
        }
        config_file = tmp_path / "cost.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        result = estimator.estimate_from_config(str(config_file))
        assert result.s3_storage > 0
        assert result.athena_queries > 0


class TestWhatIfAnalysis:
    """Tests for what-if analysis."""

    def test_additional_cost_calculated(self, estimator):
        """What-if returns cost delta."""
        base = CostBreakdown(s3_storage=2.30)
        result = estimator.what_if_add_source(base, additional_data_gb=50)
        assert result["additional_cost"] > 0
        assert result["new_total"] > base.total

    def test_percent_increase(self, estimator):
        """Percent increase is calculated correctly."""
        base = CostBreakdown(s3_storage=10.0)
        result = estimator.what_if_add_source(base, additional_data_gb=100)
        assert result["percent_increase"] > 0

    def test_zero_base_cost(self, estimator):
        """Zero base cost returns 0% increase."""
        base = CostBreakdown()
        result = estimator.what_if_add_source(base, additional_data_gb=10)
        assert result["percent_increase"] == 0
