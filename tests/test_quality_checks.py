"""Tests for data quality checks and validation framework."""

import pandas as pd
import pytest

from src.processing.quality_checks import (
    QualityChecker,
    QualityCheckResult,
    QualityReport,
    bronze_quality_checker,
    gold_quality_checker,
    silver_quality_checker,
)


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Sample DataFrame for quality checks."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", None, "Diana", "Eve"],
            "amount": [10.0, 20.0, 30.0, 40.0, 50.0],
            "category": ["A", "B", "A", "B", "A"],
        }
    )


@pytest.fixture
def empty_df() -> pd.DataFrame:
    """Empty DataFrame."""
    return pd.DataFrame({"id": [], "name": [], "amount": []})


class TestQualityCheckResult:
    """Tests for QualityCheckResult dataclass."""

    def test_creation(self) -> None:
        """Result stores all fields."""
        result = QualityCheckResult(
            check_name="test",
            passed=True,
            actual_value=0.05,
            threshold=0.1,
            message="OK",
        )
        assert result.check_name == "test"
        assert result.passed is True
        assert result.timestamp is not None


class TestQualityReport:
    """Tests for QualityReport dataclass."""

    def test_pass_rate(self) -> None:
        """Pass rate is computed correctly."""
        report = QualityReport(
            table_name="test",
            layer="silver",
            total_checks=4,
            passed_checks=3,
            failed_checks=1,
            results=[],
        )
        assert report.pass_rate == 0.75

    def test_pass_rate_zero_checks(self) -> None:
        """Pass rate is 0 with no checks."""
        report = QualityReport(
            table_name="test",
            layer="bronze",
            total_checks=0,
            passed_checks=0,
            failed_checks=0,
            results=[],
        )
        assert report.pass_rate == 0.0

    def test_to_dict(self) -> None:
        """to_dict includes all expected keys."""
        result = QualityCheckResult(
            check_name="test_check",
            passed=True,
            actual_value=1,
            threshold=1,
            message="OK",
        )
        report = QualityReport(
            table_name="orders",
            layer="silver",
            total_checks=1,
            passed_checks=1,
            failed_checks=0,
            results=[result],
        )
        d = report.to_dict()
        assert d["table_name"] == "orders"
        assert d["layer"] == "silver"
        assert d["pass_rate"] == 100.0
        assert len(d["results"]) == 1
        assert d["results"][0]["check"] == "test_check"
        assert "timestamp" in d


class TestCheckNullRate:
    """Tests for null rate quality check."""

    def test_passes_below_threshold(self, sample_df: pd.DataFrame) -> None:
        """Null rate below threshold passes."""
        checker = QualityChecker().check_null_rate("id", max_rate=0.1)
        report = checker.run(sample_df, "test", "bronze")
        assert report.passed_checks == 1

    def test_fails_above_threshold(self, sample_df: pd.DataFrame) -> None:
        """Null rate above threshold fails."""
        checker = QualityChecker().check_null_rate("name", max_rate=0.1)
        report = checker.run(sample_df, "test", "bronze")
        assert report.failed_checks == 1

    def test_passes_at_threshold(self, sample_df: pd.DataFrame) -> None:
        """Null rate exactly at threshold passes (<=)."""
        checker = QualityChecker().check_null_rate("name", max_rate=0.2)
        report = checker.run(sample_df, "test", "bronze")
        assert report.passed_checks == 1

    def test_empty_dataframe(self, empty_df: pd.DataFrame) -> None:
        """Empty DataFrame has 0 null rate."""
        checker = QualityChecker().check_null_rate("id", max_rate=0.0)
        report = checker.run(empty_df, "test", "bronze")
        assert report.passed_checks == 1


class TestCheckUniqueness:
    """Tests for uniqueness quality check."""

    def test_fully_unique(self, sample_df: pd.DataFrame) -> None:
        """Fully unique column passes at 1.0 threshold."""
        checker = QualityChecker().check_uniqueness(["id"])
        report = checker.run(sample_df, "test", "silver")
        assert report.passed_checks == 1

    def test_duplicates_fail(self, sample_df: pd.DataFrame) -> None:
        """Non-unique column fails at 1.0 threshold."""
        checker = QualityChecker().check_uniqueness(["category"])
        report = checker.run(sample_df, "test", "silver")
        assert report.failed_checks == 1

    def test_partial_uniqueness(self, sample_df: pd.DataFrame) -> None:
        """Partial uniqueness passes at lower threshold."""
        checker = QualityChecker().check_uniqueness(["category"], min_uniqueness=0.3)
        report = checker.run(sample_df, "test", "silver")
        assert report.passed_checks == 1

    def test_multi_column_uniqueness(self, sample_df: pd.DataFrame) -> None:
        """Multi-column uniqueness check works."""
        checker = QualityChecker().check_uniqueness(["id", "category"])
        report = checker.run(sample_df, "test", "silver")
        assert report.passed_checks == 1


class TestCheckValueRange:
    """Tests for value range quality check."""

    def test_within_range(self, sample_df: pd.DataFrame) -> None:
        """Values within range pass."""
        checker = QualityChecker().check_value_range("amount", min_val=0, max_val=100)
        report = checker.run(sample_df, "test", "silver")
        assert report.passed_checks == 1

    def test_below_min(self, sample_df: pd.DataFrame) -> None:
        """Values below minimum fail."""
        checker = QualityChecker().check_value_range("amount", min_val=15)
        report = checker.run(sample_df, "test", "silver")
        assert report.failed_checks == 1

    def test_above_max(self, sample_df: pd.DataFrame) -> None:
        """Values above maximum fail."""
        checker = QualityChecker().check_value_range("amount", max_val=40)
        report = checker.run(sample_df, "test", "silver")
        assert report.failed_checks == 1

    def test_no_bounds(self, sample_df: pd.DataFrame) -> None:
        """No bounds set always passes."""
        checker = QualityChecker().check_value_range("amount")
        report = checker.run(sample_df, "test", "silver")
        assert report.passed_checks == 1


class TestCheckRowCount:
    """Tests for row count quality check."""

    def test_above_min(self, sample_df: pd.DataFrame) -> None:
        """Row count above minimum passes."""
        checker = QualityChecker().check_row_count(min_rows=1)
        report = checker.run(sample_df, "test", "bronze")
        assert report.passed_checks == 1

    def test_below_min(self, empty_df: pd.DataFrame) -> None:
        """Row count below minimum fails."""
        checker = QualityChecker().check_row_count(min_rows=1)
        report = checker.run(empty_df, "test", "bronze")
        assert report.failed_checks == 1

    def test_within_max(self, sample_df: pd.DataFrame) -> None:
        """Row count within max passes."""
        checker = QualityChecker().check_row_count(min_rows=1, max_rows=10)
        report = checker.run(sample_df, "test", "bronze")
        assert report.passed_checks == 1

    def test_exceeds_max(self, sample_df: pd.DataFrame) -> None:
        """Row count exceeding max fails."""
        checker = QualityChecker().check_row_count(min_rows=1, max_rows=3)
        report = checker.run(sample_df, "test", "bronze")
        assert report.failed_checks == 1


class TestCheckSchema:
    """Tests for schema conformance check."""

    def test_all_columns_present(self, sample_df: pd.DataFrame) -> None:
        """All expected columns present passes."""
        checker = QualityChecker().check_schema(["id", "name", "amount"])
        report = checker.run(sample_df, "test", "silver")
        assert report.passed_checks == 1

    def test_missing_columns(self, sample_df: pd.DataFrame) -> None:
        """Missing columns fail."""
        checker = QualityChecker().check_schema(["id", "email", "phone"])
        report = checker.run(sample_df, "test", "silver")
        assert report.failed_checks == 1
        assert "missing" in report.results[0].message


class TestCustomCheck:
    """Tests for custom check registration."""

    def test_add_custom_check(self, sample_df: pd.DataFrame) -> None:
        """Custom check function works."""

        def no_negative_amounts(df: pd.DataFrame) -> QualityCheckResult:
            has_negative = (df["amount"] < 0).any()
            return QualityCheckResult(
                check_name="no_negatives",
                passed=not has_negative,
                actual_value=not has_negative,
                threshold=True,
                message="No negative amounts" if not has_negative else "Has negatives",
            )

        checker = QualityChecker().add_check("no_negatives", no_negative_amounts)
        report = checker.run(sample_df, "test", "gold")
        assert report.passed_checks == 1


class TestMultipleChecks:
    """Tests for running multiple checks together."""

    def test_mixed_results(self, sample_df: pd.DataFrame) -> None:
        """Multiple checks with mixed pass/fail results."""
        checker = (
            QualityChecker()
            .check_row_count(min_rows=1)
            .check_null_rate("name", max_rate=0.1)
            .check_uniqueness(["id"])
            .check_value_range("amount", min_val=0, max_val=100)
        )
        report = checker.run(sample_df, "test", "silver")
        assert report.total_checks == 4
        assert report.passed_checks == 3
        assert report.failed_checks == 1

    def test_all_pass(self, sample_df: pd.DataFrame) -> None:
        """All checks passing returns full pass."""
        checker = (
            QualityChecker()
            .check_row_count(min_rows=1)
            .check_uniqueness(["id"])
            .check_value_range("amount", min_val=0, max_val=100)
        )
        report = checker.run(sample_df, "test", "silver")
        assert report.pass_rate == 1.0


class TestErrorHandling:
    """Tests for error handling in quality checks."""

    def test_missing_column_handled(self, sample_df: pd.DataFrame) -> None:
        """Check on missing column is caught as error."""
        checker = QualityChecker().check_null_rate("nonexistent_column")
        report = checker.run(sample_df, "test", "bronze")
        assert report.failed_checks == 1
        assert "error" in report.results[0].message.lower()

    def test_incompatible_dtype(self) -> None:
        """Value range on string column is caught as error."""
        df = pd.DataFrame({"name": ["Alice", "Bob"]})
        checker = QualityChecker().check_value_range("name", min_val=0)
        report = checker.run(df, "test", "bronze")
        assert report.failed_checks == 1


class TestPrebuiltCheckers:
    """Tests for pre-built layer checkers."""

    def test_bronze_checker(self, sample_df: pd.DataFrame) -> None:
        """Bronze checker validates row count."""
        checker = bronze_quality_checker()
        report = checker.run(sample_df, "orders", "bronze")
        assert report.passed_checks == 1

    def test_silver_checker(self, sample_df: pd.DataFrame) -> None:
        """Silver checker validates row count and uniqueness."""
        checker = silver_quality_checker("id")
        report = checker.run(sample_df, "orders", "silver")
        assert report.total_checks == 2
        assert report.passed_checks == 2

    def test_gold_checker(self, sample_df: pd.DataFrame) -> None:
        """Gold checker validates row count."""
        checker = gold_quality_checker()
        report = checker.run(sample_df, "agg_revenue", "gold")
        assert report.passed_checks == 1


class TestFluentAPI:
    """Tests for fluent/chaining API."""

    def test_chaining_returns_self(self) -> None:
        """All check methods return the checker for chaining."""
        checker = (
            QualityChecker()
            .check_null_rate("col", max_rate=0.5)
            .check_uniqueness(["col"])
            .check_value_range("col", min_val=0)
            .check_row_count(min_rows=1)
            .check_schema(["col"])
        )
        assert len(checker.checks) == 5
