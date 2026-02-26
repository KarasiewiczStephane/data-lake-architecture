"""Data quality validation framework for medallion architecture layers.

Provides configurable quality checks including null rate, uniqueness,
value range, row count, and schema conformance validation.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class QualityCheckResult:
    """Result of a single quality check."""

    check_name: str
    passed: bool
    actual_value: Any
    threshold: Any
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class QualityReport:
    """Aggregated quality report for a dataset."""

    table_name: str
    layer: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    results: list[QualityCheckResult]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def pass_rate(self) -> float:
        """Fraction of checks that passed."""
        return self.passed_checks / self.total_checks if self.total_checks > 0 else 0.0

    def to_dict(self) -> dict:
        """Serialize report to dictionary."""
        return {
            "table_name": self.table_name,
            "layer": self.layer,
            "total_checks": self.total_checks,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "pass_rate": round(self.pass_rate * 100, 2),
            "results": [
                {"check": r.check_name, "passed": r.passed, "message": r.message}
                for r in self.results
            ],
            "timestamp": self.timestamp.isoformat(),
        }


class QualityChecker:
    """Configurable data quality validation with fluent API."""

    def __init__(self) -> None:
        self.checks: list[tuple[str, Callable, dict]] = []

    def add_check(
        self,
        name: str,
        check_func: Callable[[pd.DataFrame], QualityCheckResult],
        **kwargs: Any,
    ) -> "QualityChecker":
        """Register a custom quality check function.

        Args:
            name: Descriptive check name.
            check_func: Callable taking a DataFrame and returning a QualityCheckResult.
            **kwargs: Additional keyword arguments stored with the check.

        Returns:
            Self for method chaining.
        """
        self.checks.append((name, check_func, kwargs))
        return self

    def check_null_rate(self, column: str, max_rate: float = 0.1) -> "QualityChecker":
        """Add a null rate check for a column.

        Args:
            column: Column name to check.
            max_rate: Maximum allowable null fraction (0.0 to 1.0).

        Returns:
            Self for method chaining.
        """

        def check(df: pd.DataFrame) -> QualityCheckResult:
            null_count = df[column].isna().sum()
            null_rate = null_count / len(df) if len(df) > 0 else 0
            passed = null_rate <= max_rate
            return QualityCheckResult(
                check_name=f"null_rate_{column}",
                passed=passed,
                actual_value=round(null_rate, 4),
                threshold=max_rate,
                message=f"{column} null rate: {null_rate:.2%} (max: {max_rate:.2%})",
            )

        self.checks.append((f"null_rate_{column}", check, {}))
        return self

    def check_uniqueness(
        self, columns: list[str], min_uniqueness: float = 1.0
    ) -> "QualityChecker":
        """Add a uniqueness check for one or more columns.

        Args:
            columns: Column names to check for uniqueness.
            min_uniqueness: Minimum uniqueness fraction (0.0 to 1.0).

        Returns:
            Self for method chaining.
        """
        col_key = ",".join(columns)

        def check(df: pd.DataFrame) -> QualityCheckResult:
            unique_count = df[columns].drop_duplicates().shape[0]
            uniqueness = unique_count / len(df) if len(df) > 0 else 0
            passed = uniqueness >= min_uniqueness
            return QualityCheckResult(
                check_name=f"uniqueness_{col_key}",
                passed=passed,
                actual_value=round(uniqueness, 4),
                threshold=min_uniqueness,
                message=(
                    f"Uniqueness on {columns}: {uniqueness:.2%}"
                    f" (min: {min_uniqueness:.2%})"
                ),
            )

        self.checks.append((f"uniqueness_{col_key}", check, {}))
        return self

    def check_value_range(
        self,
        column: str,
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> "QualityChecker":
        """Add a value range check for a numeric column.

        Args:
            column: Column name to check.
            min_val: Minimum allowable value (inclusive).
            max_val: Maximum allowable value (inclusive).

        Returns:
            Self for method chaining.
        """

        def check(df: pd.DataFrame) -> QualityCheckResult:
            col_min = df[column].min()
            col_max = df[column].max()
            passed = True
            issues: list[str] = []
            if min_val is not None and col_min < min_val:
                passed = False
                issues.append(f"min {col_min} < {min_val}")
            if max_val is not None and col_max > max_val:
                passed = False
                issues.append(f"max {col_max} > {max_val}")
            return QualityCheckResult(
                check_name=f"range_{column}",
                passed=passed,
                actual_value={"min": col_min, "max": col_max},
                threshold={"min": min_val, "max": max_val},
                message=f"{column} range: {', '.join(issues) if issues else 'OK'}",
            )

        self.checks.append((f"range_{column}", check, {}))
        return self

    def check_row_count(
        self, min_rows: int = 1, max_rows: int | None = None
    ) -> "QualityChecker":
        """Add a row count bounds check.

        Args:
            min_rows: Minimum number of rows.
            max_rows: Maximum number of rows (None for no upper bound).

        Returns:
            Self for method chaining.
        """

        def check(df: pd.DataFrame) -> QualityCheckResult:
            count = len(df)
            passed = count >= min_rows
            if max_rows is not None:
                passed = passed and count <= max_rows
            return QualityCheckResult(
                check_name="row_count",
                passed=passed,
                actual_value=count,
                threshold={"min": min_rows, "max": max_rows},
                message=f"Row count: {count} (min: {min_rows}, max: {max_rows})",
            )

        self.checks.append(("row_count", check, {}))
        return self

    def check_schema(self, expected_columns: list[str]) -> "QualityChecker":
        """Add a schema conformance check.

        Args:
            expected_columns: Column names that must be present.

        Returns:
            Self for method chaining.
        """

        def check(df: pd.DataFrame) -> QualityCheckResult:
            missing = set(expected_columns) - set(df.columns)
            passed = len(missing) == 0
            return QualityCheckResult(
                check_name="schema",
                passed=passed,
                actual_value=list(df.columns),
                threshold=expected_columns,
                message=f"Schema: {'OK' if passed else f'missing {missing}'}",
            )

        self.checks.append(("schema", check, {}))
        return self

    def run(self, df: pd.DataFrame, table_name: str, layer: str) -> QualityReport:
        """Execute all registered checks against a DataFrame.

        Args:
            df: DataFrame to validate.
            table_name: Name of the table being checked.
            layer: Medallion layer (bronze, silver, gold).

        Returns:
            QualityReport with check results.
        """
        results: list[QualityCheckResult] = []
        for name, check_func, _kwargs in self.checks:
            try:
                result = check_func(df)
                results.append(result)
            except Exception as exc:
                results.append(
                    QualityCheckResult(
                        check_name=name,
                        passed=False,
                        actual_value=None,
                        threshold=None,
                        message=f"Check failed with error: {exc}",
                    )
                )

        passed = sum(1 for r in results if r.passed)
        report = QualityReport(
            table_name=table_name,
            layer=layer,
            total_checks=len(results),
            passed_checks=passed,
            failed_checks=len(results) - passed,
            results=results,
        )
        logger.info(
            "Quality report for %s.%s: %d/%d checks passed",
            layer,
            table_name,
            passed,
            len(results),
        )
        return report


def bronze_quality_checker() -> QualityChecker:
    """Pre-configured quality checker for bronze layer data."""
    return QualityChecker().check_row_count(min_rows=1)


def silver_quality_checker(id_column: str) -> QualityChecker:
    """Pre-configured quality checker for silver layer data.

    Args:
        id_column: Primary key column to check for uniqueness.
    """
    return QualityChecker().check_row_count(min_rows=1).check_uniqueness([id_column])


def gold_quality_checker() -> QualityChecker:
    """Pre-configured quality checker for gold layer data."""
    return QualityChecker().check_row_count(min_rows=1)
