"""Tests for the data lake architecture dashboard data generators."""

import pandas as pd

from src.dashboard.app import (
    generate_cost_breakdown,
    generate_ingestion_throughput,
    generate_layer_health,
    generate_quality_scores,
)


class TestLayerHealth:
    def test_returns_dataframe(self) -> None:
        df = generate_layer_health()
        assert isinstance(df, pd.DataFrame)

    def test_has_all_layers(self) -> None:
        df = generate_layer_health()
        assert set(df["layer"].unique()) == {"Bronze", "Silver", "Gold"}

    def test_has_all_tables(self) -> None:
        df = generate_layer_health()
        assert df["table"].nunique() == 4

    def test_record_counts_positive(self) -> None:
        df = generate_layer_health()
        assert (df["record_count"] > 0).all()

    def test_bronze_gte_silver(self) -> None:
        df = generate_layer_health()
        for table in df["table"].unique():
            table_df = df[df["table"] == table]
            bronze = table_df[table_df["layer"] == "Bronze"]["record_count"].iloc[0]
            silver = table_df[table_df["layer"] == "Silver"]["record_count"].iloc[0]
            assert bronze >= silver

    def test_reproducible(self) -> None:
        df1 = generate_layer_health(seed=99)
        df2 = generate_layer_health(seed=99)
        pd.testing.assert_frame_equal(df1, df2)


class TestQualityScores:
    def test_returns_dataframe(self) -> None:
        df = generate_quality_scores()
        assert isinstance(df, pd.DataFrame)

    def test_has_entries(self) -> None:
        df = generate_quality_scores()
        assert len(df) == 24  # 4 tables x 6 checks

    def test_scores_bounded(self) -> None:
        df = generate_quality_scores()
        assert (df["score"] >= 0).all()
        assert (df["score"] <= 1).all()

    def test_has_passed_column(self) -> None:
        df = generate_quality_scores()
        assert df["passed"].dtype == bool


class TestIngestionThroughput:
    def test_returns_dataframe(self) -> None:
        df = generate_ingestion_throughput()
        assert isinstance(df, pd.DataFrame)

    def test_has_28_days(self) -> None:
        df = generate_ingestion_throughput()
        assert len(df) == 28

    def test_records_positive(self) -> None:
        df = generate_ingestion_throughput()
        assert (df["records_ingested"] > 0).all()


class TestCostBreakdown:
    def test_returns_dataframe(self) -> None:
        df = generate_cost_breakdown()
        assert isinstance(df, pd.DataFrame)

    def test_has_services(self) -> None:
        df = generate_cost_breakdown()
        assert len(df) == 5

    def test_costs_positive(self) -> None:
        df = generate_cost_breakdown()
        assert (df["monthly_cost"] > 0).all()
