"""Streamlit dashboard for data lake architecture monitoring.

Displays medallion layer health, data quality scores, ingestion
throughput, and AWS cost breakdown using synthetic demo data.

Run with: streamlit run src/dashboard/app.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

LAYERS = ["Bronze", "Silver", "Gold"]
LAYER_COLORS = {"Bronze": "#CD7F32", "Silver": "#C0C0C0", "Gold": "#FFD700"}


def generate_layer_health(seed: int = 42) -> pd.DataFrame:
    """Generate synthetic layer health metrics."""
    rng = np.random.default_rng(seed)
    rows = []
    tables = ["customers", "transactions", "products", "events"]
    for table in tables:
        bronze_records = int(rng.integers(100000, 500000))
        silver_records = int(bronze_records * rng.uniform(0.85, 0.98))
        gold_records = int(silver_records * rng.uniform(0.90, 1.0))
        for layer, count in zip(LAYERS, [bronze_records, silver_records, gold_records]):
            rows.append(
                {
                    "table": table,
                    "layer": layer,
                    "record_count": count,
                    "size_mb": round(count * rng.uniform(0.0005, 0.002), 1),
                    "last_updated": pd.Timestamp("2024-11-28")
                    - pd.Timedelta(hours=int(rng.integers(1, 48))),
                }
            )
    return pd.DataFrame(rows)


def generate_quality_scores(seed: int = 42) -> pd.DataFrame:
    """Generate synthetic data quality check results."""
    rng = np.random.default_rng(seed)
    checks = [
        "null_rate",
        "uniqueness",
        "value_range",
        "schema_match",
        "freshness",
        "referential_integrity",
    ]
    tables = ["customers", "transactions", "products", "events"]
    rows = []
    for table in tables:
        for check in checks:
            passed = bool(rng.random() > 0.15)
            rows.append(
                {
                    "table": table,
                    "check": check,
                    "passed": passed,
                    "score": round(
                        rng.uniform(0.85, 1.0) if passed else rng.uniform(0.4, 0.84), 4
                    ),
                }
            )
    return pd.DataFrame(rows)


def generate_ingestion_throughput(seed: int = 42) -> pd.DataFrame:
    """Generate synthetic ingestion throughput over time."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-11-01", periods=28, freq="D")
    rows = []
    for date in dates:
        rows.append(
            {
                "date": date,
                "records_ingested": int(rng.integers(50000, 200000)),
                "size_gb": round(rng.uniform(0.5, 3.0), 2),
                "duration_min": round(rng.uniform(5, 45), 1),
            }
        )
    return pd.DataFrame(rows)


def generate_cost_breakdown(seed: int = 42) -> pd.DataFrame:
    """Generate synthetic AWS cost breakdown."""
    rng = np.random.default_rng(seed)
    services = ["S3 Storage", "Glue Jobs", "Athena Queries", "Lambda", "CloudWatch"]
    costs = [
        round(rng.uniform(50, 200), 2),
        round(rng.uniform(100, 400), 2),
        round(rng.uniform(30, 150), 2),
        round(rng.uniform(10, 60), 2),
        round(rng.uniform(5, 30), 2),
    ]
    return pd.DataFrame({"service": services, "monthly_cost": costs})


def render_header() -> None:
    """Render the dashboard header."""
    st.title("Data Lake Architecture Dashboard")
    st.caption(
        "Medallion architecture monitoring with layer health, "
        "data quality, ingestion throughput, and cost analysis"
    )


def render_summary_metrics(
    health_df: pd.DataFrame, quality_df: pd.DataFrame, cost_df: pd.DataFrame
) -> None:
    """Render top-level summary metric cards."""
    col1, col2, col3, col4 = st.columns(4)
    total_records = health_df[health_df["layer"] == "Gold"]["record_count"].sum()
    col1.metric("Gold Layer Records", f"{total_records:,}")
    pass_rate = quality_df["passed"].mean()
    col2.metric("Quality Pass Rate", f"{pass_rate:.0%}")
    col3.metric("Tables Tracked", health_df["table"].nunique())
    col4.metric("Monthly Cost", f"${cost_df['monthly_cost'].sum():,.0f}")


def render_layer_comparison(health_df: pd.DataFrame) -> None:
    """Render record count comparison across layers."""
    st.subheader("Record Counts by Layer")
    fig = px.bar(
        health_df,
        x="table",
        y="record_count",
        color="layer",
        barmode="group",
        color_discrete_map=LAYER_COLORS,
        text="record_count",
    )
    fig.update_traces(texttemplate="%{text:,.0f}", textposition="auto")
    fig.update_layout(
        height=400,
        margin={"l": 40, "r": 20, "t": 30, "b": 40},
    )
    st.plotly_chart(fig, use_container_width=True)


def render_quality_heatmap(quality_df: pd.DataFrame) -> None:
    """Render data quality check heatmap."""
    st.subheader("Data Quality Checks")
    pivot = quality_df.pivot(index="table", columns="check", values="score")
    fig = px.imshow(
        pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        color_continuous_scale="RdYlGn",
        zmin=0.4,
        zmax=1.0,
        text_auto=".2f",
    )
    fig.update_layout(
        height=300,
        margin={"l": 40, "r": 20, "t": 30, "b": 40},
    )
    st.plotly_chart(fig, use_container_width=True)


def render_ingestion_chart(throughput_df: pd.DataFrame) -> None:
    """Render ingestion throughput over time."""
    st.subheader("Daily Ingestion Throughput")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=throughput_df["date"],
            y=throughput_df["records_ingested"],
            mode="lines+markers",
            name="Records",
            line={"color": "#2196F3", "width": 2},
        )
    )
    fig.update_layout(
        yaxis_title="Records Ingested",
        height=350,
        margin={"l": 40, "r": 20, "t": 30, "b": 40},
    )
    st.plotly_chart(fig, use_container_width=True)


def render_cost_breakdown(cost_df: pd.DataFrame) -> None:
    """Render AWS cost breakdown pie chart."""
    st.subheader("Monthly Cost Breakdown")
    fig = px.pie(
        cost_df,
        values="monthly_cost",
        names="service",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_layout(
        height=350,
        margin={"l": 20, "r": 20, "t": 30, "b": 40},
    )
    st.plotly_chart(fig, use_container_width=True)


def main() -> None:
    """Main dashboard entry point."""
    render_header()

    health_df = generate_layer_health()
    quality_df = generate_quality_scores()
    throughput_df = generate_ingestion_throughput()
    cost_df = generate_cost_breakdown()

    render_summary_metrics(health_df, quality_df, cost_df)
    st.markdown("---")

    render_layer_comparison(health_df)
    render_quality_heatmap(quality_df)

    col_left, col_right = st.columns(2)
    with col_left:
        render_ingestion_chart(throughput_df)
    with col_right:
        render_cost_breakdown(cost_df)


if __name__ == "__main__":
    main()
