"""AWS cost estimation for data lake workloads.

Calculates monthly cost estimates for S3 storage, Glue ETL,
Athena queries, and Lambda invocations based on usage patterns.
Supports what-if analysis for capacity planning.
"""

import logging
from dataclasses import dataclass
from typing import Any

import yaml

logger = logging.getLogger(__name__)

AWS_PRICING = {
    "s3": {
        "standard_storage_gb": 0.023,
        "infrequent_storage_gb": 0.0125,
        "glacier_storage_gb": 0.004,
        "put_request_per_1k": 0.005,
        "get_request_per_1k": 0.0004,
        "data_transfer_out_gb": 0.09,
    },
    "glue": {
        "dpu_hour": 0.44,
        "crawler_dpu_hour": 0.44,
    },
    "athena": {
        "data_scanned_tb": 5.00,
    },
    "lambda": {
        "requests_per_million": 0.20,
        "duration_gb_second": 0.0000166667,
    },
}


@dataclass
class DataVolume:
    """Storage volume across data lake layers.

    Attributes:
        bronze_gb: Bronze layer storage in GB.
        silver_gb: Silver layer storage in GB.
        gold_gb: Gold layer storage in GB.
    """

    bronze_gb: float = 0.0
    silver_gb: float = 0.0
    gold_gb: float = 0.0

    @property
    def total_gb(self) -> float:
        """Total storage across all layers."""
        return self.bronze_gb + self.silver_gb + self.gold_gb


@dataclass
class QueryPattern:
    """Athena query usage pattern.

    Attributes:
        queries_per_month: Number of queries per month.
        avg_data_scanned_gb: Average data scanned per query in GB.
    """

    queries_per_month: int = 0
    avg_data_scanned_gb: float = 0.0

    @property
    def total_scanned_tb(self) -> float:
        """Total data scanned per month in TB."""
        return (self.queries_per_month * self.avg_data_scanned_gb) / 1024


@dataclass
class ETLPattern:
    """Glue ETL processing pattern.

    Attributes:
        runs_per_month: Number of ETL runs per month.
        avg_duration_minutes: Average duration per run in minutes.
        dpu_count: Number of DPUs allocated.
    """

    runs_per_month: int = 0
    avg_duration_minutes: float = 0.0
    dpu_count: int = 2

    @property
    def total_dpu_hours(self) -> float:
        """Total DPU-hours consumed per month."""
        return (self.runs_per_month * self.avg_duration_minutes / 60) * self.dpu_count


@dataclass
class LambdaPattern:
    """Lambda invocation pattern.

    Attributes:
        invocations_per_month: Number of invocations per month.
        avg_duration_ms: Average duration per invocation in ms.
        memory_mb: Allocated memory in MB.
    """

    invocations_per_month: int = 0
    avg_duration_ms: float = 100.0
    memory_mb: int = 128

    @property
    def total_gb_seconds(self) -> float:
        """Total GB-seconds consumed per month."""
        return (self.invocations_per_month * self.avg_duration_ms / 1000) * (
            self.memory_mb / 1024
        )


@dataclass
class CostBreakdown:
    """Detailed monthly cost breakdown by AWS service.

    Attributes:
        s3_storage: S3 storage cost.
        s3_requests: S3 API request cost.
        s3_transfer: S3 data transfer cost.
        glue_etl: Glue ETL processing cost.
        athena_queries: Athena query cost.
        lambda_compute: Lambda compute cost.
    """

    s3_storage: float = 0.0
    s3_requests: float = 0.0
    s3_transfer: float = 0.0
    glue_etl: float = 0.0
    athena_queries: float = 0.0
    lambda_compute: float = 0.0

    @property
    def total(self) -> float:
        """Total monthly cost across all services."""
        return (
            self.s3_storage
            + self.s3_requests
            + self.s3_transfer
            + self.glue_etl
            + self.athena_queries
            + self.lambda_compute
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to a nested dictionary format.

        Returns:
            Dict with per-service breakdowns and total.
        """
        return {
            "s3": {
                "storage": round(self.s3_storage, 2),
                "requests": round(self.s3_requests, 2),
                "transfer": round(self.s3_transfer, 2),
                "subtotal": round(
                    self.s3_storage + self.s3_requests + self.s3_transfer, 2
                ),
            },
            "glue": {"etl": round(self.glue_etl, 2)},
            "athena": {"queries": round(self.athena_queries, 2)},
            "lambda": {"compute": round(self.lambda_compute, 2)},
            "total_monthly": round(self.total, 2),
        }


class CostEstimator:
    """AWS cost estimator for data lake workloads.

    Calculates monthly cost estimates based on data volumes,
    query patterns, ETL processing, and Lambda invocations.

    Args:
        pricing: Custom pricing dictionary. Uses US-East-1 defaults if None.
    """

    def __init__(self, pricing: dict | None = None) -> None:
        self.pricing = pricing or AWS_PRICING

    def estimate(
        self,
        data_volume: DataVolume,
        query_pattern: QueryPattern | None = None,
        etl_pattern: ETLPattern | None = None,
        lambda_pattern: LambdaPattern | None = None,
        put_requests_per_month: int = 0,
        get_requests_per_month: int = 0,
        data_transfer_out_gb: float = 0.0,
    ) -> CostBreakdown:
        """Calculate monthly cost estimate.

        Args:
            data_volume: Storage volumes by layer.
            query_pattern: Athena query usage.
            etl_pattern: Glue ETL usage.
            lambda_pattern: Lambda invocation usage.
            put_requests_per_month: S3 PUT request count.
            get_requests_per_month: S3 GET request count.
            data_transfer_out_gb: Outbound data transfer in GB.

        Returns:
            Detailed cost breakdown.
        """
        breakdown = CostBreakdown()

        breakdown.s3_storage = (
            data_volume.total_gb * self.pricing["s3"]["standard_storage_gb"]
        )

        breakdown.s3_requests = (put_requests_per_month / 1000) * self.pricing["s3"][
            "put_request_per_1k"
        ] + (get_requests_per_month / 1000) * self.pricing["s3"]["get_request_per_1k"]

        breakdown.s3_transfer = (
            data_transfer_out_gb * self.pricing["s3"]["data_transfer_out_gb"]
        )

        if etl_pattern:
            breakdown.glue_etl = (
                etl_pattern.total_dpu_hours * self.pricing["glue"]["dpu_hour"]
            )

        if query_pattern:
            breakdown.athena_queries = (
                query_pattern.total_scanned_tb
                * self.pricing["athena"]["data_scanned_tb"]
            )

        if lambda_pattern:
            breakdown.lambda_compute = (
                lambda_pattern.invocations_per_month / 1_000_000
            ) * self.pricing["lambda"]["requests_per_million"] + (
                lambda_pattern.total_gb_seconds
                * self.pricing["lambda"]["duration_gb_second"]
            )

        logger.info("Estimated monthly cost: $%.2f", breakdown.total)
        return breakdown

    def estimate_from_config(self, config_path: str) -> CostBreakdown:
        """Calculate costs from a YAML configuration file.

        Args:
            config_path: Path to YAML file with cost parameters.

        Returns:
            Cost breakdown based on the configuration.
        """
        with open(config_path) as f:
            config = yaml.safe_load(f)

        data_volume = DataVolume(
            bronze_gb=config.get("data_volume", {}).get("bronze_gb", 0),
            silver_gb=config.get("data_volume", {}).get("silver_gb", 0),
            gold_gb=config.get("data_volume", {}).get("gold_gb", 0),
        )

        query_pattern = None
        if "query_pattern" in config:
            query_pattern = QueryPattern(
                queries_per_month=config["query_pattern"].get("queries_per_month", 0),
                avg_data_scanned_gb=config["query_pattern"].get(
                    "avg_data_scanned_gb", 0
                ),
            )

        etl_pattern = None
        if "etl_pattern" in config:
            etl_pattern = ETLPattern(
                runs_per_month=config["etl_pattern"].get("runs_per_month", 0),
                avg_duration_minutes=config["etl_pattern"].get(
                    "avg_duration_minutes", 0
                ),
                dpu_count=config["etl_pattern"].get("dpu_count", 2),
            )

        lambda_pattern = None
        if "lambda_pattern" in config:
            lambda_pattern = LambdaPattern(
                invocations_per_month=config["lambda_pattern"].get(
                    "invocations_per_month", 0
                ),
                avg_duration_ms=config["lambda_pattern"].get("avg_duration_ms", 100),
                memory_mb=config["lambda_pattern"].get("memory_mb", 128),
            )

        return self.estimate(
            data_volume=data_volume,
            query_pattern=query_pattern,
            etl_pattern=etl_pattern,
            lambda_pattern=lambda_pattern,
            put_requests_per_month=config.get("s3_requests", {}).get("put", 0),
            get_requests_per_month=config.get("s3_requests", {}).get("get", 0),
            data_transfer_out_gb=config.get("data_transfer_out_gb", 0),
        )

    def what_if_add_source(
        self,
        base_estimate: CostBreakdown,
        additional_data_gb: float,
        additional_queries: int = 0,
        additional_etl_runs: int = 0,
    ) -> dict[str, Any]:
        """Analyze cost impact of adding a new data source.

        Args:
            base_estimate: Current cost baseline.
            additional_data_gb: Additional storage in GB.
            additional_queries: Additional monthly queries.
            additional_etl_runs: Additional monthly ETL runs.

        Returns:
            Dict with current, additional, and new total costs.
        """
        new_storage = additional_data_gb * self.pricing["s3"]["standard_storage_gb"]
        new_queries = (additional_queries * 0.1 / 1024) * self.pricing["athena"][
            "data_scanned_tb"
        ]
        new_etl = additional_etl_runs * 0.5 * self.pricing["glue"]["dpu_hour"]

        additional_total = new_storage + new_queries + new_etl

        return {
            "current_monthly_cost": round(base_estimate.total, 2),
            "additional_cost": round(additional_total, 2),
            "new_total": round(base_estimate.total + additional_total, 2),
            "breakdown": {
                "storage": round(new_storage, 2),
                "queries": round(new_queries, 2),
                "etl": round(new_etl, 2),
            },
            "percent_increase": (
                round((additional_total / base_estimate.total) * 100, 1)
                if base_estimate.total > 0
                else 0
            ),
        }
