"""Click CLI for data lake management.

Provides commands for data ingestion, processing, querying,
catalog search, and cost estimation.
"""

import json
import logging

import click

from src.utils.config import load_config
from src.utils.logger import setup_logging

logger = logging.getLogger(__name__)


@click.group(invoke_without_command=True)
@click.option(
    "--config", "-c", default="configs/config.yaml", help="Path to config file"
)
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx: click.Context, config: str, verbose: bool) -> None:
    """Data Lake CLI - Manage your data lake with medallion architecture."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config
    setup_logging("DEBUG" if verbose else "INFO")
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command()
@click.option(
    "--source",
    "-s",
    required=True,
    type=click.Path(exists=True),
    help="Source file path (CSV, JSON, JSONL)",
)
@click.option("--table", "-t", required=True, help="Target table name")
@click.option("--source-name", required=True, help="Data source identifier")
@click.option("--schema-version", default="1.0", help="Schema version tag")
@click.pass_context
def ingest(
    ctx: click.Context,
    source: str,
    table: str,
    source_name: str,
    schema_version: str,
) -> None:
    """Ingest data into the bronze layer.

    Example:
        datalake ingest -s data.csv -t orders --source-name pos
    """
    from src.processing.bronze_loader import BronzeLoader

    config = load_config(ctx.obj["config_path"])
    loader = BronzeLoader(config=config)
    result = loader.ingest_file(
        file_path=source,
        table_name=table,
        source=source_name,
        schema_version=schema_version,
    )
    click.echo(f"Ingested {result['record_count']} records")
    click.echo(f"S3 URI: {result['s3_uri']}")
    click.echo(f"File hash: {result['file_hash']}")


@cli.command()
@click.option("--table", "-t", required=True, help="Table name to process")
@click.option(
    "--layer",
    "-l",
    required=True,
    type=click.Choice(["silver", "gold"]),
    help="Target layer",
)
@click.option("--dedup-columns", help="Comma-separated columns for deduplication")
@click.pass_context
def process(
    ctx: click.Context, table: str, layer: str, dedup_columns: str | None
) -> None:
    """Process data from one layer to the next.

    Examples:
        datalake process -t orders -l silver --dedup-columns order_id
    """
    config = load_config(ctx.obj["config_path"])
    dedup = dedup_columns.split(",") if dedup_columns else None

    if layer == "silver":
        from src.processing.silver_processor import SilverProcessor

        processor = SilverProcessor(config=config)
        result = processor.process_table(table, dedup_columns=dedup)
        click.echo(
            f"Processed {result.get('records_in', 0)} -> "
            f"{result.get('records_out', 0)} records"
        )
        if "duplicates_removed" in result:
            click.echo(f"Duplicates removed: {result['duplicates_removed']}")
    else:
        click.echo("Gold layer processing requires aggregation config.")
        click.echo("Use Python API for complex aggregations.")
        return

    if "s3_uri" in result:
        click.echo(f"Output: {result['s3_uri']}")


@cli.command()
@click.argument("sql")
@click.option(
    "--format",
    "-f",
    "output_format",
    default="json",
    type=click.Choice(["json", "csv"]),
    help="Output format",
)
@click.option("--limit", "-n", default=100, help="Limit rows (0 for no limit)")
@click.pass_context
def query(ctx: click.Context, sql: str, output_format: str, limit: int) -> None:
    """Execute SQL query using DuckDB.

    Example:
        datalake query "SELECT * FROM silver_orders LIMIT 10"
    """
    from src.query.duckdb_engine import DuckDBEngine

    config = load_config(ctx.obj["config_path"])
    engine = DuckDBEngine(config=config)
    engine.register_all_tables()

    if limit > 0 and "limit" not in sql.lower():
        sql = f"{sql} LIMIT {limit}"

    try:
        results = engine.query(sql)
        if output_format == "json":
            click.echo(json.dumps(results, indent=2, default=str))
        else:
            if results:
                headers = list(results[0].keys())
                click.echo(",".join(headers))
                for row in results:
                    click.echo(",".join(str(v) for v in row.values()))
            else:
                click.echo("No results")
    finally:
        engine.close()


@cli.group()
def catalog() -> None:
    """Data catalog operations."""


@catalog.command()
@click.option("--term", "-t", required=True, help="Search term")
@click.pass_context
def search(ctx: click.Context, term: str) -> None:
    """Search the data catalog.

    Example:
        datalake catalog search -t revenue
    """
    from src.catalog.metadata_store import MetadataStore

    config = load_config(ctx.obj["config_path"])
    store = MetadataStore(config.get("catalog", {}).get("db_path", "catalog.db"))
    results = store.search(term)

    if not results:
        click.echo(f"No matches found for '{term}'")
        return

    for r in results:
        match_type = r.get("match_type", "unknown")
        if match_type == "table":
            click.echo(f"[TABLE] {r['layer']}.{r['name']}")
            if r.get("description"):
                click.echo(f"        {r['description']}")
        elif match_type == "column":
            click.echo(f"[COLUMN] {r['layer']}.{r['table_name']}.{r['name']}")
            if r.get("description"):
                click.echo(f"         {r['description']}")


@catalog.command()
@click.option("--table", "-t", required=True, help="Table name")
@click.option("--layer", "-l", required=True, help="Layer name")
@click.pass_context
def lineage(ctx: click.Context, table: str, layer: str) -> None:
    """Show data lineage for a table."""
    from src.catalog.metadata_store import MetadataStore

    config = load_config(ctx.obj["config_path"])
    store = MetadataStore(config.get("catalog", {}).get("db_path", "catalog.db"))
    result = store.get_lineage(table, layer)

    click.echo(f"Lineage for {layer}.{table}:")
    click.echo("\nUpstream (sources):")
    for src in result.get("upstream", []):
        click.echo(f"  <- {src['layer']}.{src['name']}")
    click.echo("\nDownstream (dependents):")
    for dst in result.get("downstream", []):
        click.echo(f"  -> {dst['layer']}.{dst['name']}")


@cli.command("cost-estimate")
@click.option(
    "--config",
    "-c",
    "cost_config",
    required=True,
    type=click.Path(exists=True),
    help="Cost parameters YAML file",
)
@click.option(
    "--format",
    "-f",
    "output_format",
    default="table",
    type=click.Choice(["table", "json"]),
    help="Output format",
)
def cost_estimate(cost_config: str, output_format: str) -> None:
    """Estimate AWS costs based on usage parameters.

    Example:
        datalake cost-estimate -c configs/cost_params.yaml
    """
    from src.cost.estimator import CostEstimator

    if output_format == "json":
        logging.getLogger("src.cost.estimator").setLevel(logging.WARNING)

    estimator = CostEstimator()
    result = estimator.estimate_from_config(cost_config)
    breakdown = result.to_dict()

    if output_format == "json":
        click.echo(json.dumps(breakdown, indent=2))
    else:
        click.echo("\nAWS Monthly Cost Estimate")
        click.echo("=" * 40)
        click.echo(f"S3 Storage:        ${breakdown['s3']['storage']:.2f}")
        click.echo(f"S3 Requests:       ${breakdown['s3']['requests']:.2f}")
        click.echo(f"S3 Transfer:       ${breakdown['s3']['transfer']:.2f}")
        click.echo(f"Glue ETL:          ${breakdown['glue']['etl']:.2f}")
        click.echo(f"Athena Queries:    ${breakdown['athena']['queries']:.2f}")
        click.echo(f"Lambda Compute:    ${breakdown['lambda']['compute']:.2f}")
        click.echo("=" * 40)
        click.echo(f"TOTAL:             ${breakdown['total_monthly']:.2f}/month")


@cli.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """Initialize the data lake (create buckets)."""
    from src.storage import get_storage_client

    config = load_config(ctx.obj["config_path"])
    client = get_storage_client()

    for layer_name, bucket in config["buckets"].items():
        created = client.create_bucket(bucket)
        status = "created" if created else "exists"
        click.echo(f"Bucket '{bucket}' ({layer_name}): {status}")


def main() -> None:
    """Run the CLI."""
    cli(obj={})


if __name__ == "__main__":
    main()
