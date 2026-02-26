"""CLI entry point placeholder - full implementation in Task 10."""

import click


@click.group(invoke_without_command=True)
@click.option(
    "--config", "-c", default="configs/config.yaml", help="Path to config file"
)
@click.pass_context
def cli(ctx: click.Context, config: str) -> None:
    """Data Lake CLI - Manage your data lake with medallion architecture."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


def main() -> None:
    """Run the CLI."""
    cli(obj={})


if __name__ == "__main__":
    main()
