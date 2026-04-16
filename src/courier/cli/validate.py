"""CLI `validate` command — validates a service config file."""

from pathlib import Path

import typer

from courier.cli.config_loader import load_config
from courier.errors import GeoIPSDriverError


def validate(config_file: Path) -> None:
    """Validate a service config file without running the service."""
    if not config_file.exists():
        typer.echo(f"Error: File {config_file} not found")
        raise typer.Exit(1)

    try:
        load_config(config_file)
        typer.echo("Config valid")
    except (GeoIPSDriverError, ValueError, OSError) as e:
        typer.echo(f"Invalid config: {e}")
        raise typer.Exit(1) from e
