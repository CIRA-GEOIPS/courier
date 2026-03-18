"""Dummy CLI for testing lazylemon functionality.

Eventually this will be replaced with a real CLI in GeoIPS Core.
"""

import json
from pathlib import Path

import typer
import yaml

from lazylemon.pydantic.service_config import ServiceConfigModel

app = typer.Typer()


class UnsupportedFileTypeError(ValueError):
    """Error for unsupported file types."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        super().__init__(f"Unsupported file type: {file_path.suffix}")


def load_config(file_path: Path) -> ServiceConfigModel:
    """Load config file (.json and .yml/.yaml)."""
    if file_path.suffix == ".json":
        with Path.open(file_path) as f:
            return ServiceConfigModel(**json.load(f))
    elif file_path.suffix in [".yml", ".yaml"]:
        with Path.open(file_path) as f:
            return ServiceConfigModel(**yaml.safe_load(f))
    else:
        raise UnsupportedFileTypeError(file_path)


def run_with_config(config: ServiceConfigModel) -> None:  # noqa: ARG001
    """Do.... nothing, but is monkey patchable."""
    return


@app.command()
def run(config_file: Path) -> None:
    """Run with a config file."""
    if not config_file.exists():
        typer.echo(f"Error: File {config_file} not found")
        raise typer.Exit(1)

    config = load_config(config_file)
    run_with_config(config)


@app.command()
def validate(config_file: Path) -> None:
    """Validate a config file."""
    if not config_file.exists():
        typer.echo(f"Error: File {config_file} not found")
        raise typer.Exit(1)

    try:
        load_config(config_file)  # returns a ServiceConfigModel or raises if invalid
        typer.echo("✅ Config valid")
    except Exception as e:
        typer.echo(f"❌ Invalid config: {e}")
        raise typer.Exit(1) from e


if __name__ == "__main__":
    app()
