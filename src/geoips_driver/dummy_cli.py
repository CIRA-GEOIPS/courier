# config_cli.py
import json
from pathlib import Path

import typer
import yaml

from geoips_driver.pydantic.service_config import ServiceConfigModel

app = typer.Typer()


def load_config(file_path: Path) -> dict:
    """Load config file (.json and .yml/.yaml)."""
    if file_path.suffix == ".json":
        with Path.open(file_path) as f:
            return ServiceConfigModel(**json.load(f))
    elif file_path.suffix in [".yml", ".yaml"]:
        with Path.open(file_path) as f:
            return ServiceConfigModel(**yaml.safe_load(f))
    else:
        raise ValueError(f"Unsupported file type: {file_path.suffix}")


def run_with_config(config: dict) -> None:
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
        config = load_config(config_file)
        typer.echo("✅ Config valid")
    except Exception as e:
        typer.echo(f"❌ Invalid config: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
