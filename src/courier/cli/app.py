"""Root typer application with subcommand registration."""

import typer

from courier.cli.run import run
from courier.cli.validate import validate

app = typer.Typer()

app.command()(run)
app.command()(validate)

if __name__ == "__main__":
    app()
