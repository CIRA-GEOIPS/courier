"""Root typer application with subcommand registration."""

import typer

from lazylemon.cli.run import run
from lazylemon.cli.validate import validate

app = typer.Typer()

app.command()(run)
app.command()(validate)

if __name__ == "__main__":
    app()
