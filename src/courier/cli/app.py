"""Root typer application with subcommand registration."""

import typer

from courier.cli.queues import queues_app
from courier.cli.registry import ensure_registry
from courier.cli.run import run
from courier.cli.validate import validate

app = typer.Typer()


@app.callback()
def _pre_command() -> None:
    ensure_registry()


app.command()(run)
app.command()(validate)
app.add_typer(queues_app, name="queues")

if __name__ == "__main__":
    app()
