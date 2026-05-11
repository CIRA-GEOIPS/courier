"""Root typer application with subcommand registration."""

import typer

from courier.cli.init import init
from courier.cli.plugins import plugins_app
from courier.cli.queues import queues_app
from courier.cli.registry import ensure_registry
from courier.cli.run import run
from courier.cli.validate import validate

app = typer.Typer()


@app.callback()
def _pre_command() -> None:
    ensure_registry()


app.command()(init)
app.command()(run)
app.command()(validate)
app.add_typer(plugins_app, name="plugins")
app.add_typer(queues_app, name="queues")

try:
    from courier.viz.cli import viz_app

    app.add_typer(viz_app, name="viz")
except ImportError:
    pass  # viz extra not installed — command won't be available

if __name__ == "__main__":
    app()
