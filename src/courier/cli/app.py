"""Root typer application with subcommand registration."""

import logging

import typer

from courier.cli.init import init
from courier.cli.plugins import plugins_app
from courier.cli.queues import queues_app
from courier.cli.registry import ensure_registry
from courier.cli.run import run
from courier.cli.validate import validate
from courier.utils.logging import TRACE_LEVEL

VALID_LOG_LEVELS: dict[str, int] = {
    "TRACE": TRACE_LEVEL,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

app = typer.Typer()


@app.callback()
def _pre_command(
    ctx: typer.Context,
    log_level: str | None = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Log level: TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL "
        "[env: COURIER_LOG_LEVEL]",
    ),
) -> None:
    """Pre-command callback: validates --log-level, ensures plugin registry."""
    ctx.ensure_object(dict)
    if log_level is not None:
        upper = log_level.upper()
        if upper not in VALID_LOG_LEVELS:
            valid_levels = ", ".join(VALID_LOG_LEVELS)
            raise typer.BadParameter(
                f"'{log_level}' is not a valid log level. Choose from: {valid_levels}",
            )
        ctx.obj["log_level"] = upper
    ensure_registry()


app.command()(init)
app.command()(run)
app.command()(validate)
app.add_typer(plugins_app, name="plugins")
app.add_typer(queues_app, name="queues")

# Registered with app.command(), not app.add_typer(): a Typer sub-app is a
# *group*, so Click parses any token after the positional argument as a
# subcommand name. That made every documented invocation
# (`courier dashboard config.yaml --only-metrics`) fail with
# "No such command '--only-metrics'". Neither of these has subcommands.
try:
    from courier.viz.cli import viz

    app.command("viz")(viz)
except ImportError:
    pass  # viz extra not installed — command won't be available

try:
    from courier.dashboard.cli import dashboard

    app.command("dashboard")(dashboard)
except ImportError:
    pass  # dashboard extra not installed — command won't be available

if __name__ == "__main__":
    app()
