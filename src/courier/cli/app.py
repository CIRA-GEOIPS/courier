"""Root typer application with subcommand registration."""

import logging

import typer

from courier.cli.init import init
from courier.cli.plugins import plugins_app
from courier.cli.queues import queues_app
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

# The help text is set here rather than left to the callback's docstring.
# Typer uses ``@app.callback``'s docstring as the program description, so the
# tool used to introduce itself as "Pre-command callback: validate --log-level"
# -- an implementation detail, to someone asking what courier is.
app = typer.Typer(
    help=(
        "Watch for data, group it into jobs, and dispatch those jobs to "
        "processing workflows.\n\n"
        "A courier service is described by one YAML config. Start with "
        "'courier init' to generate one interactively, 'courier validate' to "
        "check it, then 'courier run' to start the service."
    ),
    no_args_is_help=True,
)


def _show_version(*, value: bool) -> None:
    """Print the version and exit, before any other argument is processed."""
    if value:
        from courier import __version__  # noqa: PLC0415

        typer.echo(f"courier {__version__}")
        raise typer.Exit


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
    version: bool = typer.Option(  # noqa: ARG001 - consumed by the callback
        False,
        "--version",
        "-V",
        callback=lambda value: _show_version(value=value),
        is_eager=True,
        help="Show the installed courier version and exit.",
    ),
) -> None:
    """Validate global options before dispatching to a command."""
    ctx.ensure_object(dict)
    if log_level is not None:
        upper = log_level.upper()
        if upper not in VALID_LOG_LEVELS:
            valid_levels = ", ".join(VALID_LOG_LEVELS)
            raise typer.BadParameter(
                f"'{log_level}' is not a valid log level. Choose from: {valid_levels}",
            )
        ctx.obj["log_level"] = upper


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

    # Examples live in the epilog rather than the docstring: Typer renders a
    # docstring verbatim, so a numpydoc "Examples\n--------" section printed its
    # own underline into --help as a row of literal dashes.
    app.command(
        "dashboard",
        # Typer's rich renderer rewraps the epilog and ignores Click's \b
        # escape, so a multi-line example block collapses into a run-on
        # paragraph. One example that survives rewrapping, and a pointer
        # to the reference page that lists the rest.
        epilog=(
            "Example:  courier dashboard config.yaml --split-by kind "
            "-o ./dashboards/"
        ),
    )(dashboard)
except ImportError:
    pass  # dashboard extra not installed — command won't be available

if __name__ == "__main__":
    app()
