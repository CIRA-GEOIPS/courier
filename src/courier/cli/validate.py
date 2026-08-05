"""CLI `validate` command — validates a service config file."""

from collections import Counter
from pathlib import Path
from typing import Annotated, Any

import typer

from courier.cli.feedback import load_config_or_exit, shell_quote
from courier.cli.plugins import normalize_kind

#: Interface name -> what to call one of them when talking to a human.
_KIND_LABELS = {
    "data_monitors": "data monitor",
    "job_builders": "job builder",
    "dispatchers": "dispatcher",
}


def _describe_pipeline(config: Any) -> list[str]:
    """Summarise what was validated, for the operator to sanity-check.

    ``Config valid`` alone answered "did it parse", but not the question the
    operator actually has -- did it parse *as the pipeline I meant*. A count
    per kind catches a step that was silently dropped or duplicated.
    """
    counts: Counter[str] = Counter(
        normalize_kind(entry.spec.kind) for entry in config.spec.run
    )
    parts = [
        f"{counts[kind]} {label}{'s' if counts[kind] != 1 else ''}"
        for kind, label in _KIND_LABELS.items()
        if counts[kind]
    ]
    total = sum(counts.values())
    lines = [f"  {total} pipeline step{'s' if total != 1 else ''}: {', '.join(parts)}"]

    transport = getattr(getattr(config.spec, "broker", None), "transport", None)
    if transport:
        lines.append(f"  broker: {transport}")
    return lines


def validate(
    config_file: Annotated[
        Path,
        typer.Argument(
            metavar="CONFIG",
            help="Service YAML to check. Nothing is started.",
        ),
    ],
) -> None:
    """Validate a service config file without running the service."""
    config = load_config_or_exit(config_file)

    typer.echo(f"{config_file} is valid.")
    for line in _describe_pipeline(config):
        typer.echo(line)
    typer.echo(f"\nRun it:  courier run {shell_quote(config_file)}")
