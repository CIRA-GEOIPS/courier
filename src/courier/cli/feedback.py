"""Shared, human-facing CLI output.

A CLI is a conversation: every exchange should say what happened, what it means,
and what the operator can do next. These helpers exist so that answer is the
same wherever a config is loaded, rather than each command inventing its own
phrasing and exit code.

Two problems this replaces:

* ``Error: File x.yaml not found`` was a dead end. It did not say what to do,
  and ``dashboard`` reported the same condition through Click's ``exists=True``
  with a different message *and* a different exit code (2 rather than 1).
* Validation failures printed pydantic's ``ValidationError`` verbatim, so the
  first thing an operator saw was ``[type=missing, input_value={...},
  input_type=dict]`` and a link to errors.pydantic.dev -- library internals, for
  a mistake in their own YAML.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:
    from courier.schema import ServiceConfigModel

#: Shown when a config is missing or unusable. Kept short: the operator wants
#: the fix, not a tutorial.
_NEXT_STEP = "Generate a starting config with:  courier init"

#: How many sibling configs to name before it stops being helpful.
_MAX_SUGGESTIONS = 5


def _nearby_configs(missing: Path) -> list[str]:
    """Return config-shaped files sitting next to *missing*.

    A missing path is very often a typo or a stale name, and the right file is
    usually in the same directory.
    """
    directory = missing.parent if str(missing.parent) else Path()
    try:
        candidates = sorted(
            path.name
            for path in directory.iterdir()
            if path.suffix in {".yaml", ".yml", ".json"} and path.is_file()
        )
    except OSError:
        return []
    return candidates[:_MAX_SUGGESTIONS]


def abort_missing_config(path: Path) -> None:
    """Report a config that is not there, then exit non-zero.

    Raises
    ------
    typer.Exit
        Always, with code 1. Every command reports this the same way, so a
        missing config never depends on which command noticed it.
    """
    typer.echo(f"No config file at {path}")

    nearby = _nearby_configs(path)
    if nearby:
        typer.echo("\nDid you mean one of these?")
        for name in nearby:
            typer.echo(f"  {name}")
    else:
        typer.echo(f"\n{_NEXT_STEP}")

    raise typer.Exit(1)


def load_config_or_exit(path: Path) -> ServiceConfigModel:
    """Load a service config, or report why not and exit.

    The single CLI-side entry point for reading a config. Library code raises;
    this translates. Every command funnels through here so a missing or invalid
    config reads the same and exits the same, whichever command found it.

    Raises
    ------
    typer.Exit
        With code 1 if the file is missing or fails validation.
    """
    from courier.cli.config_loader import load_config  # noqa: PLC0415
    from courier.errors import CourierError  # noqa: PLC0415

    if not path.exists():
        abort_missing_config(path)

    try:
        return load_config(path)
    except (CourierError, ValueError, OSError) as exc:
        typer.echo(format_validation_error(path, str(exc)))
        raise typer.Exit(1) from exc


def _humanise(location: str, message: str) -> str:
    """Turn one pydantic error into something an operator can act on."""
    replacements = (
        ("Field required", "required, but missing"),
        ("Input should be a valid", "should be a"),
        ("Extra inputs are not permitted", "not a recognised setting"),
    )
    for pattern, plain in replacements:
        if message.startswith(pattern):
            message = message.replace(pattern, plain, 1)
            break
    else:
        # "List should have at least 1 item after validation, not 0"
        least = re.match(
            r"List should have at least (\d+) item.* not (\d+)$", message,
        )
        if least:
            wanted, found = least.groups()
            message = f"needs at least {wanted}, found {found}"
    return f"  {location:<28} {message}"


def shell_quote(path: Path) -> str:
    """Render *path* so the suggested command can be pasted into a shell.

    Suggesting ``courier run /some/dir with spaces/config.yaml`` and having it
    fail on paste is worse than suggesting nothing.
    """
    return shlex.quote(str(path))


def format_validation_error(path: Path, error: str) -> str:
    """Render a pydantic ``ValidationError`` string as operator-facing text.

    pydantic's own rendering names the *model* that failed and appends a
    ``type=``/``input_value=`` tail plus a documentation URL to every problem.
    None of that helps someone editing YAML: they need the key and what is
    wrong with it.
    """
    problems: list[str] = []
    lines = error.splitlines()

    for index, line in enumerate(lines):
        # pydantic emits "location" then an indented "message [type=..., ...]".
        if not line or line.startswith(" ") or "validation error" in line:
            continue
        if index + 1 >= len(lines):
            continue
        detail = lines[index + 1].strip()
        detail = re.sub(r"\s*\[type=.*$", "", detail)
        if detail:
            problems.append(_humanise(line.strip(), detail))

    if not problems:
        # Not a pydantic error -- a YAML syntax error, say, which already
        # names file, line and column. Pass it through untouched.
        return f"{path} could not be loaded.\n\n  {error}"

    count = len(problems)
    header = f"{path} is not valid ({count} problem{'s' if count != 1 else ''}):"
    return "\n".join([header, "", *problems, "", _NEXT_STEP])
