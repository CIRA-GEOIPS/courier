"""CLI ``courier queues`` sub-app — list and prune broker queues.

Expected queue and exchange names come from the service YAML via the
same :class:`courier.routing.TargetResolver` the runtime uses, so there
is no drift between "what should exist" in production and "what the CLI
compares against".

``list`` prints the expected names. ``prune`` takes an explicit list of
candidate names on the command line (or piped via ``--from-file``), diffs
them against the expected set, and either reports or deletes the orphans.
The CLI deliberately does not try to list live queues off the broker:
AMQP has no uniform listing endpoint without the management plugin, so
requiring the operator to supply the candidates keeps the command
portable and auditable.
"""

from __future__ import annotations

from pathlib import Path  # noqa: TC003 — Typer reads annotation at runtime.
from typing import Annotated

import typer
from kombu import Connection
from kombu.exceptions import OperationalError

from courier.cli.config_loader import load_config
from courier.cli.plugins import normalize_kind
from courier.constants import (
    DISPATCHER_QUEUE,
)
from courier.routing import build_default_resolver

queues_app = typer.Typer(
    name="queues",
    help="Inspect and prune courier's broker queues.",
    no_args_is_help=True,
)


_CONFIG_OPTION = typer.Option(
    "--config",
    "-c",
    exists=True,
    readable=True,
    help="Path to the service YAML.",
)
_NAMESPACE_OPTION = typer.Option(
    "--namespace",
    "-n",
    help="Override the namespace read from the YAML metadata.",
)
_CANDIDATE_OPTION = typer.Option(
    "--candidate",
    help=(
        "Queue name suspected of being orphaned. Pass multiple times "
        "or as a comma-separated list."
    ),
)
_FROM_FILE_OPTION = typer.Option(
    "--from-file",
    help=(
        "Read candidate queue names from a file (one per line, '#' comments allowed)."
    ),
)
_APPLY_OPTION = typer.Option(
    "--apply/--dry-run",
    help="Actually delete orphans. Defaults to dry-run.",
)
_FORCE_OPTION = typer.Option(
    "--force",
    help=(
        "Delete orphaned queues even when they still hold messages. "
        "Without this, a non-empty queue is left alone and reported."
    ),
)


def _expected_queues(config_file: Path, namespace: str | None) -> tuple[str, set[str]]:
    """Return ``(namespace, expected_queue_names)`` from the validated config.

    Returns only queue names --- exchanges (e.g. ``FilesFoundExchange``)
    and their auto-generated consumer queues (``amq.gen-*``) are excluded
    because they are managed by the broker.
    """
    config = load_config(config_file)
    ns = namespace or config.metadata.namespace or "default"
    dispatcher_ids = {
        e.identifier for e in config.spec.run if normalize_kind(e.spec.kind) == "dispatchers"
    }
    resolver = build_default_resolver(dispatcher_ids)
    queues: set[str] = set()
    for ident in resolver.known_identifiers():
        queues.add(f"{ns}-{resolver.resolve(ident)}")
    # Note: {ns}-FilesFoundExchange is a fanout *exchange*, not a
    # queue, so it is intentionally excluded from the queue-expected set.
    # The fanout pattern uses anonymous exclusive queues (amq.gen-*)
    # which are auto-deleted by the broker and must never be pruned.
    queues.add(f"{ns}-{DISPATCHER_QUEUE}")
    return ns, queues


def _broker_url(config_file: Path) -> str:
    return load_config(config_file).spec.broker.to_url()


def _read_candidates(
    candidates: list[str] | None,
    from_file: Path | None,
) -> list[str]:
    values: list[str] = []
    if candidates:
        for item in candidates:
            values.extend(piece.strip() for piece in item.split(",") if piece.strip())
    if from_file is not None:
        for line in from_file.read_text().splitlines():
            name = line.strip()
            if name and not name.startswith("#"):
                values.append(name)
    return values


@queues_app.command("list")
def list_cmd(
    config: Annotated[Path, _CONFIG_OPTION],
    namespace: Annotated[str | None, _NAMESPACE_OPTION] = None,
) -> None:
    """Print every queue the service is expected to use.

    Exchanges are managed separately.
    """
    ns, queues = _expected_queues(config, namespace)
    typer.echo(f"namespace: {ns}")
    for name in sorted(queues):
        typer.echo(name)


@queues_app.command("prune")
def prune_cmd(  # noqa: PLR0913
    config: Annotated[Path, _CONFIG_OPTION],
    *,
    candidate: Annotated[list[str] | None, _CANDIDATE_OPTION] = None,
    from_file: Annotated[Path | None, _FROM_FILE_OPTION] = None,
    namespace: Annotated[str | None, _NAMESPACE_OPTION] = None,
    apply: Annotated[bool, _APPLY_OPTION] = False,
    force: Annotated[bool, _FORCE_OPTION] = False,
) -> None:
    """Diff a candidate queue list against the expected set and delete orphans.

    Every candidate not in the expected set is considered an orphan. In
    ``--dry-run`` mode (default) orphans are printed. In ``--apply``
    mode each orphan is deleted via ``channel.queue_delete``; failures
    are logged and exit status is non-zero if any delete failed.
    """
    ns, expected = _expected_queues(config, namespace)
    candidates = _read_candidates(candidate, from_file)

    # Fan-out consumers use server-generated exclusive queue names
    # (e.g. amq.gen-xyz...). These are auto-managed by the broker and
    # MUST NOT be deleted --- they carry live consumer state.
    _server_gen_prefix = "amq."
    unsafe = [q for q in candidates if q.startswith(_server_gen_prefix)]
    if unsafe:
        typer.echo(
            f"WARNING: refusing to consider {len(unsafe)} server-generated "
            f"queue(s) (amq.* are auto-managed by the broker): " + ", ".join(unsafe),
            err=True,
        )
    candidates = [q for q in candidates if not q.startswith(_server_gen_prefix)]

    if not candidates:
        typer.echo("no candidates provided; nothing to prune", err=True)
        raise typer.Exit(2)

    orphans = [q for q in dict.fromkeys(candidates) if q not in expected]
    preserved = [q for q in dict.fromkeys(candidates) if q in expected]

    typer.echo(f"namespace: {ns}")
    for name in preserved:
        typer.echo(f"preserve: {name}")
    for name in orphans:
        typer.echo(f"orphan:   {name}")

    if not orphans:
        typer.echo("no orphans found.")
        return
    if not apply:
        typer.echo(f"dry-run: {len(orphans)} orphan(s); rerun with --apply to delete.")
        return

    broker_url = _broker_url(config)
    failures: list[tuple[str, str]] = []
    try:
        with Connection(broker_url) as conn, conn.channel() as channel:
            for name in orphans:
                try:
                    # if_empty guards against discarding queued jobs: a
                    # dispatcher temporarily excluded via --only looks exactly
                    # like an orphan, and deleting its backlog is silent and
                    # unrecoverable. Use --force to override.
                    channel.queue_delete(name, if_empty=not force)
                    typer.echo(f"deleted:  {name}")
                except OperationalError as exc:
                    failures.append((name, str(exc)))
                    typer.echo(
                        f"failed:   {name}: {exc}"
                        + ("" if force else "  (non-empty? rerun with --force)"),
                        err=True,
                    )
    except OperationalError as exc:
        typer.echo(f"broker connection failed: {exc}", err=True)
        raise typer.Exit(1) from exc

    if failures:
        raise typer.Exit(1)
