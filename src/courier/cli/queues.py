"""CLI ``courier queues`` sub-app — list and prune broker queues.

Expected queue names come from the service YAML via the same helpers the
runtime uses, so there is no drift between "what should exist" in production
and "what the CLI compares against". :class:`courier.routing.TargetResolver`
supplies the per-dispatcher job-ready queues and
:func:`courier.constants.file_found_queue_for` the durable per-builder
file-found queues.

``list`` prints the expected names. ``prune`` takes an explicit list of
candidate names on the command line (or piped via ``--from-file``), diffs
them against the expected set, and either reports or deletes the orphans.
The CLI deliberately does not try to list live queues off the broker:
AMQP has no uniform listing endpoint without the management plugin, so
requiring the operator to supply the candidates keeps the command
portable and auditable.
"""

from __future__ import annotations

import json
from pathlib import Path  # noqa: TC003 — Typer reads annotation at runtime.
from typing import Annotated

import typer
from kombu import Connection
from kombu.exceptions import ChannelError, OperationalError

from courier.cli.feedback import load_config_or_exit
from courier.cli.plugins import normalize_kind
from courier.constants import (
    DISPATCHER_QUEUE,
    dead_letter_queue_for,
    file_found_queue_for,
    namespaced_queue_name,
)
from courier.routing import build_default_resolver

queues_app = typer.Typer(
    name="queues",
    help="Inspect and prune courier's broker queues.",
    no_args_is_help=True,
)


# Positional, not an option: `run`, `validate` and `dashboard` all take the
# config this way, and `courier queues list config.yaml` failing while
# `courier validate config.yaml` worked was a needless second grammar.
_CONFIG_ARGUMENT = typer.Argument(
    metavar="CONFIG",
    help="Path to the service YAML whose queues are being inspected.",
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

    Expected names are ``<ns>-JobReady-<dispatcher>`` per dispatcher,
    ``<ns>-FilesFound-<builder>`` per job builder, and the shared
    ``<ns>-DispatcherQueue``. Each has a ``-DeadLetter`` companion.

    Only queues are returned. The fanout exchange ``<ns>-FilesFoundExchange``
    is excluded because ``prune`` deletes queues.
    """
    config = load_config_or_exit(config_file)
    ns = namespace or config.metadata.namespace or "default"
    dispatcher_ids = {
        e.identifier
        for e in config.spec.run
        if normalize_kind(e.spec.kind) == "dispatchers"
    }
    builder_ids = {
        e.identifier
        for e in config.spec.run
        if normalize_kind(e.spec.kind) == "job_builders"
    }
    resolver = build_default_resolver(dispatcher_ids)
    queues: set[str] = set()
    for ident in resolver.known_identifiers():
        queues.add(namespaced_queue_name(ns, resolver.resolve(ident)))
    # Each job builder consumes the fanout exchange through a durable named
    # queue, which must survive a prune: it holds the backlog for a builder
    # that is down or not yet deployed. Builders previously used exclusive
    # <ns>-FilesFoundExchange-fanout-<uuid> queues that the broker deleted on
    # disconnect, so any name in that older shape is an orphan.
    for ident in sorted(builder_ids):
        queues.add(namespaced_queue_name(ns, file_found_queue_for(ident)))
    queues.add(namespaced_queue_name(ns, DISPATCHER_QUEUE))
    # Every consumed queue has a dead-letter queue alongside it, holding the
    # messages the service gave up on. They are the only copy of a message that
    # failed repeatedly, so a prune must preserve them.
    queues |= {dead_letter_queue_for(name) for name in queues}
    return ns, queues


#: Reply code RabbitMQ answers with when ``if_empty`` deletion finds messages.
_PRECONDITION_FAILED = 406


def _delete_hint(exc: Exception, *, force: bool) -> str:
    """Return the follow-up advice for a failed queue deletion.

    A non-empty queue answers with a precondition failure, and only that case
    gets the ``--force`` hint: forcing does not help a missing-queue or
    permission error.

    Parameters
    ----------
    exc : Exception
        The error the broker raised.
    force : bool
        Whether ``--force`` was already given.

    Returns
    -------
    str
        Text to append to the failure line, possibly empty.
    """
    code = getattr(exc, "reply_code", None) or getattr(exc, "code", None)
    if code == _PRECONDITION_FAILED and not force:
        return "  (non-empty? rerun with --force)"
    return ""


def _broker_url(config_file: Path) -> str:
    return load_config_or_exit(config_file).spec.broker.to_url()


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


_JSON_OPTION = typer.Option(
    "--json",
    "-j",
    help="Emit machine-readable JSON to stdout instead of plain text.",
)


@queues_app.command("list")
def list_cmd(
    config: Annotated[Path, _CONFIG_ARGUMENT],
    namespace: Annotated[str | None, _NAMESPACE_OPTION] = None,
    json_output: Annotated[bool, _JSON_OPTION] = False,
) -> None:
    """Print every queue the service is expected to use.

    Exchanges are managed separately.
    """
    ns, queues = _expected_queues(config, namespace)
    if json_output:
        # Matches `plugins list --json`, so both listings can be piped into jq
        # rather than one of them needing to be scraped.
        typer.echo(
            json.dumps({"namespace": ns, "queues": sorted(queues)}, indent=2),
        )
        return
    typer.echo(f"namespace: {ns}")
    for name in sorted(queues):
        typer.echo(name)


@queues_app.command("prune")
def prune_cmd(  # noqa: PLR0913
    config: Annotated[Path, _CONFIG_ARGUMENT],
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

    # Names starting with amq. are server-generated (reply queues, anonymous
    # consumers created by other tools). Courier does not create them, and
    # refusing them is cheap defence on a shared vhost.
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
                except (OperationalError, ChannelError) as exc:
                    failures.append((name, str(exc)))
                    typer.echo(
                        f"failed:   {name}: {exc}{_delete_hint(exc, force=force)}",
                        err=True,
                    )
    except OperationalError as exc:
        typer.echo(f"broker connection failed: {exc}", err=True)
        raise typer.Exit(1) from exc

    if failures:
        raise typer.Exit(1)
