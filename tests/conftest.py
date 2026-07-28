"""Shared test fixtures.

The in-memory Kombu transport keeps its queue and fanout-binding registries in
*class-level* dicts (``kombu.transport.memory.Channel.queues`` /
``.events``) plus a module-global ``BrokerState``. Nothing clears them when a
connection closes, so every test that spins up a ``Service`` on ``memory://``
leaks its namespaced queues into the next one. The integration suite
accumulates enough state that later tests miss their 45-second polling
deadlines and fail -- but only when several test modules run in the same
process, which is why they pass individually and fail in CI.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _reset_kombu_memory_transport() -> Iterator[None]:
    """Clear the process-global in-memory broker state after every test."""
    yield

    from kombu.transport import memory, virtual  # noqa: PLC0415

    memory.Channel.queues.clear()
    memory.Channel.events.clear()

    # Exchange table and bindings live on a module-level BrokerState; fanout
    # delivery consults it, so stale bindings from a previous test would
    # otherwise keep matching.
    state = getattr(memory.Transport, "state", None)
    if state is not None:
        for attr in ("exchanges", "bindings", "queue_index"):
            table = getattr(state, attr, None)
            if hasattr(table, "clear"):
                table.clear()

    global_state = getattr(virtual, "BrokerState", None)
    if global_state is not None and hasattr(memory.Transport, "state"):
        # Re-seat a clean state object so anything holding the old one cannot
        # resurrect bindings.
        memory.Transport.state = global_state()
