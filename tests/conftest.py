"""Shared test fixtures.

The in-memory Kombu transport keeps its queue registries in *class-level*
dicts (``kombu.transport.memory.Channel.queues`` / ``.events``) and its
exchange and binding tables on ``kombu.transport.memory.Transport.global_state``.
Nothing clears any of them when a connection closes, so every test that spins
up a ``Service`` on ``memory://`` leaks its namespaced queues into the next
one. The integration suite accumulates enough state that later tests miss
their 45-second polling deadlines and fail -- but only when several test
modules run in the same process, which is why they pass individually and fail
in CI.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


def reset_kombu_memory_transport() -> None:
    """Clear the process-global in-memory broker state.

    Queues, exchanges and bindings all live on class-level or module-level
    objects that nothing clears when a connection closes, so state leaks from
    one test into the next.

    This is a plain function rather than only a fixture body because pytest
    forbids calling a fixture directly, and a test that asserts the reset
    itself works needs to invoke it.
    """
    from kombu.transport import memory, virtual  # noqa: PLC0415

    memory.Channel.queues.clear()
    memory.Channel.events.clear()

    # Exchanges and bindings live on ``Transport.global_state``.  The previous
    # version of this reset read ``Transport.state``, which is only ever set on
    # an *instance* -- ``getattr(Transport, "state", None)`` is always None, so
    # neither the clearing loop nor the re-seat below had ever run and fanout
    # bindings leaked between tests for as long as this file has existed.
    memory.Transport.global_state = virtual.BrokerState()


@pytest.fixture(autouse=True)
def _reset_kombu_memory_transport() -> Iterator[None]:
    """Clear the process-global in-memory broker state after every test."""
    yield
    reset_kombu_memory_transport()
