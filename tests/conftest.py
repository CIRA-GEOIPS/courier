"""Shared test fixtures.

The in-memory Kombu transport keeps its queue registries in class-level dicts
(``kombu.transport.memory.Channel.queues`` / ``.events``) and its exchange and
binding tables on ``kombu.transport.memory.Transport.global_state``. Nothing
clears any of them when a connection closes, so every test that spins up a
``Service`` on ``memory://`` leaks its namespaced queues into the next one.
The integration suite accumulates enough state that later tests miss their
45-second polling deadlines and fail. That only happens when several test
modules run in the same process, so they pass individually and fail in CI.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


def reset_kombu_memory_transport() -> None:
    """Clear the process-global in-memory broker state.

    Split out of the fixture below so a test asserting that the reset works
    can invoke it; pytest forbids calling a fixture directly.
    """
    from kombu.transport import memory, virtual  # noqa: PLC0415

    memory.Channel.queues.clear()
    memory.Channel.events.clear()

    # Exchanges and bindings live on ``Transport.global_state``. ``state`` is
    # only ever set on an instance, so an earlier version of this reset read it
    # off the class, found ``None``, and cleared nothing.
    memory.Transport.global_state = virtual.BrokerState()


@pytest.fixture(autouse=True)
def _reset_kombu_memory_transport() -> Iterator[None]:
    """Clear the process-global in-memory broker state after every test."""
    yield
    reset_kombu_memory_transport()
