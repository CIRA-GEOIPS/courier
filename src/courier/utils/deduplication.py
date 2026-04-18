"""Bounded deduplication utilities for plugins with a bounded seen-set.

Several data monitor plugins (``cron_glob``, ``s3_poller``, ``sftp_poller``)
need to track which identifiers have already been emitted so that repeated
scans do not re-emit them. ``BoundedSeenSet`` provides a thread-unsafe
bounded LRU-style set backed by an ``OrderedDict``.

Not thread-safe. Each plugin owns its seen-set exclusively from its own
thread; no lock is required. Callers that need cross-thread sharing must
wrap access in their own lock.
"""

from collections import OrderedDict
from collections.abc import Hashable, Iterable
from typing import Generic, TypeVar

K = TypeVar("K", bound=Hashable)


class BoundedSeenSet(Generic[K]):  # noqa: UP046
    """Bounded LRU seen-set used by polling data monitors for deduplication.

    Implementations
    ---------------
    Used by ``cron_glob`` (``Path`` keys), ``s3_poller`` (``str`` S3 URIs),
    and ``sftp_poller`` (``str`` SFTP URIs).

    Parameters
    ----------
    max_size : int
        Upper bound on retained entries. When exceeded, the oldest
        inserted entry is evicted.

    Notes
    -----
    Thread-safe: no — owned by a single plugin thread. Do not share
    across threads without external locking.
    """

    def __init__(self, max_size: int) -> None:
        if max_size < 1:
            msg = f"max_size must be >= 1, got {max_size}"
            raise ValueError(msg)
        self._max_size = max_size
        self._entries: OrderedDict[K, None] = OrderedDict()

    def __contains__(self, key: K) -> bool:
        """Return ``True`` if ``key`` has been added and not yet evicted."""
        return key in self._entries

    def __len__(self) -> int:
        """Return current number of retained entries (``<= max_size``)."""
        return len(self._entries)

    def add(self, key: K) -> None:
        """Insert ``key`` and evict oldest entry if capacity is exceeded."""
        self._entries[key] = None
        if len(self._entries) > self._max_size:
            self._entries.popitem(last=False)

    def seed(self, keys: Iterable[K]) -> None:
        """Bulk-insert ``keys`` without yielding; used for ``ignore_existing``."""
        for key in keys:
            self.add(key)
