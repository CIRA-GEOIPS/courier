"""Redis-backed state synchronization for job builders in HA deployments.

This module is optional and requires the ``redis`` package::

    pip install lazylemon[ha]

Design
------
Each job builder instance in an HA cluster connects to a shared Redis
server.  State is stored in Redis hashes (one per job group) and changes
are broadcast via a pub/sub channel.  On each mutation:

1. The file-processing thread writes the updated job to the Redis hash.
2. It publishes a lightweight notification (event type + job ID) to the
   pub/sub channel.
3. Subscriber threads on peer instances receive the notification, fetch
   the updated job from the hash, and merge it locally using
   last-write-wins on ``Job.last_modified``.

Redis key layout::

    {prefix}:{ns}:{builder}:{group}:jobs     HASH   field = job_id
    {prefix}:{ns}:{builder}:state_changes    PubSub channel
    {prefix}:{ns}:{builder}:{job_id}:claimed STRING  SETNX emit guard
"""

from __future__ import annotations

import contextlib
import json
import threading
from typing import TYPE_CHECKING, Any

import redis
import redis.client

from lazylemon.errors import StateSyncConnectionError
from lazylemon.metrics import (
    STATE_SYNC_APPLIES,
    STATE_SYNC_EMIT_CLAIMS,
    STATE_SYNC_ERRORS,
    STATE_SYNC_PUSHES,
)
from lazylemon.utils.logging import get_logger

if TYPE_CHECKING:
    from lazylemon.schema.v1alpha1.sync_config import RedisStateSyncConfig
    from lazylemon.types.job import Job, JobGroup


class JobBuilderStateSync:
    """Redis-backed HA state synchronizer for a single job builder.

    Each instance owns two Redis connections: one for regular commands
    (HSET, HDEL, SET NX, HGETALL) and one dedicated pub/sub connection.
    The subscriber runs in a daemon thread.

    Thread-safe: ``_group_locks`` (one ``threading.Lock`` per
    ``JobGroup``) protects ``JobGroup.jobs``.  The subscriber thread
    and the file-processing thread both acquire the group lock before
    any mutation.  Thread-safe: protected by ``_group_locks[group_name]``.

    Implementations: JobBuilder (lazylemon.interfaces.module_based.job_builders)
    """

    def __init__(
        self,
        config: RedisStateSyncConfig,
        namespace: str,
        builder_name: str,
    ) -> None:
        """Store configuration; does not connect to Redis.

        Parameters
        ----------
        config : RedisStateSyncConfig
            Validated Redis connection settings.
        namespace : str
            Service namespace for Redis key namespacing.
        builder_name : str
            Job builder name for Redis key namespacing.
        """
        self._config = config
        self._namespace = namespace
        self._builder_name = builder_name
        self._logger = get_logger("sync", builder_name)
        self._stop_event = threading.Event()
        self._subscriber_thread: threading.Thread | None = None
        self._job_groups: list[JobGroup] = []
        self._group_locks: dict[str, threading.Lock] = {}
        self._client: redis.Redis | None = None
        self._pubsub: redis.client.PubSub | None = None
        self._pushes = STATE_SYNC_PUSHES
        self._applies = STATE_SYNC_APPLIES
        self._claims = STATE_SYNC_EMIT_CLAIMS
        self._errors = STATE_SYNC_ERRORS

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Establish Redis connections and verify reachability.

        Must be called before :meth:`start`.

        Raises
        ------
        StateSyncConnectionError
            If Redis is unreachable or authentication fails.
        """
        cfg = self._config
        try:
            client: redis.Redis = redis.Redis(
                host=cfg.host,
                port=cfg.port,
                db=cfg.db,
                password=cfg.password or None,
                ssl=cfg.ssl,
                socket_connect_timeout=5,
                decode_responses=True,
            )
            client.ping()
            self._client = client
            self._pubsub = client.pubsub(ignore_subscribe_messages=True)
        except redis.AuthenticationError as exc:
            msg = f"Redis authentication failed for state-sync at {cfg.host}:{cfg.port}"
            raise StateSyncConnectionError(msg) from exc
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            msg = (
                f"Cannot connect to state-sync Redis at "
                f"{cfg.host}:{cfg.port} db={cfg.db} — {exc}"
            )
            raise StateSyncConnectionError(msg) from exc
        self._logger.info(
            f"State-sync Redis connected: {cfg.host}:{cfg.port} db={cfg.db}",
        )

    def start(
        self,
        job_groups: list[JobGroup],
        group_locks: dict[str, threading.Lock],
    ) -> None:
        """Hydrate local state from Redis and launch the subscriber thread.

        Parameters
        ----------
        job_groups : list[JobGroup]
            Job groups owned by the parent ``JobBuilder``.
        group_locks : dict[str, threading.Lock]
            Per-group locks keyed by ``JobGroup.name``.
        """
        self._job_groups = job_groups
        self._group_locks = group_locks
        self.load_remote_state()
        pubsub = self._require_pubsub()
        pubsub.subscribe(self._channel)
        self._stop_event.clear()
        self._subscriber_thread = threading.Thread(
            target=self._subscriber_loop,
            name=f"{self._builder_name}-state-sync",
            daemon=True,
        )
        self._subscriber_thread.start()
        self._logger.info(
            f"State-sync subscriber started on channel {self._channel!r}",
        )

    def stop(self) -> None:
        """Shut down the subscriber thread and close the pub/sub connection."""
        self._stop_event.set()
        if self._subscriber_thread and self._subscriber_thread.is_alive():
            self._subscriber_thread.join(timeout=5)
        with contextlib.suppress(Exception):
            if self._pubsub is not None:
                self._pubsub.unsubscribe()
                self._pubsub.close()
        self._logger.info("State-sync subscriber stopped")

    # ------------------------------------------------------------------
    # Mutations pushed to Redis
    # ------------------------------------------------------------------

    def push_job_update(self, group_name: str, job_id: str, job: Job) -> None:
        """Write a job to the Redis hash and notify peers.

        Safe to call while the group lock is held; the Redis round-trip
        is fast relative to lock-hold time.
        """
        client = self._require_client()
        try:
            client.hset(self._hash_key(group_name), job_id, str(job))
            client.publish(
                self._channel,
                json.dumps(
                    {
                        "event": "job_updated",
                        "group": group_name,
                        "job_id": job_id,
                    },
                ),
            )
            self._pushes.labels(
                builder_name=self._builder_name,
                event="job_updated",
            ).inc()
        except redis.RedisError as exc:
            self._errors.labels(
                builder_name=self._builder_name,
                operation="push_update",
            ).inc()
            self._logger.warning(f"Failed to push job update for {job_id!r}: {exc}")

    def push_job_deletion(self, group_name: str, job_id: str) -> None:
        """Remove a job from the Redis hash and notify peers."""
        client = self._require_client()
        try:
            client.hdel(self._hash_key(group_name), job_id)
            client.publish(
                self._channel,
                json.dumps(
                    {
                        "event": "job_deleted",
                        "group": group_name,
                        "job_id": job_id,
                    },
                ),
            )
            self._pushes.labels(
                builder_name=self._builder_name,
                event="job_deleted",
            ).inc()
        except redis.RedisError as exc:
            self._errors.labels(
                builder_name=self._builder_name,
                operation="push_deletion",
            ).inc()
            self._logger.warning(
                f"Failed to push job deletion for {job_id!r}: {exc}",
            )

    def try_claim_emit(self, job_id: str, ttl: float) -> bool:
        """Atomically claim the right to emit a job via Redis SET NX.

        Only the instance that successfully sets the claim key may emit
        the job to the downstream queue, preventing duplicate dispatch.

        On Redis errors the method returns ``True`` (fail-open) so that
        a Redis outage does not silently swallow jobs.

        Parameters
        ----------
        job_id : str
            Unique job identifier.
        ttl : float
            Claim key expiry in seconds (minimum 1 s).

        Returns
        -------
        bool
            ``True`` if this instance acquired the claim; ``False`` if
            another instance already holds it.
        """
        client = self._require_client()
        try:
            result = client.set(
                self._claim_key(job_id),
                "1",
                nx=True,
                ex=max(1, int(ttl)),
            )
            claimed = bool(result)
            self._claims.labels(
                builder_name=self._builder_name,
                result="acquired" if claimed else "skipped",
            ).inc()
        except redis.RedisError as exc:
            self._errors.labels(
                builder_name=self._builder_name,
                operation="claim_emit",
            ).inc()
            self._logger.warning(
                f"Redis error claiming emit for {job_id!r}: {exc}. "
                "Proceeding with emit (fail-open) to avoid silent job loss.",
            )
            return True
        else:
            return claimed

    # ------------------------------------------------------------------
    # State loading
    # ------------------------------------------------------------------

    def load_remote_state(self) -> None:
        """Hydrate all local job groups from their Redis hashes.

        Called once during :meth:`start`, before the subscriber begins.
        Uses last-write-wins merge on ``Job.last_modified``.
        """
        for job_group in self._job_groups:
            self._load_group(job_group)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _require_client(self) -> redis.Redis:
        """Return the live Redis client.

        Raises
        ------
        RuntimeError
            If :meth:`connect` has not been called.
        """
        if self._client is None:
            raise RuntimeError(
                "JobBuilderStateSync.connect() must be called before use",
            )
        return self._client

    def _require_pubsub(self) -> redis.client.PubSub:
        """Return the live PubSub connection.

        Raises
        ------
        RuntimeError
            If :meth:`connect` has not been called.
        """
        if self._pubsub is None:
            raise RuntimeError(
                "JobBuilderStateSync.connect() must be called before use",
            )
        return self._pubsub

    def _load_group(self, job_group: JobGroup) -> None:
        """Load and merge remote jobs for a single group from its hash."""
        client = self._require_client()
        try:
            remote: dict[str, str] = client.hgetall(  # type: ignore[assignment]
                self._hash_key(job_group.name),
            )
        except redis.RedisError as exc:
            self._errors.labels(
                builder_name=self._builder_name,
                operation="load_state",
            ).inc()
            self._logger.warning(
                f"Failed to load remote state for group {job_group.name!r}: {exc}",
            )
            return
        lock = self._group_locks.get(job_group.name)
        with lock if lock is not None else contextlib.nullcontext():
            for job_id, job_json in remote.items():
                self._merge_job(job_group, job_id, job_json)

    def _merge_job(
        self,
        job_group: JobGroup,
        job_id: str,
        job_json: str,
    ) -> None:
        """Apply a remote job to local state using last-write-wins."""
        try:
            remote_job = job_group.job.from_string(job_json)
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            self._logger.warning(
                f"Failed to deserialize remote job {job_id!r}: {exc}",
            )
            return
        local = job_group.jobs.get(job_id)
        if local is None or remote_job.last_modified > local.last_modified:
            job_group.jobs[job_id] = remote_job
            self._applies.labels(builder_name=self._builder_name).inc()
            self._logger.debug(
                f"Merged remote job {job_id!r} into group {job_group.name!r}",
            )

    def _subscriber_loop(self) -> None:
        """Background thread: receive pub/sub messages and apply changes."""
        self._logger.debug("Subscriber loop started")
        pubsub = self._require_pubsub()
        while not self._stop_event.is_set():
            try:
                message = pubsub.get_message(timeout=1.0)
            except redis.RedisError as exc:
                self._errors.labels(
                    builder_name=self._builder_name,
                    operation="subscribe",
                ).inc()
                self._logger.warning(f"Pub/sub receive error: {exc}")
                continue
            if message is None:
                continue
            self._handle_message(message)
        self._logger.debug("Subscriber loop exited")

    def _handle_message(self, message: dict[str, Any]) -> None:
        """Parse and dispatch a single pub/sub notification."""
        if message.get("type") != "message":
            return
        try:
            payload: dict[str, str] = json.loads(message["data"])
        except (json.JSONDecodeError, KeyError) as exc:
            self._logger.warning(f"Malformed state-sync message: {exc}")
            return
        event = payload.get("event", "")
        group_name = payload.get("group", "")
        job_id = payload.get("job_id", "")
        job_group = next(
            (jg for jg in self._job_groups if jg.name == group_name),
            None,
        )
        if job_group is None:
            return
        self._apply_event(job_group, event, job_id)

    def _apply_event(
        self,
        job_group: JobGroup,
        event: str,
        job_id: str,
    ) -> None:
        """Apply a ``job_updated`` or ``job_deleted`` event under the group lock."""
        lock = self._group_locks.get(job_group.name)
        with lock if lock is not None else contextlib.nullcontext():
            if event == "job_updated":
                self._fetch_and_merge(job_group, job_id)
            elif event == "job_deleted":
                job_group.jobs.pop(job_id, None)

    def _fetch_and_merge(self, job_group: JobGroup, job_id: str) -> None:
        """Fetch a job from the Redis hash and merge it into the local group."""
        client = self._require_client()
        try:
            job_json: str | None = client.hget(  # type: ignore[assignment]
                self._hash_key(job_group.name),
                job_id,
            )
        except redis.RedisError as exc:
            self._errors.labels(
                builder_name=self._builder_name,
                operation="fetch_merge",
            ).inc()
            self._logger.warning(
                f"Failed to fetch updated job {job_id!r}: {exc}",
            )
            return
        if job_json is None:
            return
        self._merge_job(job_group, job_id, job_json)

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    @property
    def _channel(self) -> str:
        """Pub/sub channel name for this builder."""
        p = self._config.channel_prefix
        return f"{p}:{self._namespace}:{self._builder_name}:state_changes"

    def _hash_key(self, group_name: str) -> str:
        """Redis hash key for a job group's accumulated state."""
        p = self._config.channel_prefix
        return f"{p}:{self._namespace}:{self._builder_name}:{group_name}:jobs"

    def _claim_key(self, job_id: str) -> str:
        """Redis key used to claim exclusive emit rights for a job."""
        p = self._config.channel_prefix
        return f"{p}:{self._namespace}:{self._builder_name}:{job_id}:claimed"
