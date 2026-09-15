"""Redis-backed state synchronization for job builders in HA deployments.

This module is optional and requires the ``redis`` package::

    pip install data-courier[ha]

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

from courier.errors import StateSyncConnectionError
from courier.metrics import (
    STATE_SYNC_APPLIES,
    STATE_SYNC_EMIT_CLAIMS,
    STATE_SYNC_ERRORS,
    STATE_SYNC_PUSHES,
)
from courier.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from courier.schema.v1alpha1.sync_config import RedisStateSyncConfig
    from courier.types.job import Job, JobGroup


#: Server-side union of one job into a hash field.
#:
#: Replicas of one builder identifier are competing consumers, so each holds a
#: different subset of a job's files. A blind write means whoever wrote last
#: erases the other's files, and a client-side read-merge-write still loses one
#: side when two replicas interleave. Doing the merge inside the script makes
#: the whole read-decide-write one atomic step.
#:
#: The script is deliberately minimal: it merges the ``files`` list, keeps the
#: larger ``last_modified``, and prefers the existing value for every other
#: field so an identifier or correlation id cannot flip-flop between writers.
_UNION_JOB_LUA = """
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if not existing then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  return 1
end
local ok_old, old = pcall(cjson.decode, existing)
local ok_new, new = pcall(cjson.decode, ARGV[2])
if not ok_old or not ok_new then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  return 1
end
local seen = {}
local merged = {}
for _, source in ipairs({old.files, new.files}) do
  if source then
    for _, item in ipairs(source) do
      if not seen[item] then
        seen[item] = true
        merged[#merged + 1] = item
      end
    end
  end
end
new.files = merged
if old.last_modified and new.last_modified and
   tonumber(old.last_modified) > tonumber(new.last_modified) then
  new.last_modified = old.last_modified
end
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(new))
return 1
"""


def _as_str(value: bytes | str) -> str:
    """Decode a Redis hash value to ``str``."""
    return value.decode("utf-8") if isinstance(value, bytes) else value


class JobBuilderStateSync:
    """Redis-backed HA state synchronizer for a single job builder.

    Each instance owns two Redis connections: one for regular commands
    (HSET, HDEL, SET NX, HGETALL) and one dedicated pub/sub connection.
    The subscriber runs in a daemon thread.

    Thread-safe: ``_group_locks`` (one ``threading.Lock`` per
    ``JobGroup``) protects ``JobGroup.jobs``.  The subscriber thread
    and the file-processing thread both acquire the group lock before
    any mutation.  Thread-safe: protected by ``_group_locks[group_name]``.

    Implementations: JobBuilder (courier.interfaces.job_builders)
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
        builder_identifier : str
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
        self._script: Any = None
        self._scripting_unavailable = False
        self._on_merged: Callable[[JobGroup], None] | None = None
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

    def set_merge_callback(self, callback: Callable[[JobGroup], None]) -> None:
        """Register what to run after a peer's update is merged in.

        Parameters
        ----------
        callback : Callable[[JobGroup], None]
            Invoked with the affected group once a merge changed local state.
        """
        self._on_merged = callback

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
                self._pubsub = None
        # Close the command connection too: leaving it open leaked a Redis
        # connection on every plugin restart.
        with contextlib.suppress(Exception):
            if self._client is not None:
                self._client.close()
                self._client = None
        self._logger.info("State-sync subscriber stopped")

    # ------------------------------------------------------------------
    # Mutations pushed to Redis
    # ------------------------------------------------------------------

    def push_job_update(self, group_name: str, job_id: str, job: Job) -> None:
        """Merge a job into the Redis hash and notify peers.

        The write is a server-side union rather than a blind overwrite. Under
        competing consumers two replicas hold different halves of the same
        job, and whichever wrote last used to erase the other's files
        entirely. Unioning on the server also removes the read-modify-write
        race that a client-side merge would still have.

        Safe to call while the group lock is held; the Redis round-trip is
        fast relative to lock-hold time.

        Parameters
        ----------
        group_name : str
            Job group the job belongs to.
        job_id : str
            Identifier of the job being written.
        job : Job
            The local view of the job, whose files are merged in.
        """
        client = self._require_client()
        try:
            self._merge_into_redis(client, group_name, job_id, job)
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

    def _merge_into_redis(
        self,
        client: redis.Redis,
        group_name: str,
        job_id: str,
        job: Job,
    ) -> None:
        """Union *job* into whatever the hash already holds for *job_id*.

        Parameters
        ----------
        client : redis.Redis
            Connected client.
        group_name : str
            Job group the job belongs to.
        job_id : str
            Identifier of the job being written.
        job : Job
            The local view of the job.
        """
        script = self._union_script(client)
        if script is not None:
            try:
                script(keys=[self._hash_key(group_name)], args=[job_id, str(job)])
            except redis.RedisError:
                # Scripting can fail at execution as well as registration --
                # disabled on the server, or an in-process fake without a Lua
                # runtime. Fall back once and stay fallen back.
                self._scripting_unavailable = True
                self._script = None
                self._logger.warning(
                    "Redis scripting failed; job updates fall back to a plain "
                    "write. Two replicas of one builder identifier can then "
                    "lose files belonging to the same job.",
                )
            else:
                return
        # A plain write still beats dropping the update, and a single replica
        # has no second writer to race with.
        client.hset(self._hash_key(group_name), job_id, str(job))

    def _union_script(self, client: redis.Redis) -> Any:
        """Return the registered union script, or ``None`` if unsupported.

        Parameters
        ----------
        client : redis.Redis
            Connected client.

        Returns
        -------
        Any
            A callable registered script, or ``None`` when the server does not
            support scripting.
        """
        if self._script is not None:
            return self._script
        if self._scripting_unavailable:
            return None
        try:
            self._script = client.register_script(_UNION_JOB_LUA)
        except Exception:  # any failure to register means fall back
            self._scripting_unavailable = True
            self._logger.warning(
                "Redis scripting is unavailable; job updates fall back to a "
                "plain write. Two replicas of one builder identifier can then "
                "lose files for the same job.",
            )
            return None
        return self._script

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

    def release_emit_claim(self, job_id: str) -> None:
        """Release an emit claim acquired by :meth:`try_claim_emit`.

        Callers use this after a fatal publish failure for a specific
        fan-out target so that a restart can retry that target. Callers
        MUST NOT release a claim after a successful emit — the claim's
        TTL and the broker's delivery record are what deduplicate
        future retries.

        Parameters
        ----------
        job_id : str
            The same key passed to :meth:`try_claim_emit`. For per-target
            fan-out this is typically ``f"{job.identifier}::{target}"``.
        """
        client = self._require_client()
        try:
            client.delete(self._claim_key(job_id))
        except redis.RedisError as exc:
            self._errors.labels(
                builder_name=self._builder_name,
                operation="release_claim",
            ).inc()
            self._logger.warning(
                f"Redis error releasing emit claim for {job_id!r}: {exc}",
            )

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
            remote = client.hgetall(
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
                self._merge_job(job_group, _as_str(job_id), _as_str(job_json))

    def _merge_job(
        self,
        job_group: JobGroup,
        job_id: str,
        job_json: str,
    ) -> None:
        """Union a remote job into local state.

        A last-write-wins *replacement* is wrong once replicas share a queue:
        each holds a different subset of the job's files, so replacing drops
        whichever subset lost the race. Files are a set of value-comparable
        objects, so unioning them is both safe and idempotent -- a file seen
        twice collapses.

        Parameters
        ----------
        job_group : JobGroup
            Group the job belongs to.
        job_id : str
            Identifier of the remote job.
        job_json : str
            Serialized remote job.
        """
        try:
            remote_job = job_group.job.from_string(job_json)
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            self._logger.warning(
                f"Failed to deserialize remote job {job_id!r}: {exc}",
            )
            return
        local = job_group.jobs.get(job_id)
        if local is None:
            job_group.jobs[job_id] = remote_job
        else:
            before = len(local.files)
            local.files |= remote_job.files
            local.last_modified = max(local.last_modified, remote_job.last_modified)
            if len(local.files) == before:
                return
        job_group.adopt_job(job_id)
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
        """Apply a ``job_updated`` or ``job_deleted`` event under the group lock.

        Parameters
        ----------
        job_group : JobGroup
            Group the event refers to.
        event : str
            Either ``job_updated`` or ``job_deleted``.
        job_id : str
            Identifier the event refers to.
        """
        lock = self._group_locks.get(job_group.name)
        with lock if lock is not None else contextlib.nullcontext():
            if event == "job_updated":
                self._fetch_and_merge(job_group, job_id)
            elif event == "job_deleted":
                job_group.jobs.pop(job_id, None)
        if event == "job_updated" and self._on_merged is not None:
            # Outside the lock: the callback emits, which publishes, and
            # holding a group lock across a broker round-trip would stall
            # every other file for that group.
            #
            # Merging is the only moment a replica learns that a job it holds
            # only part of is now complete. Without this a job assembled from
            # files that arrived on different replicas would sit unemitted
            # until another file happened to arrive, or a timeout reaper
            # noticed it.
            self._on_merged(job_group)

    def _fetch_and_merge(self, job_group: JobGroup, job_id: str) -> None:
        """Fetch a job from the Redis hash and merge it into the local group."""
        client = self._require_client()
        try:
            job_json = client.hget(
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
        self._merge_job(job_group, job_id, _as_str(job_json))

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
