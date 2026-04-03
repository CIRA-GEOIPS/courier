"""Unit tests for Redis-backed job builder state synchronization.

Uses fakeredis so no real Redis server is required.
"""

from __future__ import annotations

import json
import threading
import time

import fakeredis
import pytest

from lazylemon.errors import StateSyncConnectionError
from lazylemon.schema.v1alpha1.sync_config import RedisStateSyncConfig
from lazylemon.sync.job_builder_state_sync import JobBuilderStateSync
from lazylemon.types.file import FrozenFile
from lazylemon.types.job import Job, JobGroup


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CFG = RedisStateSyncConfig()  # all defaults


def _sync(builder_name: str = "test-builder") -> JobBuilderStateSync:
    """Return a JobBuilderStateSync connected to an in-process fake Redis."""
    s = JobBuilderStateSync(
        config=_CFG,
        namespace="test-ns",
        builder_name=builder_name,
    )
    server = fakeredis.FakeServer()
    fake_client = fakeredis.FakeRedis(server=server, decode_responses=True)
    fake_pubsub = fake_client.pubsub(ignore_subscribe_messages=True)
    s._client = fake_client
    s._pubsub = fake_pubsub
    return s


def _make_group(name: str = "grp") -> JobGroup:
    """Return a minimal JobGroup."""
    jg = JobGroup(name, {})
    return jg


def _make_job(
    identifier: str = "job-1",
    last_modified: float | None = None,
) -> Job:
    file = FrozenFile(file=None, hostname=None, source=None, instrument=None,
                      processing_stage=None, domain=None)
    j = Job(name="test", identifier=identifier, config={})
    j.files.add(file)
    if last_modified is not None:
        j.last_modified = last_modified
    return j


# ---------------------------------------------------------------------------
# RedisStateSyncConfig schema
# ---------------------------------------------------------------------------


class TestRedisStateSyncConfig:
    """Schema validation for RedisStateSyncConfig."""

    def test_defaults(self):
        cfg = RedisStateSyncConfig()
        assert cfg.host == "localhost"
        assert cfg.port == 6379
        assert cfg.db == 0
        assert cfg.password is None
        assert cfg.ssl is False
        assert cfg.channel_prefix == "lazylemon"

    def test_custom_values(self):
        cfg = RedisStateSyncConfig(
            host="redis.prod",
            port=6380,
            db=2,
            password="secret",
            ssl=True,
            channel_prefix="myapp",
        )
        assert cfg.host == "redis.prod"
        assert cfg.port == 6380
        assert cfg.db == 2
        assert cfg.password == "secret"
        assert cfg.ssl is True
        assert cfg.channel_prefix == "myapp"

    def test_port_boundaries(self):
        assert RedisStateSyncConfig(port=1).port == 1
        assert RedisStateSyncConfig(port=65535).port == 65535

    def test_invalid_port_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="port"):
            RedisStateSyncConfig(port=0)

    def test_negative_db_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="db"):
            RedisStateSyncConfig(db=-1)

    def test_empty_host_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="host"):
            RedisStateSyncConfig(host="")

    def test_empty_channel_prefix_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="channel_prefix"):
            RedisStateSyncConfig(channel_prefix="")

    def test_frozen(self):
        from pydantic import ValidationError
        cfg = RedisStateSyncConfig()
        with pytest.raises(ValidationError):
            cfg.host = "other"  # type: ignore[misc]

    def test_extra_fields_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="extra"):
            RedisStateSyncConfig(bogus="x")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# connect() / _require_client()
# ---------------------------------------------------------------------------


class TestConnect:
    """Tests for connection establishment and error handling."""

    def test_require_client_raises_before_connect(self):
        s = JobBuilderStateSync(
            config=_CFG,
            namespace="ns",
            builder_name="b",
        )
        with pytest.raises(RuntimeError, match="connect\\(\\)"):
            s._require_client()

    def test_require_pubsub_raises_before_connect(self):
        s = JobBuilderStateSync(
            config=_CFG,
            namespace="ns",
            builder_name="b",
        )
        with pytest.raises(RuntimeError, match="connect\\(\\)"):
            s._require_pubsub()

    def test_connect_unreachable_raises(self):
        cfg = RedisStateSyncConfig(host="192.0.2.1", port=9999)  # TEST-NET, unreachable
        s = JobBuilderStateSync(config=cfg, namespace="ns", builder_name="b")
        with pytest.raises(StateSyncConnectionError):
            s.connect()

    def test_connected_client_is_reachable(self):
        s = _sync()
        assert s._require_client() is not None


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------


class TestKeyHelpers:
    """Tests for Redis key construction methods."""

    def test_channel(self):
        s = _sync("my-builder")
        assert s._channel == "lazylemon:test-ns:my-builder:state_changes"

    def test_hash_key(self):
        s = _sync("my-builder")
        assert s._hash_key("grp") == "lazylemon:test-ns:my-builder:grp:jobs"

    def test_claim_key(self):
        s = _sync("my-builder")
        assert s._claim_key("job-99") == "lazylemon:test-ns:my-builder:job-99:claimed"

    def test_custom_prefix(self):
        cfg = RedisStateSyncConfig(channel_prefix="prod")
        s = JobBuilderStateSync(config=cfg, namespace="ns", builder_name="b")
        server = fakeredis.FakeServer()
        s._client = fakeredis.FakeRedis(server=server, decode_responses=True)
        s._pubsub = s._client.pubsub(ignore_subscribe_messages=True)
        assert s._channel.startswith("prod:")


# ---------------------------------------------------------------------------
# push_job_update / push_job_deletion
# ---------------------------------------------------------------------------


class TestPushOperations:
    """Tests for pushing state mutations to Redis."""

    def test_push_job_update_stores_in_hash(self):
        s = _sync()
        job = _make_job("j1")
        s.push_job_update("grp", "j1", job)
        stored = s._client.hget(s._hash_key("grp"), "j1")
        assert stored is not None
        recovered = Job.from_string(stored)
        assert recovered.identifier == "j1"

    def test_push_job_deletion_removes_from_hash(self):
        s = _sync()
        job = _make_job("j1")
        s._client.hset(s._hash_key("grp"), "j1", str(job))
        s.push_job_deletion("grp", "j1")
        assert s._client.hget(s._hash_key("grp"), "j1") is None

    def test_push_update_publishes_notification(self):
        from unittest.mock import patch

        s = _sync()
        with patch.object(s._client, "publish") as mock_pub:
            s.push_job_update("grp", "j1", _make_job("j1"))
        mock_pub.assert_called_once()
        channel, raw_msg = mock_pub.call_args[0]
        assert channel == s._channel
        payload = json.loads(raw_msg)
        assert payload["event"] == "job_updated"
        assert payload["job_id"] == "j1"
        assert payload["group"] == "grp"

    def test_push_deletion_publishes_notification(self):
        from unittest.mock import patch

        s = _sync()
        with patch.object(s._client, "publish") as mock_pub:
            s.push_job_deletion("grp", "j1")
        mock_pub.assert_called_once()
        channel, raw_msg = mock_pub.call_args[0]
        assert channel == s._channel
        payload = json.loads(raw_msg)
        assert payload["event"] == "job_deleted"
        assert payload["job_id"] == "j1"


# ---------------------------------------------------------------------------
# try_claim_emit
# ---------------------------------------------------------------------------


class TestTryClaimEmit:
    """Tests for atomic emit claim via Redis SETNX."""

    def test_first_claim_succeeds(self):
        s = _sync()
        assert s.try_claim_emit("job-x", ttl=60.0) is True

    def test_second_claim_fails(self):
        s = _sync()
        s.try_claim_emit("job-x", ttl=60.0)
        assert s.try_claim_emit("job-x", ttl=60.0) is False

    def test_different_jobs_both_succeed(self):
        s = _sync()
        assert s.try_claim_emit("job-a", ttl=60.0) is True
        assert s.try_claim_emit("job-b", ttl=60.0) is True

    def test_two_instances_only_one_claims(self):
        """Two JobBuilderStateSync sharing a fake server — only one claims."""
        server = fakeredis.FakeServer()
        def make_instance() -> JobBuilderStateSync:
            inst = JobBuilderStateSync(
                config=_CFG,
                namespace="test-ns",
                builder_name="shared-builder",
            )
            inst._client = fakeredis.FakeRedis(server=server, decode_responses=True)
            inst._pubsub = inst._client.pubsub(ignore_subscribe_messages=True)
            return inst

        a, b = make_instance(), make_instance()
        results = [a.try_claim_emit("job-1", 60.0), b.try_claim_emit("job-1", 60.0)]
        assert sorted(results) == [False, True]

    def test_ttl_minimum_clamped_to_one(self):
        s = _sync()
        # Should not raise even with sub-second ttl
        assert s.try_claim_emit("job-z", ttl=0.1) is True


# ---------------------------------------------------------------------------
# load_remote_state / _merge_job
# ---------------------------------------------------------------------------


class TestLoadRemoteState:
    """Tests for startup state hydration from Redis."""

    def test_loads_job_from_redis_into_group(self):
        s = _sync()
        jg = _make_group("g1")
        job = _make_job("j1")
        s._client.hset(s._hash_key("g1"), "j1", str(job))
        s._job_groups = [jg]
        s._group_locks = {"g1": threading.Lock()}
        s.load_remote_state()
        assert "j1" in jg.jobs
        assert jg.jobs["j1"].identifier == "j1"

    def test_last_write_wins_on_conflict(self):
        s = _sync()
        jg = _make_group("g1")
        old_job = _make_job("j1", last_modified=100.0)
        new_job = _make_job("j1", last_modified=200.0)
        jg.jobs["j1"] = old_job
        s._client.hset(s._hash_key("g1"), "j1", str(new_job))
        s._job_groups = [jg]
        s._group_locks = {"g1": threading.Lock()}
        s.load_remote_state()
        assert jg.jobs["j1"].last_modified == 200.0

    def test_local_newer_than_remote_kept(self):
        s = _sync()
        jg = _make_group("g1")
        local_job = _make_job("j1", last_modified=300.0)
        older_remote = _make_job("j1", last_modified=100.0)
        jg.jobs["j1"] = local_job
        s._client.hset(s._hash_key("g1"), "j1", str(older_remote))
        s._job_groups = [jg]
        s._group_locks = {"g1": threading.Lock()}
        s.load_remote_state()
        assert jg.jobs["j1"].last_modified == 300.0  # local kept

    def test_corrupt_job_json_skipped(self):
        s = _sync()
        jg = _make_group("g1")
        s._client.hset(s._hash_key("g1"), "bad", "not-json")
        s._job_groups = [jg]
        s._group_locks = {"g1": threading.Lock()}
        s.load_remote_state()  # should not raise
        assert "bad" not in jg.jobs

    def test_empty_redis_leaves_group_empty(self):
        s = _sync()
        jg = _make_group("g1")
        s._job_groups = [jg]
        s._group_locks = {"g1": threading.Lock()}
        s.load_remote_state()
        assert jg.jobs == {}


# ---------------------------------------------------------------------------
# Subscriber thread (_handle_message / _apply_event)
# ---------------------------------------------------------------------------


class TestSubscriberThread:
    """Tests for applying pub/sub notifications to local state."""

    def test_job_updated_message_merges_into_group(self):
        s = _sync()
        jg = _make_group("g1")
        job = _make_job("j2")
        s._client.hset(s._hash_key("g1"), "j2", str(job))
        s._job_groups = [jg]
        s._group_locks = {"g1": threading.Lock()}
        msg = {
            "type": "message",
            "data": json.dumps(
                {"event": "job_updated", "group": "g1", "job_id": "j2"},
            ),
        }
        s._handle_message(msg)
        assert "j2" in jg.jobs

    def test_job_deleted_message_removes_from_group(self):
        s = _sync()
        jg = _make_group("g1")
        jg.jobs["j3"] = _make_job("j3")
        s._job_groups = [jg]
        s._group_locks = {"g1": threading.Lock()}
        msg = {
            "type": "message",
            "data": json.dumps(
                {"event": "job_deleted", "group": "g1", "job_id": "j3"},
            ),
        }
        s._handle_message(msg)
        assert "j3" not in jg.jobs

    def test_unknown_group_ignored(self):
        s = _sync()
        jg = _make_group("g1")
        s._job_groups = [jg]
        s._group_locks = {}
        msg = {
            "type": "message",
            "data": json.dumps(
                {"event": "job_updated", "group": "no-such-group", "job_id": "j9"},
            ),
        }
        s._handle_message(msg)  # should not raise

    def test_non_message_type_ignored(self):
        s = _sync()
        s._job_groups = [_make_group()]
        # subscribe/unsubscribe notifications have type != "message"
        s._handle_message({"type": "subscribe", "data": 1})  # should not raise

    def test_malformed_json_ignored(self):
        s = _sync()
        s._job_groups = [_make_group()]
        s._handle_message({"type": "message", "data": "{{bad json"})

    def test_subscriber_thread_starts_and_stops(self):
        s = _sync()
        jg = _make_group("g1")
        lock = threading.Lock()
        s.start([jg], {"g1": lock})
        assert s._subscriber_thread is not None
        assert s._subscriber_thread.is_alive()
        s.stop()
        assert not s._subscriber_thread.is_alive()


# ---------------------------------------------------------------------------
# Thread safety: concurrent file-processing and subscriber
# ---------------------------------------------------------------------------


class TestThreadSafety:
    """Verify that group locks prevent data races."""

    def test_lock_acquired_during_merge(self):
        """Subscriber must wait for the lock to be released."""
        s = _sync()
        jg = _make_group("g1")
        job = _make_job("j4")
        s._client.hset(s._hash_key("g1"), "j4", str(job))
        s._job_groups = [jg]
        lock = threading.Lock()
        s._group_locks = {"g1": lock}

        order: list[str] = []

        def hold_lock():
            with lock:
                order.append("locked")
                time.sleep(0.1)
                order.append("released")

        holder = threading.Thread(target=hold_lock)
        holder.start()
        time.sleep(0.02)  # ensure holder has the lock first
        # _load_group will block until lock is free
        s._load_group(jg)
        order.append("merged")
        holder.join()
        assert order == ["locked", "released", "merged"]
