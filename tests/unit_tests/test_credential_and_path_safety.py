"""Regression tests for credential handling and untrusted-string path safety.

Each test here pins a failure mode that was silent in production: a secret
written somewhere it should not be, or an operator-controlled string used
directly as a filesystem path.
"""

from __future__ import annotations

from pathlib import Path

import kombu
import pytest

from courier.broker.kombu import redact_broker_url
from courier.dashboard.topology import _build_config_summary, _is_secret_key
from courier.plugins.classes.dispatchers.serial_bash import _ingest_courier_metrics
from courier.schema.v1alpha1.broker_config import AmqpBrokerConfig, RedisBrokerConfig
from courier.utils.functional import slugify_for_filename

# ── Broker URL construction ────────────────────────────────────────────────

# Reserved characters that appear routinely in generated secrets.
_AWKWARD_PASSWORDS = ["s3cret", "p@ssw0rd", "a/b", "x#y", "u:v", "sl/ash#h@sh"]


@pytest.mark.parametrize("password", _AWKWARD_PASSWORDS)
def test_amqp_url_round_trips_reserved_characters(password: str) -> None:
    """A password containing URL-reserved characters must survive parsing.

    Unescaped ``/`` or ``#`` used to make Kombu read part of the secret as a
    host and port, failing at startup with ``Port could not be cast to
    integer`` -- an error that points at the wrong setting entirely.
    """
    config = AmqpBrokerConfig(
        host="rabbit.internal",
        username="cour/ier",
        password=password,
    )
    conn = kombu.Connection(config.to_url())

    assert conn.hostname == "rabbit.internal"
    assert conn.userid == "cour/ier"
    assert conn.password == password


@pytest.mark.parametrize("password", _AWKWARD_PASSWORDS)
def test_redis_url_round_trips_reserved_characters(password: str) -> None:
    """Same guarantee for the Redis broker config."""
    config = RedisBrokerConfig(host="cache.internal", password=password)
    assert kombu.Connection(config.to_url()).password == password


# ── Credential redaction in logs ───────────────────────────────────────────


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "amqp://admin:hunter2@rabbit:5672/vhost",
            "amqp://admin:***@rabbit:5672/vhost",
        ),
        ("redis://:topsecret@cache:6379/0", "redis://:***@cache:6379/0"),
        ("memory://", "memory://"),
        ("amqp://rabbit:5672/", "amqp://rabbit:5672/"),
    ],
)
def test_redact_broker_url(url: str, expected: str) -> None:
    """Broker URLs are logged at DEBUG, the default level -- redact the secret."""
    assert redact_broker_url(url) == expected


@pytest.mark.parametrize(
    "garbage",
    [
        "http://[bad",  # unclosed IPv6 bracket -- urlsplit raises ValueError
        "://[not-a-url",
        "not a url at all",
        "",
    ],
)
def test_redact_broker_url_survives_garbage(garbage: str) -> None:
    """Redaction must never raise: it runs on a logging path.

    A malformed URL should degrade to a placeholder or pass through unchanged,
    but must not turn a debug log line into an exception.
    """
    result = redact_broker_url(garbage)
    assert isinstance(result, str)


def test_redact_broker_url_flags_unparseable_urls() -> None:
    """The ValueError branch returns a placeholder rather than leaking input."""
    assert redact_broker_url("http://[bad") == "<unparseable broker url>"


# ── Dashboard secret redaction ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "key",
    [
        "password",
        "rabbitmq_password",
        "aws_secret_access_key",
        "token",
        "private_key_path",
        "private_key_passphrase",
        "sasl_plain_password",
        "api_key",
    ],
)
def test_secret_config_keys_are_recognised(key: str) -> None:
    """Every credential field the shipped plugins accept must be detected."""
    assert _is_secret_key(key) is True


@pytest.mark.parametrize("key", ["host", "bucket", "files_per_job", "url", "region"])
def test_non_secret_config_keys_are_not_redacted(key: str) -> None:
    """Redaction must not swallow the values operators need to see."""
    assert _is_secret_key(key) is False


def test_config_summary_redacts_secrets_but_keeps_context() -> None:
    """Generated dashboard JSON is committed and widely readable."""
    summary = _build_config_summary(
        {"host": "sftp.example.gov", "password": "hunter2", "username": "courier"},
    )
    assert "hunter2" not in summary
    assert "sftp.example.gov" in summary
    assert "courier" in summary


# ── Untrusted strings used as filesystem paths ─────────────────────────────


@pytest.mark.parametrize(
    "identifier",
    [
        "/data/goes18/OR_ABI.nc",  # the default JobGroup bucket ID
        "../../etc/passwd",
        "..",
        "/",
        "a b/c",
        "goes18-abi-2026-07-27T12:00",
    ],
)
def test_slugify_produces_a_single_safe_segment(identifier: str) -> None:
    """Job identifiers reach log/script filenames; they must not escape the dir."""
    slug = slugify_for_filename(identifier)

    assert slug, "slug must never be empty"
    assert "/" not in slug
    assert ".." not in slug

    log_dir = Path("/var/log/courier")
    path = log_dir / f"dispatch_{slug}_ts.log"
    assert path.parent == log_dir


def test_slugify_keeps_distinct_identifiers_distinct() -> None:
    """Stripping characters must not merge two different identifiers."""
    assert slugify_for_filename("/a/b") != slugify_for_filename("/a-b")


def test_slugify_leaves_already_safe_values_untouched() -> None:
    """A clean identifier should stay readable, with no hash suffix."""
    assert slugify_for_filename("goes18-abi-full_disk.v2") == "goes18-abi-full_disk.v2"


# ── Poison-pill protection on the stdout metric conduit ────────────────────


@pytest.mark.parametrize(
    "line",
    [
        "COURIER_METRIC: version 1.2.3",
        "COURIER_METRIC: rate 5--",
        "COURIER_METRIC: x 1e",
        "COURIER_METRIC: y +-",
    ],
)
def test_malformed_metric_line_does_not_raise(line: str) -> None:
    """A bad metric line must not escape as ValueError.

    ``_ingest_courier_metrics`` runs inside ``get_execution_log``; a raised
    ValueError is not a CourierError, so the dispatcher loop does not catch it
    and the process exits via ``os._exit(1)`` with the message unacked --
    which then kills the service again on redelivery.
    """
    _ingest_courier_metrics(line, "disp-poison")


def test_wellformed_metric_line_is_still_recorded() -> None:
    """Hardening must not stop valid metrics from being ingested."""
    from courier.metrics import COURIER_CUSTOM_GAUGE

    _ingest_courier_metrics("COURIER_METRIC: files_written 42", "disp-ok")
    gauge = COURIER_CUSTOM_GAUGE.labels(
        dispatcher_identifier="disp-ok",
        metric_name="files_written",
    )
    assert gauge._value.get() == 42.0
