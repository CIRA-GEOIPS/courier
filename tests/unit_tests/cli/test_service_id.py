"""Which identity a service reports in its logs, traces and metrics.

The CLI used to overwrite the identifier with the config's metadata name
unconditionally, so ``SERVICE_ID`` was dead and every replica of one YAML
document reported the same identity. Telling replicas apart is the whole
point of the variable, and replicas are routine now that a builder can be
scaled at runtime.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from courier.cli.run import _resolve_service_id


def _config(service_id: str, metadata_name: str = "shipping-service") -> SimpleNamespace:
    """Return a stand-in config carrying only the fields under test.

    Parameters
    ----------
    service_id : str
        Value sitting on the service config block.
    metadata_name : str, optional
        Document's metadata name.

    Returns
    -------
    SimpleNamespace
        Object shaped like the validated config model.
    """
    return SimpleNamespace(
        spec=SimpleNamespace(service_config=SimpleNamespace(service_id=service_id)),
        metadata=SimpleNamespace(name=metadata_name),
    )


def test_configured_identifier_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """An identifier written in the YAML outranks everything else."""
    monkeypatch.setenv("SERVICE_ID", "from-the-environment")

    assert _resolve_service_id(_config("from-the-yaml")) == "from-the-yaml"


def test_the_environment_beats_the_metadata_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The variable is honoured again.

    Reverted check: pass ``config.metadata.name`` straight through, as the
    CLI used to. This returns the document name and the assertion fails.
    """
    monkeypatch.setenv("SERVICE_ID", "replica-2")

    assert _resolve_service_id(_config("")) == "replica-2"


def test_the_metadata_name_is_the_last_resort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With nothing configured the document still names the service."""
    monkeypatch.delenv("SERVICE_ID", raising=False)

    assert _resolve_service_id(_config("")) == "shipping-service"


def test_the_generated_placeholder_does_not_outrank_the_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A generated default is not a deliberate choice.

    The service config's own default mints ``watcher-service-<random>`` when
    the variable is unset, so treating any non-empty value as configured
    would make the document's name unreachable -- and would give every
    restart of one replica a different identity.
    """
    monkeypatch.delenv("SERVICE_ID", raising=False)

    resolved = _resolve_service_id(_config("watcher-service-0a1b2c3d"))

    assert resolved == "shipping-service"


def test_the_placeholder_yields_to_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A variable set after the config was built is still picked up.

    The service config's default is evaluated once, at import, so a value
    exported later never reaches it. Reading the environment here is what
    makes the variable work in a container that sets it at start-up.
    """
    monkeypatch.setenv("SERVICE_ID", "replica-7")

    resolved = _resolve_service_id(_config("watcher-service-0a1b2c3d"))

    assert resolved == "replica-7"


def test_an_identifier_that_merely_looks_generated_is_honoured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``watcher-service-prod`` is a deliberate choice, not a placeholder.

    The placeholder check used to be a bare prefix test, so any identifier an
    operator wrote beginning ``watcher-service-`` was silently discarded in
    favour of the document name. Only the generated shape -- the prefix plus
    eight hex characters -- may be overridden.
    """
    monkeypatch.delenv("SERVICE_ID", raising=False)

    assert _resolve_service_id(_config("watcher-service-prod")) == (
        "watcher-service-prod"
    )
    assert _resolve_service_id(_config("watcher-service-eu-west-1")) == (
        "watcher-service-eu-west-1"
    )
