"""Drift guards: shipped artefacts must stay consistent with the code.

Courier ships YAML that operators copy — the root ``config.yaml``, the example
configs under ``tests/``, and the satellite metadata configs inside the
package. None of it is exercised by importing the library, so a schema change
or a renamed plugin breaks them silently and the failure only surfaces when
someone runs ``courier run`` in anger.

Each test here pins a contract *between* two artefacts rather than the shape of
either one.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from courier.cli.config_loader import load_config
from courier.cli.plugins import PLUGIN_REGISTRIES, normalize_kind
from courier.errors import ConfigurationError

_REPO_ROOT = Path(__file__).resolve().parent.parent

_SHIPPED_CONFIGS = sorted(
    [_REPO_ROOT / "config.yaml", *(_REPO_ROOT / "tests").glob("*.yaml")],
)

# Not service configs: broker fixtures and compose files live here too.
_NOT_SERVICE_CONFIGS = {"rabbitmq.conf", "docker-compose.rabbitmq-testing.yaml"}

_SERVICE_CONFIGS = [
    path for path in _SHIPPED_CONFIGS if path.name not in _NOT_SERVICE_CONFIGS
]
_IDS = [str(path.relative_to(_REPO_ROOT)) for path in _SERVICE_CONFIGS]


def test_shipped_configs_were_discovered() -> None:
    """Guard the guard: an empty glob would make every test below vacuous."""
    assert len(_SERVICE_CONFIGS) >= 4, f"only found {_IDS}"


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_shipped_config_validates(config_path: Path) -> None:
    """Every YAML we ship must pass the same validation ``courier run`` uses."""
    try:
        load_config(config_path)
    except ConfigurationError as exc:
        pytest.fail(f"{config_path.name} no longer validates:\n{exc}")


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_shipped_config_references_real_plugins(config_path: Path) -> None:
    """A renamed plugin must not leave a shipped config pointing at nothing.

    ``run_service`` skips unknown kinds silently, so a stale plugin name here
    produces a service that starts up and does nothing at all.
    """
    config = load_config(config_path)
    missing: list[str] = []

    for entry in config.spec.run:
        registry = PLUGIN_REGISTRIES.get(normalize_kind(entry.spec.kind))
        if registry is None:
            missing.append(f"{entry.identifier}: unknown kind {entry.spec.kind!r}")
            continue
        available = {plugin.name for plugin in registry.get_plugins()}
        if entry.spec.name not in available:
            missing.append(
                f"{entry.identifier}: {entry.spec.kind}/{entry.spec.name!r} "
                f"not registered (have {sorted(available)})",
            )

    assert not missing, "\n".join(missing)


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_shipped_config_heartbeat_is_plausible_in_seconds(
    config_path: Path,
) -> None:
    """``heartbeat_interval`` is seconds, and was documented as milliseconds.

    Every shipped config once said ``1000 # in milliseconds``, which meant the
    service published health metrics every ~17 minutes. Anything above a few
    minutes is almost certainly the same unit confusion returning.
    """
    interval = load_config(config_path).spec.heartbeat_interval
    assert 0 < interval <= 300, (
        f"{config_path.name}: heartbeat_interval={interval}s is implausible; "
        f"the field is seconds, not milliseconds"
    )


#: Filter keys removed in 0.2.0, mapped to their canonical replacements.
#: A filter using one of these silently matches nothing -- the lookup falls
#: through both the metadata dict and the File attributes and returns False.
_REMOVED_FILTER_KEYS = {
    "platform": "source",
    "sensor": "instrument",
    "level": "processing_stage",
    "sector": "domain",
}


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_shipped_config_avoids_removed_filter_keys(config_path: Path) -> None:
    """Legacy filter keys were removed; a config using one matches nothing.

    Note this checks ``filters`` only. ``field_map`` legitimately uses
    ``platform`` and ``sensor`` as *canonical* names on its left-hand side.
    """
    problems: list[str] = []

    for entry in load_config(config_path).spec.run:
        config = entry.spec.config or {}
        if not isinstance(config, dict):
            continue
        filter_blocks = [config.get("filters") or {}]
        filter_blocks += [
            route.get("filters") or {}
            for route in (config.get("routes") or [])
            if isinstance(route, dict)
        ]
        for block in filter_blocks:
            for key in block:
                if key in _REMOVED_FILTER_KEYS:
                    problems.append(
                        f"{entry.identifier}: filters on removed key {key!r}; "
                        f"use {_REMOVED_FILTER_KEYS[key]!r}",
                    )

    assert not problems, "\n".join(problems)


@pytest.mark.parametrize("config_path", _SERVICE_CONFIGS, ids=_IDS)
def test_shipped_config_filters_reference_real_file_fields(
    config_path: Path,
) -> None:
    """Filter keys must name a ``File`` attribute or be free-form metadata.

    Catches the class of mistake where a config filters on ``instrument:
    goes18`` — a platform value in a sensor field — which only ever matched
    because of a bug in the monitor.
    """
    from courier.types.file import File

    file_attrs = set(File().__dict__)
    platform_like = {"goes16", "goes17", "goes18", "goes19", "himawari9", "gk2a"}
    problems: list[str] = []

    for entry in load_config(config_path).spec.run:
        config = entry.spec.config or {}
        if not isinstance(config, dict):
            continue
        for key, value in (config.get("filters") or {}).items():
            if key not in file_attrs:
                continue  # metadata-dict key, checked at runtime
            if key == "instrument" and value in platform_like:
                problems.append(
                    f"{entry.identifier}: filters on instrument={value!r}, "
                    f"which is a platform — did you mean source?",
                )

    assert not problems, "\n".join(problems)
