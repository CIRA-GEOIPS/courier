"""The example Grafana dashboard must chart only metrics courier still exports.

``examples/grafana/courier_dashboard.py`` is a standalone script, so nothing
imported it: when http_dispatcher and parallel_bash were removed it kept an
HTTP row, an HTTP status-code variable and a parallel-workers panel, all over
metrics that no longer existed, and every one rendered permanently empty.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from types import ModuleType

import pytest

pytest.importorskip("grafanalib")

from grafanalib._gen import DashboardEncoder  # noqa: E402
from prometheus_client import REGISTRY  # noqa: E402

import courier.metrics  # noqa: E402, F401 -- registers every metric

_SCRIPT = Path(__file__).resolve().parents[3] / "examples/grafana/courier_dashboard.py"
_METRIC = re.compile(r"courier_[a-z][a-z0-9_]*")
_SUFFIXES = ("_total", "_created", "_sum", "_count", "_bucket")


def _load_example() -> ModuleType:
    spec = importlib.util.spec_from_file_location("courier_dashboard_example", _SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def dashboard_json() -> str:
    module = _load_example()
    return json.dumps(module.build_dashboard().to_json_data(), cls=DashboardEncoder)


def _registered_names() -> set[str]:
    names: set[str] = set()
    for family_names in REGISTRY._collector_to_names.values():
        for name in family_names:
            names.add(name)
            names.update(f"{name}{suffix}" for suffix in _SUFFIXES)
    return names


def test_every_charted_metric_is_registered(dashboard_json: str) -> None:
    charted = set(_METRIC.findall(dashboard_json))
    assert charted, "no courier metrics found in the example dashboard"

    unknown = sorted(name for name in charted if name not in _registered_names())

    assert not unknown, f"example dashboard charts removed metrics: {unknown}"


def _panels(panels: list[dict]) -> list[dict]:
    """Return *panels* and every panel nested in a row, depth first."""
    found: list[dict] = []
    for panel in panels:
        found.append(panel)
        found.extend(_panels(panel.get("panels", [])))
    return found


def test_ratio_gauges_use_a_fractional_scale(dashboard_json: str) -> None:
    """A ``percentunit`` gauge shows a 0-1 ratio, so its range and thresholds are 0-1.

    The success-ratio gauge used ``percent`` with ``max=100`` over a 0-1
    query: the needle sat near zero and the gauge stayed red at 100% success.
    """
    gauges = [
        panel
        for panel in _panels(json.loads(dashboard_json)["panels"])
        if panel.get("type") == "gauge"
        and panel["fieldConfig"]["defaults"].get("unit") == "percentunit"
    ]
    assert gauges, "no ratio gauges found in the example dashboard"

    for gauge in gauges:
        defaults = gauge["fieldConfig"]["defaults"]
        steps = [s["value"] for s in defaults["thresholds"]["steps"]]
        assert defaults["max"] == 1, gauge["title"]
        assert all(v is None or v == "null" or 0 <= v <= 1 for v in steps), (
            gauge["title"],
            steps,
        )
