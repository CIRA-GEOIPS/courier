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


def test_removed_dispatchers_leave_no_panels_or_variables(dashboard_json: str) -> None:
    assert "status_code" not in dashboard_json
    assert "HTTP" not in dashboard_json
    assert "parallel_workers" not in dashboard_json


def test_payload_metrics_are_charted(dashboard_json: str) -> None:
    assert "courier_payload_jobs_processed_total" in dashboard_json
    assert "courier_payload_job_execution_duration_seconds_bucket" in dashboard_json
    assert '"name": "payload_name"' in dashboard_json
