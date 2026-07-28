"""Every plugin-scoped metric must be attributable to a single instance.

Without an identifier label, two instances of the same plugin class collapse
into one time series and an operator cannot tell which one is failing.

These tests drive each metric and read the *exported sample* back, rather than
asserting against ``metric._labelnames``. The previous version made 26
hand-written assertions about that private attribute: it verified the label was
declared, not that a scrape can distinguish two instances — and a hand-written
list silently omits any metric added later.
"""

from __future__ import annotations

import pytest
from prometheus_client import REGISTRY
from prometheus_client.metrics import MetricWrapperBase

from courier import metrics

# Metric-name prefix -> the label that must distinguish two instances.
_IDENTIFIER_LABEL_BY_PREFIX = {
    "courier_data_monitor_": "monitor_identifier",
    "courier_rabbitmq_": "monitor_identifier",
    "courier_job_builder_": "job_builder_identifier",
    "courier_dispatcher_": "dispatcher_identifier",
    "courier_plugin_": "plugin_identifier",
}

# Not plugin-scoped: the unit is a queue, a builder name, or the service.
_NOT_PLUGIN_SCOPED = (
    "courier_service_",
    "courier_broker_",
    "courier_state_sync_",
    "courier_custom_gauge",
)


def _plugin_scoped_metrics() -> list[tuple[str, MetricWrapperBase, str]]:
    """Return ``(name, metric, expected_identifier_label)`` for each metric.

    Discovered from the module so a newly added plugin metric is covered
    automatically rather than quietly escaping the check.
    """
    found: list[tuple[str, MetricWrapperBase, str]] = []
    for value in vars(metrics).values():
        if not isinstance(value, MetricWrapperBase):
            continue
        name = value._name
        if name.startswith(_NOT_PLUGIN_SCOPED):
            continue
        for prefix, label in _IDENTIFIER_LABEL_BY_PREFIX.items():
            if name.startswith(prefix):
                found.append((name, value, label))
                break
    return found


_METRICS = _plugin_scoped_metrics()
_IDS = [name for name, _, _ in _METRICS]


def _sample_suffix(metric: MetricWrapperBase) -> str:
    return {"counter": "_total", "histogram": "_count"}.get(metric._type, "")


def _drive(metric: MetricWrapperBase, labels: dict[str, str]) -> None:
    """Record one observation, whatever kind of metric this is."""
    child = metric.labels(**labels)
    if metric._type == "counter":
        child.inc()
    elif metric._type == "gauge":
        child.set(1)
    elif metric._type == "histogram":
        child.observe(1.0)
    else:  # pragma: no cover - no other metric types are defined
        pytest.fail(f"unhandled metric type {metric._type!r}")


def test_the_metric_scan_found_something() -> None:
    """Guard the guard: a broken scan would silently test nothing at all."""
    assert len(_METRICS) > 15, f"only discovered {len(_METRICS)} plugin metrics"


@pytest.mark.parametrize(("name", "metric", "identifier_label"), _METRICS, ids=_IDS)
def test_metric_declares_an_instance_identifier(
    name: str,
    metric: MetricWrapperBase,
    identifier_label: str,
) -> None:
    """A plugin metric without an identifier cannot be attributed."""
    assert identifier_label in metric._labelnames, (
        f"{name} has no {identifier_label}; two instances of the same plugin "
        f"would collapse into one time series"
    )


@pytest.mark.parametrize(("name", "metric", "identifier_label"), _METRICS, ids=_IDS)
def test_two_instances_produce_separate_scrapeable_series(
    name: str,
    metric: MetricWrapperBase,
    identifier_label: str,
) -> None:
    """Driving one metric from two instances must yield two distinct samples.

    This is the property that matters when triaging: splitting by identifier
    has to show *which* instance is misbehaving.
    """
    labels_a = {label: f"{label}-a" for label in metric._labelnames}
    labels_b = {**labels_a, identifier_label: f"{identifier_label}-b"}

    _drive(metric, labels_a)
    _drive(metric, labels_b)

    suffix = _sample_suffix(metric)
    sample_a = REGISTRY.get_sample_value(name + suffix, labels_a)
    sample_b = REGISTRY.get_sample_value(name + suffix, labels_b)

    assert sample_a is not None, f"{name} exported no sample for instance A"
    assert sample_b is not None, f"{name} exported no sample for instance B"


@pytest.mark.parametrize(("name", "metric", "identifier_label"), _METRICS, ids=_IDS)
def test_identifier_value_reaches_the_exported_sample(
    name: str,
    metric: MetricWrapperBase,
    identifier_label: str,
) -> None:
    """The identifier must appear on the scraped sample, not merely be declared."""
    labels = {label: f"{label}-probe" for label in metric._labelnames}
    _drive(metric, labels)

    suffix = _sample_suffix(metric)
    matching = [
        sample
        for family in metric.collect()
        for sample in family.samples
        if sample.name == name + suffix
        and sample.labels.get(identifier_label) == f"{identifier_label}-probe"
    ]
    assert matching, (
        f"{name} exported no sample carrying "
        f"{identifier_label}={identifier_label}-probe"
    )
