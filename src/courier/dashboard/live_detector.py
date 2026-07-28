"""Live plugin detection for the Courier dashboard.

Queries a running Courier instance's Prometheus ``/metrics`` endpoint
and auto-detects which plugins are active so the dashboard can render
only the relevant panels for the current node.
"""

from __future__ import annotations

import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from courier.constants import PluginRunState

# ---------------------------------------------------------------------------
# Prometheus metric name we watch
# ---------------------------------------------------------------------------

_PLUGIN_STATE_METRIC: str = "courier_plugin_state"

_HTTP_OK: int = 200

# ---------------------------------------------------------------------------
# Active plugin states — built from the PluginRunState enum so the mapping
# stays correct even if the enum integer values change.
# ---------------------------------------------------------------------------

_ACTIVE_STATES: frozenset[int] = frozenset(
    {
        PluginRunState.STARTING.value,
        PluginRunState.RUNNING.value,
        PluginRunState.RESTARTING.value,
    },
)

# ---------------------------------------------------------------------------
# Regex helpers for parsing Prometheus exposition format
# ---------------------------------------------------------------------------

# Matches a single data line:  metric_name{labels="..."} value
# Group 1: metric name
# Group 2: label string (everything inside braces)
# Group 3: numeric value (float-compatible)
_METRIC_LINE_RE = re.compile(r"^(\w+)\{([^}]*)\}\s+([-+]?[\d.eE]+)")

# Extracts individual label=value pairs from the label string
_LABEL_RE = re.compile(r'(\w+)="([^"]*)"')


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass
class LiveDetectionResult:
    """Result of live plugin detection from a running Courier instance.

    Attributes
    ----------
    identifiers : set[str]
        Plugin identifiers that are currently active (STARTING, RUNNING,
        or RESTARTING).
    plugin_states : dict[str, int]
        Every discovered plugin name mapped to its ``PluginRunState``
        enum integer value.
    is_reachable : bool
        Whether the metrics endpoint responded successfully.
    error_message : str or None
        Human-readable error description when *is_reachable* is ``False``.
    raw_metrics : dict or None
        Full parsed Prometheus metrics, keyed by metric name.  Each value
        is a list of ``{"labels": dict, "value": float}`` entries.  Set
        to ``None`` when the endpoint was unreachable.
    """

    identifiers: set[str] = field(default_factory=set)
    plugin_states: dict[str, int] = field(default_factory=dict)
    is_reachable: bool = False
    error_message: str | None = None
    raw_metrics: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_active_plugins(
    host: str = "localhost",
    port: int = 8000,
    *,
    timeout: float = 5.0,
) -> LiveDetectionResult:
    """Query a running Courier instance and return active plugin identifiers.

    Fetches Prometheus metrics from ``http://{host}:{port}/metrics``,
    parses the ``courier_plugin_state`` metric, and maps the discovered
    ``plugin_name`` labels back to identifiers.

    Parameters
    ----------
    host : str
        Prometheus hostname (default: ``"localhost"``).
    port : int
        Prometheus metrics port (default: ``8000``, matches
        ``ServiceConfig`` default).
    timeout : float
        HTTP request timeout in seconds.

    Returns
    -------
    LiveDetectionResult
        Detection result with active identifiers and raw state data.
        Never raises — all errors are captured in the result object.
    """
    url = f"http://{host}:{port}/metrics"

    # ------------------------------------------------------------------
    # Law 1 (Early Exit): handle all fetch failures at the boundary.
    # ------------------------------------------------------------------
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310
            if response.status != _HTTP_OK:
                return LiveDetectionResult(
                    error_message=(
                        f"Metrics endpoint returned HTTP {response.status}"
                    ),
                )
            raw_text = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return LiveDetectionResult(
            error_message=f"HTTP {exc.code}: {exc.reason}",
        )
    except urllib.error.URLError as exc:
        return LiveDetectionResult(
            error_message=f"Connection failed: {exc.reason}",
        )
    except OSError as exc:
        return LiveDetectionResult(
            error_message=f"Connection failed: {exc}",
        )

    # ------------------------------------------------------------------
    # Law 2 (Parse, Don't Validate): convert raw text to structured data
    # at the boundary so downstream code trusts the types.
    # ------------------------------------------------------------------
    raw_metrics = _parse_prometheus_text(raw_text)
    plugin_states = _extract_plugin_states(raw_text)

    # ------------------------------------------------------------------
    # Determine active identifiers — those whose state is in _ACTIVE_STATES.
    # ------------------------------------------------------------------
    identifiers = {
        name for name, state in plugin_states.items()
        if state in _ACTIVE_STATES
    }

    return LiveDetectionResult(
        identifiers=identifiers,
        plugin_states=plugin_states,
        is_reachable=True,
        raw_metrics=raw_metrics,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_prometheus_text(text: str) -> dict[str, list[dict[str, Any]]]:
    """Parse Prometheus exposition-format text into a structured dict.

    Handles only the data lines (metric lines with labels and values).
    Comment lines (``# HELP``, ``# TYPE``) are ignored.

    Parameters
    ----------
    text : str
        Raw response body from the ``/metrics`` endpoint.

    Returns
    -------
    dict
        Mapping of ``metric_name`` to a list of entries, each of the
        form ``{"labels": dict, "value": float}``.
    """
    result: dict[str, list[dict[str, Any]]] = {}

    for raw_line in text.splitlines():
        stripped = raw_line.strip()

        # Skip comments and blank lines
        if not stripped or stripped.startswith("#"):
            continue

        parsed = _METRIC_LINE_RE.match(stripped)
        if parsed is None:
            continue

        metric_name = parsed.group(1)
        labels_str = parsed.group(2)
        value_str = parsed.group(3)

        labels: dict[str, str] = {}
        for label_match in _LABEL_RE.finditer(labels_str):
            labels[label_match.group(1)] = label_match.group(2)

        try:
            value = float(value_str)
        except (ValueError, OverflowError):
            continue

        entry = {"labels": labels, "value": value}
        result.setdefault(metric_name, []).append(entry)

    return result


def _extract_plugin_states(text: str) -> dict[str, int]:
    """Extract ``courier_plugin_state`` values from Prometheus text.

    Only examines lines belonging to the ``courier_plugin_state`` metric.
    Every other line is a no-op.

    Parameters
    ----------
    text : str
        Raw response body from the ``/metrics`` endpoint.

    Returns
    -------
    dict[str, int]
        Mapping of ``plugin_identifier`` label to ``PluginRunState`` integer
        value. Returns an empty dict if no plugin-state lines are found.

    Notes
    -----
    Keyed on ``plugin_identifier``, not ``plugin_name``: the caller feeds these
    into ``parse_config(run_identifiers=...)``, which matches against the YAML
    ``run[*].identifier``. ``plugin_name`` carries the plugin *class* name
    ("serial_bash"), so the two sets never intersected and ``--live`` always
    resolved to an empty sub-section. Falls back to ``plugin_name`` for
    metrics scraped from an older courier that predates the identifier label.
    """
    result: dict[str, int] = {}

    for raw_line in text.splitlines():
        stripped = raw_line.strip()

        if not stripped or stripped.startswith("#"):
            continue

        if not stripped.startswith(_PLUGIN_STATE_METRIC):
            continue

        parsed = _METRIC_LINE_RE.match(stripped)
        if parsed is None:
            continue

        if parsed.group(1) != _PLUGIN_STATE_METRIC:
            continue

        labels_str = parsed.group(2)
        value_str = parsed.group(3)

        identifier = _extract_label(labels_str, "plugin_identifier") or _extract_label(
            labels_str,
            "plugin_name",
        )
        if identifier is None:
            continue

        try:
            state = int(float(value_str))
        except (ValueError, OverflowError):
            continue

        result[identifier] = state

    return result


def _extract_label(labels_str: str, key: str) -> str | None:
    """Return the value of *key* from a Prometheus label string.

    Parameters
    ----------
    labels_str : str
        The label portion of a Prometheus metric line, e.g.
        ``plugin_name="my-plugin",other="val"``.
    key : str
        The label key to look up.

    Returns
    -------
    str or None
        The label value if found, otherwise ``None``.
    """
    for label_match in _LABEL_RE.finditer(labels_str):
        if label_match.group(1) == key:
            return label_match.group(2)
    return None
