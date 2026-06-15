"""Courier dashboard generation system — public API surface.

Re-exports the core data model, config parser, and (once implemented)
the dashboard generator and serializers.
"""

from __future__ import annotations

from enum import Enum, auto

from courier.dashboard.config_parser import (
    DashboardModel,
    PluginInfo,
    PluginKind,
    parse_config,
)
from courier.dashboard.generator import generate_dashboard
from courier.dashboard.serializers import serialize_dashboard


class DashboardGenerationMode(Enum):
    """Controls how a dashboard is split or unified during generation."""

    UNIFIED = auto()
    """Single dashboard file covering the entire pipeline."""

    SPLIT_BY_KIND = auto()
    """Separate dashboard per plugin kind (data_monitor, job_builder, dispatcher)."""

    SPLIT_BY_PLUGIN = auto()
    """Separate dashboard per individual plugin instance."""


__all__ = [
    "DashboardGenerationMode",
    "DashboardModel",
    "PluginInfo",
    "PluginKind",
    "generate_dashboard",
    "parse_config",
    "serialize_dashboard",
]
