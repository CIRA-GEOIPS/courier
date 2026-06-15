"""Dashboard serialization and file output.

Converts :class:`grafanalib.core.Dashboard` objects to JSON and writes
them to files.  Handles both single-dashboard output and multi-dashboard
(split mode) output.
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from grafanalib.core import Dashboard


# ---------------------------------------------------------------------------
# JSON encoder fallback
# ---------------------------------------------------------------------------


class _FallbackEncoder(json.JSONEncoder):
    """Fallback JSON encoder when ``DashboardEncoder`` is unavailable."""

    def default(self, obj: object) -> object:
        from datetime import date, datetime  # noqa: PLC0415

        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)


# ---------------------------------------------------------------------------
# Core serialization
# ---------------------------------------------------------------------------


def _dashboard_to_json(dashboard: Dashboard, indent: int = 2) -> str:
    """Convert a single dashboard to a JSON string.

    Uses ``grafanalib._gen.DashboardEncoder`` when available; falls back
    to a custom encoder that handles ``datetime`` and ``set`` types.

    Raises
    ------
    ImportError
        When ``grafanalib`` is not installed.
    """
    try:
        import grafanalib  # noqa: F401, PLC0415
    except ImportError:
        raise ImportError(
            "grafanalib is required for dashboard serialization. "
            "Install with: pip install courier[grafana]",
        ) from None

    try:
        from grafanalib._gen import DashboardEncoder  # noqa: PLC0415
    except ImportError:
        encoder_cls: type[json.JSONEncoder] = _FallbackEncoder
    else:
        encoder_cls = DashboardEncoder

    return json.dumps(
        dashboard.to_json_data(),
        cls=encoder_cls,
        indent=indent,
        sort_keys=True,
    )


# ---------------------------------------------------------------------------
# File I/O helpers
# ---------------------------------------------------------------------------


def _warn_if_exists(path: Path) -> None:
    """Emit a warning when *path* already exists and will be overwritten."""
    if path.exists():
        warnings.warn(f"Overwriting existing file: {path}", stacklevel=3)


def _safe_write(path: Path, content: str) -> None:
    """Write *content* to *path*, raising a descriptive error on failure."""
    try:
        path.write_text(content)
    except OSError as exc:
        raise OSError(
            f"Failed to write dashboard to {path}: {exc}",
        ) from exc


# ---------------------------------------------------------------------------
# File output strategies
# ---------------------------------------------------------------------------


def _write_single_file_mode(
    dashboards: list[Dashboard],
    output: Path,
    indent: int,
) -> str:
    """Write one or more dashboards to ``.json`` file(s).

    When multiple dashboards are provided, each receives a ``_N`` suffix
    inserted before the ``.json`` extension (e.g. ``mydash_1.json``).
    """
    if len(dashboards) == 1:
        _warn_if_exists(output)
        _safe_write(output, _dashboard_to_json(dashboards[0], indent))
        return f"Dashboard written to {output}"

    stem = output.stem
    parent = output.parent
    written: list[str] = []
    for i, d in enumerate(dashboards, start=1):
        fname = parent / f"{stem}_{i}.json"
        _warn_if_exists(fname)
        _safe_write(fname, _dashboard_to_json(d, indent))
        written.append(str(fname))
    return "Dashboards written to:\n  " + "\n  ".join(written)


def _write_directory_mode(
    dashboards: list[Dashboard],
    output: Path,
    indent: int,
) -> str:
    """Write each dashboard to ``<uid>.json`` inside *output* directory."""
    output.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for d in dashboards:
        uid = d.uid or "dashboard"
        fname = output / f"{uid}.json"
        _warn_if_exists(fname)
        _safe_write(fname, _dashboard_to_json(d, indent))
        written.append(str(fname))
    return "Dashboards written to:\n  " + "\n  ".join(written)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def serialize_dashboard(
    dashboards: list[Dashboard],
    *,
    output: Path | str | None = None,
    indent: int = 2,
) -> str:
    """Serialize one or more dashboards to JSON and optionally write to files.

    Parameters
    ----------
    dashboards : list[Dashboard]
        Dashboard objects to serialize.
    output : Path | str | None
        Output target:

        - ``None`` → return JSON string; don't write files.
        - Single ``.json`` file path → write one dashboard to this file.
          When multiple dashboards are provided, each receives a ``_N``
          suffix inserted before ``.json`` (e.g. ``mydash_1.json``).
        - Directory path → write one ``.json`` file per dashboard,
          named by the dashboard's ``uid``.
    indent : int
        JSON indentation level (default 2).

    Returns
    -------
    str
        When *output* is ``None`` and only one dashboard exists, returns the
        JSON string.  When *output* is provided, returns a summary string of
        files written.

    Raises
    ------
    ValueError
        When multiple dashboards are provided but *output* is ``None``.
        An output path is required to write multiple dashboards to files.
    """
    if not dashboards:
        return "No dashboards to serialize."

    if output is None:
        if len(dashboards) > 1:
            raise ValueError(
                f"Generated {len(dashboards)} dashboards but no output path "
                f"specified. Use --output to write files individually.",
            )
        # Single dashboard without output: return JSON string
        return _dashboard_to_json(dashboards[0], indent)

    output_path = Path(output)
    return _write_dashboards(dashboards, output_path, indent)


def _write_dashboards(
    dashboards: list[Dashboard],
    output: Path,
    indent: int,
) -> str:
    """Write dashboards to files and return a summary message."""
    if output.suffix == ".json":
        return _write_single_file_mode(dashboards, output, indent)
    return _write_directory_mode(dashboards, output, indent)
