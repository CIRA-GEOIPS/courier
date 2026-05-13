"""Live metrics visualizer TUI for Courier — ``courier viz``.

Phase 1 provides the foundational data layer: typed metric models,
a Prometheus fetcher/parser, design tokens, and the CLI entry point.
The Textual TUI application (Phase 2) builds on these foundations.
"""

from courier.viz.cli import viz_app

__all__ = ["viz_app"]
