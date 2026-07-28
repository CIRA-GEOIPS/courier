"""CLI entry point for ``courier viz``."""

from __future__ import annotations

import asyncio
from typing import Annotated

import typer


def viz(
    host: Annotated[
        str,
        typer.Option("--host", "-h", help="Courier host"),
    ] = "localhost",
    port: Annotated[
        int,
        typer.Option("--port", "-p", help="Prometheus metrics port"),
    ] = 8000,
    refresh: Annotated[
        int,
        typer.Option("--refresh", "-r", help="Refresh interval in seconds"),
    ] = 5,
) -> None:
    """Open the live metrics visualizer for a running courier instance."""
    # Lazy import — textual is only required when the command runs
    try:
        from courier.viz.app import CourierViz  # noqa: PLC0415
    except ImportError:
        typer.echo(
            "The 'viz' command requires the Textual library.\n"
            "Install it with: pip install courier[viz]",
        )
        raise typer.Exit(1) from None

    from courier.viz.design import REFRESH_RATES  # noqa: PLC0415

    # Validate refresh rate
    effective_refresh = refresh
    if refresh not in REFRESH_RATES:
        typer.echo(
            f"Warning: refresh rate {refresh}s not in supported "
            f"rates {REFRESH_RATES}. Using {REFRESH_RATES[2]}s.",
        )
        effective_refresh = REFRESH_RATES[2]  # 5s default

    app = CourierViz(host=host, port=port, refresh_interval=effective_refresh)
    asyncio.run(app.run_async())
