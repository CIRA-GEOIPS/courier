"""CLI ``courier plugins`` sub-app — list plugins.
"""
from __future__ import annotations

import json
from pathlib import Path  # noqa: TC003 — Typer reads annotation at runtime.
from typing import Annotated
from collections.abc import Iterator

import typer
from rich.console import Console
from rich.table import Table
from rich import box

from pluginify.interfaces.base import BaseClassInterface, BaseYamlInterface
from lexeme_type.lexeme import Lexeme

from courier.cli.config_loader import load_config
from courier.cli.registry import COURIER_NAMESPACE
from courier.interfaces import (
    data_monitor_configs,
    data_monitors,
    dispatchers,
    job_builders,
)
from courier.schema.v1alpha1.service_config import ServiceConfigModel

plugins_app = typer.Typer(
    name="plugins",
    help="Inspect courier's plugins.",
    no_args_is_help=True,
)

_CONFIG_OPTION = typer.Option(
    "--config",
    "-c",
    exists=True,
    readable=True,
    help="Path to a service YAML. Returns only plugins referenced in the service config.",
)
_NAMESPACE_OPTION = typer.Option(
    "--namespace",
    "-n",
    help="Override the namespace read from the YAML metadata.",
)
_JSON_OPTION = typer.Option(
    "--json",
    "-j",
    help="Emit machine-readable JSON to stdout instead of a table.",
)

PLUGIN_REGISTRIES: dict[str, BaseYamlInterface | BaseClassInterface] = {
    "data_monitor_configs": data_monitor_configs,
    "data_monitors": data_monitors,
    "dispatchers": dispatchers,
    "job_builders": job_builders,
}

# Distinct color per plugin type for at-a-glance scanning.
_TYPE_STYLES: dict[str, str] = {
    "data_monitor_configs": "cyan",
    "data_monitors": "magenta",
    "dispatchers": "green",
    "job_builders": "yellow",
}


def _discover_plugins() -> Iterator[tuple[str, str]]:
    """Discover all plugins by querying each interface's registry."""
    for kind, registry in PLUGIN_REGISTRIES.items():
        for plugin in registry.get_plugins():
            yield kind, plugin.name


def _config_references_plugin(config: ServiceConfigModel, plugin_type: str, plugin_name: str) -> bool:
    """Return True if the config references the plugin."""
    for section in config.spec.run:
        if Lexeme(section.spec.kind) == plugin_type and Lexeme(section.spec.name) == plugin_name:
            return True
    return False


def get_plugins(
    config_file: Path | ServiceConfigModel | None, namespace: str | None
) -> tuple[str, list[list[str]]]:
    """Return ``(namespace, [[plugin_type, plugin_name], ...])``."""
    if not isinstance(config_file, Path):
        config = config_file
    else:
        config = load_config(config_file)
    if config:
        namespace = config.metadata.namespace if config.metadata.namespace else namespace
    ns = namespace or COURIER_NAMESPACE
    plugins: list[list[str]] = []
    for plugin_type, registry in PLUGIN_REGISTRIES.items():
        for plugin in registry.get_plugins():
            plugin_name = plugin.name
            if not config or _config_references_plugin(config, plugin_type, plugin_name):
                plugins.append([plugin_type, plugin_name])
    return ns, plugins


def _render_plugins_table(
    namespace: str,
    plugins: list[list[str]],
    config_file: Path | None,
) -> None:
    """Render plugins as a rich table, grouped by plugin type."""
    console = Console()

    if config_file is not None:
        caption = f"Filtered by config: [italic]{config_file}[/italic]"
    else:
        caption = f"[dim]{len(plugins)} plugin(s) discovered[/dim]"

    table = Table(
        title=f"Courier plugins  ·  namespace: [bold]{namespace}[/bold]",
        caption=caption,
        box=box.ROUNDED,
        header_style="bold white on blue",
        show_lines=False,
        expand=False,
    )
    table.add_column("Plugin type", style="bold", no_wrap=True)
    table.add_column("Name", overflow="fold")

    if not plugins:
        table.add_row("[dim]—[/dim]", "[dim italic]no plugins found[/dim italic]")
        console.print(table)
        return

    grouped: dict[str, list[str]] = {}
    for plugin_type, plugin_name in plugins:
        grouped.setdefault(plugin_type, []).append(plugin_name)

    last_type = list(grouped.keys())[-1]
    for plugin_type, names in grouped.items():
        style = _TYPE_STYLES.get(plugin_type, "white")
        for i, name in enumerate(sorted(names)):
            type_cell = f"[{style}]{plugin_type}[/{style}]" if i == 0 else ""
            table.add_row(type_cell, name)
        if plugin_type != last_type:
            table.add_section()

    console.print(table)


def _render_plugins_json(namespace: str, plugins: list[list[str]]) -> None:
    """Emit plugins as JSON on stdout (clean, pipeable, no styling)."""
    payload = {
        "namespace": namespace,
        "plugins": [
            {"type": plugin_type, "name": plugin_name}
            for plugin_type, plugin_name in plugins
        ],
    }
    # Plain print so output is clean for piping into jq, files, etc.
    typer.echo(json.dumps(payload, indent=2))


@plugins_app.command("list")
def list_cmd(
    config: Annotated[Path, _CONFIG_OPTION] = None,
    namespace: Annotated[str | None, _NAMESPACE_OPTION] = None,
    json_output: Annotated[bool, _JSON_OPTION] = False,
) -> None:
    """Print every plugin the service is expected to use."""
    ns, plugins = get_plugins(load_config(config) if config else None, namespace)
    if json_output:
        _render_plugins_json(ns, plugins)
    else:
        _render_plugins_table(ns, plugins, config)