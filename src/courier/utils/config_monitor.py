"""Monitor changes to the designated config file."""

from __future__ import annotations

import difflib
import re
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import yaml
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from courier.interfaces.job_builders import JobBuilder
from courier.utils.logging import ContextAdapter

if TYPE_CHECKING:
    from courier.interfaces.plugin_protocol import ServicePlugin
    from courier.managers import PluginManager, PluginStateInfo
from courier.errors import CourierError
from courier.schema.v1alpha1.service_config import MicroserviceModel


def get_diff_lines(old_src: str, new_src: str) -> list[int]:
    """Get the line numbers of modifications.

    Zero-indexed.

    Parameters
    ----------
    old_src : str
        The old file.
    new_src : str
        The modified file.

    Returns
    -------
    diff_lines : list[int]
        A list of all differing lines.
    """
    old = old_src.splitlines()
    new = new_src.splitlines()
    d = difflib.Differ()

    diffs = d.compare(old, new)
    line_num = 0

    diff_lines = []

    for line in diffs:
        code = line[:2]

        if code == "  ":
            line_num += 1

        elif code == "+ ":
            diff_lines.append(line_num)
            line_num += 1

        elif code == "- " and new:
            diff_lines.append(min(line_num, len(new) - 1))

    return sorted(set(diff_lines))


def get_spec_from_line(src_str: str, line_num: int) -> tuple[int, int]:
    """Get the full spec string from a specified line.

    Parameters
    ----------
    src_str : str
        The source string to get the spec from
        i.e. the full config file as read text.
    line_num: int
        The line number that exists inside of a spec,
        presumably.

    Raises
    ------
    ValueError
        If the line does not exist within a courier service.

    Returns
    -------
    upper_and_lower : tuple[int, int]
        The upper and lower line indexed of src_str, zero-indexed.
    """
    # move up to get the identifier
    identifier_pattern = r"^\s*-\s*identifier:\s*(.+)$"
    lines = src_str.splitlines()
    curr_line_num = line_num
    id_line = -1
    upper_indentation = -1
    while curr_line_num >= 0:
        line = lines[curr_line_num]
        match = re.search(identifier_pattern, line)
        if match:
            id_line = curr_line_num
            upper_indentation = len(line) - len(line.lstrip())
            break
        curr_line_num -= 1
    if id_line == -1:
        raise ValueError(f"No identifier found at or before line {line_num}")

    # move down to get the lower bound
    curr_line_num = id_line + 1

    while curr_line_num < len(lines):
        curr_line = lines[curr_line_num]

        # Ignore blank or whitespace-only lines
        if not curr_line.strip():
            curr_line_num += 1
            continue

        curr_indentation = len(curr_line) - len(curr_line.lstrip())

        if curr_indentation <= upper_indentation:
            break

        curr_line_num += 1

    lower_bound = curr_line_num - 1
    return (id_line, lower_bound)


def get_spec_from_buffer(spec: str) -> MicroserviceModel:
    """Safely load and return a MicroserviceModel from a specified spec.

    Parameters
    ----------
    spec: str
        The full string to be converted.

    Returns
    -------
    res: MicroserviceModel
        A MicroserviceModel based on the spec string.
    """
    data = yaml.safe_load(textwrap.dedent(spec))

    if isinstance(data, list):
        data = data[0]

    return MicroserviceModel.model_validate(data)


class ConfigMonitorEventHandler(FileSystemEventHandler):
    """Event handler for the config monitor observer."""

    def __init__(self, config_monitor: ConfigMonitor):
        self._config_monitor: ConfigMonitor = config_monitor

    def on_any_event(self, event: FileSystemEvent) -> None:
        """Filter events only for modification to the config file."""
        if event.is_directory:
            return
        if event.event_type not in {"modified", "created"}:
            return

        event_path = Path(str(event.src_path)).resolve()
        config_path = Path(self._config_monitor._config_file).resolve()

        if event_path == config_path:
            try:
                self._config_monitor._on_config_changed()
            except Exception:
                self._config_monitor._logger.exception(
                    "Unexpected error processing config change",
                )


class ConfigMonitor:
    """Utility class for the config monitor functionality."""

    def __init__(self, config_file: str, parent_service: Any):
        self._config_file: Path = Path(config_file)
        self._last_read_text = self._config_file.read_text()
        self._parent_service: PluginManager = parent_service
        self._reload_mode: str = self._parent_service._config.live_reload
        self._observer = Observer()
        self._observer.schedule(
            ConfigMonitorEventHandler(self),
            str(Path(self._config_file).parent),
            recursive=False,
        )
        self._logger = self._parent_service._logger or ContextAdapter(None)

    def _register_and_start_plugin(
        self,
        old_plugin: PluginStateInfo,
        plugin_type: type[ServicePlugin],
        config: dict[str, Any],
        identifier: str,
        kind: str,
    ) -> None:
        """Rollback plugins based on their info in the registry."""
        # stop the old thread and check for timeout
        self._parent_service._wipe_plugin(old_plugin)

        # plugin registration
        self._parent_service.register_plugin(plugin_type, config, identifier)

        restored_plugin = self._parent_service._plugins[identifier]

        if (
            kind == "job_builders"
            and isinstance(restored_plugin.plugin, JobBuilder)
            and not restored_plugin.plugin.targets
            and isinstance(old_plugin.plugin, JobBuilder)
        ):
            restored_plugin.plugin.targets = old_plugin.plugin.targets

        self._parent_service._start_plugin(restored_plugin)

    def _bg_register_and_start_plugin(
        self,
        old_plugin: PluginStateInfo,
        plugin_type: type[ServicePlugin],
        config: dict[str, Any],
        identifier: str,
        kind: str,
    ):
        """Blue-green handling for registering plugins.

        Register and start the new service, then check if it passes.
        """
        # generate random identifier so registrations don't clash
        dummy_identifier = f"dummy-{uuid4().hex}"
        restored_plugin: PluginStateInfo
        try:
            self._parent_service.register_plugin(plugin_type, config, dummy_identifier)
            restored_plugin = self._parent_service._plugins[identifier]
        except (ValueError, KeyError):
            self._logger.exception(
                f"Registration failed for blue-green reload on service {identifier}",
            )
            with self._parent_service._lock:
                if dummy_identifier in self._parent_service._plugins:
                    self._parent_service._plugins.pop(dummy_identifier, None)
            return

        if (
            kind == "job_builders"
            and isinstance(restored_plugin.plugin, JobBuilder)
            and not restored_plugin.plugin.targets
            and isinstance(old_plugin.plugin, JobBuilder)
        ):
            restored_plugin.plugin.targets = old_plugin.plugin.targets

    def _hard_reload_system(
        self,
        old_plugin: PluginStateInfo,
        plugin_class: type[ServicePlugin],
        config: dict[str, Any],
        identifier: str,
        kind: str,
    ):
        """Full reload-rollback system for hard live reloads."""
        try:
            self._register_and_start_plugin(
                old_plugin,
                plugin_class,
                config,
                identifier,
                kind,
            )
        except Exception as e:
            try:
                self._register_and_start_plugin(
                    old_plugin,
                    old_plugin.plugin_type,
                    old_plugin.config,
                    identifier,
                    kind,
                )
            except Exception as rollback_error:
                raise CourierError(
                    f"Rollback failed for plugin {identifier}",
                ) from rollback_error

            # rollback succeeded
            raise CourierError(f"Error registering plugin {identifier}") from e

    def _replace_running_plugin(self, service: MicroserviceModel) -> None:
        from courier.cli.plugins import (  # noqa: PLC0415
            PLUGIN_REGISTRIES,
            RUN_KINDS,
            normalize_kind,
        )

        """Create, start, and replace plugins stored in the plugin manager.

        Parameters
        ----------
        service : MicroserviceModel
            The service to be registered

        Raises
        ------
        ValueError
            If the identifier was changed.
        """

        identifier = service.identifier
        identified_plugin = self._parent_service._plugins.get(identifier, None)
        if not identified_plugin:
            raise ValueError(
                f"Error creating new plugin {identifier};"
                "please do not change the identifier when updating"
                "service configs",
            )

        kind = normalize_kind(service.spec.kind)

        if kind not in RUN_KINDS:
            raise ValueError(
                f"{service.identifier!r}: "
                f"{service.spec.kind!r} is not a runnable kind.",
            )

        try:
            plugin_class = PLUGIN_REGISTRIES[kind].get_plugin(service.spec.name)
        except KeyError as e:
            raise ValueError(
                f"Invalid plugin provided - {service.spec.name} not in registry.",
            ) from e
        config = service.spec.config or {}

        old_plugin = self._parent_service._plugins[identifier]
        if not old_plugin.plugin_type:
            raise CourierError(
                f"Plugin {identifier} to be replaced does not have valid type"
                " - impossible rollback; exiting",
            )

        if self._reload_mode == "blue-green":
            pass
        else:
            self._hard_reload_system(old_plugin, plugin_class, config, identifier, kind)

    def _on_config_changed(self) -> None:
        """Act on a changed config."""
        self._logger.debug("Detected config change...")
        new_config = self._config_file.read_text()
        if not new_config.strip():
            return

        old_config = self._last_read_text

        diff_lines = get_diff_lines(old_config, new_config)

        updated_buffers: set[tuple[int, int]] = set()
        for line_num in diff_lines:
            # skip duplicate services
            if any(start <= line_num < end for start, end in updated_buffers):
                continue
            try:
                updated_buffers.add(get_spec_from_line(new_config, line_num))
            except ValueError:
                self._logger.debug(
                    f"Line {line_num} is outside any service spec; ignoring",
                )
        updated_models = []
        for upper, lower in updated_buffers:
            spec = "\n".join(new_config.splitlines()[upper : lower + 1])
            updated_models.append(get_spec_from_buffer(spec))

        for service in updated_models:
            try:
                self._replace_running_plugin(service)
            except Exception:
                self._logger.exception(
                    f"Unable to load new configuration."
                    f"Rollback triggered for {service.identifier}",
                )

        self._last_read_text = new_config

    def start(self) -> None:
        """Start the observer."""
        self._observer.start()

    def stop(self) -> None:
        """Stop the observer."""
        if self._observer.is_alive():
            self._observer.stop()
            self._observer.join()
