"""Monitor changes to the designated config file."""

import difflib
from multiprocessing import Value
import os
from pathlib import Path
import re
import yaml
import textwrap
from typing import Any, TYPE_CHECKING

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from courier.interfaces.job_builders import JobBuilder

if TYPE_CHECKING:
    from courier.managers import PluginManager
from courier.errors import InvalidIdentifierError, PluginError
from courier.managers.base import ServiceManager
from courier.schema.v1alpha1.service_config import MicroserviceModel

def get_diff_lines(old_src: str, new_src: str) -> list[int]:
    old = old_src.splitlines()
    new = new_src.splitlines()
    d = difflib.Differ()

    diffs = d.compare(old, new)
    line_num = 0

    diff_lines = []

    for line in diffs:
        code = line[:2]

        if code in ("  ", "+ "):
            line_num += 1
        if code == "+ ":
            # zero-indexed
            diff_lines.append(line_num - 1)
    return diff_lines

def get_spec_from_line(src_str, line_num) -> tuple[int, int]:
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
        curr_indentation = len(curr_line) - len(curr_line.lstrip())

        if curr_indentation <= upper_indentation:
            break

        curr_line_num += 1

    lower_bound = curr_line_num - 1
    return (id_line, lower_bound)

def get_spec_from_buffer(spec: str) -> MicroserviceModel:
    data = yaml.safe_load(textwrap.dedent(spec))

    if isinstance(data, list):
        data = data[0]

    return MicroserviceModel.model_validate(data)



class ConfigMonitorEventHandler(FileSystemEventHandler):
    def __init__(self, config_monitor: ConfigMonitor):
        self._config_monitor: ConfigMonitor = config_monitor

    def on_any_event(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        if event.event_type not in {"modified", "created"}:
            return

        event_path = Path(str(event.src_path)).resolve()
        config_path = Path(self._config_monitor._config_file).resolve()
    
        if event_path == config_path:
            self._config_monitor._on_config_changed()

class ConfigMonitor:
    def __init__(self,
                 config_file: str,
                 parent_service: Any):
        self._config_file: Path = Path(config_file)
        self._last_read_text = self._config_file.read_text()
        self._parent_service : PluginManager = parent_service
        self._observer = Observer()
        self._observer.schedule(ConfigMonitorEventHandler(self), str(Path(self._config_file).parent), recursive=False)
    def _replace_running_plugin(self, service: MicroserviceModel) -> None:
        from courier.cli.plugins import PLUGIN_REGISTRIES, RUN_KINDS, normalize_kind
        id = service.identifier
        identified_plugin = self._parent_service._plugins.get(id, None)
        if not identified_plugin:
            raise ValueError(f"Error creating new plugin {id};"
                             "please do not change the identifier when updating"
                             "service configs")

        self._parent_service._stop_plugin(identified_plugin)
        self._parent_service._plugins.pop(id)

        kind = normalize_kind(service.spec.kind)
        
        if kind not in RUN_KINDS:
            raise ValueError(f"{service.identifier!r}: {service.spec.kind!r} is not a runnable kind.")

        plugin_class = PLUGIN_REGISTRIES[kind].get_plugin(service.spec.name)
        config = service.spec.config or {}
        
        self._parent_service.register_plugin(
            plugin_class,
            config,
            id
        )
        plugin_info = self._parent_service._plugins[id]

        if (kind == "job_builders" 
            and isinstance(plugin_info.plugin, JobBuilder)
            and not plugin_info.plugin.targets
            and isinstance(identified_plugin.plugin, JobBuilder)):
            plugin_info.plugin.targets = identified_plugin.plugin.targets
        if not plugin_info:
            raise PluginError(
                    f"Error registering plugin {id}")
        self._parent_service._start_plugin(plugin_info)
    def _on_config_changed(self):
        new_config = self._config_file.read_text()
        old_config = self._last_read_text

        diff_lines = get_diff_lines(old_config, new_config)

        updated_buffers = set()
        for line_num in diff_lines:
            if any(start <= line_num < end for start, end in updated_buffers):
                continue
            updated_buffers.add(get_spec_from_line(new_config, line_num))
        updated_models = []
        for upper, lower in updated_buffers:
            spec = "\n".join(new_config.splitlines()[upper:lower+1])
            updated_models.append(get_spec_from_buffer(spec))

        for service in updated_models:
            self._replace_running_plugin(service)

    def start(self):
        self._observer.start()
    def stop(self):
        self._observer.stop()
        self._observer.join()
