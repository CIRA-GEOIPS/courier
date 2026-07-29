"""Plugin discovery through Python entry points.

Every courier plugin is declared as an entry point in one group per interface:

* ``courier.data_monitors``
* ``courier.job_builders``
* ``courier.dispatchers``
* ``courier.data_monitor_configs``

The entry-point *name* is the plugin name operators write in a service config
(``spec.run[*].spec.name``, or an entry in a monitor's ``metadata-tools``), and
the value points at the object implementing it — a plugin class for the first
three groups, a validated config object for the fourth.

Two properties matter and are covered by tests:

* **Names are listable without importing anything.** ``courier plugins list``
  reads entry-point metadata only, so it does not pay the import cost of
  plugins whose optional dependencies are absent.
* **A loaded plugin is the same object a direct import would give.** Entry
  points go through the normal import system, so
  ``data_monitors.get_plugin("cron_glob") is cron_glob.CronGlob``. The previous
  registry executed plugin files with ``exec_module``, producing a second class
  object with an identical ``__module__`` — a duplicate that broke identity and
  ``isinstance`` checks invisibly.

Adding a plugin means declaring it in ``pyproject.toml`` *and* reinstalling, as
entry points live in installed distribution metadata. Nothing here can detect a
plugin that was never declared; ``tests/test_shipped_config_drift.py`` is what
catches that.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from importlib.metadata import EntryPoint, entry_points
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from courier.errors import PluginNotFoundError, PluginValidationError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pydantic import BaseModel

    from courier.interfaces.plugin_protocol import ServicePlugin

# Prefix for every courier entry-point group. The interface name is appended,
# so the ``data_monitors`` interface reads ``courier.data_monitors``.
ENTRY_POINT_PREFIX = "courier"

ModelT = TypeVar("ModelT", bound="BaseModel")


@functools.cache
def _entry_points(group: str) -> Mapping[str, EntryPoint]:
    """Return the entry points in *group*, keyed by name.

    Cached because ``importlib.metadata.entry_points`` walks every installed
    distribution, and discovery is on the path of every CLI invocation. Call
    :func:`refresh` after installing or uninstalling a distribution in-process.

    Parameters
    ----------
    group : str
        Entry-point group name, e.g. ``"courier.dispatchers"``.

    Returns
    -------
    Mapping[str, EntryPoint]
        Entry points keyed by their declared name.
    """
    return {entry_point.name: entry_point for entry_point in entry_points(group=group)}


def refresh() -> None:
    """Discard the cached entry points.

    Needed only when installed distributions change inside a running process —
    principally tests that install a throwaway plugin package.
    """
    _entry_points.cache_clear()


@dataclass(frozen=True)
class _EntryPointRegistry:
    """Shared lookup and loading for one entry-point group.

    Attributes
    ----------
    name : str
        Interface name, e.g. ``"dispatchers"``. Callers key registries by this.
    group : str
        Entry-point group to read, e.g. ``"courier.dispatchers"``.
    """

    name: str
    group: str

    def names(self) -> list[str]:
        """Return every declared plugin name, sorted, importing nothing."""
        return sorted(_entry_points(self.group))

    def _entry_point(self, name: str) -> EntryPoint:
        """Look up *name* in this group, or raise with the alternatives.

        Raises
        ------
        PluginNotFoundError
            If no plugin is declared under *name*. The message lists what is
            available, because the usual cause is a typo in a service config
            and the operator needs the correct spelling, not a ``KeyError``.
        """
        declared = _entry_points(self.group)
        if name not in declared:
            available = ", ".join(sorted(declared)) or "(none)"
            raise PluginNotFoundError(
                f"No {self.name} plugin named {name!r}. Available: {available}.",
            )
        return declared[name]

    def _load(self, name: str) -> object:
        """Import and return the object behind *name*.

        Raises
        ------
        PluginValidationError
            If importing the entry point fails. The original exception is
            chained; this wrapper exists so the message names the plugin and
            group rather than only the module that happened to fail.
        """
        entry_point = self._entry_point(name)
        try:
            return entry_point.load()
        except Exception as exc:
            raise PluginValidationError(
                f"{self.group} plugin {name!r} could not be loaded from "
                f"{entry_point.value!r}: {exc}",
            ) from exc


@dataclass(frozen=True)
class ClassPluginRegistry(_EntryPointRegistry):
    """Registry of plugin *classes* for one interface.

    Returns classes, not instances: :func:`courier.service.create_service_with_plugins`
    takes ``type[ServicePlugin]``, and the plugin manager constructs each one
    with the service, its config, and its identifier.

    Attributes
    ----------
    expected_base : type
        Base class every plugin in this group must subclass.
    """

    expected_base: type

    def get_plugin(self, name: str) -> type[ServicePlugin]:
        """Return the plugin class declared under *name*.

        Raises
        ------
        PluginNotFoundError
            If no plugin is declared under *name*.
        PluginValidationError
            If the entry point does not resolve to a subclass of
            :attr:`expected_base`. Caught here rather than at construction so
            the error names the declaration that is wrong.
        """
        loaded = self._load(name)
        if not isinstance(loaded, type) or not issubclass(loaded, self.expected_base):
            raise PluginValidationError(
                f"{self.group} plugin {name!r} must be a subclass of "
                f"{self.expected_base.__name__}, got {loaded!r}.",
            )
        return cast("type[ServicePlugin]", loaded)

    def get_plugins(self) -> list[type[ServicePlugin]]:
        """Return every plugin class in this group, importing each one."""
        return [self.get_plugin(name) for name in self.names()]


@dataclass(frozen=True)
class ConfigPluginRegistry(_EntryPointRegistry, Generic[ModelT]):  # noqa: UP046
    """Registry of validated *config objects* for one interface.

    Unlike :class:`ClassPluginRegistry` these are instances, because a config
    is data rather than behaviour. They are constructed and validated at import
    time by the module declaring them, so a malformed config fails discovery
    instead of silently matching nothing at runtime.

    Attributes
    ----------
    model : type
        Pydantic model every config in this group must be an instance of.
    """

    model: type[ModelT]

    def get_plugin(self, name: str) -> ModelT:
        """Return the config object declared under *name*.

        Raises
        ------
        PluginNotFoundError
            If no config is declared under *name*.
        PluginValidationError
            If the entry point does not resolve to a :attr:`model` instance.
        """
        loaded = self._load(name)
        if not isinstance(loaded, self.model):
            raise PluginValidationError(
                f"{self.group} plugin {name!r} must be a "
                f"{self.model.__name__} instance, got {loaded!r}.",
            )
        return loaded

    def get_plugins(self) -> list[ModelT]:
        """Return every config object in this group, importing each one."""
        return [self.get_plugin(name) for name in self.names()]
