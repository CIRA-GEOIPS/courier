"""Registry initialization helpers for the courier CLI."""

import logging
from typing import Final

LOG = logging.getLogger(__name__)

# Must match [tool.poetry.plugins."..."] key in pyproject.toml
COURIER_NAMESPACE: Final[str] = "runcourier.dev.plugin_packages"


def ensure_registry() -> None:
    """Create pluginify registries for the courier namespace if not already present.

    Idempotent — safe to call on every CLI invocation.

    Raises
    ------
    RegistryInitError
        Wraps pluginify.errors.PluginRegistryError if registry creation fails.
    """
    # Deferred imports: avoid import-time side effects and circular import risk
    from pluginify.errors import PluginRegistryError  # noqa: PLC0415
    from pluginify.plugin_registry import PluginRegistry  # noqa: PLC0415

    from courier.errors import RegistryInitError  # noqa: PLC0415

    try:
        LOG.debug("Initializing plugin registry for namespace %r", COURIER_NAMESPACE)
        PluginRegistry(namespace=COURIER_NAMESPACE).create_registries()
    except PluginRegistryError as exc:
        raise RegistryInitError(
            f"Failed to initialize plugin registry for namespace "
            f"{COURIER_NAMESPACE!r}: {exc}",
        ) from exc
