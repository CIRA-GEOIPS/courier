"""Unit tests for courier.cli.registry."""

from unittest.mock import MagicMock, patch

import pytest

from courier.cli.registry import COURIER_NAMESPACE, ensure_registry
from courier.errors import RegistryInitError


def test_ensure_registry_uses_correct_namespace() -> None:
    mock_registry = MagicMock()
    with patch("courier.cli.registry.PluginRegistry", return_value=mock_registry, create=True):
        # patch both the deferred import path and the module-level name
        with patch("pluginify.plugin_registry.PluginRegistry") as MockClass:
            MockClass.return_value = mock_registry
            # Re-import to exercise deferred import path
            import importlib
            import courier.cli.registry as reg_module
            importlib.reload(reg_module)

    assert COURIER_NAMESPACE == "runcourier.dev.plugin_packages"


def test_ensure_registry_calls_create_registries() -> None:
    mock_instance = MagicMock()

    with patch("pluginify.plugin_registry.PluginRegistry") as MockClass:
        MockClass.return_value = mock_instance
        ensure_registry()

    MockClass.assert_called_once_with(namespace=COURIER_NAMESPACE)
    mock_instance.create_registries.assert_called_once_with()


def test_ensure_registry_wraps_plugin_registry_error() -> None:
    from pluginify.errors import PluginRegistryError

    mock_instance = MagicMock()
    mock_instance.create_registries.side_effect = PluginRegistryError("boom")

    with patch("pluginify.plugin_registry.PluginRegistry", return_value=mock_instance):
        with pytest.raises(RegistryInitError):
            ensure_registry()


def test_ensure_registry_idempotent() -> None:
    mock_instance = MagicMock()

    with patch("pluginify.plugin_registry.PluginRegistry", return_value=mock_instance):
        ensure_registry()
        ensure_registry()

    assert mock_instance.create_registries.call_count == 2
