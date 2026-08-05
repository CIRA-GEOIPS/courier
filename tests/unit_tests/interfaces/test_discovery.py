"""Behavioural tests for entry-point plugin discovery.

These drive real installed distribution metadata rather than a monkeypatched
lookup: a throwaway ``.dist-info`` is written to a temp directory and put on
``sys.path``, so ``importlib.metadata`` does the work it does in production.
Patching ``_entry_points`` would test the registry's arithmetic while assuming
away the part that actually breaks — whether a declaration in packaging
metadata reaches courier at all.

Tests covering courier's *own* declared plugins live in
``tests/test_shipped_config_drift.py`` alongside the other code-vs-artefact
guards.
"""

from __future__ import annotations

import sys
import textwrap
from typing import TYPE_CHECKING

import pytest

from courier.errors import PluginNotFoundError, PluginValidationError
from courier.interfaces.discovery import (
    ClassPluginRegistry,
    ConfigPluginRegistry,
    refresh,
)
from courier.interfaces.dispatchers import Dispatcher
from courier.schema import DataMonitorConfig

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

# A group of courier's own so nothing here perturbs the real registries, except
# in the one test that deliberately extends a real group.
_TEST_GROUP = "courier.discovery_test_plugins"

_PLUGIN_MODULE = textwrap.dedent(
    r'''
    """Throwaway plugin package used by the discovery tests."""

    from typing import ClassVar

    from courier.interfaces.dispatchers import Dispatcher
    from courier.schema import DataMonitorConfig


    class ThirdPartyDispatcher(Dispatcher):
        """A dispatcher shipped by someone other than courier."""

        interface: ClassVar[str] = "dispatchers"
        name: ClassVar[str] = "third_party_dispatcher"
        version: ClassVar[str] = "9.9.9"


    CONFIG = DataMonitorConfig(
        name="third_party_config",
        spec={
            "file_metadata": {
                "entry": {
                    "source": "somewhere",
                    "date": r".*s(?P<YYYY>\d{4})(?P<JJJ>\d{3}).*",
                    "match": [r".*\.nc"],
                },
            },
        },
    )

    NOT_A_PLUGIN = 42
    '''
)

_BROKEN_MODULE = 'raise RuntimeError("this plugin is broken on import")\n'


def _install_fake_distribution(root: Path, entry_points_txt: str) -> None:
    """Write a minimal installed distribution under *root*.

    Only ``METADATA`` and ``entry_points.txt`` are needed for
    ``importlib.metadata.entry_points`` to see the declarations.
    """
    (root / "fake_courier_plugins.py").write_text(_PLUGIN_MODULE)
    (root / "broken_courier_plugin.py").write_text(_BROKEN_MODULE)

    dist_info = root / "fake_courier_plugins-0.1.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: fake-courier-plugins\nVersion: 0.1\n",
    )
    (dist_info / "entry_points.txt").write_text(entry_points_txt)


@pytest.fixture
def fake_dist(tmp_path: Path) -> Iterator[Path]:
    """Install a throwaway plugin distribution for the duration of one test."""
    _install_fake_distribution(
        tmp_path,
        textwrap.dedent(
            f"""
            [{_TEST_GROUP}]
            third_party_dispatcher = fake_courier_plugins:ThirdPartyDispatcher
            third_party_config = fake_courier_plugins:CONFIG
            not_a_plugin = fake_courier_plugins:NOT_A_PLUGIN
            missing_attribute = fake_courier_plugins:nope
            broken_on_import = broken_courier_plugin:anything
            """,
        ),
    )
    sys.path.insert(0, str(tmp_path))
    refresh()
    try:
        yield tmp_path
    finally:
        sys.path.remove(str(tmp_path))
        for module in ("fake_courier_plugins", "broken_courier_plugin"):
            sys.modules.pop(module, None)
        refresh()


@pytest.fixture
def class_registry() -> ClassPluginRegistry:
    return ClassPluginRegistry(
        name="discovery_test_plugins",
        group=_TEST_GROUP,
        expected_base=Dispatcher,
    )


@pytest.fixture
def config_registry() -> ConfigPluginRegistry[DataMonitorConfig]:
    return ConfigPluginRegistry(
        name="discovery_test_plugins",
        group=_TEST_GROUP,
        model=DataMonitorConfig,
    )


# ── discovery ───────────────────────────────────────────────────────────────


class TestDiscovery:
    def test_a_declared_plugin_is_found_and_loaded(
        self,
        fake_dist: Path,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """An entry point in installed metadata is all a plugin package needs.

        This is the whole third-party contract: no registry rebuild, no cache
        file, no naming convention beyond the group.
        """
        loaded = class_registry.get_plugin("third_party_dispatcher")

        assert loaded.name == "third_party_dispatcher"
        assert issubclass(loaded, Dispatcher)

    def test_names_are_listed_without_importing_the_plugins(
        self,
        fake_dist: Path,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """Listing must stay free of import cost and import side effects.

        ``courier plugins list`` enumerates every group. If listing imported,
        it would drag in every optional dependency — and one broken plugin
        would take the whole listing down.
        """
        assert "fake_courier_plugins" not in sys.modules

        names = class_registry.names()

        assert "third_party_dispatcher" in names
        assert "fake_courier_plugins" not in sys.modules

    def test_a_broken_plugin_does_not_break_listing(
        self,
        fake_dist: Path,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """``broken_on_import`` raises at import; listing must not care."""
        assert "broken_on_import" in class_registry.names()

    def test_returned_class_is_the_one_a_direct_import_gives(
        self,
        fake_dist: Path,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """Discovery must go through the import system, not re-execute files.

        The previous registry loaded plugin files with ``exec_module``, so the
        class it handed back was a *different object* from the one an ordinary
        import produced, despite an identical ``__module__``. Identity and
        ``isinstance`` checks against the imported class silently failed.
        """
        loaded = class_registry.get_plugin("third_party_dispatcher")

        import fake_courier_plugins  # noqa: PLC0415

        assert loaded is fake_courier_plugins.ThirdPartyDispatcher


# ── error reporting ─────────────────────────────────────────────────────────


class TestErrorReporting:
    def test_unknown_name_lists_the_alternatives(
        self,
        fake_dist: Path,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """A config typo is the common case; the fix must be in the message."""
        with pytest.raises(PluginNotFoundError) as caught:
            class_registry.get_plugin("third_party_dispatchr")

        message = str(caught.value)
        assert "third_party_dispatchr" in message
        assert "third_party_dispatcher" in message

    def test_unknown_name_in_an_empty_group_still_explains_itself(
        self,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """No fake dist installed: the group is empty.

        An empty group is what an operator sees when the package was never
        reinstalled, so the message must not be a bare ``KeyError``.
        """
        with pytest.raises(PluginNotFoundError, match="anything"):
            class_registry.get_plugin("anything")

    def test_wrong_type_behind_an_entry_point_is_rejected(
        self,
        fake_dist: Path,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """A declaration pointing at a non-plugin must fail at discovery.

        Left to run, ``42`` would fail much later inside the plugin manager as
        ``'int' object is not callable``, naming neither the plugin nor the
        declaration that is wrong.
        """
        with pytest.raises(PluginValidationError) as caught:
            class_registry.get_plugin("not_a_plugin")

        assert "not_a_plugin" in str(caught.value)
        assert "Dispatcher" in str(caught.value)

    def test_a_config_object_is_not_accepted_as_a_plugin_class(
        self,
        fake_dist: Path,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """Declaring a config in a class group is a plausible copy-paste slip."""
        with pytest.raises(PluginValidationError):
            class_registry.get_plugin("third_party_config")

    def test_missing_attribute_names_the_plugin_and_its_target(
        self,
        fake_dist: Path,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """A renamed class leaves the entry point pointing at nothing."""
        with pytest.raises(PluginValidationError) as caught:
            class_registry.get_plugin("missing_attribute")

        message = str(caught.value)
        assert "missing_attribute" in message
        assert "fake_courier_plugins:nope" in message

    def test_import_failure_is_chained_not_swallowed(
        self,
        fake_dist: Path,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """The wrapper adds context; it must not hide the real cause."""
        with pytest.raises(PluginValidationError) as caught:
            class_registry.get_plugin("broken_on_import")

        assert isinstance(caught.value.__cause__, RuntimeError)
        assert "this plugin is broken on import" in str(caught.value)


# ── config registries ───────────────────────────────────────────────────────


class TestConfigRegistry:
    def test_a_declared_config_is_returned_as_a_validated_model(
        self,
        fake_dist: Path,
        config_registry: ConfigPluginRegistry[DataMonitorConfig],
    ) -> None:
        """Callers consume ``spec.file_metadata``; a raw dict would defer
        validation to first use, i.e. to a file that then fails to match."""
        config = config_registry.get_plugin("third_party_config")

        assert isinstance(config, DataMonitorConfig)
        assert "entry" in config.spec.file_metadata

    def test_a_plugin_class_is_not_accepted_as_a_config(
        self,
        fake_dist: Path,
        config_registry: ConfigPluginRegistry[DataMonitorConfig],
    ) -> None:
        with pytest.raises(PluginValidationError):
            config_registry.get_plugin("third_party_dispatcher")

    def test_get_plugins_skips_nothing_and_raises_on_the_first_bad_one(
        self,
        fake_dist: Path,
        config_registry: ConfigPluginRegistry[DataMonitorConfig],
    ) -> None:
        """Bulk loading must not quietly drop entries it cannot load.

        Silently skipping is how a renamed plugin turns into a service that
        starts up and processes nothing.
        """
        with pytest.raises(PluginValidationError):
            config_registry.get_plugins()


# ── caching ─────────────────────────────────────────────────────────────────


class TestCaching:
    def test_a_newly_installed_distribution_is_visible_after_refresh(
        self,
        tmp_path: Path,
        class_registry: ClassPluginRegistry,
    ) -> None:
        """Discovery is cached because it walks every installed distribution.

        The cache is why ``refresh()`` exists; without it a test that installs
        a plugin mid-session would see a stale, empty group.
        """
        refresh()
        assert class_registry.names() == []

        _install_fake_distribution(
            tmp_path,
            f"[{_TEST_GROUP}]\nlate = fake_courier_plugins:ThirdPartyDispatcher\n",
        )
        sys.path.insert(0, str(tmp_path))
        try:
            assert class_registry.names() == [], "expected the cached empty group"

            refresh()

            assert class_registry.names() == ["late"]
        finally:
            sys.path.remove(str(tmp_path))
            sys.modules.pop("fake_courier_plugins", None)
            refresh()
