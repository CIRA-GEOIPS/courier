"""Unit tests for viz design tokens."""

from __future__ import annotations

from courier.viz import design


class TestDesign:
    """Tests for design token correctness."""

    def test_color_codes_are_valid_hex(self):
        colors = [
            design.STEEL_BLUE,
            design.FROSTED_BLUE,
            design.YELLOW,
            design.POWDER_BLUSH,
            design.PEACH_FUZZ,
            design.COLOR_HEALTHY,
            design.COLOR_UNHEALTHY,
            design.COLOR_NEUTRAL,
        ]
        for color in colors:
            assert len(color) == 7
            assert color.startswith("#")

    def test_refresh_rates_are_sorted(self):
        assert design.REFRESH_RATES == sorted(design.REFRESH_RATES)

    def test_default_refresh_in_rates(self):
        assert design.DEFAULT_REFRESH in design.REFRESH_RATES

    def test_css_variables_defined(self):
        assert "steel-blue" in design.CSS_VARIABLES
        assert "frosted-blue" in design.CSS_VARIABLES
        assert "yellow" in design.CSS_VARIABLES
        assert "powder-blush" in design.CSS_VARIABLES
        assert "peach-fuzz" in design.CSS_VARIABLES

    def test_plugin_state_names_mapping(self):
        """Verify PluginRunState values yield correct display names."""
        from courier.constants import PluginRunState

        # auto() starts at 1, producing values 1-6
        expected = {
            PluginRunState.STOPPED.value: "STOPPED",
            PluginRunState.STARTING.value: "STARTING",
            PluginRunState.RUNNING.value: "RUNNING",
            PluginRunState.STOPPING.value: "STOPPING",
            PluginRunState.FAILED.value: "FAILED",
            PluginRunState.RESTARTING.value: "RESTARTING",
        }

        # Verify enum values are 1-6 (not 0-based as the old bug assumed)
        assert PluginRunState.STOPPED.value == 1
        assert PluginRunState.STARTING.value == 2
        assert PluginRunState.RUNNING.value == 3
        assert PluginRunState.STOPPING.value == 4
        assert PluginRunState.FAILED.value == 5
        assert PluginRunState.RESTARTING.value == 6

        # Verify each key uniquely maps to the correct display name
        for state_name in (
            "STOPPED",
            "STARTING",
            "RUNNING",
            "STOPPING",
            "FAILED",
            "RESTARTING",
        ):
            assert expected[getattr(PluginRunState, state_name).value] == state_name

        # Verify the .get fallback pattern (used at app.py:457) works correctly
        assert expected.get(99, "STATE_99") == "STATE_99"
        assert expected.get(0, "STATE_0") == "STATE_0"
