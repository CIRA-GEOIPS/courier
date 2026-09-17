"""Container-level tests that drive real images built from the Dockerfile.

This lives in a subdirectory rather than beside the other integration modules
because two drift guards glob ``tests/*.yaml`` NON-recursively
(``tests/test_shipped_config_drift.py`` and
``tests/unit_tests/cli/test_cli_smoke.py``).  Fixture configs written here are
therefore invisible to both, and neither of their duplicated exclusion sets
needs an entry.  Move this package up a level and both guards will start
failing on its fixtures.
"""
