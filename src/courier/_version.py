"""Version, read from installed distribution metadata.

These were hand-written constants and drifted from ``pyproject.toml`` twice --
most recently sitting at ``1.0.0-alpha.12`` while the project shipped
``1.0.0-alpha.29``, so ``courier.__version__`` reported a version that had not
existed for months. Deriving means a release bumps one file and cannot lie.

Reading metadata requires the distribution to be installed, which is already a
hard requirement: entry-point plugin discovery cannot work without it. The
fallback exists only so a bare source checkout stays importable.

``packaging`` would parse this more rigorously but is available only
transitively, so this uses the standard library.
"""

import re
from importlib.metadata import PackageNotFoundError, version

#: PyPI distribution name. Differs from the import package (``courier``)
#: because ``courier`` was already taken; see RELEASE.md.
DISTRIBUTION_NAME = "data-courier"

try:
    __version__ = version(DISTRIBUTION_NAME)
except PackageNotFoundError:  # pragma: no cover - source tree with no install
    __version__ = "0.0.0.dev0"

# Numeric release prefix only: "1.0.0a29" -> (1, 0, 0), matching the value this
# module has always exposed.
_numeric = re.match(r"^(\d+)\.(\d+)\.(\d+)", __version__)
__version_tuple__ = (
    tuple(int(part) for part in _numeric.groups()) if _numeric else (0, 0, 0)
)
