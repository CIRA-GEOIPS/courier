"""Smoke probes against the runtime image that actually ships.

Nothing in this repository had ever executed the published image's default
command, which is why ``CMD ["python", "-m", "courier"]`` shipped for months
against a package that has no ``__main__`` module.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import tomllib
from packaging.version import Version

from tests.docker.conftest import image_labels, run

#: Dependencies the published no-extras image must carry.
CORE_DEPENDENCIES = ("kombu", "watchdog", "jinja2", "pydantic")
#: Dependencies that belong to optional extras and must NOT be present.
#: Plugins needing these import them lazily inside functions, so they stay
#: importable and keep appearing in ``plugins list`` -- the installed set, not
#: the plugin catalogue, is what pins what the image can actually run.
EXTRA_ONLY_DEPENDENCIES = ("croniter", "boto3", "httpx", "kafka")

#: Generous ceiling.  The image measured ~33 MB when this was written; the
#: point is to catch a future COPY or extras change shipping hundreds of MB.
MAX_IMAGE_BYTES = 400 * 1024 * 1024

_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_image_default_command_succeeds(docker_image: str) -> None:
    """The shipped default command runs and exits zero.

    Defends the broken ``python -m courier`` default: there is no
    ``src/courier/__main__.py``, so the published image's default command
    failed for every user who did not override it.
    """
    result = run(["docker", "run", "--rm", docker_image])
    assert result.returncode == 0, result.stderr
    assert "Usage: courier" in result.stdout


def test_image_reports_a_real_version(docker_image: str) -> None:
    """``courier --version`` reports the packaged version, not the fallback.

    The version is read from installed distribution metadata, so a copy that
    loses that metadata degrades to ``0.0.0.dev0`` -- which would also silently
    kill entry-point plugin discovery.  Comparison is by parsed version because
    the image prints the PEP 440 normalised form, e.g. ``1.0.0a29`` for a
    declared ``1.0.0-alpha.29``.
    """
    result = run(["docker", "run", "--rm", docker_image, "courier", "--version"])
    assert result.returncode == 0, result.stderr

    printed = result.stdout.strip().split()[-1]
    assert printed != "0.0.0.dev0", "distribution metadata was lost in the image"

    declared = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())
    expected = declared["project"]["version"]
    assert Version(printed) == Version(expected)


def test_image_validates_the_shipped_config(docker_image: str) -> None:
    """The image can parse the configuration this project ships."""
    config = _REPO_ROOT / "config.yaml"
    result = run(
        [
            "docker", "run", "--rm",
            "-v", f"{config}:/cfg/config.yaml:ro",
            docker_image, "courier", "validate", "/cfg/config.yaml",
        ],
    )
    assert result.returncode == 0, result.stderr
    assert "is valid" in result.stdout


def test_image_carries_the_no_extras_dependency_set(docker_image: str) -> None:
    """Core dependencies are installed and optional-extra ones are not.

    This pins what the published image can actually run.  Asserting on
    ``plugins list`` would not: that reads entry-point metadata, so every
    plugin is listed whether or not its optional dependency is installed.
    """
    probe = (
        "import importlib.util, json;"
        "names = %r + %r;"
        "print(json.dumps({n: importlib.util.find_spec(n) is not None"
        " for n in names}))" % (list(CORE_DEPENDENCIES), list(EXTRA_ONLY_DEPENDENCIES))
    )
    result = run(["docker", "run", "--rm", docker_image, "python", "-c", probe])
    assert result.returncode == 0, result.stderr

    present = json.loads(result.stdout)
    for name in CORE_DEPENDENCIES:
        assert present[name], f"{name} is a core dependency but is missing"
    for name in EXTRA_ONLY_DEPENDENCIES:
        assert not present[name], (
            f"{name} belongs to an optional extra but shipped in the image; "
            "COURIER_EXTRAS may have changed"
        )


def test_image_keeps_its_distribution_metadata(docker_image: str) -> None:
    """Dependency metadata and plugin entry points survive the install.

    The runtime stage copies an install prefix rather than the source tree, so
    this is what proves the copy did not drop the ``dist-info``.
    """
    probe = (
        "import importlib.metadata as m, json;"
        "print(json.dumps({"
        "'requires': len(m.metadata('data-courier').get_all('Requires-Dist') or []),"
        "'dispatchers': sorted(e.name for e in"
        " m.entry_points(group='courier.dispatchers')),"
        "'monitors': sorted(e.name for e in"
        " m.entry_points(group='courier.data_monitors'))}))"
    )
    result = run(["docker", "run", "--rm", docker_image, "python", "-c", probe])
    assert result.returncode == 0, result.stderr

    meta = json.loads(result.stdout)
    assert meta["requires"] > 0, "no Requires-Dist: the dist-info was lost"
    assert "serial_bash" in meta["dispatchers"]
    assert "file_system_poller_watchdog" in meta["monitors"]


def test_image_provides_bash_at_the_hardcoded_path(docker_image: str) -> None:
    """``/bin/bash`` exists, because the bash executor execs that exact path."""
    result = run(
        ["docker", "run", "--rm", docker_image, "/bin/bash", "-c", "echo bash-ok"],
    )
    assert result.returncode == 0, result.stderr
    assert "bash-ok" in result.stdout


def test_image_runs_as_a_non_root_user(docker_image: str) -> None:
    """The runtime image does not run as root."""
    result = run(["docker", "run", "--rm", docker_image, "id", "-u"])
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() != "0"


@pytest.mark.parametrize(
    "label",
    ["org.opencontainers.image.source", "org.opencontainers.image.revision"],
)
def test_published_image_carries_provenance_labels(
    docker_image: str,
    label: str,
) -> None:
    """Published images carry source and revision labels.

    The registry links a package to its repository through the source label, so
    without it the package page is orphaned.  Locally built images have no
    labels, so this only asserts when at least one is present.
    """
    labels = image_labels(docker_image)
    if not labels:
        pytest.skip("locally built image carries no labels; CI images do")
    assert labels.get(label), f"{label} is missing from the image"


def test_image_stays_under_the_size_budget(docker_image: str) -> None:
    """The image stays small.

    A drift guard: nothing else prevents a future ``COPY`` or an extras change
    from quietly shipping a multi-hundred-megabyte image.
    """
    result = run(["docker", "image", "inspect", "-f", "{{.Size}}", docker_image])
    assert result.returncode == 0, result.stderr

    size = int(result.stdout.strip())
    assert size < MAX_IMAGE_BYTES, (
        f"image is {size / 1048576:.0f} MB, over the "
        f"{MAX_IMAGE_BYTES / 1048576:.0f} MB budget; check .dockerignore and "
        "COURIER_EXTRAS"
    )
