"""Process-level guard: ``courier run`` must exit when told to.

Every other test in this suite runs the service *in-process*, where a thread
that never stops is invisible: pytest reports success and the hang happens
afterwards, in ``threading._shutdown``. That is exactly how the shutdown bug
survived — the interpreter wedged after the tests passed, so the only symptom
was a CI job that ran until its own timeout.

These tests therefore drive the real ``courier`` console script as a
subprocess and assert on its exit status. They are the only coverage of the
``console_scripts`` entry point, and the only thing that would catch a
non-daemon thread being reintroduced anywhere in the plugin machinery.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

#: Generous enough for a loaded CI box, far below the point at which a genuine
#: hang would be mistaken for slowness.
_SHUTDOWN_GRACE_SECONDS = 30.0

#: How long to wait for the pipeline to prove every thread is live.
_STARTUP_TIMEOUT_SECONDS = 60.0


def _courier_command() -> list[str]:
    """Return the command that runs courier, preferring the console script."""
    console_script = shutil.which(
        "courier",
        path=str(Path(sys.executable).parent) + os.pathsep + (os.environ.get("PATH") or ""),
    )
    if console_script:
        return [console_script]
    pytest.skip("courier console script is not installed in this environment")
    raise AssertionError  # unreachable, keeps type checkers happy


def _write_config(tmp_path: Path, input_dir: Path, output_dir: Path) -> Path:
    """Write a minimal end-to-end service config on the memory transport."""
    config = tmp_path / "service.yaml"
    config.write_text(
        textwrap.dedent(
            f"""\
            apiVersion: runcourier.dev/v1alpha1
            kind: Service
            metadata:
              name: shutdown-probe
              namespace: shutdown-probe
              description: Process lifecycle probe service.
            spec:
              heartbeat_interval: 2
              run:
                - watcher:
                    kind: data_monitor
                    name: cron_glob
                    config:
                      path: {input_dir}
                      glob_pattern: "*.nc"
                      cron_expression: "* * * * *"
                      run_on_start: true
                      ignore_existing: false
                - grouper:
                    kind: job_builder
                    name: DummyJobBuilder
                - runner:
                    kind: dispatcher
                    name: serial_bash
                    config:
                      bash_script: |
                        cp {{{{ files[0].file }}}} {output_dir}/
            """,
        ),
    )
    return config


def _spawn(config: Path) -> subprocess.Popen[str]:
    """Launch ``courier run`` in its own process group."""
    env = {
        **os.environ,
        # No collector is listening; a 5s force_flush per shutdown would
        # muddy the timing this test measures.
        "COURIER_TRACING_ENABLED": "false",
        "PROMETHEUS_PORT": "0",
        "COURIER_LOG_LEVEL": "INFO",
    }
    return subprocess.Popen(
        [*_courier_command(), "run", str(config)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        start_new_session=True,
    )


def _terminate(process: subprocess.Popen[str]) -> None:
    """Best-effort cleanup so a failing test cannot leak a live service."""
    if process.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError, OSError):
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    with contextlib.suppress(subprocess.TimeoutExpired):
        process.wait(timeout=10)


def _wait_for_pipeline(output_file: Path, process: subprocess.Popen[str]) -> None:
    """Block until the pipeline produces *output_file*.

    Waiting on real output is what makes the shutdown assertion meaningful:
    it proves the monitor, builder and dispatcher threads are all running
    their broker loops. Signalling before that could pass even with the bug.
    """
    deadline = time.time() + _STARTUP_TIMEOUT_SECONDS
    while time.time() < deadline:
        if output_file.exists():
            return
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            pytest.fail(
                f"courier run exited early with {process.returncode}:\n{output}",
            )
        time.sleep(0.25)
    _terminate(process)
    pytest.fail(f"pipeline never produced {output_file} within "
                f"{_STARTUP_TIMEOUT_SECONDS}s")


@pytest.mark.parametrize(
    ("signal_name", "sig"),
    [("SIGTERM", signal.SIGTERM), ("SIGINT", signal.SIGINT)],
)
def test_courier_run_exits_on_signal(
    tmp_path: Path,
    signal_name: str,
    sig: signal.Signals,
) -> None:
    """A running service must terminate on a shutdown signal.

    Regression guard for the consume loop never observing its stop event: the
    plugin threads were non-daemon, so the process stayed alive indefinitely
    after the managers had stopped and a container needed SIGKILL.
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    (input_dir / "sample.nc").write_text("payload")

    config = _write_config(tmp_path, input_dir, output_dir)
    process = _spawn(config)

    try:
        _wait_for_pipeline(output_dir / "sample.nc", process)

        os.killpg(os.getpgid(process.pid), sig)
        started_waiting = time.time()
        try:
            process.wait(timeout=_SHUTDOWN_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            pytest.fail(
                f"courier run did not exit within {_SHUTDOWN_GRACE_SECONDS}s of "
                f"{signal_name}; a plugin thread is not observing its stop event",
            )
        elapsed = time.time() - started_waiting
    finally:
        _terminate(process)

    assert elapsed < _SHUTDOWN_GRACE_SECONDS
    # Exit status is not asserted beyond "it exited": a signal-terminated
    # process and a clean-exit-after-signal are both acceptable shutdowns.


def test_courier_run_leaves_no_orphaned_process_group(tmp_path: Path) -> None:
    """After shutdown, nothing from the service's process group survives.

    Dispatchers launch bash subprocesses; a stop that leaves those running
    holds the resources the operator was trying to reclaim.
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    (input_dir / "sample.nc").write_text("payload")

    config = _write_config(tmp_path, input_dir, output_dir)
    process = _spawn(config)
    group_id = os.getpgid(process.pid)

    try:
        _wait_for_pipeline(output_dir / "sample.nc", process)
        os.killpg(group_id, signal.SIGTERM)
        process.wait(timeout=_SHUTDOWN_GRACE_SECONDS)
    finally:
        _terminate(process)

    # os.killpg with signal 0 probes for the group's existence without
    # signalling it; ProcessLookupError means nothing is left.
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            os.killpg(group_id, 0)
        except (ProcessLookupError, PermissionError):
            return
        time.sleep(0.25)
    pytest.fail(f"process group {group_id} still alive after shutdown")


def test_courier_validate_exits_zero_without_starting_a_service(
    tmp_path: Path,
) -> None:
    """``validate`` must be a pure check: exit promptly, start nothing."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    config = _write_config(tmp_path, input_dir, output_dir)

    result = subprocess.run(
        [*_courier_command(), "validate", str(config)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Config valid" in result.stdout
