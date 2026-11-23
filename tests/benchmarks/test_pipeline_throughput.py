"""End-to-end Courier pipeline throughput benchmark.

Measures batches of file events through a running Courier service. pytest-benchmark
reports elapsed time per batch; extra_info records Courier-specific throughput.
"""

import math
import os
import shlex
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import yaml


@pytest.mark.performance
@pytest.mark.integration
@pytest.mark.rabbitmq
def test_pipeline_throughput(tmp_path: Path, benchmark) -> None:
    broker_url = os.environ.get("COURIER_TEST_AMQP_URL")
    if not broker_url:
        pytest.skip("Set COURIER_TEST_AMQP_URL to run the RabbitMQ benchmark")

    file_count = int(os.environ.get("COURIER_BENCHMARK_FILES", "100"))
    timeout = float(os.environ.get("COURIER_BENCHMARK_TIMEOUT", "90"))
    rounds = int(os.environ.get("COURIER_BENCHMARK_ROUNDS", "3"))
    assert file_count > 0 and timeout > 0 and rounds > 0

    inputs = tmp_path / "incoming"
    outputs = tmp_path / "completed"
    inputs.mkdir()
    outputs.mkdir()

    # Each test gets a fresh broker namespace, so previous runs cannot interfere.
    namespace = f"bench-{uuid.uuid4().hex[:12]}"
    config_path = tmp_path / "pipeline.yaml"
    log_path = tmp_path / "courier.log"

    # Every completed payload creates one marker corresponding to its source file.
    script = (
        "set -eu\n"
        'source_file="$1"\n'
        'filename="${source_file##*/}"\n'
        f"output_dir={shlex.quote(str(outputs))}\n"
        'printf "" > "$output_dir/$filename.done"\n'
    )

    config = {
        "apiVersion": "runcourier.dev/v1alpha1",
        "kind": "Service",
        "metadata": {
            "name": "pipeline-benchmark",
            "namespace": namespace,
            "description": "End-to-end CI throughput benchmark",
        },
        "spec": {
            "broker": {"transport": "url", "url": broker_url},
            "service_config": {
                "service_id": namespace,
                "prometheus_port": 0,
                "log_level": "INFO",
                "broker_max_retries": 1,
            },
            "run": [
                {
                    "watch-files": {
                        "kind": "data_monitor",
                        "name": "file_system_poller_watchdog",
                        "config": {"path": str(inputs)},
                    }
                },
                {
                    "build-jobs": {
                        "kind": "job_builder",
                        "name": "filter_and_group",
                        "config": {
                            "files_per_job": 1,
                            "min_files": 1,
                            "targets": ["run-jobs"],
                            "payload": {
                                "write-marker": {
                                    "kind": "payload",
                                    "name": "bash_payload",
                                    "config": {
                                        "script": script,
                                        "suffix_args": ["{{ files[0].file }}"],
                                    },
                                }
                            },
                        },
                    }
                },
                {
                    "run-jobs": {
                        "kind": "dispatcher",
                        "name": "local_dispatcher",
                        "config": {"max_workers": 1},
                    }
                },
            ],
        },
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    def log_tail() -> str:
        return log_path.read_text(encoding="utf-8", errors="replace")[-10000:]

    with log_path.open("w", encoding="utf-8") as log_stream:
        process = subprocess.Popen(
            [sys.executable, "-m", "courier.cli.app", "run", str(config_path)],
            stdout=log_stream,
            stderr=subprocess.STDOUT,
            cwd=tmp_path,
        )

        try:
            startup_deadline = time.monotonic() + 30
            while "Watching for new" not in log_tail():
                assert (
                    process.poll() is None
                ), f"Courier exited at startup:\n{log_tail()}"
                assert (
                    time.monotonic() < startup_deadline
                ), f"Courier watchdog did not start:\n{log_tail()}"
                time.sleep(0.1)

            round_reports: list[dict] = []

            def run_batch() -> None:
                # pytest-benchmark repeats this function. Use new
                # filenames on every round, otherwise old .done files may make
                # subsequent rounds appear to finish instantly.
                prefix = f"round-{uuid.uuid4().hex[:8]}"
                input_names = [f"{prefix}-input-{i:06d}.dat" for i in range(file_count)]
                created_at_ns: dict[str, int] = {}
                pending = set(input_names)
                latency_ms: list[float] = []

                start = time.perf_counter()
                for name in input_names:
                    created_at_ns[name] = time.time_ns()
                    (inputs / name).touch()

                deadline = time.monotonic() + timeout
                while pending and time.monotonic() < deadline:
                    assert (
                        process.poll() is None
                    ), f"Courier exited before completing jobs:\n{log_tail()}"
                    for name in tuple(pending):
                        marker = outputs / f"{name}.done"
                        if marker.exists():
                            latency_ms.append(
                                max(
                                    0.0,
                                    (marker.stat().st_mtime_ns - created_at_ns[name])
                                    / 1e6,
                                )
                            )
                            pending.remove(name)
                    if pending:
                        time.sleep(0.05)

                elapsed = time.perf_counter() - start
                completed = file_count - len(pending)
                assert not pending, (
                    f"Only {completed}/{file_count} jobs completed; "
                    f"missing (first 10): {sorted(pending)[:10]}\n{log_tail()}"
                )

                actual_markers = {
                    path.name for path in outputs.glob(f"{prefix}-input-*.dat.done")
                }
                expected_markers = {f"{name}.done" for name in input_names}
                assert (
                    actual_markers == expected_markers
                ), "Unexpected completion markers"

                latency_ms.sort()
                p95_index = math.ceil(0.95 * len(latency_ms)) - 1
                round_reports.append(
                    {
                        "completed_jobs": completed,
                        "elapsed_seconds": round(elapsed, 3),
                        "jobs_per_second": round(completed / elapsed, 2),
                        "p95_end_to_end_latency_ms": round(latency_ms[p95_index], 2),
                    }
                )

            # Avoid automatic calibration of an expensive, stateful workload.
            # Courier is started once, and 3 distinct batches are timed.
            benchmark.pedantic(run_batch, rounds=rounds, iterations=1)

            benchmark.extra_info["pipeline"] = (
                "watchdog -> filter_and_group -> local_dispatcher -> bash"
            )
            benchmark.extra_info["broker"] = "rabbitmq"
            benchmark.extra_info["files_per_round"] = file_count
            benchmark.extra_info["rounds"] = round_reports
            benchmark.extra_info["aggregate_jobs_per_second"] = round(
                sum(r["completed_jobs"] for r in round_reports)
                / sum(r["elapsed_seconds"] for r in round_reports),
                2,
            )
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
