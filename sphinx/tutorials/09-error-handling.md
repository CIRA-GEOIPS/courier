# Tutorial 9: Error Handling and Recovery

**Level:** Advanced | **Time:** 35 minutes

Learn comprehensive error handling strategies for production GeoIPS
Driver deployments. Implement retry logic, graceful degradation, dead
letter queues, and monitoring for failures.

## Learning Objectives

By the end of this tutorial, you will:

-   Implement robust error handling in plugins
-   Configure retry mechanisms
-   Set up dead letter queues for failed jobs
-   Handle partial failures gracefully
-   Monitor and alert on errors
-   Debug failures effectively
-   Implement circuit breakers

## Prerequisites

-   Completed `` `01-simple-file-watcher ``<span class="title-ref">
    through
    :doc:</span><span class="title-ref">08-production-deployment</span>\`
-   Understanding of failure modes in distributed systems
-   Familiarity with Python exception handling
-   Experience with RabbitMQ

## Understanding Failure Modes

**Common failure scenarios:**

1.  **Data Monitor failures**: File system unavailable, permission
    errors
2.  **Job Builder failures**: Timeout waiting for complete scan, invalid
    metadata
3.  **Dispatcher failures**: GeoIPS errors, resource exhaustion, network
    issues
4.  **Infrastructure failures**: RabbitMQ connection loss, disk full

**Strategies:**

-   **Retry**: Transient failures (network timeouts)
-   **Skip**: Corrupt data, invalid files
-   **Dead Letter Queue**: Persistent failures for later investigation
-   **Circuit Breaker**: Prevent cascading failures
-   **Graceful Degradation**: Continue with partial data

## Step 1: Plugin-Level Error Handling

Create a robust data monitor with error handling:

`plugins/robust_data_monitor.py`:

    """Data monitor with comprehensive error handling."""

    import time
    from pathlib import Path
    from typing import Iterator

    from geoips_driver.interfaces.module_based.data_monitors import DataMonitorBasePlugin
    from geoips_driver.types.file import File

    interface = "data_monitors"
    family = "standard"
    name = "robust_file_monitor"


    class RobustFileMonitor(DataMonitorBasePlugin):
        """File monitor with error handling and retry logic."""

        name = "robust_file_monitor"
        version = "1.0.0"

        def __init__(self, service, config):
            super().__init__(service, config)

            self.max_retries = config.get("max_retries", 3)
            self.retry_delay = config.get("retry_delay_seconds", 5)
            self.skip_on_error = config.get("skip_on_error", True)

        def find_file(self) -> Iterator[File]:
            """Find files with error handling."""
            watch_path = Path(self.config["path"])

            while True:
                try:
                    # Check if path exists
                    if not watch_path.exists():
                        self._logger.error(f"Watch path does not exist: {watch_path}")
                        time.sleep(self.retry_delay)
                        continue

                    # Check if path is accessible
                    if not self._check_path_accessible(watch_path):
                        self._logger.error(f"Watch path not accessible: {watch_path}")
                        time.sleep(self.retry_delay)
                        continue

                    # Scan for files
                    for file_path in watch_path.glob("**/*"):
                        if not file_path.is_file():
                            continue

                        try:
                            file_obj = self._process_file_with_retry(file_path)
                            if file_obj:
                                yield file_obj
                        except Exception as e:
                            self._logger.error(
                                f"Failed to process file {file_path}: {e}",
                                exc_info=True
                            )
                            if not self.skip_on_error:
                                raise

                    time.sleep(1)

                except KeyboardInterrupt:
                    self._logger.info("Monitor stopped by user")
                    break
                except Exception as e:
                    self._logger.error(
                        f"Error in file monitor: {e}",
                        exc_info=True
                    )
                    time.sleep(self.retry_delay)

        def _process_file_with_retry(self, file_path: Path) -> File | None:
            """Process file with retry logic."""
            for attempt in range(self.max_retries):
                try:
                    # Verify file is complete (not still being written)
                    if not self._is_file_complete(file_path):
                        self._logger.debug(f"File not complete, retry {attempt + 1}")
                        time.sleep(self.retry_delay)
                        continue

                    # Create File object
                    file_obj = File(file=file_path, hostname="localhost")

                    # Apply metadata
                    # ... metadata extraction with error handling ...

                    return file_obj

                except PermissionError:
                    self._logger.warning(
                        f"Permission denied for {file_path}, attempt {attempt + 1}"
                    )
                    time.sleep(self.retry_delay)
                except Exception as e:
                    self._logger.error(
                        f"Error processing {file_path}, attempt {attempt + 1}: {e}"
                    )
                    if attempt == self.max_retries - 1:
                        raise
                    time.sleep(self.retry_delay)

            return None

        def _is_file_complete(self, file_path: Path) -> bool:
            """Check if file is complete (not being written)."""
            try:
                # Check file size twice with delay
                size1 = file_path.stat().st_size
                time.sleep(0.5)
                size2 = file_path.stat().st_size

                # File is complete if size hasn't changed
                return size1 == size2 and size1 > 0

            except Exception:
                return False

        def _check_path_accessible(self, path: Path) -> bool:
            """Check if path is readable."""
            try:
                list(path.iterdir())
                return True
            except Exception:
                return False


    def call() -> None:
        """Raise error if called directly."""
        raise NotImplementedError("You cannot call this plugin directly.")

## Step 2: Job Builder Timeout Handling

Handle incomplete scans gracefully:

`plugins/timeout_job_builder.py`:

    """Job builder with timeout and partial scan handling."""

    import time
    from typing import Any

    from geoips_driver.interfaces.module_based.job_builders import (
        Job,
        JobBuilder,
        JobGroup,
    )
    from geoips_driver.types.file import File, FrozenFile

    interface = "job_builders"
    family = "standard"
    name = "timeout_job_builder"


    class TimeoutJob(Job):
        """Job with timeout and partial completion logic."""

        def __init__(self, name, identifier, config):
            super().__init__(name, identifier, config)

            self.min_files = config.get("min_files_to_process", 1)
            self.allow_partial = config.get("allow_partial_scans", True)

        def ready(self) -> bool:
            """Job ready when:
            1. All expected files received, OR
            2. Timeout exceeded AND have minimum files AND partial allowed
            """
            if not self.files:
                return False

            # Get expected count
            first_file = next(iter(self.files))
            expected = first_file.num_expected or 1

            # Check if complete
            if len(self.files) >= expected:
                return True

            # Check timeout
            if not self.is_timeout():
                return False

            # Partial completion allowed?
            if self.allow_partial and len(self.files) >= self.min_files:
                return True

            return False


    class TimeoutJobGroup(JobGroup):
        """Job group with timeout handling."""

        def __init__(self, config: dict[str, Any]) -> None:
            super().__init__("TimeoutJob", config)
            self.job = TimeoutJob

            self.timeout = config.get("timeout_seconds", 300)
            self.check_interval = config.get("check_interval", 10)
            self.dead_letter_enabled = config.get("dead_letter_enabled", True)

        def check_timeouts(self) -> list[str]:
            """Check for timed out jobs."""
            timed_out = []
            current_time = time.time()

            for job_id, job in list(self.jobs.items()):
                if job.is_timeout():
                    if job.ready():
                        # Job is ready (partial completion)
                        self._logger.warning(
                            f"Job {job_id} ready with partial data: "
                            f"{len(job.files)}/{job.files[0].num_expected} files"
                        )
                    else:
                        # Job failed - not enough files
                        self._logger.error(
                            f"Job {job_id} timed out with insufficient files: "
                            f"{len(job.files)}/{job.files[0].num_expected}"
                        )

                        if self.dead_letter_enabled:
                            self._send_to_dead_letter(job)

                        timed_out.append(job_id)
                        del self.jobs[job_id]

            return timed_out

        def _send_to_dead_letter(self, job: Job) -> None:
            """Send failed job to dead letter queue."""
            self._logger.info(f"Sending job {job.identifier} to dead letter queue")
            # Implementation depends on RabbitMQ setup
            # ... send to DLQ ...

## Step 3: Dispatcher Retry Logic

Implement retry with exponential backoff:

`plugins/retry_dispatcher.py`:

    """Dispatcher with comprehensive retry logic."""

    import subprocess
    import time
    from typing import Any

    from geoips_driver.interfaces.module_based.dispatchers import (
        Dispatcher,
        ExecutionLog,
    )
    from geoips_driver.interfaces.module_based.job_builders import Job

    interface = "dispatchers"
    family = "standard"
    name = "retry_dispatcher"


    class RetryDispatcher(Dispatcher):
        """Dispatcher with exponential backoff retry."""

        name = "retry_dispatcher"
        version = "1.0.0"

        # Errors that should trigger retry
        RETRYABLE_ERRORS = [
            "ConnectionError",
            "TimeoutError",
            "TemporaryFailure",
        ]

        # Errors that should not be retried
        FATAL_ERRORS = [
            "FileNotFoundError",
            "PermissionError",
            "ValidationError",
        ]

        def __init__(self, service, config: dict[str, Any]) -> None:
            super().__init__(service, config)

            self.max_retries = config.get("max_retries", 3)
            self.initial_delay = config.get("initial_delay_seconds", 5)
            self.max_delay = config.get("max_delay_seconds", 300)
            self.backoff_factor = config.get("backoff_factor", 2)
            self.dead_letter_enabled = config.get("dead_letter_enabled", True)

        def get_execution_log(self, job: Job) -> list[ExecutionLog]:
            """Execute job with retry logic."""
            logs = []
            delay = self.initial_delay

            for attempt in range(self.max_retries):
                self._logger.info(
                    f"Executing job {job.identifier}, attempt {attempt + 1}/{self.max_retries}"
                )

                try:
                    log = self._execute_job(job)
                    logs.append(log)

                    if log.return_code == 0:
                        self._logger.info(f"Job {job.identifier} succeeded")
                        return logs

                    # Check if error is retryable
                    if self._is_fatal_error(log):
                        self._logger.error(
                            f"Job {job.identifier} failed with fatal error"
                        )
                        break

                    # Retry on retryable error
                    if attempt < self.max_retries - 1:
                        self._logger.warning(
                            f"Job {job.identifier} failed, retrying in {delay}s"
                        )
                        time.sleep(delay)
                        delay = min(delay * self.backoff_factor, self.max_delay)

                except Exception as e:
                    self._logger.error(
                        f"Exception executing job {job.identifier}: {e}",
                        exc_info=True
                    )

                    if attempt < self.max_retries - 1:
                        time.sleep(delay)
                        delay = min(delay * self.backoff_factor, self.max_delay)

            # All retries exhausted
            self._logger.error(
                f"Job {job.identifier} failed after {self.max_retries} attempts"
            )

            if self.dead_letter_enabled:
                self._send_to_dead_letter(job, logs)

            return logs

        def _execute_job(self, job: Job) -> ExecutionLog:
            """Execute the job once."""
            file_paths = " ".join(str(f.file) for f in job.files)

            script = self.config["bash_script"].replace("{file}", file_paths)

            result = subprocess.run(
                ["/bin/bash", "-c", script],
                capture_output=True,
                text=True,
                timeout=self.config.get("timeout_seconds", 3600),
            )

            return ExecutionLog(
                return_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                hostname="localhost",
            )

        def _is_fatal_error(self, log: ExecutionLog) -> bool:
            """Check if error is fatal (should not retry)."""
            for error in self.FATAL_ERRORS:
                if error in log.stderr:
                    return True
            return False

        def _send_to_dead_letter(self, job: Job, logs: list[ExecutionLog]) -> None:
            """Send failed job to dead letter queue."""
            self._logger.info(
                f"Sending job {job.identifier} to dead letter queue"
            )
            # Implementation: send to DLQ with failure details

    [``
    Step 4: Circuit Breaker Pattern
    -------------------------------

    Prevent cascading failures:

    ``plugins/circuit_breaker_dispatcher.py``:

    ```python

    """Dispatcher with circuit breaker pattern."""

    import time
    from enum import Enum
    from typing import Any

    from geoips_driver.interfaces.module_based.dispatchers import Dispatcher

    interface = "dispatchers"
    family = "standard"
    name = "circuit_breaker_dispatcher"


    class CircuitState(Enum):
        """Circuit breaker states."""
        CLOSED = "closed"  # Normal operation
        OPEN = "open"      # Failing, reject requests
        HALF_OPEN = "half_open"  # Testing if recovered


    class CircuitBreaker:
        """Circuit breaker for fault tolerance."""

        def __init__(self, config: dict[str, Any]):
            self.failure_threshold = config.get("failure_threshold", 5)
            self.recovery_timeout = config.get("recovery_timeout_seconds", 60)
            self.half_open_max_calls = config.get("half_open_max_calls", 3)

            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None
            self.half_open_calls = 0

        def call(self, func, *args, **kwargs):
            """Execute function with circuit breaker."""
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to_half_open()
                else:
                    raise Exception("Circuit breaker is OPEN")

            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except Exception as e:
                self._on_failure()
                raise

        def _on_success(self):
            """Handle successful call."""
            if self.state == CircuitState.HALF_OPEN:
                self.half_open_calls += 1
                if self.half_open_calls >= self.half_open_max_calls:
                    self._transition_to_closed()
            else:
                self.failure_count = 0

        def _on_failure(self):
            """Handle failed call."""
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == CircuitState.HALF_OPEN:
                self._transition_to_open()
            elif self.failure_count >= self.failure_threshold:
                self._transition_to_open()

        def _should_attempt_reset(self) -> bool:
            """Check if enough time has passed to try recovery."""
            if self.last_failure_time is None:
                return True
            return time.time() - self.last_failure_time >= self.recovery_timeout

        def _transition_to_open(self):
            """Transition to OPEN state."""
            self.state = CircuitState.OPEN
            self.last_failure_time = time.time()

        def _transition_to_half_open(self):
            """Transition to HALF_OPEN state."""
            self.state = CircuitState.HALF_OPEN
            self.half_open_calls = 0

        def _transition_to_closed(self):
            """Transition to CLOSED state."""
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.half_open_calls = 0



    class CircuitBreakerDispatcher(Dispatcher):
        """Dispatcher with circuit breaker."""

        name = "circuit_breaker_dispatcher"
        version = "1.0.0"

        def __init__(self, service, config):
            super().__init__(service, config)
            self.circuit_breaker = CircuitBreaker(config)

        def get_execution_log(self, job):
            """Execute with circuit breaker protection."""
            try:
                return self.circuit_breaker.call(self._execute_job, job)
            except Exception as e:
                if "Circuit breaker is OPEN" in str(e):
                    self._logger.error(
                        "Circuit breaker is OPEN - rejecting job to prevent "
                        "cascading failures"
                    )
                    # Return error log
                    return [ExecutionLog(
                        return_code=-1,
                        stdout="",
                        stderr="Circuit breaker is OPEN",
                        hostname="localhost",
                    )]
                raise

## Step 5: Dead Letter Queue Setup

Configure RabbitMQ dead letter exchange:

`scripts/setup_dlq.sh`:

    #!/bin/bash

    Setup dead letter queues in RabbitMQ
    ====================================

    RABBITMQ_HOST="localhost"
    RABBITMQ_PORT="15672"
    RABBITMQ_USER="admin"
    RABBITMQ_PASS="admin"

    Create dead letter exchange
    ===========================
    curl -u ${RABBITMQ_USER}:${RABBITMQ_PASS} \
        -X PUT \
        http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/exchanges/%2F/dlx \
        -H "content-type:application/json" \
        -d '{"type":"topic","durable":true}'

    Create dead letter queue
    ========================
    curl -u ${RABBITMQ_USER}:${RABBITMQ_PASS} \
        -X PUT \
        http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/queues/%2F/failed-jobs \
        -H "content-type:application/json" \
        -d '{"durable":true}'

    Bind dead letter queue to exchange
    ==================================
    curl -u ${RABBITMQ_USER}:${RABBITMQ_PASS} \
        -X POST \
        http://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api/bindings/%2F/e/dlx/q/failed-jobs \
        -H "content-type:application/json" \
        -d '{"routing_key":"#"}'

Configure queues to use DLX:

    In RabbitMQ manager setup
    =========================
    channel.queue_declare(
        queue="JobReadyQueue",
        durable=True,
        arguments={
            "x-dead-letter-exchange": "dlx",
            "x-message-ttl": 86400000,  # 24 hours
            "x-max-length": 10000,
        }
    )

## Step 6: Monitoring and Alerting for Errors

Create error metrics:

    from prometheus_client import Counter, Gauge

    Error counters
    ==============
    errors_total = Counter(
        'errors_total',
        'Total errors by type and component',
        ['component', 'error_type', 'severity']
    )

    Dead letter queue depth
    =======================
    dlq_depth = Gauge(
        'dead_letter_queue_depth',
        'Number of messages in dead letter queue'
    )

    Circuit breaker state
    =====================
    circuit_breaker_state = Gauge(
        'circuit_breaker_state',
        'Circuit breaker state (0=closed, 1=half-open, 2=open)',
        ['component']
    )

Alert rules:

    groups:
      - name: error_alerts
        rules:
          - alert: HighErrorRate
            expr: rate(errors_total{severity="error"}[5m]) > 0.1
            for: 5m
            annotations:
              summary: "High error rate detected"

          - alert: DeadLetterQueueGrowing
            expr: delta(dead_letter_queue_depth[10m]) > 10
            for: 10m
            annotations:
              summary: "Dead letter queue is growing"

          - alert: CircuitBreakerOpen
            expr: circuit_breaker_state == 2
            for: 1m
            annotations:
              summary: "Circuit breaker is open"

## Step 7: Complete Error-Resilient Configuration

`tutorial09-error-handling/config.yaml`:

    apiVersion: geoips_driver/v1
    kind: Service
    name: error-resilient-service
    description: Production service with comprehensive error handling.

    spec:
      service_namespace: error_resilient
      heartbeat_interval: 30

      rabbitmq:
        host: localhost
        port: 5672
        username: admin
        password: admin_test

      run:
        - monitor:
            kind: data_monitor
            name: robust_file_monitor
            config:
              path: ./data/incoming
              metadata-tools: [goes18_abi]
              max_retries: 3
              retry_delay_seconds: 5
              skip_on_error: true

        - build:
            kind: job_builder
            name: timeout_job_builder
            config:
              timeout_seconds: 600
              min_files_to_process: 10
              allow_partial_scans: true
              dead_letter_enabled: true

        - process:
            kind: dispatcher
            name: circuit_breaker_dispatcher
            config:
              bash_script: |
                geoips run single_source {file}
              max_retries: 3
              initial_delay_seconds: 5
              backoff_factor: 2
              failure_threshold: 5
              recovery_timeout_seconds: 60
              dead_letter_enabled: true

## Step 8: Error Analysis and Debugging

Tools for debugging failed jobs:

`scripts/analyze_failures.py`:

    """Analyze failed jobs from dead letter queue."""

    import pika
    import json

    def analyze_dlq():
        """Pull and analyze messages from DLQ."""
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        channel = connection.channel()

        failures_by_type = {}

        while True:
            method, properties, body = channel.basic_get('failed-jobs')

            if method is None:
                break

            # Parse failed job
            job_data = json.loads(body)
            error_type = job_data.get('error_type', 'unknown')

            if error_type not in failures_by_type:
                failures_by_type[error_type] = []

            failures_by_type[error_type].append(job_data)

            # Acknowledge message
            channel.basic_ack(method.delivery_tag)

        # Print summary
        print("Failed Jobs Summary:")
        for error_type, jobs in failures_by_type.items():
            print(f"  {error_type}: {len(jobs)} jobs")

        connection.close()

    if __name__ == "__main__":
        analyze_dlq()

## What You Learned

✅ Comprehensive error handling strategies ✅ Retry with exponential
backoff ✅ Dead letter queue configuration ✅ Circuit breaker pattern ✅
Partial failure handling ✅ Error monitoring and alerting ✅ Debugging
failed jobs

## Next Steps

-   `` `10-testing-plugins ``\` - Test error handling
-   :doc:`../user-guide/troubleshooting` - Debugging guide
-   :doc:`../developer-guide/testing` - Testing strategies

## Challenge Exercises

1.  **Implement rate limiting** - Prevent resource exhaustion
2.  **Add failure notifications** - Email/Slack alerts
3.  **Create recovery scripts** - Reprocess DLQ messages
4.  **Implement bulkheads** - Isolate failure domains

## Complete Code

\`tutorial09-error-handling/
\](<https://github.com/biosafetylvl5/geoips_driver/tree/main/examples/tutorials/09-error-handling>)
