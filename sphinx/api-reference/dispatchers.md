# Dispatchers Reference

A **dispatcher** executes jobs. It consumes from its own queue,
`<namespace>-JobReady-<identifier>`, and runs the {doc}`payload <payloads>`
each job carries. What runs is configured on the job builder; the dispatcher
decides where and how it runs, and what happens to the output.

Courier ships two dispatchers:

| Plugin             | Runs payloads                                 |
| ------------------ | --------------------------------------------- |
| `local_dispatcher` | As subprocesses on the dispatcher's own host. |
| `slurm_dispatcher` | As Slurm batch jobs submitted with `sbatch`.  |

Both accept `shell_payload`, `bash_payload` and `python_payload`.

## How a dispatcher handles a job

1. **Parse.** A message body that is not a valid job is parked (see step 3).
1. **Deduplicate.** The dispatcher remembers the last 1024 jobs it has seen,
   keyed on `(payload identifier, job identifier)`. A repeat is skipped with an
   INFO line, `Duplicate job <id> (payload '<payload>'); skipping`, and counted
   in `courier_dispatcher_dedupe_skips_total`. Jobs from two builders that
   happen to share an identifier are both run.
1. **Resolve the payload.** The dispatcher loads the payload plugin named on
   the job, picks the representation to run it as, validates the payload's
   config, and checks its `toolchain` (once per payload configuration; see
   `toolchain` and `toolchain_prepend` in {doc}`payloads` for each payload's
   probe). If any of this fails, or the job carries no payload at all, the
   job is **unexecutable**: the dispatcher logs an ERROR, counts it as
   `courier_dispatcher_jobs_processed_total{status="unexecutable"}`, and parks
   the original message on `<namespace>-JobReady-<identifier>-DeadLetter`
   with the reason in the `x-courier-park-reason` header. Parked messages also
   count towards `courier_broker_messages_dead_lettered_total`.
1. **Write the script.** The rendered script the job carries is written to a
   new file with a random name, created exclusively with mode `0755`:
   `courier-XXXXXXXX<suffix>` in the temporary directory (`$TMPDIR` when set)
   for `local_dispatcher`, `<job id>-XXXXXXXX<suffix>` in `slurm_output_dir`
   for `slurm_dispatcher`. The dispatcher fills in the values only it knows
   (`script_path`, `hostname`, `dispatcher.*`) as it writes, and renders the
   command arguments. A payload with no script (a `binary` only) gets no file.
1. **Run it.** The payload's command runs with the options below. When it
   returns, `local_dispatcher` removes the script; see
   {ref}`script-lifetime` for when `slurm_dispatcher` does.
1. **Collect output files.** The `output_files` patterns are matched against
   the job's output (see {ref}`output-files`).
1. **Publish.** Each output file is published to the file-found exchange,
   then each execution log to the dispatcher queue, and the job is counted as
   `courier_dispatcher_jobs_processed_total{status="success"}`. That status
   means the dispatcher executed the job. A script that exits non-zero is
   logged at ERROR with its return code, marks the trace span as an error, and
   is counted in `courier_payload_jobs_processed_total{status="failure"}`.

If anything else goes wrong while preparing or running a job, or while
collecting its output files (a dispatcher-only value the template uses that
this dispatcher does not define, an error rendering the command arguments, a
script that cannot be written, a bug in a custom dispatcher's hook), the error
is logged at ERROR with its traceback, the script is removed (unless a
submitted Slurm job may still read it), and the job counts as
`status="failure"`. The dispatcher carries on with the next job either way.

Only a fault the dispatcher cannot pin on one job ends the `courier run`
process: a failure to publish a job's output files or execution logs
(`TransientBrokerError`, `FatalBrokerError` or a raw transport error), a
failure to consume, or a failure to park a message. A publish failure is the
broker's, not the job's, so the job is counted as neither a success nor a
failure. The message in hand is not acknowledged as done: it is retried as
described in {doc}`../concepts/adr/0010-poison-message-handling`. A retried
job runs again, so output files published before the fault can be published
twice.

`local_dispatcher` logs nothing at INFO for a job that runs normally
(`slurm_dispatcher` logs each submission). To follow jobs in the log, set
`log_to_logger: true` (the script's output is then logged at DEBUG and
WARNING), or watch `courier_dispatcher_jobs_processed_total`.

### Parked jobs

A parked message is kept verbatim, so it can be moved back onto the
`JobReady` queue once the cause is fixed: install the missing payload plugin
or tool, or fix the dispatcher's config. `courier queues list <config>` names
each dead-letter queue; read its depth with the broker's own tools
(`rabbitmqctl list_queues -p <vhost> name messages`, or the management UI).
A job parked because it carries no payload was published by a job builder
from an older release, since every current builder attaches one; see
{doc}`../getting-started/upgrading` before re-driving it.

## Options for every dispatcher

These options are accepted by every dispatcher (`DispatcherGroupConfig`).
Unknown keys are rejected, and keys removed from older dispatchers fail with a
message that names their replacement (see
{doc}`../getting-started/upgrading`). A payload setting placed here (a field
of any installed payload's config model, such as `script` or `prefix_args`)
is reported as belonging in the job builder's payload block.

| Option            | Type                         | Default  | Description                                                                                                                                                                                                                     |
| ----------------- | ---------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `timeout_seconds` | `float` > 0                  | `3600.0` | Maximum run time of a job's payload process. On expiry the whole process group is sent SIGTERM (then SIGKILL after 5 s), the return code is `-1`, and stderr ends with `Script execution timed out after <n>s`.                 |
| `log_to_logger`   | `bool`                       | `false`  | Log the payload's output line by line through the payload plugin's logger: stdout at DEBUG, stderr at WARNING, each line prefixed with `[job: <id>] [stdout]` or `[job: <id>] [stderr]`.                                        |
| `log_to_file`     | `bool`                       | `false`  | Write the payload's stdout and stderr, each line prefixed `[stdout]` or `[stderr]`, to `<log_dir>/dispatch_<job-id>_<timestamp>.log`. The execution log's `log_file_path` names the file. Requires `log_dir`.                   |
| `log_dir`         | `str`                        | `""`     | Directory for `log_to_file`. See [The log directory](#the-log-directory).                                                                                                                                                       |
| `log_only_errors` | `bool`                       | `false`  | Discard the payload's stdout entirely: it is not logged, not written to the log file, and not kept in the execution log. `COURIER_METRIC:` lines are then never seen, and `output_files` can only match stderr (`scan_stderr`). |
| `scan_stderr`     | `bool`                       | `false`  | Also scan stderr for `output_files` patterns.                                                                                                                                                                                   |
| `output_files`    | list of patterns (see below) | `None`   | Patterns that find output files in the payload's output and feed them back into the pipeline.                                                                                                                                   |

The payload process inherits the dispatcher's environment, and runs with the
dispatcher's working directory.

```yaml
- process:
    kind: dispatcher
    name: local_dispatcher
    config:
      timeout_seconds: 1800
      log_to_logger: true
      log_to_file: true
      log_dir: /var/log/courier
```

### The log directory

`log_dir` is checked, and created, only with `log_to_file: true`, which
requires it (`log_dir is required when log_to_file=True`).

- **`courier run`** prepares it when it builds the dispatcher at startup, on
  the host (or in the container) the dispatcher runs on, and only in a process
  that runs that dispatcher step. A missing directory is created, with its
  parents. Startup fails if the path is not a directory
  (`log_dir is not a directory: <path>`), cannot be created
  (`log_dir cannot be created: <path>: <error>`), or is not writable
  (`log_dir is not writable: <path>`). `slurm_dispatcher` inherits this, so it
  creates `log_dir` too, although `log_to_file` does not apply to Slurm jobs.
- **`courier validate`** never creates it, because it usually runs somewhere
  else. It checks the path where it runs and, when the directory is not
  usable there, prints one of these notes (a note, not an error: the config
  is still valid):

```text
note: process.config.log_dir: /var/log/courier does not exist here; `courier run` creates it when it builds the dispatcher at startup, and fails to start if it cannot create or write it where it runs
note: process.config.log_dir: /var/log/courier exists here but is not writable; `courier run` does not change its permissions, and fails to start unless it is writable where it runs
note: process.config.log_dir: /var/log/courier exists here but is not a directory; `courier run` fails to start unless it is a writable directory, or can be created as one, where it runs
```

(output-files)=

## Feeding output back into the pipeline

`output_files` lists regular expressions that are matched against the
payload's stdout (and stderr with `scan_stderr`) in multi-line mode, so `^` and
`$` anchor to each line. Each pattern must have a named group `file`. Every
distinct path the patterns match is emitted as a new file, with the
dispatcher's host name, into the same exchange data monitors publish to, so
downstream job builders can pick it up. This is how multi-stage pipelines are
chained; see {ref}`pipeline-feedback-example`.

| Field              | Type             | Default      | Description                                                            |
| ------------------ | ---------------- | ------------ | ---------------------------------------------------------------------- |
| `pattern`          | `str`            | *(required)* | Regular expression with a `(?P<file>...)` group. Validated at startup. |
| `source`           | `str`            | `None`       | Source for every matched file. Lower-cased.                            |
| `instrument`       | `str`            | `None`       | Instrument for every matched file. Lower-cased.                        |
| `processing_stage` | `str`            | `None`       | Processing stage for every matched file. Lower-cased.                  |
| `domain`           | `str`            | `None`       | Domain for every matched file. Upper-cased.                            |
| `metadata`         | `dict[str, Any]` | `{}`         | Static metadata merged into every matched file.                        |

Named groups called `source`, `instrument`, `processing_stage` or `domain`
override the static values, with the same case rules. A named group
`timestamp` in `YYYYmmddTHHMM` form sets the file's timestamp (as UTC). Any
other named group is stored in the file's `metadata`.

```yaml
config:
  output_files:
    - pattern: '^OUTPUT:\s+(?P<file>/data/l2/.+)$'
      processing_stage: l2
```

```{warning}
Give emitted files metadata that the producing stage's job builder filters
out, such as a new `processing_stage`. Otherwise the stage picks up its own
output and loops forever.
```

Avoid nested quantifiers such as `(a+)+` in patterns: they can backtrack
catastrophically on long output.

## `local_dispatcher`

`courier.plugins.dispatchers.local_dispatcher.LocalDispatcher` runs each job's
payload as a subprocess on the dispatcher's host, one job at a time. It takes
only the options above.

To run more jobs at once, run more replicas of the same dispatcher identifier
(each in its own process or container) against the same queue. Within one job
the payload decides what runs in parallel.

(courier-metric-stdout)=

### Custom metrics: the `COURIER_METRIC:` stdout protocol

After every job, `local_dispatcher` scans the job's stdout for lines of the
form

```text
COURIER_METRIC: <metric_name> <numeric_value>
```

and sets the Prometheus gauge

```text
courier_custom_gauge{dispatcher_identifier="<dispatcher identifier>", metric_name="<metric_name>"}
```

to the value. Leading and trailing whitespace on the line is ignored,
`metric_name` is any run of non-space characters, and the value is parsed as a
float. A line that starts with `COURIER_METRIC:` but does not carry a name and
a numeric value (`COURIER_METRIC: x abc`, `... 1.2.3`, `... nan`) is skipped
with a WARNING; it never fails the job. The gauge
keeps its last value until the next job updates it, and is served on the
service's `/metrics` endpoint with every other courier metric.
`slurm_dispatcher` does not read these lines, and with `log_only_errors` there
is no stdout to read.

A bash payload that reports scan-to-product latency:

```bash
NOW=$(date -u +%s)
SCAN_EPOCH=$(date -u -d "$SCAN_TIME" +%s)
echo "COURIER_METRIC: scan_to_product_latency_seconds $((NOW - SCAN_EPOCH))"
```

The same from a Python payload:

```python
import datetime as dt

latency = (dt.datetime.now(dt.UTC) - scan_time).total_seconds()
print(f"COURIER_METRIC: scan_to_product_latency_seconds {latency:.1f}")
```

Query it by dispatcher and metric name:

```text
courier_custom_gauge{dispatcher_identifier="process", metric_name="scan_to_product_latency_seconds"}
```

The generated Grafana dashboard's "Pipeline Latency (scan-to-product)" panel
reads this gauge.

## `slurm_dispatcher`

`courier.plugins.dispatchers.slurm_dispatcher.SlurmDispatcher` submits each
job's payload to Slurm with `sbatch` and, by default, waits for it to finish.
The host running the dispatcher must be a Slurm submit host: `sbatch` must be
on `PATH`, and `sacct` too when `wait_for_completion` is on. The dispatcher
refuses to start otherwise, or if it cannot create `slurm_output_dir`.

### Options

`SlurmDispatcherConfig` adds these options to the ones every dispatcher takes:

| Option                       | Type        | Default      | Description                                                                                                                                                                 |
| ---------------------------- | ----------- | ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `slurm_output_dir`           | `str`       | *(required)* | Directory for job scripts and Slurm's `.out`/`.err` files, made absolute on the dispatcher host. Created at startup. Must be on a filesystem shared with the compute nodes. |
| `poll_interval_seconds`      | `float` > 0 | `30.0`       | Seconds between `sacct` polls while waiting.                                                                                                                                |
| `max_concurrent_jobs`        | `int` ≥ 1   | `10`         | Most submissions this dispatcher has in flight at once.                                                                                                                     |
| `partition`                  | `str`       | `None`       | `--partition`.                                                                                                                                                              |
| `account`                    | `str`       | `None`       | `--account`.                                                                                                                                                                |
| `qos`                        | `str`       | `None`       | `--qos`.                                                                                                                                                                    |
| `time_limit`                 | `str`       | `None`       | `--time`, in any format Slurm accepts (`01:00:00`).                                                                                                                         |
| `ntasks`                     | `int` ≥ 1   | `None`       | `--ntasks`.                                                                                                                                                                 |
| `mem_per_node`               | `str`       | `None`       | `--mem`.                                                                                                                                                                    |
| `wait_for_completion`        | `bool`      | `true`       | Wait for the Slurm job to reach a terminal state and report its outcome. When `false`, a job is done as soon as `sbatch` accepts it.                                        |
| `submission_timeout_seconds` | `float` > 0 | `60.0`       | Time limit for each `sbatch` and `sacct` call.                                                                                                                              |
| `polling_timeout_seconds`    | `float` > 0 | `86400.0`    | How long to wait for a submitted job to finish before giving up on it.                                                                                                      |
| `sbatch_extra_args`          | `list[str]` | `[]`         | Extra `sbatch` options, appended after the ones above (`["--gres=gpu:1"]`).                                                                                                 |

Every submission also gets `--parsable`, `--job-name=courier-<job id>`,
`--output=<slurm_output_dir>/<job id>-%j.out` and
`--error=<slurm_output_dir>/<job id>-%j.err`, where `<job id>` is the courier
job identifier made safe for a file name and `%j` is the Slurm job id, so two
submissions of one job never share output files.

Of the options every dispatcher takes, `timeout_seconds`, `log_to_file`,
`log_dir` and `log_only_errors` describe a local process and do not apply to
the Slurm job: `sbatch` and `sacct` are bounded by
`submission_timeout_seconds`, the job's run time by `time_limit`, and its
output goes to the `.out` and `.err` files. Setting one logs a WARNING at
startup. `log_to_logger`, `output_files` and `scan_stderr` apply to the job's
output in wait mode, once the job has finished.

```yaml
- submit:
    kind: dispatcher
    name: slurm_dispatcher
    config:
      slurm_output_dir: /shared/courier/slurm
      partition: batch
      time_limit: "02:00:00"
      sbatch_extra_args: ["--cpus-per-task=4"]
```

### How a payload is submitted

The rendered script is written to `slurm_output_dir` under a new, random name
that starts with the job identifier, created exclusively. In a payload
template, the reserved name `output_dir` holds `slurm_output_dir`; like every
dispatcher-only value it is used as a bare value, `{{ output_dir }}/x`, as
{doc}`payloads` describes.

- **As a batch script.** A `shell_payload` or `bash_payload` whose script runs
  directly (no `binary`, no `prefix_args`) is submitted as the batch script
  itself: `sbatch <options> <script> [suffix_args...]`. (`toolchain_prepend`
  has no bearing on this: it is never part of a job's command.
  `python_payload` puts it in front of the interpreter in its `toolchain`
  probes only, and `shell_payload` and `bash_payload` ignore it.) If the
  script has no shebang line, one naming the payload's interpreter is added
  (`#!/usr/bin/env bash`, or `#!<path>` for an absolute `default_binary`).
  `#SBATCH` directives at the top of the script apply, except where the
  command line sets the same option: the dispatcher always passes
  `--job-name`, `--output` and `--error`, and passes `--partition`,
  `--account`, `--qos`, `--time`, `--ntasks`, `--mem` and `sbatch_extra_args`
  when configured, and `sbatch` lets command-line options override `#SBATCH`
  lines.
- **With `--wrap`.** Everything else is submitted as a wrapped command:
  `python_payload`, payloads with a `binary`, and payloads with
  `prefix_args`, whose interpreter options must never be read as `sbatch`
  options. The wrapped command is the argv a `local_dispatcher` would run,
  each argument shell-quoted (an argument that renders to an empty string
  stays an empty argument). It reads the script from `slurm_output_dir` when
  the job starts, so the directory must be visible from the compute nodes,
  and `#SBATCH` lines in the script are not read.

Each accepted submission is logged at INFO:
`Submitted job '<id>' as SLURM job <n>`. When `sbatch` places the job on a
named cluster (`--clusters` in `sbatch_extra_args`), it reports
`<n>;<cluster>`, the log line ends `on cluster <cluster>`, and `sacct` is
asked about the job on that cluster.

In wait mode the dispatcher polls `sacct` until the job reaches a terminal
state, then reports the job's own outcome: return code `0` for `COMPLETED`,
otherwise the job's exit code (or `-1`), with the `.out` and `.err` files as
stdout and stderr, and a `SLURM job <n> ended with state <state>` line added to
stderr for a failed job. `output_files` are scanned in that output, and the
payload metrics record the Slurm job (its run time as `sacct` reports it), not
the `sbatch` call. If `polling_timeout_seconds` expires first, the execution
log has return code `-1` and says the job may still be running, and nothing is
recorded in the payload metrics.

In no-wait mode the execution log only records the submission
(`SLURM job <n> submitted`), so there is nothing to scan, and the dispatcher
logs where the job's output will be.

When `sbatch` rejects a submission, the rejection is logged at ERROR and
counted, and the execution log carries `sbatch`'s return code and its stderr.

The payload's `toolchain` is checked on the submit host, not on a compute node,
with `python_payload`'s `toolchain_prepend` in front of each probe.

(script-lifetime)=

### How long the script is kept

- **Batch script.** Slurm keeps its own copy, so the dispatcher's copy is
  removed as soon as the dispatcher is done with the job: right after
  submission in no-wait mode (even though the job may not have started),
  once polling ends in wait mode (including when `polling_timeout_seconds`
  expires), and when `sbatch` rejects it. `{{ script_path }}` names the
  dispatcher's copy, so a batch script should not rely on it in no-wait mode.
- **`--wrap` command.** The job reads the script from `slurm_output_dir` when
  it starts, so the script is kept for as long as a job may still read it.
  It is removed only when `sbatch` refuses the submission with a non-zero exit
  code of its own, or, in wait mode, when the job reaches a terminal state
  other than `PREEMPTED` or `NODE_FAIL` (states Slurm may requeue from). It
  is left in `slurm_output_dir` in no-wait mode, when polling gives up, after
  `PREEMPTED` or `NODE_FAIL`, and when the outcome of `sbatch` is unknown (it
  timed out, was killed, or exited 0 without printing a job id). Courier does
  not clean up the scripts it leaves.

### Slurm metrics

| Metric                                       | Type    | Labels                                               | Meaning                                                                                 |
| -------------------------------------------- | ------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `courier_dispatcher_slurm_submissions_total` | Counter | `dispatcher_name`, `dispatcher_identifier`, `status` | `status="submitted"` right after `sbatch` accepts a job; `status="rejected"` otherwise. |
| `courier_dispatcher_slurm_jobs_pending`      | Gauge   | `dispatcher_name`, `dispatcher_identifier`           | Jobs submitted and not yet in a terminal state, in wait mode.                           |

## Dispatcher metrics

Every dispatcher exports these, labelled with `dispatcher_name` (the plugin)
and `dispatcher_identifier` (the step):

| Metric                                              | Type      | Meaning                                                                 |
| --------------------------------------------------- | --------- | ----------------------------------------------------------------------- |
| `courier_dispatcher_jobs_processed_total`           | Counter   | Jobs handled, by `status`: `success`, `failure` or `unexecutable`.      |
| `courier_dispatcher_job_execution_duration_seconds` | Histogram | Time spent on each job.                                                 |
| `courier_dispatcher_active_jobs`                    | Gauge     | Jobs in progress.                                                       |
| `courier_dispatcher_execution_logs_emitted_total`   | Counter   | Execution logs published.                                               |
| `courier_dispatcher_queue_wait_duration_seconds`    | Histogram | Time from the job's last change on the builder to the start of its run. |

These are labelled with `dispatcher_identifier` only:

| Metric                                        | Type      | Meaning                                                         |
| --------------------------------------------- | --------- | --------------------------------------------------------------- |
| `courier_dispatcher_jobs_consumed_total`      | Counter   | Messages received from the job queue.                           |
| `courier_dispatcher_dispatch_latency_seconds` | Histogram | Time from the builder's emit to receipt.                        |
| `courier_dispatcher_queue_depth`              | Gauge     | Messages ready in the job queue when the last job was received. |
| `courier_dispatcher_dedupe_skips_total`       | Counter   | Duplicate jobs skipped.                                         |

`courier_dispatcher_queue_depth` is sampled only when the dispatcher receives
a job, and counts the messages ready for delivery at that moment: the job in
hand and any other unacknowledged message are not included. It keeps that
value until the next job arrives, and is set to 0 when the broker does not
answer the probe. It therefore cannot show that a queue has drained; ask the
broker (see {ref}`upgrading-drain-check`).

Payload metrics are listed in {doc}`payloads`.
