# Upgrading: Payloads Move to the Job Builder

This release changes where the work a pipeline runs is configured. The script
moves from the dispatcher to the job builder, the bash dispatchers are
replaced, and jobs carry what they execute. Configs, dashboards, alerts and
running deployments all need attention. This page lists every breaking change
and how to migrate. The design is recorded in
{doc}`../concepts/adr/0011-dispatchers-execute-jobs`.

Run `courier validate` on every config after migrating. It reports removed
keys by name, together with their replacement.

## What changed

- **The script moves to the job builder.** Every job builder nests exactly one
  `payload` block. The payload holds the script (`script:` or `file:`) and how
  to launch it; see {doc}`../api-reference/payloads`. A job builder without a
  payload is a configuration error, whichever builder it is, your own
  included: `courier validate` reports it, and `courier run` does not start.
- **Dispatchers only execute.** A dispatcher runs whatever payload each job
  carries, and keeps the execution options: timeout, logging and
  `output_files`. See {doc}`../api-reference/dispatchers`.
- **Plugins were removed or replaced**, as listed in the table below.
- **Extras removed:** `data-courier[http]` and `data-courier[all-dispatchers]`.
  Installing them now installs nothing extra.
- **Stricter configs.** Dispatcher and payload configs reject unknown keys, so
  a half-migrated config fails at `courier validate` rather than silently
  ignoring the old settings.
- **Stricter templates.** A typo or a missing key in a payload template is an
  error on the job builder instead of being rendered as an empty string. See
  [Template changes](#template-changes).
- **New job wire format.** Jobs published by an older builder cannot be
  executed by a new dispatcher. See
  [Upgrading a running deployment](#upgrading-a-running-deployment).

| Before                                    | After                                                                          |
| ----------------------------------------- | ------------------------------------------------------------------------------ |
| `serial_bash` dispatcher                  | `local_dispatcher`, plus a `bash_payload` on the job builder                   |
| `parallel_bash` dispatcher                | `local_dispatcher`, plus a `bash_payload`; no per-file concurrency (see below) |
| `slurm_dispatcher` with `sbatch_template` | `slurm_dispatcher` with new options, plus a payload (see below)                |
| `http_dispatcher`                         | Removed; no replacement                                                        |

## Migrating `serial_bash`

Move `bash_script` into a `bash_payload` on the job builder that feeds the
dispatcher, and rename the dispatcher to `local_dispatcher`. Execution options
stay on the dispatcher.

Before:

```yaml
- just-pass:
    kind: job_builder
    name: filter_and_group
    config:
      files_per_job: 1

- run-bash-command:
    kind: dispatcher
    name: serial_bash
    config:
      timeout_seconds: 600
      bash_script: |
        echo "This is running on {{ files[0].file }}"
```

After:

```yaml
- just-pass:
    kind: job_builder
    name: filter_and_group
    config:
      files_per_job: 1
      targets:
        - run-bash-command
      payload:
        echo-file:
          kind: payload
          name: bash_payload
          config:
            script: |
              echo "This is running on {{ files[0].file }}"

- run-bash-command:
    kind: dispatcher
    name: local_dispatcher
    config:
      timeout_seconds: 600
```

Two behaviours differ. `serial_bash` skipped jobs that had no files; a payload
now runs for every job it receives. And `serial_bash` logged
`Executing job: ...` at INFO for every job; `local_dispatcher` logs a job only
when it fails (at ERROR), or its output line by line with `log_to_logger`.

## Migrating `parallel_bash`

`parallel_bash` rendered the script once per file and ran up to `max_workers`
copies at once, with `fail_fast` to stop at the first failure. Nothing replaces
per-file concurrency inside one dispatcher: each dispatcher runs one job at a
time. Choose one of:

- **One file per job, several replicas.** Set `files_per_job: 1` on the
  builder, use `{{ files[0].file }}` where the old script used
  `{{ file.file }}`, and run as many replicas of the dispatcher as you want
  concurrent runs (each with `--only <dispatcher>`, against the same broker).
- **One job, parallel inside the script.** Keep the files together and start
  them in the background from the script:

```yaml
payload:
  per-file:
    kind: payload
    name: bash_payload
    config:
      script: |
        pids=()
        {% for f in files %}
        process_one "{{ f.file }}" & pids+=($!)
        {% endfor %}
        status=0
        for pid in "${pids[@]}"; do wait "$pid" || status=1; done
        exit "$status"
```

The per-file `file` template variable no longer exists; loop over `files`
instead.

## Migrating the Slurm dispatcher

`sbatch_template` is gone. The script is now the payload's, and the batch
options come from the dispatcher's options, from `sbatch_extra_args`, or from
`#SBATCH` lines in a shell payload's script. A shell or bash payload whose
script runs directly (no `binary`, no `prefix_args`) is submitted as the batch
script itself, so its `#SBATCH` lines apply; other payloads are submitted with
`--wrap` and their `#SBATCH` lines are ignored. See
{doc}`../api-reference/dispatchers` for all options.

Before:

```yaml
- submit:
    kind: dispatcher
    name: slurm_dispatcher
    config:
      slurm_output_dir: /shared/courier/slurm
      partition: batch
      sbatch_template: |
        #!/bin/bash
        #SBATCH --cpus-per-task=4
        process {{ files | join(" ") }}
```

After:

```yaml
- build:
    kind: job_builder
    name: filter_and_group
    config:
      targets:
        - submit
      payload:
        process:
          kind: payload
          name: bash_payload
          config:
            script: |
              #!/bin/bash
              #SBATCH --cpus-per-task=4
              process {{ files | map(attribute="file") | join(" ") }}

- submit:
    kind: dispatcher
    name: slurm_dispatcher
    config:
      slurm_output_dir: /shared/courier/slurm
      partition: batch
```

The template context changed too:

| In `sbatch_template`       | In a payload template                                           |
| -------------------------- | --------------------------------------------------------------- |
| `files`: a list of paths   | `files`: a list of file dictionaries; use `f.file` for the path |
| `job`: the job object      | `job`: a dictionary with the same fields                        |
| `config`: the dispatcher's | `dispatcher.config`; `config` is now the job's config           |
| (none)                     | `output_dir`: the dispatcher's `slurm_output_dir`               |

`slurm_output_dir` must be on a filesystem the compute nodes can read: jobs
submitted with `--wrap` read their script from there, and the dispatcher reads
the jobs' `.out` and `.err` files from there. The Slurm dispatcher now also accepts
`output_files` and `log_to_logger`, which it applies to the job's output in
wait mode. Slurm's output files are now named
`<job id>-<Slurm job id>.out` and `.err` (they were `<job id>.out` and
`.err`), so scripts or tools that read them need the new names.

## Removed configuration keys

These keys are rejected with a message that says what to do instead:

| Key               | Where it was                   | Instead                                                                                                                                                                                                                               |
| ----------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bash_script`     | `serial_bash`, `parallel_bash` | `script:` (or `file:`) in the job builder's `payload` block.                                                                                                                                                                          |
| `max_workers`     | `parallel_bash`                | Per-file parallel execution within one job is not supported. Scale with more dispatcher replicas or smaller jobs.                                                                                                                     |
| `fail_fast`       | `parallel_bash`                | As for `max_workers`.                                                                                                                                                                                                                 |
| `python_venv`     | `serial_bash`, `parallel_bash` | Set `python_payload`'s `default_binary` to the environment's interpreter (`/opt/venvs/x/bin/python`), or activate the environment in a shell script. `toolchain_prepend` is not a replacement (see {doc}`../api-reference/payloads`). |
| `sbatch_template` | `slurm_dispatcher`             | Use the dispatcher's scheduler options or `sbatch_extra_args`, or put `#SBATCH` directives in a `shell_payload`/`bash_payload` script with no `binary` or `prefix_args` (see {doc}`../api-reference/dispatchers`).                    |

An option every dispatcher takes, put in a payload block, or a setting every
payload takes, put in a dispatcher block, is reported with the block it
belongs in; any other unknown key is reported only as unknown (see
{ref}`validation-errors`). A removed dispatcher plugin (`serial_bash`,
`parallel_bash`, `http_dispatcher`) named in a step is reported with its
replacement; `http_dispatcher` settings have no new home, so remove the step.

## Template changes

- **Strict on the builder.** A name, attribute, key or index that does not
  exist is an error when the job builder emits the job, and that job is not
  published (`serial_bash` rendered it as an empty string). Use
  `| default(...)` or `is defined` for values that are legitimately optional.
- **Checked at startup.** A template syntax error, or a reserved name used
  other than as a bare value (below), stops the service at startup, and
  `courier validate` reports it.
- **Dispatcher values are reserved names.** `dispatcher` (with
  `dispatcher.config`), `script_path`, `hostname` and `output_dir` are filled
  in on the dispatcher and can only be used as bare values, such as
  `{{ dispatcher.config.log_dir }}/x`, not in filters, operators or
  conditions. See {doc}`../api-reference/payloads`.
- **Use `{{ files[0].file }}` for the file.** Older tutorials showed a
  `{file}` placeholder; it was never substituted.

## Metrics, labels and traces

Update dashboards and alerts:

- **Label values.** `dispatcher_name` is now `local_dispatcher` where it was
  `serial_bash` or `parallel_bash`. A query or alert on
  `dispatcher_name="serial_bash"` silently matches nothing after the upgrade.
- **Removed metrics:** `courier_dispatcher_http_response_codes_total`,
  `courier_dispatcher_http_request_duration_seconds` and
  `courier_dispatcher_parallel_workers_active`.
- **New metrics:** `courier_payload_jobs_processed_total` and
  `courier_payload_job_execution_duration_seconds`, labelled with
  `payload_name`, `payload_identifier` (and `status`). A non-zero exit code is
  a `status="failure"` here.
- **New status value.** `courier_dispatcher_jobs_processed_total` gains
  `status="unexecutable"` for jobs parked on the dead-letter queue.
- **Slurm.** `courier_dispatcher_slurm_submissions_total{status="submitted"}`
  is counted as soon as `sbatch` accepts a job, and
  `courier_dispatcher_slurm_jobs_pending` covers the whole time a job is queued
  or running in wait mode.
- **Traces.** Each payload run is a `payload.get_payload_from_job` span under
  `dispatcher.execute_job`.
- **`COURIER_METRIC:`** lines are read by `local_dispatcher` only.

Regenerate generated dashboards with `courier dashboard <config>`.

## Plugin authors

- Register payload plugins in `courier.payloads`.
- **Every job builder requires a payload**, a custom one included: a custom
  builder must call `super().__init__`, which validates the `payload` block
  and constructs the payload as `self.payload`, must accept the `payload` key
  in its own config validation, and must not copy the block into the config
  its jobs carry.
- A custom dispatcher must list the payload classes it can run in
  `representations`, or no job builder can target it.
- A dispatcher or payload with options of its own names its config model in
  `config_class`; unknown keys are rejected against that model.

See {doc}`../contribute/writing-a-plugin`.

## Upgrading a running deployment

The job queues keep their names (`<namespace>-JobReady-<dispatcher>`), but the
job message format changed: the executable now travels in the job's `payload`.

- A **new dispatcher** that receives a job from an **old builder** cannot run
  it (the job has no payload). It parks the job on
  `<namespace>-JobReady-<dispatcher>-DeadLetter` and counts it as
  `status="unexecutable"`.
- An **old dispatcher** that receives a job from a **new builder** ignores the
  payload and runs its own configured script, as before.

So a dispatcher must never be upgraded while jobs from an old builder can
still reach it. Either of these orders is safe.

**Stop, drain, upgrade.** Works for every deployment, including one that runs
builders and dispatchers in the same process:

1. Stop new data from entering the pipeline: stop the data monitors, or pause
   whatever feeds them.
1. Let the old builders and dispatchers finish: wait until the builders have
   emitted the jobs they are holding and every
   `<namespace>-JobReady-<dispatcher>` queue is drained, as checked below.
1. Stop every instance.
1. Install the new release, migrate the configs, and run `courier validate` on
   each.
1. Start the dispatchers and job builders, then the data monitors.

**Builders first, then dispatchers.** For split deployments, where builders and
dispatchers run in separate processes (`--only`):

1. Upgrade every job builder, with the migrated config. The old dispatchers
   keep running with their old config and keep executing their own scripts,
   so the new payloads take effect only in the last step.
1. Once no old builder is left, wait until every
   `<namespace>-JobReady-<dispatcher>` queue has drained at least once, as
   checked below, so no job from an old builder is still queued.
1. Upgrade the dispatchers.

(upgrading-drain-check)=

### Checking that the job queues have drained

A `JobReady` queue has drained when it holds no message ready for delivery
and none delivered but not yet acknowledged: a dispatcher acknowledges a job
only once it has finished with it. `courier queues list <config>` names the
queues. On RabbitMQ, read both counts in the management UI (the queue's
**Ready** and **Unacked** columns) or with:

```bash
rabbitmqctl list_queues -p <vhost> name messages_ready messages_unacknowledged
```

Every `<namespace>-JobReady-<dispatcher>` row must show `0 0`. The
`<namespace>-JobReady-<dispatcher>-DeadLetter` queues are not consumed, so they
never drain on their own, and they do not hold up the upgrade. A job an old
dispatcher parked there carries no payload either: re-submit its files (see
[Jobs parked or held during the upgrade](#jobs-parked-or-held-during-the-upgrade))
rather than moving it back. On another broker, read the same two counts with
that broker's own tools.

`courier_dispatcher_queue_depth` cannot show this: it is sampled only when a
dispatcher receives a job (see {doc}`../api-reference/dispatchers`).

### Jobs parked or held during the upgrade

If new dispatchers did receive old jobs, they are on the dead-letter queues
with the park reason in the `x-courier-park-reason` header. Moving them back
onto the `JobReady` queue parks them again, because they still carry no
payload. Instead, once the builders are upgraded, re-submit the files the
parked jobs list (the `files` field of each parked message) through the
upgraded job builders, the same way those files first entered the pipeline:
for example, copy them into the watched directory again.

Jobs a builder holds in state sync (Redis) are not affected: the payload is
rendered when a job is emitted, so an upgraded builder attaches it to jobs it
inherited.
