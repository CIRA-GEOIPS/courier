# ADR-0011: Dispatchers Execute Jobs; Payloads Travel With Them

## Status

Accepted.

Supersedes {doc}`./0009-dispatcher-refactoring`.

## Context

ADR-0009 split dispatchers into a falconer (the manager of an execution
environment) and a falcon (the payload to run), then paired the two at service
startup through a "marriage". That split was in the right direction but bound
the pair too early:

- The falcon was configured *under the dispatcher*, so which payload ran was a
  property of where it ran. A payload could not be built once and dispatched to
  a pool of heterogeneous dispatchers.
- The falconer vocabulary leaked into every operator surface: YAML keys, plugin
  names, metric labels and CLI output.
- The pairing had to be declared as a triple (`dispatcher`, `falconer`,
  `falcon`) and validated by `Service._populate_falconer_map`, which meant the
  relationship lived in the service rather than on the work itself.

Alongside the falconer pair, three script-running dispatchers (`serial_bash`,
`parallel_bash`, and the template-based `slurm_dispatcher`) and one
non-script dispatcher (`http_dispatcher`) each carried their own copy of
templating, logging and output scanning.

## Decision

The falconer concept is removed and the falcon concept is renamed.

1. **Falconers become dispatchers.** The former `local_falconer` and
   `slurm_falconer` are now `local_dispatcher` and `slurm_dispatcher`. A
   dispatcher declares the *payload representations* it can execute
   (`representations`) and how to run them in its environment, and nothing
   else. The `courier.falconers` entry-point group is gone.

1. **Falcons become payloads.** A payload describes what to execute: a
   language/representation hierarchy (`PythonPayload` lowers to `BashPayload`
   lowers to `ShellPayload`), a template file or inline script, and the
   arguments to launch it. `courier.payloads` replaces `courier.falcons`; the
   shipped plugins are `shell_payload`, `bash_payload` and `python_payload`.

1. **Payloads belong to job builders.** Every job builder nests exactly one
   `payload` block, as a dispatcher used to nest a falcon. `JobBuilder.__init__`
   validates the block (`courier.interfaces.job_builders.parse_payload_block`)
   and constructs the payload plugin it names, which reads, parses and checks
   the template. A builder without a valid payload therefore cannot be
   constructed, third-party builders included, and no job without a payload
   can be published. `courier validate` runs the same code. The block is the
   builder's own: no builder copies it into the config its jobs carry. For
   each emitted job the builder renders the template (pass one) and attaches
   the result to the job as a `PayloadSpec`, so the payload travels with the
   work.

1. **Dispatchers execute the payload the job carries.** For each job a
   dispatcher looks up the payload plugin named on the job, picks the most
   specific class in its hierarchy that it lists in `representations`,
   rebuilds and validates the payload from the spec, and checks the payload's
   `toolchain` on its own host (caching a success). It then writes the
   carried script to a new private file, filling in the values only it knows
   (pass two), runs it, and publishes the job's output files and execution
   logs.

1. **The other script dispatchers are deleted.** `serial_bash`,
   `parallel_bash` and `http_dispatcher` are removed. The template-based
   `slurm_dispatcher` is replaced by the falconer-derived one, which keeps the
   name.

## Two-pass rendering

A payload template can mix values the builder knows (`{{ files[0].file }}`)
with values only the dispatcher knows (`{{ script_path }}`). The builder
renders the template with `StrictUndefined` against `files`, `job`, `config`
and `builder`. Four reserved names, `dispatcher`, `script_path`, `hostname`
and `output_dir`, are dispatcher-only, and a template may use them only as a
**bare value**: a `{{ ... }}` holding the name or a literal path below it,
with literal text around it. The rule is checked on the template's syntax
tree when the payload plugin is constructed, comparing names in their NFKC
form so a look-alike spelling cannot slip past, so `courier validate` and
startup reject a violation whatever the data.

Pass one renders each bare value as a marker carrying a per-job nonce
(`PayloadSpec.defer_nonce`) and the value's path. Pass two, on the
dispatcher, replaces only the markers with that nonce by walking the path
through its own context; it runs no Jinja, so nothing that came from job data
is ever evaluated. A path the dispatcher does not define fails the job. The
command parts (`binary`, `prefix_args`, `suffix_args`) are rendered only on
the dispatcher, in one strict pass with its real values, each as its own argv
entry.

{doc}`../../api-reference/payloads` has the full rule and context.

### Alternatives rejected

<!-- cspell:ignore dirr -->

Two earlier, unreleased designs were tried. Deferring *every* undefined name
to the dispatcher made pass one lenient: a typo rendered as an empty string
(`rm -rf {{ output_dirr }}/` became `rm -rf /`), and data-keyed lookups were
evaluated on the dispatcher with job data spliced in. Letting dispatcher-only
values flow through filters and operators, guarded at run time, left gaps
(comparisons and truthiness operated on the marker text), and every fix
added a guard. A fixed allow-list with a static bare-value rule makes pass
one an ordinary strict template, moves every error to startup, and leaves
pass two nothing to evaluate. The cost is that a dispatcher-only value cannot
be filtered, concatenated with `~`, branched on or passed to a macro; literal
template text, the script itself or the command arguments cover those uses.

## Wire format

A job carries its payload as `job.payload`, a `PayloadSpec` (`name`,
`identifier`, `config`, `script`, `suffix`, `defer_nonce`). The template
travels once, rendered, in `script`; `config` carries everything else, with
the command parts still as templates because the dispatcher renders them. A
payload rebuilt from a spec runs it and cannot render a job again. See
{ref}`payload-wire-format`.

## Failure handling

Every error fails the smallest unit that owns it. A configuration error,
including a payload that cannot run on a dispatcher its builder targets, stops
`courier run` at startup and is reported by `courier validate`. A pass-one
render error drops that job on the builder. An error while a dispatcher
prepares, runs or publishes a job fails that job. A message a dispatcher
cannot execute at all is parked on its dead-letter queue
({doc}`./0010-poison-message-handling`). Only a failure to consume or to park
a message ends the dispatcher's process, so the message is retried rather than
lost. {doc}`../../api-reference/dispatchers` and
{doc}`../../api-reference/payloads` list each case.

## Slurm

`slurm_dispatcher` keeps its name with new options (`SlurmDispatcherConfig`).
A shell or bash script with no `binary` or `prefix_args` is submitted as the
batch script, so its `#SBATCH` lines apply; everything else goes through
`--wrap`. The script lives in `slurm_output_dir`, which must be shared with
the compute nodes. See {doc}`../../api-reference/dispatchers`.

## Consequences

- A single dispatcher pool can execute jobs carrying different payloads, and
  one payload can be dispatched to several environments. Routing is unchanged.
- A dispatcher deduplicates jobs on
  `(payload identifier, job identifier)`, because two builders that share a
  dispatcher can emit jobs with the same identifier.
- Payload/dispatcher compatibility is checked at startup from the
  configuration, from both ends of a route in a split `--only` deployment, and
  by `courier validate`, rather than discovered as parked jobs.
- Payloads are not service plugins. The plugin manager does not run them, so
  they have no thread, health check or `courier_plugin_state` series of their
  own, and cannot mask a failed builder.
- Toolchain validation moves from dispatcher startup to the first execution of
  a given payload configuration, where it is cached. For `slurm_dispatcher` the
  probe runs on the submitting host, not on the compute node.
- `local_dispatcher` keeps the `COURIER_METRIC:` stdout conduit; it is the only
  dispatcher that ingests it.
- Dispatchers become first-class plugin registries. `courier plugins list` and
  `courier init` no longer treat them as "necessary" base classes, and
  `NECESSARY_REGISTRIES` is gone.
- Entry-point groups are now
  `courier.{data_monitors,job_builders,dispatchers,payloads}` plus the
  `courier.data_monitor_configs` config group.
- Payload metrics (`courier_payload_jobs_processed_total`,
  `courier_payload_job_execution_duration_seconds`) are labelled with the
  configured payload plugin name, even when a dispatcher runs the payload as a
  lower representation.
- `DispatcherGroupConfig` and `PayloadConfig` forbid unknown keys, and each
  dispatcher and payload validates with its own `config_class`. Keys removed by
  this change (`bash_script`, `max_workers`, `fail_fast`, `python_venv`,
  `sbatch_template`) fail validation with a pointer to the upgrade guide.
- The job wire format changed. Jobs queued by a pre-upgrade builder are
  parked, not executed; see {doc}`../../getting-started/upgrading`.

### What did not carry over

`serial_bash` and `parallel_bash` are replaced by `local_dispatcher` plus a
payload on the builder; per-file concurrency within a job (`max_workers`,
`fail_fast`) is gone. `http_dispatcher` is removed with no replacement, along
with its extras and metrics. `python_venv` and `sbatch_template` are removed.
{doc}`../../getting-started/upgrading` gives the migration for each.

## Trade-offs accepted

- **Dispatcher-only values are bare values.** Anything a template needs to
  decide from one must be decided inside the script at run time, or in the
  command arguments, which the dispatcher renders with its real values. A
  template that tries is rejected at startup, even in a branch no job would
  take.
- **A payload's template file need not exist on the dispatcher host.** The
  carried, rendered script is authoritative. The flip side is that every job
  message carries its script, once, rendered.
- **A render failure drops the job.** A template that fails for one job's data
  cannot succeed on a retry, so the job is not re-queued. It is logged at ERROR
  with its files and counted, and the builder carries on.
- **A publish fault fails the job.** A broker fault while a dispatcher
  publishes a job's results counts that job as failed and acknowledges it, so
  those results are lost rather than the job re-run.
- **The broker is a code-execution trust boundary.** A dispatcher runs the
  script, interpreter and arguments that a job message carries. The nonce stops
  job *data* from being evaluated as template syntax. It does not authenticate
  the message, and anyone who can publish to a `JobReady` queue can run code on
  that queue's dispatchers. Broker credentials must be treated accordingly.
- **Lowering wraps the payload's own command.** A payload run as a lower
  representation (a `python_payload` on a dispatcher that lists only
  `BashPayload`) is still hydrated as its own class. Its own command is
  passed through `Payload.render_script`, which by default hands it to the
  lower class's `wrap_command`, so the lower interpreter launches the
  payload's interpreter rather than reading its script. Slurm submits a
  lowered payload with `--wrap`, never as the batch script.
