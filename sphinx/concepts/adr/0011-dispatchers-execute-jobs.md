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

1. **Payloads are sub-plugins of job builders.** Every job builder nests
   exactly one `payload` block, as a dispatcher used to nest a falcon. The
   payload plugin is constructed when the service starts, and construction
   reads and parses its template. For each emitted job the builder renders the
   template (pass one, below) and attaches the serialized `PayloadSpec` to the
   `Job`, so the payload travels with the work (see
   [Wire format](#wire-format)).

   The requirement is enforced in the `JobBuilder` base class, so it holds for
   third-party builders too. `JobBuilder.__init__` validates the block
   (`courier.interfaces.job_builders.parse_payload_block`) before anything
   else, and raises `InvalidPluginConfigError` naming the builder when it is
   missing, malformed or nests a plugin whose kind is not `payload`;
   `courier run` parses the block with the same function before it registers
   the nested plugin, and `courier validate` reports the same configs.
   Service preflight then binds the payload plugin to the builder
   (`JobBuilder.payload`, whose setter accepts only the `Payload` the block
   names). Until one is bound, `start()` and `emit()` raise
   `ConfigurationError`, so a job without a payload can never be published.
   The block is the builder's own: no builder copies it into the config its
   jobs carry.

1. **Dispatchers execute the payload the job carries.** `Dispatcher` no longer
   holds a falconer. For each job it:

   1. looks up the payload class named by `job.payload.name` in the payload
      registry and picks the most specific class in that payload's hierarchy
      that it lists in `representations`;
   1. validates the spec and its config with that class's `config_class`;
   1. checks the payload's `toolchain` on its own host, caching a successful
      check per payload configuration;
   1. writes the carried script to a fresh file made with `tempfile.mkstemp`
      (random name, created exclusively, mode `0755`, in `$TMPDIR` by default
      and in `slurm_output_dir` for Slurm), resolving the deferred values
      (pass two) as it writes;
   1. runs it, and removes the script unless the dispatcher marked it to be
      kept (`ExecutionPayload.keep_file`, for a Slurm job that may still need
      it);
   1. collects the job's output files (`_collect_output_files`: the
      `output_files` matches, plus any a subclass adds);
   1. publishes the output files, then the execution logs. This step runs
      outside the job's error containment; see
      [Failure handling](#failure-handling).

1. **The other script dispatchers are deleted.** `serial_bash`,
   `parallel_bash` and `http_dispatcher` are removed. The template-based
   `slurm_dispatcher` is replaced by the falconer-derived one, which keeps the
   name. See [What did not carry over](#what-did-not-carry-over).

## Two-pass rendering

A payload template can mix values the builder knows (`{{ files[0].file }}`)
with values only the dispatcher knows (`{{ script_path }}`). The builder cannot
wait for the dispatcher, and the dispatcher must not re-render text that came
from job data, so the template is rendered once on each side with a strict
division of labour.

### Pass one: the builder

The builder renders the template in a sandboxed Jinja environment with
`StrictUndefined`, against the context it owns:

- `files`, `job` and `config` (an alias for `job.config`). `config` is the
  JSON-normalised plain-data form of the job's config: exactly what the
  dispatcher sees after the job has crossed the broker. It never holds the
  builder's `payload` block.
- `builder`: `name`, `identifier`, `targets`.

Exactly four top-level names are dispatcher-only, listed in
`courier.interfaces.payloads.DISPATCHER_CONTEXT_NAMES`: `dispatcher`,
`script_path`, `hostname` and `output_dir`. In pass one each is bound to a
deferred placeholder. Nothing else is deferred: a typo, a missing metadata key
or an out-of-range index raises Jinja's ordinary `UndefinedError` on the
builder, and `| default(...)`, `is defined` and `{% if x is defined %}` over
builder-side values behave exactly as in stock Jinja.

A deferred placeholder records the full access path taken from the template
source, for example `dispatcher.config.log_dir` or `dispatcher.config['k']`.
Attribute names must be identifiers and not dunders; subscripts must be `str`
or `int` values and are spliced into the path as Python literals, so no data
value can become expression syntax. Rendering a placeholder emits a marker:

```text
\x00COURIER-DEFER:<nonce>:<base64(path)>\x00
```

`<nonce>` is random per emitted job and travels on `PayloadSpec.defer_nonce`.

Dispatcher-only values support leaf interpolation only: `{{ script_path }}`,
concatenation with `~`, and plain string conversion (as one element of a list
passed to `join`, `'%s' %`, `'{}'.format`). Every filter and test in the
pass-one environment rejects a deferred argument, and conditionals, loops,
arithmetic, comparisons and calling one (or one of its methods) are rejected
too. A string built from one with `~` carries its marker and is treated the
same way: filters, tests, attribute access (its methods), subscripts, and `%`
or `str.format` conversions with a width, precision or conversion flag, or a
`%(...)` key that contains a parenthesis, refuse it. Each raises
`DeferredExpressionError` on the builder instead of rendering a wrong branch
or a mangled marker.

A call may receive a deferred value, or a string built from one, as an
argument when it only stores or returns it: a macro, `namespace()`, `dict()`,
`cycler()` and `loop.cycle()`, `joiner()`, `list.append()`, and the default of
`dict.get()`. Refusing them would buy nothing: whatever such a call returns is
still subject to the guards above when the template uses it, and to the output
check below. A call that would compute with the value is refused: any string
method given it as an argument (`'a/b'.split(x)`, `' '.join(x)`), and a
deferred key to `get`, `pop` or `setdefault`.

Each render records the markers it emitted, and pass one then checks its
output: every marker must be intact, carry the job's nonce and be one this
render emitted, and the nonce may not appear outside a marker. When the render
emitted a marker, no NUL character may appear outside one either, since a NUL
is how a truncated marker shows; a render that emitted none has no marker to
truncate, so a NUL in its job data is kept. This catches a marker that was
escaped on the way out (`| tojson` or `| urlencode` over a container holding
one) or rewritten character by character, which the guards above cannot see.
What no check can see is a template *inspecting* such a string -- `==`, `in`,
truthiness, iteration, sorting, searching (`list.count`) -- which operates on
the marker text; the documentation tells template authors not to branch on
dispatcher-only values at all.

### Pass two: the dispatcher

The dispatcher never parses the carried script as a template. It scans for
markers and, for each one:

1. rejects it unless it carries the job's nonce;
1. decodes the path and rejects it unless it is a plain access path whose root
   name is one of `DISPATCHER_CONTEXT_NAMES` (defence in depth: the builder can
   emit nothing else);
1. evaluates the path in a sandbox against its own context: `dispatcher`
   (`name`, `identifier`, `config`), `script_path`, `hostname`, and for
   `slurm_dispatcher` `output_dir`. The same `files`, `job` and `config` are
   present, normalised the same way;
1. raises `DeferredExpressionError` naming the expression if the result is
   undefined: `{{ output_dir }}` sent to a `local_dispatcher`, for example, is
   an error, not an empty string;
1. applies the same finalisation as pass one (`None` and `[]` render as an
   empty string) and substitutes `str(value)`.

Text between markers, including any `{{` or `{%` that arrived in a file name
or in metadata, is copied through literally. A template that changes a marker's
text after it was rendered, for example with `{% filter upper %}` or by slicing
a string that contains it, is rejected on the builder, and the dispatcher
rejects any marker that no longer matches its authenticated form.

The command parts (`binary`, `prefix_args`, `suffix_args`) are not part of the
template. The dispatcher renders each of them, as one argument, in a single
strict pass with the pass-two context (which has no `builder` namespace), and
passes each to the process as its own argv entry, an empty one included: a
rendered value is never spliced unquoted into a shell command line or into
Python source (a Slurm `--wrap` command shell-quotes each argument). They are
the only templates in the command; the script path (under `TMPDIR` or
`slurm_output_dir`) and the interpreter are passed literally.

### Why an allow-list

<!-- cspell:ignore dirr -->

The first implementation of this ADR, never released, deferred *every*
undefined name to the dispatcher. That made pass one lenient in exactly the
wrong places. A data-keyed lookup that missed on the builder
(`{{ config.modes[files[0].metadata.band] }}`) was signed and evaluated on the
dispatcher with the data spliced into the expression, which is template
injection past the nonce. A typo rendered as an empty string, so
`rm -rf {{ output_dirr }}/` became `rm -rf /`. A builder-side
`{{ config.hostname }}` silently resolved to the dispatcher's host name. And
`| default` over optional builder data raised instead of defaulting. With a
fixed allow-list, pass one is an ordinary strict template everywhere except at
four reserved names.

## Wire format

A job carries its payload as `job.payload`, a `courier.types.payload.PayloadSpec`
with `name` (the configured payload plugin), `identifier`, `config`, `script`,
`suffix` and `defer_nonce`. The template travels exactly once, rendered:

- `PayloadSpec.script` is the builder's pass-one output, from the template
  `file` or the inline `script`, and `None` for a binary-only payload. It is
  the only copy of the template on the wire.
- `PayloadSpec.config` is the payload's validated config **without** `script`.
  `file` stays, as a path string for information only (the dispatcher host
  need not have it). `binary`, `prefix_args` and `suffix_args` travel as
  templates, because the dispatcher renders them.
- When the dispatcher hydrates the spec (`Payload.from_job_spec`) it sets the
  hydrated config's `script` to `PayloadSpec.script`, so a rendered script
  satisfies the "one of `file`, `script` or `binary`" rule, and dispatcher-side
  code that reads `config.script` sees the script that will run. A spec whose
  `config` still carries the raw template in `script` is hydrated the same
  way: the rendered copy replaces it.
- A spec with no rendered `script` and neither `file` nor `binary` fails
  validation, and the job is parked as unexecutable, rather than running the
  interpreter with no script and reporting success.
- A hydrated payload refuses `to_job_spec` (`CourierError`): its
  `config.script` is already-rendered text that may hold job data, and
  rendering it again would evaluate that data as a template.

The first implementation of this ADR, never released, also sent the raw
template in `PayloadSpec.config`, so every job message carried its script
twice.

## Failure handling

Template and job errors fail the smallest unit that owns them. None of them
ends the process.

- **At startup.** A job builder with no valid `payload` block raises
  `InvalidPluginConfigError` (see the decision above), and a template file
  that cannot be read, or a template with a Jinja syntax error, makes payload
  construction raise `ValueError` naming the payload, the file (or "inline
  script") and the line. Either way `courier run` stops before the service
  starts, and `courier validate` reports the same config.
- **Pass one.** Any exception from `Payload.to_job_spec` inside
  `JobBuilder.emit` is contained per job. It is logged at ERROR with the job
  identifier, its files and the exception, and
  `courier_job_builder_emit_failures_total{reason="render"}` is incremented
  once per target. Nothing is published. Rendering happens before any
  per-target emit claim is taken, so no claim is left held. The job's files
  are not returned to the group, because they would fail the same render
  again; the ERROR line is the record of them. `emit` returns normally, so the
  consumer loop, the timeout reapers, the Redis-merge callback and startup
  hydration all keep running. A builder with no payload bound is not a
  per-job failure but a wiring error: `emit` raises `ConfigurationError`
  before rendering, and `start()` refuses to run at all.
- **Dispatch.** Every non-`CourierError` exception raised while preparing the
  environment, executing, or collecting the job's output files becomes a
  `CourierError`; one raised while resolving the payload makes the job
  unexecutable (below). A failed job is logged at ERROR, counted as
  `courier_dispatcher_jobs_processed_total{status="failure"}`, and the
  dispatcher moves on to the next job. A script that runs and exits non-zero
  is logged at ERROR with its return code and marks the span as an error.
- **Publishing results.** Output files and execution logs are published after
  the job, outside that containment. A publish failure is the broker's, not
  the job's: a `TransientBrokerError` or `FatalBrokerError` (both
  `CourierError`s, which the containment above would have caught) or a raw
  transport error propagates, and the dispatcher's supervisor ends the
  process. The job is counted as neither success nor failure, the dedupe LRU
  forgets it, and its message is retried as
  {doc}`./0010-poison-message-handling` describes instead of being
  acknowledged as a failed job with its results lost. A subclass that
  decides in code which files a job produced extends `_collect_output_files`
  rather than calling `emit_file` while the job runs, so its publishes get the
  same treatment.
- **Unexecutable jobs.** When a dispatcher cannot execute a job for reasons
  that have nothing to do with the job's own run, it raises
  `UnexecutableJobError`. The reasons are: the job carries no payload (it was
  published by a pre-upgrade builder); the payload plugin is not installed on
  this host; no representation is compatible; the `PayloadSpec` or its config
  is invalid; the toolchain is unavailable; or the message body cannot be
  parsed as a job. The dispatcher logs at ERROR, counts the job as
  `courier_dispatcher_jobs_processed_total{status="unexecutable"}`, and parks
  the original message on the dead-letter queue of its job queue
  (`<namespace>-JobReady-<dispatcher>-DeadLetter`, see
  {doc}`./0010-poison-message-handling`) through `Service.park_message`, with
  the reason in the `x-courier-park-reason` header. The original is then
  acknowledged. If parking itself fails, the error propagates and ends the
  process, and the message is retried rather than lost. Such broker- or
  consume-level faults, and the publish failures above, are the only ones
  that still end a dispatcher's process.

## Deduplication

The dispatcher's LRU of recently seen jobs is keyed on
`(payload identifier, job identifier)`. Two builders that share a dispatcher
can emit jobs with the same identifier (both group the same file), and a key of
the job identifier alone dropped the second builder's job as a duplicate.
Genuine redeliveries of the same job are still skipped. A job that is parked is
forgotten, so re-driving it once the deployment is fixed runs it.

## Compatibility and health

- **Static compatibility.** Preflight checks every builder's payload against
  each of its targets with the class-level
  `Dispatcher.compatible_representation(payload_cls)`. Targets that run in the
  same process are checked directly. In a split `--only` deployment, a target
  that is not registered in this process is resolved to its dispatcher class
  from the full service config. A dispatcher-only process checks every builder
  in the config that targets it: the builder's payload plugin must be installed
  locally and compatible. A failure is a `ConfigurationError` at startup, not a
  parked job at run time. `courier validate` runs the same check from the
  config alone.
- **Health.** Payloads are non-threaded sub-plugins (`Payload.threaded = False`).
  The plugin manager starts them without a thread and leaves them out of the
  aggregate `is_healthy()` and of the "all plugins failed to start" abort, which
  describe runnable plugins only. A payload that is always healthy therefore
  cannot mask a failed builder. A payload whose `start()` fails is recorded as
  `FAILED` (with its error message) and exported as such on
  `courier_plugin_state`.

## Slurm

`slurm_dispatcher` validates its config with `SlurmDispatcherConfig`, which
adds the Slurm options to the options every dispatcher takes. It refuses to
start without `sbatch` on `PATH` (and `sacct` when it waits for jobs). The
carried script is materialized in `slurm_output_dir`, which must be on a
filesystem shared with the compute nodes. The script of a shell or bash payload
that runs directly is submitted as the batch script itself, with a shebang
ensured, so its `#SBATCH` directives apply (options the dispatcher passes on
the command line take precedence). A Python payload, a payload with a
`binary`, and any payload with interpreter options (`prefix_args`) is
submitted with `--wrap`, so those options never reach `sbatch`.
(`toolchain_prepend` does not force `--wrap`: it is never part of a job's
command. Only `python_payload` uses it, in front of its toolchain probes, and
shell and bash payloads ignore it.) `sbatch` runs directly under `submission_timeout_seconds`, never
through the payload's job-execution path, and a rejection reports its return
code and stderr. Output
files are named with the Slurm job id, so two submissions of one job never
share them. The script is kept whenever the submitted job may still need it
(`keep_file`). In wait mode the payload metrics and the execution log describe
the Slurm job's outcome, not the `sbatch` call. See
{doc}`../../api-reference/dispatchers`.

## Consequences

- A single dispatcher pool can execute jobs carrying different payloads, and
  one payload can be dispatched to several environments. Routing is unchanged.
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
  `falcon`, `falconer`, `sbatch_template`) fail validation with a message that
  names the replacement.
- `local_dispatcher` does not log a line at INFO for every job it runs, as
  `serial_bash` did. Failures are logged at ERROR; the script's own output is
  logged only with `log_to_logger`.
- The job wire format changed: the executable now travels in `job.payload`
  (see [Wire format](#wire-format)). Jobs queued by a pre-upgrade builder are
  parked, not executed. See
  {doc}`../../getting-started/upgrading`.

### What did not carry over

- **`serial_bash`**: replaced by `local_dispatcher` plus a `bash_payload` on the
  job builder. The script moves from the dispatcher's `bash_script` to the
  payload's `script` (or `file`). Templates are strict where `serial_bash`
  rendered undefined names as empty strings.
- **`parallel_bash`**: per-file concurrent execution within one job
  (`max_workers`, `fail_fast`, the per-file `{{ file }}` variable) is not
  supported. Each dispatcher runs one job at a time. Scale with more dispatcher
  replicas on the same queue, or with smaller jobs (`files_per_job: 1`), and
  loop over `files` in the script when one job holds several files.
- **`http_dispatcher`**: removed with no replacement plugin, along with the
  `data-courier[http]` and `data-courier[all-dispatchers]` extras and the
  `courier_dispatcher_http_*` metrics. Its request options (method, auth,
  retries, success status codes) have no equivalent. A payload can call an HTTP
  endpoint itself (for example with `curl`), or a custom dispatcher can be
  written.
- **`python_venv`**: removed. Point `python_payload`'s `default_binary` at the
  environment's interpreter (`/opt/venvs/x/bin/python`), or activate the
  environment inside a shell script. `toolchain_prepend` is not a replacement:
  it is never part of a job's command. `python_payload` puts it in front of
  the interpreter in its toolchain probes only
  (`[*toolchain_prepend, <interpreter>, -c, <shutil.which check>]`), and
  shell and bash payloads ignore it.
- **Template-based `slurm_dispatcher`**: `sbatch_template` is gone. The payload
  script is the batch script: put `#SBATCH` directives in a shell payload's
  script, or pass options through the config keys and `sbatch_extra_args`. In
  the template, `files` is a list of file dictionaries rather than of paths,
  `config` is the job's config, and the dispatcher's own config is under
  `dispatcher.config`.

## Trade-offs accepted

- **Dispatcher-only values are leaf interpolations.** A template cannot branch
  on, filter, or test a value only the dispatcher knows. Anything the template
  needs to decide must be decided on the builder side, or inside the script at
  run time (in shell or Python, not Jinja).
- **Four names are reserved.** `dispatcher`, `script_path`, `hostname` and
  `output_dir` always mean the dispatcher's values in a payload template.
- **A payload's template file need not exist on the dispatcher host.** The
  carried, rendered script is authoritative. The flip side is that every job
  message carries its script, once, rendered.
- **A render failure drops the job.** A template that fails for one job's data
  cannot succeed on a retry, so the job is not re-queued. It is logged at ERROR
  with its files and counted, and the builder carries on.
- **The broker is a code-execution trust boundary.** A dispatcher runs the
  script, interpreter and arguments that a job message carries. The nonce stops
  job *data* from being evaluated as template syntax. It does not authenticate
  the message, and anyone who can publish to a `JobReady` queue can run code on
  that queue's dispatchers. Broker credentials must be treated accordingly.
- **Lowering uses the lower class's command.** A payload hydrated as a lower
  representation (a third-party `BashPayload` subclass on a dispatcher that
  lists only `BashPayload`) runs with that lower class's interpreter and
  command construction.
- **No per-file parallelism inside a job.** See above.
