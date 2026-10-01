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
   reads, parses and checks its template (see
   [Two-pass rendering](#two-pass-rendering)). For each emitted job the
   builder renders the
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
from job data. So the builder renders the template, leaving a placeholder
wherever a dispatcher-only value goes, and the dispatcher fills the
placeholders in without evaluating anything.

### Dispatcher-only names are bare values

Exactly four top-level names are dispatcher-only, listed in
`courier.interfaces.payloads.DISPATCHER_CONTEXT_NAMES`: `dispatcher`,
`script_path`, `hostname` and `output_dir`. A payload template may use them
in one way only, as a **bare value**: a `{{ ... }}` output whose expression is
nothing but a path rooted at one of the names, made of attribute access with
identifier names that do not start with `_` and subscripts that are `str` or
`int` literals (`{{ script_path }}`, `{{ dispatcher.config.log_dir }}`,
`{{ dispatcher.config['log_dir'] }}`). `script_path`, `hostname` and
`output_dir` are always strings, so no step may follow them: pass two could
not resolve `{{ hostname[0] }}`, so the check rejects it at startup instead.
Literal template text may stand next to the value (`{{ script_path }}.log`,
`--out={{ output_dir }}/x`). The output may sit at the top level, in the body,
`elif` or `else` of an `{% if %}`, or in the body or `else` of a `{% for %}`
that is not `recursive`, nested to any depth, and nowhere else.

The rule is checked statically, on the template's syntax tree. The template is
parsed with the pass-one environment, and the check:

1. collects the `Output` nodes that are reachable only through those
   containers (the template itself, `If` branches, and the body and `else` of
   a non-recursive `For`);
1. marks the root `Name` of each expression in those outputs that is a bare
   path rooted at a dispatcher-only name as allowed;
1. rejects every other `Name` node, in any context (load, store or parameter),
   whose name is dispatcher-only, and every macro name, import alias or
   namespace assignment that is one.

That rejects every filter, test, call and method call; every operator (`~`,
`+`, `%`, comparisons, `in`, `not`, `and`/`or`, conditional expressions);
slicing and subscripts that are not literals; use in an `{% if %}` condition
or a `{% for %}` iterable; any use inside `{% set %}` (inline or block),
`{% with %}`, `{% block %}`, macros, call blocks, filter blocks,
`{% autoescape %}` and recursive loops; and every binding that would shadow
one of the names (assignment, loop and `with` targets, macro parameters,
import aliases). The error names the dispatcher-only name and the template
line, and says how to write it instead: the bare value, with any text around
it written as literal template text.

Names are compared in their NFKC form. Jinja accepts non-ASCII identifiers
and compiles template names into Python identifiers, which Python
NFKC-normalizes, so a look-alike spelling in fullwidth or mathematical
letters would otherwise be a different name to the check but the same
variable at render time. A look-alike of a reserved name is rejected, as a
value and as a binding; only the plain ASCII spelling can be a bare value.

The check runs when the payload plugin is constructed, next to the syntax
check, so `courier validate` and `courier run` reject such a template at
startup whatever the data, the way they reject a syntax error; construction
reports a violation as a `ValueError`, chained from `DeferredExpressionError`,
that names the template. `Payload.render_script` runs the check again, before
compiling, on any template it renders with a `defer_nonce` (pass one), and
raises `DeferredExpressionError` for a violation.

### Pass one: the builder

The builder renders the template in a plain sandboxed Jinja environment with
`StrictUndefined`, against the context it owns:

- `files`, `job` and `config` (an alias for `job.config`). `config` is the
  JSON-normalised plain-data form of the job's config: exactly what the
  dispatcher sees after the job has crossed the broker. It never holds the
  builder's `payload` block.
- `builder`: `name`, `identifier`, `targets`.

Each dispatcher-only name that the caller does not supply is bound to a
placeholder. The placeholder records its access path, one step per attribute
or subscript (`dispatcher.config.log_dir` and `dispatcher.config['log_dir']`
are both `["dispatcher", "config", "log_dir"]`), and renders as a marker:

```text
\x00COURIER-DEFER:<nonce>:<base64(JSON list of path steps)>\x00
```

`<nonce>` is random per emitted job (`secrets.token_hex(16)`) and travels on
`PayloadSpec.defer_nonce`. The placeholder does nothing else and does not
guard itself at run time: the static rule already guarantees that it is only
ever rendered in place, as a bare value, so every marker reaches the output
whole. Nothing else is deferred: a typo, a missing metadata key or an
out-of-range index raises Jinja's ordinary `UndefinedError` on the builder,
and `| default(...)`, `is defined` and `{% if x is defined %}` over
builder-side values behave exactly as in stock Jinja.

### Pass two: the dispatcher

The dispatcher never parses the carried script as a template, and runs no
Jinja on it. It finds the markers with a regular expression and, for each one
that carries the job's nonce:

1. decodes the path, raising `DeferredExpressionError` if the marker is
   malformed or the path's root is not one of `DISPATCHER_CONTEXT_NAMES`;
1. walks the path through its own context -- `dispatcher` (`name`,
   `identifier`, `config`), `script_path`, `hostname`, and for
   `slurm_dispatcher` `output_dir` -- taking each step as a key of a mapping
   or an `int` index into a list or tuple;
1. raises `DeferredExpressionError`, saying the value is not defined by this
   dispatcher, if the root or any step does not exist: `{{ output_dir }}` sent
   to a `local_dispatcher`, for example, is an error, not an empty string. A
   step below a value that is neither a mapping nor a list or tuple
   (`dispatcher.config.log_dir.parent`) raises it too, naming that value's
   type;
1. applies the same finalisation as pass one (`None` and `[]` render as an
   empty string) and substitutes `str(value)`.

Everything else is copied through unchanged: whatever pass one rendered from
job data, including a `{{` or `{%` in a file name or in metadata, a NUL
character, or text that looks like a marker but does not carry the job's
nonce. A forged marker is never resolved, and no data value is evaluated on
either side.

The command parts (`binary`, `prefix_args`, `suffix_args`) are not part of the
template, and the bare-value rule does not apply to them. The dispatcher
renders each of them, as one argument, in a single strict Jinja pass with its
real context (which has no `builder` namespace), and passes each to the
process as its own argv entry, an empty one included: a rendered value is
never spliced unquoted into a shell command line or into Python source (a
Slurm `--wrap` command shell-quotes each argument). They are the only
templates in the command; the script path (under `TMPDIR` or
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

### Why a static bare-value rule

This section amends the decision. A later revision, also never released, let
dispatcher-only values travel further through Jinja and guarded them at run
time instead: a pass-one environment subclass wrapped every filter and test to
refuse a placeholder, intercepted calls, string methods and `%` and
`str.format` conversions that would compute with one, recorded the markers
each render emitted and checked the output for intact markers and stray NULs,
and pass two compiled each decoded path as a Jinja expression. It allowed
`~`, `join` and plain string formatting over a dispatcher-only value. The
static bare-value rule replaces all of it, because:

- **It is simpler.** One walk over the syntax tree at construction replaces
  guards spread over the environment, the placeholder and the output, and pass
  two becomes a walk through the dispatcher's context.
- **It has no gaps.** A run-time guard sees only what reaches it. Comparisons,
  `in`, truthiness, iteration and sorting operated on the placeholder's marker
  text, so a template that branched on a dispatcher-only value silently took
  the wrong branch, and an escaped copy of a marker cut short before its nonce
  left nothing for the output check to find. Each fix added a guard and a new
  edge. The static rule decides from the template alone, so none of those uses
  can reach a render. It compares names as the compiled template will see
  them (in NFKC form, see above), so a look-alike spelling cannot slip a
  reserved name past it.
- **Errors move to startup.** A guard fired only when some job's data reached
  the offending expression, and dropped that job. The static rule rejects the
  template in `courier validate` and at startup, whichever branches the data
  would take.
- **Pass two evaluates nothing.** Resolving a decoded list of keys cannot run
  template code, whatever a marker holds.

The cost is expressiveness, accepted below: a dispatcher-only value can no
longer be concatenated, joined, formatted or passed to a macro in the
template. Literal template text next to the bare value covers concatenation,
and the script itself or the command arguments cover the rest.

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
  that cannot be read, a template with a Jinja syntax error, or a template
  that uses a dispatcher-only name other than as a bare value makes payload
  construction fail with an error naming the payload, the file (or "inline
  script") and the line. Either way `courier run` stops before the service
  starts, and `courier validate` reports the same config.
- **Pass one.** Any exception from `Payload.to_job_spec` inside
  `JobBuilder.emit` is contained per job, and so is a `to_job_spec` (a
  subclass's override) that returns something other than a `PayloadSpec`, so
  no job is published without one. It is logged at ERROR with the job
  identifier, its files and the exception, and
  `courier_job_builder_emit_failures_total{reason="render"}` is incremented
  once per target. Nothing is published. Rendering happens before any
  per-target emit claim is taken, so no claim is left held. The job's files
  are not returned to the group, because they would fail the same render
  again; the ERROR line is the record of them. `emit` returns normally, so the
  consumer loop, the timeout reapers, the Redis-merge callback and startup
  hydration all keep running. A builder with no payload bound is not a
  per-job failure but a wiring error: `start()` refuses to run at all, and
  code that drives an unbound builder directly gets `ConfigurationError`
  before a job leaves its group, so no file is lost (and `emit` itself raises
  it before rendering).
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
shell and bash payloads ignore it.) `sbatch` runs directly under
`submission_timeout_seconds`, never through the payload's job-execution path,
and a rejection reports its return code and stderr. Output files are named
with the Slurm job id, so two submissions of one job never share them. The script is kept whenever the submitted job may still need it
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

- **Dispatcher-only values are bare values.** A template interpolates a
  dispatcher-only value as `{{ name }}` or a literal path below it, with
  literal text around it, and does nothing else with it. It cannot branch on,
  filter, test, concatenate, format or pass along such a value, and a template
  that tries is rejected at startup, even in a branch no job would take. Anything the
  template needs to decide must be decided on the builder side, inside the
  script at run time (in shell or Python, not Jinja), or in the command
  arguments, which the dispatcher renders with its real values.
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
