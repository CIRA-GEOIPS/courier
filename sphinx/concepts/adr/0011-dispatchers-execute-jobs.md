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

## Decision

The falconer concept is removed and the falcon concept is renamed.

1. **Falconers become dispatchers.** The former `local_falconer` and
   `slurm_falconer` are now `local_dispatcher` and `slurm_dispatcher`. A
   dispatcher declares the *payload representations* it can execute
   (`representations`) and nothing else. `courier.falconers` is gone.

2. **Falcons become payloads.** A payload describes what to execute — a
   language/representation hierarchy (`PythonPayload` lowers to `BashPayload`
   lowers to `ShellPayload`), a template or inline script, and the arguments to
   launch it. The functionality is unchanged; only the vocabulary and the
   ownership are. `courier.payloads` replaces `courier.falcons`.

3. **Payloads are sub-plugins of job builders.** A job builder nests a
   `payload` block, exactly as a dispatcher used to nest a falcon. The builder
   renders the template once and attaches the serialized `PayloadSpec` to every
   `Job` it emits, so the payload travels with the work.

4. **Dispatchers execute the payload the job carries.** `Dispatcher` no longer
   holds a falconer. On each job it hydrates the payload class named by
   `job.payload` from the payload registry, lowers it to the most specific
   representation it supports, checks the toolchain on its own host (cached per
   payload configuration) and executes it.

## Two-pass rendering

Both sides render the same template, chained:

- **Pass one (builder).** The builder renders the payload template with the
  context it alone has — `files`, `job`, `config`, and a `builder` namespace
  (`name`, `identifier`, `targets`). Values only the dispatcher can resolve are
  left verbatim (a chaining undefined that preserves `{{ ... }}`), because the
  builder runs before a target or a script path exists.
- **Pass two (dispatcher).** After choosing the script path, the dispatcher
  renders the carried script again with a `dispatcher` namespace (`name`,
  `identifier`, `config`), `script_path`, `hostname` and, for Slurm,
  `output_dir`. The command and its arguments are rendered in the same pass.

A template may therefore mix builder-owned values (`{{ files[0].file }}`) with
dispatcher-owned values (`{{ script_path }}`, `{{ dispatcher.config.* }}`).

## Consequences

- A single dispatcher pool can execute jobs carrying different payloads, and
  one payload can be dispatched to several environments. Routing is unchanged.
- Compatibility is validated statically at preflight for declared targets
  (`Service._validate_payload_compatibility`) and at execution time for
  implicitly-routed jobs.
- Toolchain validation moves from dispatcher startup to first execution of a
  given payload configuration, where it is cached. Every execution pays for a
  cheap dictionary lookup rather than a probe.
- `serial_bash`, `parallel_bash`, `slurm_dispatcher` and `http_dispatcher` are
  deleted; their executable behaviour is provided by payloads plus the two
  dispatchers. `local_dispatcher` retains the `COURIER_METRIC:` stdout conduit.
- Dispatchers become first-class plugin registries. `courier plugins list` and
  `courier init` no longer treat them as "necessary" base classes, and
  `NECESSARY_REGISTRIES` is gone.
- Entry-point groups are now `courier.{data_monitors,job_builders,dispatchers,payloads}`
  plus the `courier.data_monitor_configs` config group.

## Trade-offs accepted

- A literal `{{` produced by pass-one data would be re-evaluated by pass two;
  templates that need a literal delimiter must use `{% raw %}`.
- A payload's template file need not exist on the dispatcher host; the carried
  rendered script is authoritative.

## Amendment: deferred expressions and re-entry

The initial implementation re-rendered the builder's pass-one output with Jinja on
the dispatcher. That made any template syntax substituted from job data (file
names, metadata) execute at dispatch time — a template-injection vector.

This is fixed by **deferred expressions**:

- Pass one emits an unforgeable marker for each value only the dispatcher can
  resolve: `\x00COURIER-DEFER:<nonce>:<base64(expression)>\x00`, where `<nonce>`
  is random per emitted job and carried on `PayloadSpec.defer_nonce`.
- Pass two evaluates *only* markers authenticated by that nonce, using a
  sandboxed expression compiler. The surrounding text — including any `{{ ... }}`
  that came from data — is never parsed as a template.

Two-pass templates therefore support **leaf interpolation only**
(`{{ script_path }}`, `{{ dispatcher.config.* }}`). Conditionals, loops and
filters over dispatcher-only values raise `DeferredExpressionError` rather than
silently rendering the wrong branch or leaking a marker.

Output-file re-entry is retained: `DispatcherGroupConfig.output_files` is scanned
after execution by the base dispatcher and each discovered `File` is re-emitted
through `Dispatcher.emit_file` into the file-found exchange.

Payloads are registered as non-threaded sub-plugins (`Payload.threaded = False`);
the plugin manager starts them without forking a thread or health-checking a
thread that does not exist.

A pass-one render failure is intentionally **not** contained: it propagates out of
`JobBuilder.emit` and terminates the builder process (the existing
`_run_handle_incoming_files` behaviour). This is accepted for now and should be
revisited if template errors become a live operational concern.
