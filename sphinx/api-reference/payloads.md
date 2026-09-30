# Payloads Reference

A **payload** describes what a job executes: a script (a Jinja2 template file or
an inline template) and how to launch it. Payloads are configured on the **job
builder**, not on the dispatcher. The builder renders the payload for each job
it emits and attaches the result to the job. The dispatcher that receives the
job writes the script to disk and runs it.

For the design rationale, see {doc}`../concepts/adr/0011-dispatchers-execute-jobs`.
For the dispatcher side (timeouts, logging, output scanning), see
{doc}`dispatchers`.

## Configuring a payload

Every job builder nests exactly one payload under `config.payload`:

```yaml
spec:
  run:
    - build:
        kind: job_builder
        name: filter_and_group
        config:
          files_per_job: 1
          targets:
            - process
          payload:
            convert: # payload identifier
              kind: payload # must be "payload"
              name: bash_payload # payload plugin
              config:
                script: |
                  echo "Converting {{ files[0].file }}"
                  convert.sh "{{ files[0].file }}"

    - process:
        kind: dispatcher
        name: local_dispatcher
```

The payload identifier (`convert` above) must be unique among all steps and
payloads in the file. It appears as the `payload_identifier` label on payload
metrics and is part of the dispatcher's deduplication key.

The block is required: a job builder without one is a configuration error,
whichever builder it is. `courier validate` reports a missing block as

```text
build.config.payload         required, but missing: every job builder needs a payload block: its config nests exactly one payload plugin under `payload:`, which is what its jobs execute
```

and `courier run` refuses to start with an `InvalidPluginConfigError` that
names the builder and shows a minimal block (see
{doc}`plugins`). A block that is not a mapping, nests no plugin or more than
one, or nests a plugin whose `kind` is not `payload` is rejected the same way.
At startup, service preflight binds the payload plugin to its builder; a
builder refuses to start without it, and every job it emits carries it.

The payload plugin is constructed when `courier run` starts the job builder.
Construction reads the template (`file`, or the inline `script`) once and
parses it; editing the file afterwards takes effect on the next restart. A
template that cannot be read, or that has a Jinja syntax error, stops
`courier run` at startup with an error naming the payload, the file (or
"inline script") and the line. `courier validate` builds the payload the same
way, so it reports the same errors, as well as unknown config keys and a
payload that cannot run on the dispatchers its builder targets. A template
`file` that is not visible from where `courier validate` runs is noted, not
checked.

## Shipped payload plugins

| Plugin           | Class           | Interpreter | Script suffix |
| ---------------- | --------------- | ----------- | ------------- |
| `shell_payload`  | `ShellPayload`  | `sh`        | `.sh`         |
| `bash_payload`   | `BashPayload`   | `bash`      | `.sh`         |
| `python_payload` | `PythonPayload` | `python`    | `.py`         |

The interpreter is looked up on the dispatcher host's `PATH` unless
`default_binary` names another one. The payload process inherits the
dispatcher's environment.

### Representations and lowering

Payload classes form a hierarchy: `PythonPayload` is a `BashPayload`, which is
a `ShellPayload`. A dispatcher lists the classes it can execute in
`representations`, and runs a job's payload as the most specific class in the
payload's hierarchy that it lists. Both shipped dispatchers list all three
classes, so a shipped payload always runs as itself. A third-party payload
that subclasses one of them, sent to a dispatcher that does not list it, runs
as the nearest shipped class, with that class's interpreter and command.

Preflight checks every builder's payload against every dispatcher it targets,
including dispatchers that run in another container of a split `--only`
deployment. An incompatible pair is a startup error. The payload plugin must
also be installed wherever a dispatcher that receives it runs.

## Payload config fields

`PayloadConfig`, shared by all three shipped payloads. Unknown keys are
rejected. A key that belongs to a dispatcher, meaning a field of any installed
dispatcher's config model (`timeout_seconds`, or `partition`, which only
`slurm_dispatcher` defines), is reported as belonging on the dispatcher, by
`courier validate` and `courier run` alike. At least one of `file`, `script`
or `binary` must be set.

| Field               | Type        | Default | Description                                                                                                                                                                                                                                                                                                                                                     |
| ------------------- | ----------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `file`              | path        | `None`  | Jinja2 template file, read on the **builder** host when the plugin is constructed. It does not need to exist on the dispatcher host. Its suffix is kept for the script the dispatcher writes, so name Python templates `*.py`. If both `file` and `script` are set, `file` is used.                                                                             |
| `script`            | `str`       | `None`  | Inline Jinja2 template, rendered like `file`.                                                                                                                                                                                                                                                                                                                   |
| `binary`            | `str`       | `None`  | A program to run instead of running the script with the interpreter. See [How each payload builds its command](#how-each-payload-builds-its-command).                                                                                                                                                                                                           |
| `default_binary`    | `str`       | `None`  | Interpreter to use instead of the class default (`sh`, `bash`, `python`), for example `/opt/venvs/geo/bin/python`.                                                                                                                                                                                                                                              |
| `toolchain`         | `list[str]` | `[]`    | Executables that must exist on the dispatcher host, probed with `<interpreter> -c 'command -v <tool>'` (shell, bash) or a Python `shutil.which` check (python). Checked before the first job with this payload configuration runs, and a successful check is cached. A missing tool makes the job unexecutable: it is parked on the dead-letter queue, not run. |
| `toolchain_prepend` | `list[str]` | `[]`    | Used by `python_payload` only, and only in its `toolchain` check: each `toolchain` entry is probed with `[*toolchain_prepend, <interpreter>, -c, <shutil.which check>]` (for example `["conda", "run", "-n", "geo"]`). It is never part of a job's command. `shell_payload` and `bash_payload` accept it and ignore it.                                         |
| `prefix_args`       | `list[str]` | `[]`    | Arguments placed between the interpreter and the script: interpreter options such as `-x` for bash or `-u` for Python.                                                                                                                                                                                                                                          |
| `suffix_args`       | `list[str]` | `[]`    | Arguments placed after the script: the script's own arguments (`$1`..., `sys.argv[1:]`).                                                                                                                                                                                                                                                                        |

`binary`, `prefix_args` and `suffix_args` are Jinja2 templates too, but they
are rendered only on the dispatcher. See [Command arguments](#command-arguments).

## How each payload builds its command

The dispatcher writes the rendered script to a new file with a random name,
mode `0755`, created exclusively. `local_dispatcher` names it
`courier-XXXXXXXX<suffix>` in the temporary directory (`$TMPDIR`, else the
platform default); `slurm_dispatcher` writes it to `slurm_output_dir`, under a
name that starts with the job identifier (`<job-id>-XXXXXXXX<suffix>`). Below,
`<script>` is that path. How long the file lives depends on the dispatcher:

- **`local_dispatcher`** removes it once the job has run, and also when
  preparing the job fails after the file was written.
- **`slurm_dispatcher`** removes a script submitted as the batch script once
  it is done with the job, since Slurm runs its own copy: right after
  submission in no-wait mode, after polling ends in wait mode. A script run
  through `--wrap` is read by the job when it starts, so it is kept until no
  job can read it any more, and is sometimes left in `slurm_output_dir` for
  good; see {ref}`script-lifetime`.

Every part of the command is a separate argument: nothing is joined into a
shell command line (a Slurm `--wrap` command shell-quotes each argument, so
the shell splits it back into exactly those arguments), so a rendered value (a
file name containing a quote, say) is never re-parsed, and the script path
itself is never rendered as a template. An argument that renders to an empty
string is passed as an empty argument, not dropped.

### `shell_payload` and `bash_payload`

- **Without `binary`**, the script is run by the interpreter:
  `sh|bash [prefix_args...] <script> [suffix_args...]`.
- **With `binary`**, the interpreter executes the program with its arguments:
  `<binary> [prefix_args...] [<script>] [suffix_args...]`. `<script>` is
  included only if the payload also has a `file` or `script`. (The argv is
  `sh|bash -c ': "${1:?no command to run}"; "$@"' sh|bash <binary> ...`.)

### `python_payload`

- **Python source**: an inline `script`, or a `file` ending in `.py`, with no
  `binary`. The script is run by Python:
  `python [prefix_args...] <script> [suffix_args...]`.
- **Subprocess**: a `binary`, or a `file` that does not end in `.py`. Python
  runs the program with `subprocess.run(..., check=True)`:
  `python -c 'import subprocess, sys; subprocess.run(sys.argv[1:], check=True)' [<binary>] [prefix_args...] [<script>] [suffix_args...]`.
  Without a `binary`, the program is the first `prefix_args` entry if there is
  one, otherwise the rendered script itself, which then needs a shebang line. A
  failing program surfaces as return code `1`, with Python's
  `CalledProcessError` traceback on stderr.

### Examples

| Payload config                                                               | Command run on the dispatcher                                                                      |
| ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `bash_payload`, `script: ...`                                                | `bash /tmp/courier-k2j9x1.sh`                                                                      |
| `bash_payload`, `script: ...`, `prefix_args: ["-x"]`, `suffix_args: ["out"]` | `bash -x /tmp/courier-k2j9x1.sh out`                                                               |
| `bash_payload`, `binary: gdalinfo`, `suffix_args: ["{{ files[0].file }}"]`   | `gdalinfo /data/a.nc`, run through `bash -c`                                                       |
| `python_payload`, `script: ...`, `prefix_args: ["-u"]`                       | `python -u /tmp/courier-k2j9x1.py`                                                                 |
| `python_payload`, `file: /opt/pipeline/run.py`                               | `python /tmp/courier-k2j9x1.py`                                                                    |
| `python_payload`, `script: ...`, `default_binary: /opt/venvs/geo/bin/python` | `/opt/venvs/geo/bin/python /tmp/courier-k2j9x1.py`                                                 |
| `python_payload`, `binary: gdalinfo`, `suffix_args: ["/data/a.nc"]`          | `python -c 'import subprocess, sys; subprocess.run(sys.argv[1:], check=True)' gdalinfo /data/a.nc` |

## Template context

A payload template is rendered in two passes: once by the builder when it emits
the job, and once by the dispatcher when it writes the script.

### Pass one: the builder

| Name      | Content                                                                                                                                                                                                                                 |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `files`   | The job's files, sorted by path. Each is a dictionary with `file` (path string), `hostname`, `source`, `instrument`, `processing_stage`, `domain`, `metadata` (dictionary), `num_expected` and `timestamp` (ISO 8601 string or `None`). |
| `job`     | `name`, `identifier`, `config`, `last_modified`, `timeout`, `correlation_id`, `emit_time`, `targets`.                                                                                                                                   |
| `config`  | Alias for `job.config`, as plain JSON data: exactly what the dispatcher sees after the job has crossed the broker. It never contains the builder's `payload` block.                                                                     |
| `builder` | The emitting builder: `name`, `identifier`, `targets`.                                                                                                                                                                                  |

Pass one is strict. A name, attribute, key or index that does not exist, such
as a typo or a metadata key a file does not have, is an error, not an empty
string. Jinja's own tools for optional values work as usual on these names:

```jinja
{{ files[0].metadata.sector | default("full-disk") }}
{% if files[0].metadata.band is defined %}--band {{ files[0].metadata.band }}{% endif %}
```

`None` and an empty list render as an empty string.

### Pass two: the dispatcher

Four names are reserved for values only the dispatcher knows:

| Name          | Content                                                                                                              |
| ------------- | -------------------------------------------------------------------------------------------------------------------- |
| `dispatcher`  | `name`, `identifier`, and `config`: the dispatcher's own validated config (for example `dispatcher.config.log_dir`). |
| `script_path` | Path of the script file the dispatcher wrote for this job.                                                           |
| `hostname`    | Host name of the dispatcher.                                                                                         |
| `output_dir`  | `slurm_dispatcher` only: its `slurm_output_dir`.                                                                     |

In pass one each reserved name renders as an opaque marker, and the dispatcher
replaces the markers when it writes the script. Nothing else in the script is
re-rendered: `{{` or `{%` that arrives in a file name or in metadata stays
literal text.

Reserved names support **leaf interpolation** only:

```jinja
echo "running {{ script_path }} on {{ hostname }}"
log={{ dispatcher.config.log_dir }}/{{ job.identifier }}.log
tag={{ "courier-" ~ dispatcher.identifier }}
```

Anything that would need the value on the builder is rejected when the builder
renders the job: filters (`{{ hostname | upper }}`), tests
(`{% if output_dir is defined %}`), `| default`, conditionals
(`{% if script_path %}`), loops, arithmetic, comparisons, calling it or one of
its methods (`{{ hostname.upper() }}`), and changing the rendered text around
one (`{% filter upper %}`, slicing). Decide such things in the script itself,
at run time.

A string built from a reserved name with `~` (`output_dir ~ "/x"`) is treated
the same way: filters, tests, its methods (`.replace(...)`) and subscripts
are rejected, as are `%` and `str.format` conversions other than a plain `%s`
or `{}` (a width, precision or conversion such as `'%5s'`, `'{:>9}'` or
`'{!r}'`, or a `%(...)` key that contains a parenthesis).

A call may receive a reserved value, or a string built from one, when it only
stores or returns it: a macro argument, `namespace(p=hostname)`,
`dict(p=hostname)`, `cycler(hostname, "x")` and `loop.cycle(...)`,
`joiner(hostname)`, `list.append(hostname)`, and the default of
`dict.get(key, hostname)`. A call that would compute with it is rejected:
passing it to a string method (`"a/b".split(x)`, `" ".join(x)`) or using it as
the key of `get`, `pop` or `setdefault`.

The rendered job is checked too. Every marker in it must be one this render
emitted, unchanged, so a reserved value that was escaped or rewritten on the
way out (`| tojson` or `| urlencode` over a dictionary holding one) fails the
job at the builder rather than reaching the script. A NUL character is how a
truncated marker shows, so when a template interpolates a reserved value, a
NUL anywhere else in the rendered script fails the job too: job data that
contains a NUL is refused in such a script, and passes through unchanged in
one that uses no reserved value.

What the builder cannot catch is *inspecting* such a string: comparisons
(`==`, `in`), truthiness, iteration, sorting, de-duplication and searching
(`list.count`) see a placeholder, not the dispatcher's value, so never branch
on them.

A reserved name the receiving dispatcher does not define, such as
`{{ output_dir }}` sent to a `local_dispatcher`, fails that job on the
dispatcher.

### Command arguments

`binary`, `prefix_args` and `suffix_args` are rendered only on the dispatcher,
in one strict pass, each as a single argument. Their context is `files`, `job`,
`config` and the four dispatcher names. It has no `builder` namespace. They
are the only templates in the command: the script path, the interpreter and
the wrappers courier adds are passed literally. For
example, `suffix_args: ["{{ files[0].file }}", "{{ hostname }}"]` passes the
first file's path and the dispatcher's host name as two arguments.

(payload-wire-format)=

## What a job carries

The builder attaches the rendered payload to each job it emits as
`job.payload`, a `courier.types.payload.PayloadSpec`:

| Field         | Content                                                                                                                                                                                                                                                                                                                       |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`        | The payload plugin the builder configured, such as `bash_payload`.                                                                                                                                                                                                                                                            |
| `identifier`  | The payload identifier.                                                                                                                                                                                                                                                                                                       |
| `config`      | The payload's validated config **without** `script`: `file` (as a path string, for information only), `binary`, `default_binary`, `toolchain`, `toolchain_prepend`, `prefix_args`, `suffix_args`, and any field of the plugin's own config model. The argument templates are sent as written, and rendered on the dispatcher. |
| `script`      | The script as the builder rendered it (pass one), from `file` or the inline `script`. `None` for a payload with only a `binary`.                                                                                                                                                                                              |
| `suffix`      | Suffix of the file the dispatcher writes the script to.                                                                                                                                                                                                                                                                       |
| `defer_nonce` | The random nonce that authenticates this job's markers for reserved names.                                                                                                                                                                                                                                                    |

`PayloadSpec.script` is the only copy of the template a job carries: the raw
template is never sent, and a template `file` is read on the builder, so it
need not exist on the dispatcher host. When the dispatcher rebuilds the
payload from the spec, it sets the rebuilt config's `script` to the rendered
`PayloadSpec.script`, so anything on the dispatcher side that reads
`config.script` sees the script that will run. A spec whose `config` still
carries a raw `script` is rebuilt the same way: the rendered copy replaces
it. A spec with no rendered `script` whose `config` sets neither `file` nor
`binary` has nothing to run, and the job is parked as unexecutable. A payload
rebuilt from a spec runs that script but cannot render a job
(`to_job_spec` raises `CourierError`), because its `config.script` is
already-rendered text that may hold job data.

## Errors and where they surface

| Problem                                                                                                                                                                                    | Surfaces                             | Effect                                                                                                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Job builder with no `payload` block, or a malformed one                                                                                                                                    | `courier validate` and `courier run` | The service does not start.                                                                                                                                                                            |
| Template file unreadable; Jinja syntax error in the template or in `binary`, `prefix_args` or `suffix_args`                                                                                | `courier validate` and `courier run` | The service does not start.                                                                                                                                                                            |
| Unknown or misplaced payload config key                                                                                                                                                    | `courier validate` and `courier run` | Validation error naming the key.                                                                                                                                                                       |
| Undefined name, attribute, key or index in the template; unsupported use of a reserved name; any other data-dependent render error                                                         | Builder, when the job is emitted     | That job is dropped, not published. ERROR log with the job identifier, its files and the error; `courier_job_builder_emit_failures_total{reason="render"}` once per target. The builder keeps running. |
| Reserved name this dispatcher does not define; undefined name in `binary`, `prefix_args` or `suffix_args`                                                                                  | Dispatcher                           | The job fails: ERROR log, `courier_dispatcher_jobs_processed_total{status="failure"}`.                                                                                                                 |
| Job has no payload (published by an older builder); payload plugin not installed; no compatible representation; invalid payload spec, including one with nothing to run; toolchain missing | Dispatcher                           | The job is parked on the dispatcher's dead-letter queue: `courier_dispatcher_jobs_processed_total{status="unexecutable"}`. See {doc}`dispatchers`.                                                     |
| Script exits non-zero or times out                                                                                                                                                         | Dispatcher                           | ERROR log with the return code; `courier_payload_jobs_processed_total{status="failure"}`. The execution log carries the return code and output.                                                        |

## Payload metrics

| Metric                                           | Type      | Labels                                         |
| ------------------------------------------------ | --------- | ---------------------------------------------- |
| `courier_payload_jobs_processed_total`           | Counter   | `payload_name`, `payload_identifier`, `status` |
| `courier_payload_job_execution_duration_seconds` | Histogram | `payload_name`, `payload_identifier`           |

`payload_name` is the configured payload plugin (for example `bash_payload`),
even when a dispatcher runs the payload as a lower representation, and
`status` is `success` for return code 0 and `failure` otherwise. Toolchain
checks are not counted. Under `slurm_dispatcher` in wait mode the metrics
describe the Slurm job, not the `sbatch` call.

`local_dispatcher` wraps each run in a `payload.get_payload_from_job` span, a
child of `dispatcher.execute_job`; see {doc}`../operations/tracing`.

To write a payload plugin of your own, see {doc}`../contribute/writing-a-plugin`.
