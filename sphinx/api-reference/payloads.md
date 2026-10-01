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

The block is required, whichever builder it is: a builder without one, or
with a malformed one, stops `courier run` at startup with an error that names
the builder and shows a minimal block. The builder constructs the payload
plugin when it is itself constructed, and construction reads the template
(`file`, or the inline `script`) once, parses it and checks it, so editing the
file takes effect on the next restart. `courier validate` builds the payload
the same way, except that a template `file` it cannot see is noted, not
checked. See [Errors and where they surface](#errors-and-where-they-surface).

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
classes, so a shipped payload always runs as itself.

A payload run as an ancestor is **lowered**: it still runs with its own
interpreter, launched by the ancestor's. The dispatcher builds the payload's
own command and passes it to `Payload.render_script`, which hands it to the
ancestor's `wrap_command`. A `python_payload` on a dispatcher that lists only
`BashPayload` runs as
`bash -c '"$@"' bash python [prefix_args...] <script> [suffix_args...]`, and
one lowered to `PythonPayload` runs from `python -c` with `subprocess.run`.
Each argument stays a separate argument. The `toolchain` check is lowered the
same way. A payload class can override `render_script` to lower differently.

Preflight checks every builder's payload against every dispatcher it targets,
including dispatchers that run in another container of a split `--only`
deployment. An incompatible pair is a startup error. The payload plugin must
also be installed wherever a dispatcher that receives it runs.

## Payload config fields

`PayloadConfig`, shared by all three shipped payloads. Unknown keys are
rejected. An option every dispatcher takes (`timeout_seconds`, `log_to_file`
and the others in {doc}`dispatchers`) is reported as belonging on the
dispatcher; any other unknown key, such as `slurm_dispatcher`'s `partition`,
is reported only as unknown. At least one of `file`, `script` or `binary` must
be set.

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

Below, `<script>` is the file the dispatcher writes the rendered script to;
{doc}`dispatchers` gives its name and how long it is kept
({ref}`script-lifetime` for Slurm).

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

A payload template is rendered with Jinja once, by the builder, when it emits
the job. Where the template uses a value that only the dispatcher knows, the
builder leaves a placeholder, and the dispatcher fills the placeholders in when
it writes the script, without rendering the script again.

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

### Values only the dispatcher knows

Four names are reserved for values only the dispatcher knows:

| Name          | Content                                                                                                              |
| ------------- | -------------------------------------------------------------------------------------------------------------------- |
| `dispatcher`  | `name`, `identifier`, and `config`: the dispatcher's own validated config (for example `dispatcher.config.log_dir`). |
| `script_path` | Path of the script file the dispatcher wrote for this job.                                                           |
| `hostname`    | Host name of the dispatcher.                                                                                         |
| `output_dir`  | `slurm_dispatcher` only: its `slurm_output_dir`.                                                                     |

A template can use these names in one way only: as a **bare value**. That is
a `{{ ... }}` that holds the name and nothing else, or a path below
`dispatcher` made of attribute names that do not start with `_` (`.log_dir`)
and string or integer literals in brackets (`['log_dir']`, `[0]`).
`script_path`, `hostname` and `output_dir` are strings, so nothing follows
them. Text next to the value is written as literal template text:

```jinja
echo "running {{ script_path }} on {{ hostname }}"
exec > {{ script_path }}.log 2>&1
log={{ dispatcher.config.log_dir }}/{{ job.identifier }}.log
log={{ dispatcher.config['log_dir'] }}/{{ job.identifier }}.log
tag=courier-{{ dispatcher.identifier }}
process --out={{ output_dir }}/{{ files[0].metadata.band }}
```

A bare value can stand at the top level of the template, in any branch of an
`{% if %}` (`{% elif %}` and `{% else %}` included), or in the body or
`{% else %}` of a `{% for %}` loop that is not `recursive`, nested to any
depth. The condition and the loop's iterable use builder values only:

```jinja
{% if config.keep_log | default(false) %}
exec > {{ dispatcher.config.log_dir }}/{{ job.identifier }}.log 2>&1
{% endif %}
{% for f in files %}
cp "{{ f.file }}" "{{ output_dir }}/"
{% endfor %}
```

Every other use of a reserved name is rejected:

- a filter, a test or a call: `{{ hostname | upper }}`,
  `{{ output_dir | default("/tmp") }}`, `{% if output_dir is defined %}`,
  `{{ hostname.upper() }}`, `{{ namespace(h=hostname) }}`;
- an operator: `~`, `+`, `%`, a comparison, `in`, `not`, `and`, `or`, or an
  inline `... if ... else ...`, as in `{{ "courier-" ~ dispatcher.identifier }}`
  or `{{ output_dir ~ "/x" }}`;
- a slice, or a subscript that is not a literal: `{{ script_path[:-3] }}`,
  `{{ dispatcher.config[key] }}`;
- an attribute or subscript after `script_path`, `hostname` or `output_dir`:
  `{{ hostname[0] }}`, `{{ output_dir.parent }}`;
- a condition or a loop over one: `{% if script_path %}`,
  `{% for d in dispatcher.config.dirs %}`;
- any use inside another tag, such as `{% set %}` (inline or as a block),
  `{% with %}`, `{% block %}`, `{% macro %}`, `{% call %}`, `{% filter %}`,
  `{% autoescape %}` or a `recursive` loop, even as a bare value;
- binding a reserved name yourself: `{% set hostname = "x" %}`,
  `{% for output_dir in ... %}`, a macro parameter, a `{% with %}` target or
  an import alias. The four names always mean the dispatcher's values;
- spelling a reserved name with look-alike non-ASCII letters, such as
  fullwidth letters. Python, which runs the compiled template, reads such a
  name as the plain one, so the check compares names in that form (NFKC) and
  refuses the look-alike.

The rule is checked on the template's text when the payload plugin is
constructed, so it covers every branch, whether or not any job takes it. A
template that breaks it stops `courier run` at startup, and `courier validate`
reports it. The error names the reserved name and the line it is used on, and
says how to write it instead: the name as a bare value, with any text around
it written literally. For example, `tag={{ "courier-" ~ dispatcher.identifier }}`
becomes `tag=courier-{{ dispatcher.identifier }}`. Anything that has to be
decided from a dispatcher's value, such as a branch, a default or a
transformation, belongs in the script itself, at run time, or in the
[command arguments](#command-arguments), which the dispatcher renders with its
real values.

### Pass two: the dispatcher

In pass one each bare value renders as an opaque marker that holds a random
nonce issued for the job and the value's path (`dispatcher`, `config`,
`log_dir`). When the dispatcher writes the script, it replaces each marker
that carries the job's nonce with the value from its own context. Each step of
the path is looked up as a key, so `.log_dir` and `['log_dir']` find the same
value, or as an index into a list; nothing is called. The value is rendered as
in pass one: `None` and an empty list become an empty string.

Nothing else in the script changes, and nothing in it is rendered as a
template again. A `{{` or `{%` that arrives in a file name or in metadata stays
literal text, and so does a NUL character or text that looks like a marker but
does not carry the job's nonce.

A reserved name or key the receiving dispatcher does not define, such as
`{{ output_dir }}` sent to a `local_dispatcher`,
`{{ dispatcher.config.no_such_option }}`, or a step below a null value, fails
that job on the dispatcher as not defined by that dispatcher. So does a step
below a value that is not a dictionary or a list, such as
`{{ dispatcher.config.log_dir.parent }}`: the error says which value it is.

### Command arguments

`binary`, `prefix_args` and `suffix_args` are rendered only on the dispatcher,
in one strict pass, each as a single argument. Their context is `files`, `job`,
`config` and the four dispatcher names, which hold the dispatcher's real
values here, so the bare-value rule does not apply: `"{{ hostname | lower }}"`
works. It has no `builder` namespace. They are the only templates in the
command: the script path, the interpreter and the wrappers courier adds are
passed literally. For example,
`suffix_args: ["{{ files[0].file }}", "{{ hostname }}"]` passes the first
file's path and the dispatcher's host name as two arguments.

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
| `defer_nonce` | The random nonce issued for this job. The dispatcher fills in only the markers for reserved names that carry it.                                                                                                                                                                                                              |

`PayloadSpec.script` is the only copy of the template a job carries, and a
template `file` is read on the builder, so it need not exist on the dispatcher
host. The dispatcher rebuilds the payload with `config.script` set to the
rendered `PayloadSpec.script` (replacing any raw `script` an older builder
put in `config`). A rebuilt payload runs that script but cannot render a job again,
and a spec with nothing to run is parked as unexecutable.

(payload-errors)=

## Errors and where they surface

| Problem                                                                                                                                                                                    | Surfaces                             | Effect                                                                                                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Job builder with no `payload` block, or a malformed one; payload plugin not installed where the builder runs; payload that cannot run on a dispatcher its builder targets                  | `courier validate` and `courier run` | The service does not start.                                                                                                                                                                            |
| Template file unreadable; Jinja syntax error in the template or in `binary`, `prefix_args` or `suffix_args`; a reserved name used in the template other than as a bare value               | `courier validate` and `courier run` | The service does not start.                                                                                                                                                                            |
| Unknown or misplaced payload config key                                                                                                                                                    | `courier validate` and `courier run` | Validation error naming the key.                                                                                                                                                                       |
| Undefined name, attribute, key or index in the template; any other data-dependent render error                                                                                             | Builder, when the job is emitted     | That job is dropped, not published. ERROR log with the job identifier, its files and the error; `courier_job_builder_emit_failures_total{reason="render"}` once per target. The builder keeps running. |
| Reserved name, key or index this dispatcher does not define; undefined name in `binary`, `prefix_args` or `suffix_args`                                                                    | Dispatcher                           | The job fails: ERROR log, `courier_dispatcher_jobs_processed_total{status="failure"}`.                                                                                                                 |
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
