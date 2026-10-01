# Interactive Service Config Generator

The `courier init` command creates a new service configuration file through
an interactive prompt-based workflow. It guides you step-by-step through
selecting data monitors, job builders (each with the payload its jobs run),
and dispatchers, then generates a validated YAML file ready to run.

## Usage

```bash
courier init
```

Follow the prompts:

1. **Service metadata** — name, namespace, and description for your service
1. **Data monitors** — pick from available monitors like file system pollers,
   RabbitMQ watchers, S3 pollers, Kafka consumers, and cron-based triggers
1. **Job builders** — choose how incoming files are grouped into processing
   jobs. Each job builder then asks for its **payload**, the script its jobs
   run (`bash_payload`, `python_payload` or `shell_payload`). A job builder
   needs exactly one, so this prompt cannot be skipped.
1. **Dispatchers** — select where jobs are executed: on this host
   (`local_dispatcher`) or on a Slurm cluster (`slurm_dispatcher`)
1. **Review and save** — preview your configuration before writing to disk

The command generates a `{name}-service.yaml` file ready to run with
`courier run {name}-service.yaml`.

## Selecting a Plugin

Each category prints a numbered table of the plugins available to it. At the
prompt you can enter any of:

`2`
: The number in the `#` column — the quickest option, and the one to reach for.

`s3_poller`
: The full plugin name, case-insensitively.

`s3`
: Any prefix that matches exactly one plugin. A prefix matching several
(`fi` → `file_count_builder`, `filter_and_group`) is refused and lists the
candidates rather than guessing.

Whichever form you use, the resolved plugin name is echoed back before you are
asked to configure it, so a mistyped number is caught immediately. Press
{kbd}`Enter` on its own to move to the next category. The payload prompt is
the exception: it has no skip, and asks again until you choose one.

## Options

`--dry-run`
: Print the generated configuration to stdout without writing a file.
Useful for previewing the output before creating a file.

## Walkthrough

Here is a typical session creating a file watcher service (some prompts and
tables abridged):

```bash
$ courier init

╭──────────────────────────────────────────────────────────╮
│ Courier Init — interactive service config generator      │
│ Follow the prompts to create your service configuration. │
╰──────────────────────────────────────────────────────────╯
Service name (my-processor): my-processor
Namespace (my-processor):
Description (A courier service: my-processor): Watches for data and processes it

# ... add file_system_poller_watchdog, with path /data/incoming ...

Add a job builder (1-4, name, or Enter to skip): 1
  ✓ DummyJobBuilder
  Configure DummyJobBuilder? [y/n] (y): y
  Configure DummyJobBuilderConfig:
    targets (Optional list of targets to route to): dispatcher-local-dispatcher
  ✓ Configuration complete
╭─────────────────────────────────────────────────────────────────╮
│ Payload — Job Builder DummyJobBuilder needs exactly one payload │
╰─────────────────────────────────────────────────────────────────╯
                       Available Payloads
╭───┬────────────────┬──────────────────────────────────────────╮
│ # │ Name           │ Description                              │
├───┼────────────────┼──────────────────────────────────────────┤
│ 1 │ bash_payload   │ Payload class for Bash script execution. │
│ 2 │ python_payload │ Payload for Python execution.            │
│ 3 │ shell_payload  │ Payload class for shell execution.       │
╰───┴────────────────┴──────────────────────────────────────────╯
Choose the payload (1-3 or name): 1
  ✓ bash_payload
  Configure bash_payload? [y/n] (y): y
  Configure BashPayloadConfig:
    file:
    script: echo "Files assigned: {{ files | length }}"
    ...
  ✓ Configuration complete
  Add another job builder? [y/n] (n): n

# ... add local_dispatcher, preview and save ...
```

The preview lists each job builder's payload under it. The identifiers are
derived from the kind and plugin name, so a builder's `targets` can name a
dispatcher before you have added it (`dispatcher-local-dispatcher` above).
After the file is written, `courier init` prints the `courier validate` and
`courier run` commands to try next.

```{note}
**Configuration format compatibility**

`courier init` generates pipeline steps, and each job builder's payload, using
the nested `identifier:` / `spec:` format. Other examples throughout these docs
use a flat `- <name>:` singleton mapping (`payload: {<name>: {...}}` for a
payload). Both formats are valid. For hand-written configurations, use the
flat mapping style — it's shorter and matches the examples in
{doc}`quick-start`, {doc}`configuration`, and the tutorials.
```

## Generated File Structure

The generated YAML follows the `runcourier.dev/v1alpha1` API version. For the
session above it is:

```yaml
apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: my-processor
  namespace: my-processor
  description: Watches for data and processes it
spec:
  run:
  - identifier: data-monitor-file-system-poller-watchdog
    spec:
      kind: data_monitor
      name: file_system_poller_watchdog
      config:
        path: /data/incoming
        hostname: localhost
  - identifier: job-builder-dummyjobbuilder
    spec:
      kind: job_builder
      name: DummyJobBuilder
      config:
        targets:
        - dispatcher-local-dispatcher
        payload:
          identifier: payload-bash-payload
          spec:
            kind: payload
            name: bash_payload
            config:
              script: 'echo "Files assigned: {{ files | length }}"'
  - identifier: dispatcher-local-dispatcher
    spec:
      kind: dispatcher
      name: local_dispatcher
      config: null
```

The written file also spells out every default: `docstring`, `labels` and
`annotations` under `metadata`, and `broker` (in-memory), `allow_implicit_target`
and `service_config` under `spec`. They are omitted above.

Each pipeline step has an `identifier` (a DNS-safe name derived from
the kind and plugin) and a `spec` containing the plugin kind, name, and
any configuration values you provided during the prompts. A job builder's
payload is nested the same way, under `config.payload`.

## Default Broker

By default, the generated configuration uses the in-memory transport (no external broker). For production, add a `broker:` block. For all transport options — AMQP, Redis, in-memory, and generic Kombu URLs — see the {doc}`configuration` reference.

```yaml
spec:
  broker:
    transport: amqp
    host: rabbitmq.internal
    port: 5672
    username: courier
    password: ${COURIER_BROKER_PASSWORD}
  run:
    ...
```

## Next Steps

- {doc}`installation` — Install Courier
- {doc}`quick-start` — Your first pipeline step by step
- {doc}`configuration` — Full configuration reference
- {doc}`../operations/high-availability` — HA deployment guide
