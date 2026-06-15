# Interactive Service Config Generator

The `courier init` command creates a new service configuration file through
an interactive prompt-based workflow. It guides you step-by-step through
selecting data monitors, job builders, and dispatchers, then generates a
validated YAML file ready to run.

## Usage

```bash
courier init
```

Follow the prompts:

1. **Service metadata** — name, namespace, and description for your service
2. **Data monitors** — pick from available monitors like file system pollers,
   RabbitMQ watchers, S3/SFTP pollers, Kafka consumers, and cron-based triggers
3. **Job builders** — choose how incoming files are grouped into processing jobs
4. **Dispatchers** — select how jobs are executed (bash, Slurm, HTTP)
5. **Review and save** — preview your configuration before writing to disk

The command generates a `{name}-service.yaml` file ready to run with
`courier run {name}-service.yaml`.

## Options

`--dry-run`
: Print the generated configuration to stdout without writing a file.
  Useful for previewing the output before creating a file.

## Walkthrough

Here is a typical session creating a file watcher service:

```bash
$ courier init

╭──────────────────────────────────────────────╮
│ Courier Init — interactive service config    │
│ generator                                    │
╰──────────────────────────────────────────────╯

╭── Service Metadata ──────────────────────────╮
│ basic information about your service         │
╰───────────────────────────────────────────────╯
Service name [my-processor]: my-processor
Namespace [my-processor]:
Description [A courier service: my-processor]: Watches for data and processes it

╭── Data Monitors ─────────────────────────────╮
│ select which data monitor plugins to use     │
╰───────────────────────────────────────────────╯
┌──────────────────────────────────────────────────────────────────┐
│ Available Data Monitors                                          │
│ ...                                                              │
└──────────────────────────────────────────────────────────────────┘
Add a data monitor (name or Enter to skip): file_system_poller_watchdog
  Configure file_system_poller_watchdog? [y/n] (y): y
    path * (Directory path to watch for new files): /data/incoming
    hostname (Hostname to attach to emitted files) [localhost]:

Add another data monitor? [y/n] (n): n

# ... similar for Job Builders and Dispatchers ...

╭── Configuration Preview ─────────────────────╮
│ ...                                           │
╰───────────────────────────────────────────────╯
Proceed with this configuration? [y/n] (y): y
Output path [./my-processor-service.yaml]:

✓ Config written to my-processor-service.yaml

╭── Ready! ────────────────────────────────────╮
│ Next steps:                                  │
│   Validate: courier validate my-processor-service.yaml
│   Run:      courier run my-processor-service.yaml
│                                              │
│ The default Memory broker works for local    │
│ testing. To configure AMQP for production,   │
│ add a broker section to spec in the          │
│ generated YAML.                              │
╰──────────────────────────────────────────────╯
```

```{note}
**Configuration format compatibility**

`courier init` generates pipeline steps using the nested `identifier:` /
`spec:` format. Other examples throughout these docs use a flat
`- <name>:` singleton mapping. Both formats are valid. For
hand-written configurations, use the flat mapping style — it's shorter
and matches the examples in {doc}`quick-start`, {doc}`configuration`,
and the tutorials.
```

## Generated File Structure

The generated YAML follows the `runcourier.dev/v1alpha1` API version:

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
    - identifier: job-builder-dummy-job-builder
      spec:
        kind: job_builder
        name: DummyJobBuilder
    - identifier: dispatcher-serial-bash
      spec:
        kind: dispatcher
        name: serial_bash
        config:
          command: echo "Files assigned: {{ files | length }}"
```

Each pipeline step has an `identifier` (a DNS-safe name derived from
the kind and plugin) and a `spec` containing the plugin kind, name, and
any configuration values you provided during the prompts.

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
