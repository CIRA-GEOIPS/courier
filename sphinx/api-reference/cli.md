# Command Reference

Every courier command takes the same shape:

```bash
courier <command> CONFIG [OPTIONS]
```

`CONFIG` is always a positional path to a service YAML — the same argument, in
the same place, for every command that needs one. Run `courier <command> --help`
for the full option list; this page covers what each command is *for*.

```bash
courier --version     # which courier is installed
courier --help        # the command list
```

______________________________________________________________________

## I want to create a service

### `courier init`

Walks through building a config: service metadata, then a numbered table of
available plugins for each stage. Pick by number, by name, or by an unambiguous
prefix — `5`, `s3_poller` and `s3` all select the same data monitor. Each job
builder also asks for the payload its jobs run; see {doc}`../getting-started/init`.

```bash
courier init             # writes <name>-service.yaml
courier init --dry-run   # print the YAML instead of writing it
```

Finishes by printing the exact `validate` and `run` commands for what it built.
It will not overwrite an existing file without asking.

______________________________________________________________________

## I want to check a config before deploying

### `courier validate`

Loads and validates a config without starting anything, then reports what it
found so you can confirm it is the pipeline you meant:

```console
$ courier validate config.yaml
config.yaml is valid.
  3 pipeline steps: 1 data monitor, 1 job builder, 1 dispatcher
  just-pass runs payload echo-payload (bash_payload)
  broker: memory

Run it:  courier run config.yaml
```

A step count that surprises you is the point — it catches a stage you thought
you had configured and hadn't. Beyond the schema it checks each step's plugin
is installed, that every job builder nests exactly one payload, the dispatcher
and payload settings (including keys removed in earlier releases), that each
payload template compiles, and that each payload can run on the dispatchers
its builder targets. On failure it names each problem by config key and exits
`1`; {ref}`validation-errors` lists the common messages.

It creates nothing and connects to nothing, so it does not check a
`log_to_file` directory (see
[The log directory](dispatchers.md#the-log-directory)). A template `file` that
is not visible where it runs is printed as a `note:` line, not a problem.

### `courier plugins list`

Which plugins this installation can actually use. With a config, only the ones
that config references:

```bash
courier plugins list                 # everything installed
courier plugins list config.yaml     # only what this config uses
courier plugins list --json          # for jq
```

Listing never imports a plugin, so it works on a minimal install even when
optional extras are absent. If a plugin you expect is missing, it was never
declared as an entry point — see {doc}`../contribute/writing-a-plugin`.

### `courier queues list`

The broker queues this config implies, before anything is created:

```bash
courier queues list config.yaml
courier queues list config.yaml --json
```

______________________________________________________________________

## I want to run a service

### `courier run`

Starts the service described by the config and stays in the foreground until
interrupted.

```bash
courier run config.yaml
```

`--only` runs a subset of the pipeline, so one config can serve several
containers. Each gets the same file and a different slice:

```bash
courier run config.yaml --only watch          # the data monitor
courier run config.yaml --only build,dispatch # the processing half
```

The values are `spec.run` *identifiers* — the YAML keys — not plugin names.

______________________________________________________________________

## I want to see what a running service is doing

### `courier viz`

A terminal dashboard reading a running instance's Prometheus endpoint.

```bash
courier viz --host localhost --port 8000
```

Requires `pip install data-courier[viz]`.

### `courier dashboard`

Generates Grafana dashboard JSON tailored to a config — panels only for the
plugins actually configured.

```bash
courier dashboard config.yaml -o dashboard.json
courier dashboard config.yaml --split-by kind -o ./dashboards/
courier dashboard config.yaml --only-metrics
courier dashboard config.yaml --live      # detect active plugins from Prometheus
```

Requires `pip install data-courier[grafana]`.

______________________________________________________________________

## I want to clean up after a topology change

### `courier queues prune`

Renaming a dispatcher leaves its old queue on the broker, still bound and still
accumulating messages. `prune` finds them.

```bash
# what would be deleted -- dry run is the default
courier queues prune config.yaml --candidate old-dispatcher-queue

# read candidates from a file, then actually delete
courier queues prune config.yaml --from-file candidates.txt --apply
```

Any candidate not in the expected set is an orphan. A non-empty queue is
reported and left alone unless you pass `--force`.

______________________________________________________________________

## Exit codes

| Code | Meaning                                                                             |
| ---- | ----------------------------------------------------------------------------------- |
| `0`  | Success                                                                             |
| `1`  | Courier could not do what you asked — missing config, invalid config, failed delete |
| `2`  | The command line itself was wrong — unknown flag, missing argument                  |

`1` and `2` are distinct on purpose: `2` means fix your typing, `1` means fix
your config or your broker.

## Global options

`--log-level` / `-l`
: `TRACE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Also read from
`COURIER_LOG_LEVEL`.

`--version` / `-V`
: Print the installed version and exit.
