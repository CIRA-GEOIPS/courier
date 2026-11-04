# ADR-0014: Programmatic Data Injection

## Status

Planning

## Context

Courier workflows are triggered by the presence of data. As it stands now, the
data must be present on a source before Courier can recognize it. There is no
direct program-to-courier pipeline, which presents quite a costly overhead in
time and space.

## Decision

`courier song/signal/call`: A programmatic way to inject data into the found
datum queue. It will serialize the supplied data in to a Datum object and
publish it to the existing FilesFoundExchange through Courier's configured
message broker. This allows the supplied data to enter the job building and
dispatching pipeline directly from the call.

## Syntax

`courier song/signal/call` can be called through the command-line and through
Courier's scripting API as so:

### Command-line

`courier signal {DATA} courier-service.yaml`
`python generate_data.py | courier signal courier-service.yaml`
`courier signal -f {FILE} courier-service.yaml`

### Scripting API

```
client = CourierClient(config='courier.yaml')
client.signal(data)
```

## Consequences

- Courier will support programmatic data injection without requiring data to
  first be written to a monitored source.
- External applications and scripts will be able to initiate Courier workflows
  directly through the CLI or scripting API.
- Data injection will reuse Courier's existing message-based architecture and
  allow submitted data to flow through the existing job builders and
  dispatchers.
- Directly injected data will bypass the data-monitoring stage, potentially
  requiring callers to supply metadata normally provided by data monitors.

## Trade-offs

- Requires access to a running message broker.
- In-memory data must be serialized and transmitted through the broker, causing
  some network overhead.
- Data size limitations may apply when submitting large amounts of data through
  RabbitMQ
- External programs must construct valid data payloads and supply metadata,
  rather than relying on data monitors to perform these tasks.
