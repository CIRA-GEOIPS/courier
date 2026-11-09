# ADR-0014: Programmatic Data Injection

## Status

Planning

## Context

Direct program-to-courier pipelines have been proven as a very useful way to
trigger Courier workflows for all of the following use cases:

    - Submitting data without writing it to a file
    - Programmatic data submission
    - CLI-driven data submission

A user who wants to use this program-to-courier pipeline may be inexperienced or
working on a small project that does not call for a broker setup. This calls for
a native, easy, and customizable solution to data injection in Courier.

## Decision

`courier song/signal/call`: A programmatic way to inject data into the found
datum queue. It will serialize the supplied data into a Datum object and publish
it to the existing FilesFoundExchange through Courier's configured message
broker. This allows the supplied data to enter the job building and dispatching
pipeline directly from the call.

If a service is configured with this functionality, a broker and broker data
watcher will be attached to the service at startup. The call will prompt the
plugin manager for the exchange to publish to. It will act as a catch-all
approach to data injection, as the same syntax will apply to all available
message queue services.

If a file is passed, its metadata will be collected with Courier's built-in
metadata collection functionality. If a piece of in-memory data is passed, it
will be populated with a timestamp and the service's configured hostname.

## Preconditions

1. The broker must be initialized after all the job builders are initialized,
   predeclaring the fanout exchange and attaching itself to the FilesFound queue
   for each builder.

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
- Directly injected data will bypass the data-monitoring stage, so callers
  must supply the metadata that data monitors would otherwise add: the
  `Datum` attributes `source`, `instrument`, `processing_stage`, `domain`,
  `hostname`, `num_expected` and `timestamp`, plus any `metadata` keys the
  configured job-builder filters read. A filter key present in neither the
  `metadata` dict nor the `Datum` attributes makes the file fail the filter
  and be dropped, so injected data missing a configured filter key never
  reaches a job.

## Trade-offs

- Requires access to a running message broker.
- In-memory data must be serialized and transmitted through the broker, causing
  some network overhead.
- A payload larger than the broker's maximum message size is rejected by the
  broker: the publish raises a broker error rather than truncating or silently
  dropping the data, so an oversized injection fails loudly. Callers should
  keep injected payloads small and pass large data by reference.
- External programs must construct valid data payloads and supply metadata,
  rather than relying on data monitors to perform these tasks.
