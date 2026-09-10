# ADR-0009: Dispatcher Refactoring

## Status

Pending Acceptance

## Context

Complications occur when trying to "swap out" a job manager dispatcher such as `slurm_dispatcher` with a straight-ahead dispatcher such as `serial_geoips` or `serial_bash` because - while they both may be executing bash - , and it becomes clear that perhaps what it means to be a dispatcher has been lost. Thus the question arises: is a dispatcher a job manager or a job executor? 

The argument for a dispatcher acting as a job executor comes from the main functionality of a dispatcher: they execute workflows. 

The argument for a dispatcher acting as a job manager stems from the same idea: that dispatchers execute things, but to manage multiple jobs, a job manager such as `slurm` needs to be executed itself. 

Both of these are correct! But a dispatcher should not exist as both an executor and a manager, or lots of flexibility is lost. The solution: we need to define *when* (not if[^1]) a dispatcher is acting as a job manager.


## Decision


## Alternatives Considered

- **Direct pika/aio-pika**: Low-level AMQP client. Simpler dependency graph, but no
  in-memory backend; all tests would require a running broker.
- **Celery**: Higher-level task queue. Adds significant complexity and opinions about
  task serialization that conflict with the existing `Job` domain type.

## Trade-offs Accepted

- Kombu is untyped (`py.typed` marker absent) — all imports carry `# type: ignore[import-untyped]`.
- Connection-error handling and retry logic must be implemented manually
  (`rabbit_mq_watcher.py` implements exponential backoff on `OperationalError`).
- Less control.

## Consequences

- **Dependency**: All plugins transitively depend on Kombu through the
  `MessageBrokerManager` facade. Plugin authors never import Kombu directly, keeping
  the broker abstraction sealed behind the `Service` layer.
- **Testability**: The `memory://` transport enables the full integration test suite to
  run without a broker daemon. CI pipelines require no external services for the core
  test matrix.
- **Type safety**: Kombu's lack of a `py.typed` marker means every Kombu import carries
  `# type: ignore[import-untyped]`. This is a known, accepted limitation on type safety
  within the broker module.
- **Resilience**: Connection-error handling and exponential-backoff retry logic live in
  plugins rather than being provided by the library. This gives the team
  full control over reconnection policy but must be maintained as Kombu evolves.
Please note that this feature is unnamed, and all undecided naming conventions will be surrounded by `@` symbols for ease of replacement.

# Background
# The @bus@
So, the simple solution is for dispatchers to have an optional `@bus@` field that links to other dispatchers. For instance:
```
  - identifier: dispatcher-slurm-dispatcher
    spec:
      kind: dispatcher
      name: slurm_dispatcher
      config:
        poll_interval_seconds: 30.0
        max_concurrent_jobs: 10
        wait_for_completion: true
        submission_timeout_seconds: 60.0
        polling_timeout_seconds: 86400.0
        sbatch_extra_args:
        - PydanticUndefined
      @bus@: [dispatcher-serial-geoips]
  - identifier: dispatcher-serial-geoips
    spec:
      kind: dispatcher
      name: serial_geoips
      config:
        workflow_name: abi_airmass
        timeout_seconds: 3600.0
        log_to_file: false
        only_log_stderr: false
        scan_stderr: false
  allow_implicit_target: true
```
With the `@bus@` field, `slurm_dispatcher` knows what it's managing, and will (with added implementation) run each linked dispatcher accordingly.

# Problems with @bus@

## Arbitrary Python Execution
A glaring problem with @bus@ is the fact that some job managers such as `slurm` only accept files as inputs, and that Courier executes its dispatchers through their embedded functions. The current implementation for `slurm_dispatcher` accepts a string that acts as an in-place shell file when rendered as a Jinja2 template. 

A solution to this comes from [this stackoverflow thread]("https://stackoverflow.com/questions/6036082/call-a-python-function-from-jinja2"), where inputted code is executed as a Jinja2 template.

## Circular Linking
Exception handling needs to be implemented for service configs where two dispatchers with @bus@ functionality can link infinitely. Ignoring this could cause high CPU usage with no work being done, so it should be caught at runtime.

[^1]: Not if, because previously-implemented dispatchers that act as job managers should still keep their original implementation of both management and execution. The added functionality is the choice between the two.
