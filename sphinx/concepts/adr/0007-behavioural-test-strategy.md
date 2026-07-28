# ADR-0007: Behavioural Test Strategy

## Status

Accepted

## Context

Courier had 1,294 passing tests at 68% line coverage. A manual review of the
source found seven critical defects. The suite caught none of them. Among
them:

- `metadata_router` dropped every job it was given.
- Two job-identity paths lost files when identifiers collided.
- The GOES-16/19 ABI channel pattern `M6C[01][1-6]` never matched channels
  7–10, so a quarter of the instrument's products were silently ignored.
- Files whose timestamps differed only in `tzinfo` were bucketed into separate
  jobs.
- A broker password containing a URL-reserved character crashed startup.

Worse than missing them, around ten test sites *asserted* the defective
behaviour. Fixing the GOES pattern broke a test that pinned the wrong regex;
fixing the job-identity path broke a test that pinned the lossy result. The
suite had stopped being a safety net and become a ratchet holding the defects
in place.

The common cause was not laziness or low coverage. It was that a large share
of the tests asserted the *shape* of the code rather than what it does:

```python
# What the suite mostly did — restates the implementation.
def test_dispatcher_has_a_queue():
    assert hasattr(dispatcher, "incoming_queue")

def test_emit_is_called():
    builder.emit_job(job)
    assert service.emit.called

def test_lru_size():
    assert _DEDUPE_LRU_SIZE == 1000
```

Assertions like these are tautological. They restate the implementation in a
second file, so they pass for *any* implementation — including a wrong one —
and they fail whenever the code is refactored correctly. They execute lines,
so they generate coverage. They cannot detect a defect, because nothing about
the system's behaviour is being claimed.

Two operational problems compounded this. The default `pytest` run took 3m15s,
74% of it inside a single unit test that opened a socket to a TEST-NET-1
address and waited for the connection to time out. The integration suite never
terminated at all — plugin threads were non-daemon and observed no stop event,
so the run wedged until CI killed it hours later. A suite developers will not
run catches nothing regardless of how it is written.

## Decision

Five rules govern the test suite.

### 1. Assert observable behaviour, not structure

A test states something the system does that a user or a downstream stage can
observe: a returned value, a persisted state change, a message placed on a
queue, an exit code, a metric delta. If the assertion can be satisfied by
reading the implementation and copying it, it is not a test.

Specifically rejected: asserting `hasattr`; asserting a mock was called
without asserting the effect of the call; asserting a module constant equals
its own literal; asserting a function signature; asserting a log line's exact
wording as a proxy for behaviour.

```python
# What replaced them — feed input, assert the consequence.
def test_repeated_identifier_is_skipped():
    dispatcher = _dispatcher(service, "dedupe-basic")
    _feed(dispatcher, service, _job("same-id"), _job("same-id"))
    assert len(dispatcher.executed) == 1
```

### 2. Tier the suite by cost, and make the fast tier the default

`tests/unit_tests/` uses no broker, no network, and no sleeps, and runs in
~23 seconds. Integration tests carry the `integration` marker, are deselected
by `addopts`, and are opted into with `-m integration`. CI runs both. The
default run has to be fast enough that running it is not a decision.

### 3. Test the boundary the user actually crosses

In-process tests of a CLI's callback function do not test the CLI. Courier
therefore has smoke tests that drive the real Typer application with real
plugin registries over the configs the package ships, and process-lifecycle
tests that spawn the real `courier` console script as a subprocess and send it
real signals.

This tier is small (36 + 4 tests) and earns its keep. It found that
`courier dashboard config.yaml --only-metrics` — the invocation printed in
`examples/grafana/README.md` and in the command's own help — failed with
`No such command '--only-metrics'`, because registering the command with
`add_typer` made it a Click *group*. Nearly 1,300 in-process tests could not
see that, because none of them crossed the argument parser.

### 4. Guard against drift between the code and the artefacts it ships

Shipped YAML configs, plugin registries, dashboard generators, and metric
names drift apart silently: each side is individually valid. Dedicated guards
assert they agree — every shipped config validates against the live registry
(26 tests), every satellite filename pattern matches the filenames it claims
to (300 tests), every dashboard selector names a metric the code actually
registers.

These found `filter_pass`, a plugin named in two shipped configs that has
never existed in any registry, and the GOES channel regex above.

### 5. Measure the suite with mutation testing, not coverage

Coverage measures which lines ran. Shape assertions run lines abundantly. To
answer "are these tests load-bearing?", mutmut runs weekly against the core
domain and plumbing packages (see `[tool.mutmut]` in `pyproject.toml`). A
surviving mutant is a change to production code that no test objected to.

The first run over `courier.types.job` produced 2,433 mutants with 13
survivors. Two were genuine gaps and now have tests: turning a `continue` into
a `break` in the bucket-fanout loop (a file would reach only its first bucket),
and relaxing an `and` to an `or` in `_open_job_for` (a stale job pointer would
be dereferenced instead of replaced). Both are silent data-loss bugs that the
full suite had accepted.

Tests written to kill a mutant say so in their docstring, as do tests written
to prevent a specific production failure. A reader should never have to guess
what a test is defending.

## Alternatives Considered

- **A coverage gate (e.g. fail under 85%).** Rejected. Across this work the
  suite grew by 186 tests, gained the ability to catch every one of the seven
  criticals, and moved line coverage from 68% to 71%. Coverage was nearly
  invariant to the change that mattered, and the original suite would have
  passed a gate set anywhere it could realistically be set. Coverage is still
  reported; it is not a gate.

- **Making mutation testing a required check.** Rejected. Mutation scores move
  for legitimate reasons — equivalent mutants, refactors that delete mutable
  branches — and a gate that cries wolf gets muted. The workflow is scheduled
  and non-blocking; a rising survivor count is a prompt to ask which behaviour
  stopped being asserted.

- **Deleting the suite and starting over.** Rejected. The existing tests
  encode real domain knowledge in their fixtures — filename conventions,
  broker topologies, plugin config shapes — that would have been expensive to
  reconstruct. Rewriting assertions in place preserved that.

- **Keeping integration tests in the default run.** Rejected; see rule 2.

- **Mocking the broker everywhere to make integration tests fast.** Rejected.
  The defects worth catching at that tier are precisely the ones that live in
  real broker semantics — a fanout exchange discards a message published
  before any queue is bound, and no mock reproduces that unless you already
  know to write it.

## Trade-offs Accepted

- Behavioural tests are more work to write. They often need a real object
  graph where a `MagicMock` would have been three lines, and they need the
  author to know what the code is *for*, not just what it says.

- The unit/integration split means real-broker defects are invisible to the
  default run. Mitigated by CI running both tiers, not by asking developers to
  remember.

- Mutation runs are slow — thousands of mutants per module — and are scoped to
  `types`, `routing`, `interfaces/module_based`, `utils`, `broker`, and
  `managers`. The dashboard and viz packages get no mutation signal, because
  their mutants are overwhelmingly cosmetic (a changed panel width, a reworded
  description) and would bury the real findings.

- A small number of tests deliberately reach into private state. The dashboard
  drift guard reads `prometheus_client` internals because declared label names
  have no public accessor: `describe()` and `collect()` report only labels some
  child has already been given, so a metric that has never been incremented
  looks label-less and every selector on it is reported as phantom. Rewriting
  it onto the public API was tried and did exactly that to 20 valid selectors.
  Such sites are annotated with the reason and paired with a guard —
  `test_registry_scan_is_not_vacuous` — that fails if an upstream rename turns
  the scan into an empty mapping and makes the checks vacuously pass.

- Rule 1 has no linter. It is a review standard, and it degrades if reviewers
  stop applying it.

## Consequences

- The default run is 1,480 tests in ~23 seconds; `-m integration` is 18 tests
  in ~2.5 minutes; line coverage is 71% and is reported, not enforced.

- New tests are expected to answer "what does this fail on?". For bug fixes,
  the answer is verified rather than asserted: the fix is reverted, the test is
  confirmed to fail, and the revert is undone. Several tests in this suite were
  rewritten after that check showed they passed against the reintroduced bug.

- Parametrisation and property-based tests replaced copy-pasted near-duplicates
  where the underlying claim was one claim. `tests/unit_tests/types/test_file.py`
  went from 971 to 559 lines while raising coverage of that module to 98%.

- `.github/workflows/mutation.yaml` runs weekly and on dispatch, writes an
  outcome table and survivor list to the job summary, and uploads
  `mutants.sqlite`. Targeting one module ad hoc:

  ```bash
  mutmut run "courier.types.job.*"
  ```

- Reviewers have a concrete question to ask of any new test, and a concrete
  reason to reject one: an assertion that would hold for a knowingly broken
  implementation is not worth merging. See {doc}`../../contribute/code-style`.
