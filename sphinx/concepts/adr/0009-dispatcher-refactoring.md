# ADR-0009: Dispatcher Refactoring

## Status

Proposed - in debate

## Context

Courier has three main plugin interfaces (`data_monitors`, `job_builders`, `dispatchers`).
They all conform to one `ServicePlugin` protocol and are designed to be easily swappable for
easy transfer of Courier Services to other environments.

However, complications occur when trying to "swap out" a job manager dispatcher such as
`slurm_dispatcher` with a straight-ahead dispatcher such as `serial_geoips` or `serial_bash`
because, while they both may be executing bash, the config that drives one is useless to
the other. And it becomes clear that perhaps what it means to be a dispatcher has been lost.
Thus the question arises: is a dispatcher a job manager or a job executor?

The argument for a dispatcher acting as a job executor comes from the main functionality of a
dispatcher: they execute workflows.

The argument for a dispatcher acting as a job manager stems from the same idea: that
dispatchers execute things, but to manage multiple jobs, a job manager such as `slurm` needs to
be executed itself.

Both of these are correct! But a dispatcher should not exist as both an executor and a manager,
or lots of flexibility is lost. If we don't, things will stay as they are and every new environment concern must be
individually hand-rolled per plugin. This conflation is (currently) very visible in the user experience, for example we have a k8s,
slurm, parallel_bash and serial_bash plugins that........ all of which "just" run bash!!!

## Decision

We are:

A. Splitting dispatchers into two parts: a **manager**, which accounts for differing execution
   environments, and an **executor**, which accounts for differing things that wish to be
   executed.

B. Naming these after the art of falconry — that is, the art of a manager (the **falconer**)
   sending a targeted executor (the **falcon**) into a new environment to complete a specific
   task. The word play "execute" as in *catch and kill prey* as well as *execute* as in run an
   executable is an unintended bonus.

There are some complications in determining falcon and falconer compatibility.

For example, while a local bash executor can execute both bash and Python code, something like
an AWS Lambda falconer may only be able to support Python and not bash. Of course there are
environments with bash and not Python as well.

Aka we have overlapping sets that cannot be treated as strict subsets. There is some (large)
overlap though!

Thus, we have decided that most falcons should export their runnability via an inheritance
structure — e.g. the `sh` falcon class is the parent to the `bash` falcon class, and the `bash` falcon is
the parent to `perl`, Python, Haskell etc. Those falcons should have the ability to *both*
export the information needed to run the user-provided payload via `sh` *as well as* the "raw"
payload itself.

### The compatibility rule

The inheritance chain is a lowering path, not a strict subtyping claim about what
environments can run:

- A falcon's representations are its own node plus every ancestor it can "lower" to.
  `PythonFalcon` exports `{python, bash, sh}`; `BashFalcon` exports `{bash, sh}`.
- A falconer declares the representations it can execute natively. A BusyBox container
  declares `{sh}`; `LambdaFalconer` declares `{python}`; `LocalFalconer` declares
  `{sh, bash, python}`.
- They are compatible iff the intersection is non-empty. The falconer takes the **most
  specific** member — deepest in the falcon's ancestry, which the MRO already orders.

A falconer that declares `sh` supports every shell-launchable falcon (Python, Perl, Haskell,
and falcons not yet born) without having to name any of them.

The declaration set therefore carries two different things:

- **Support** — *can this run here at all?* For a shell environment, `sh` answers this for the (almost) all of the shell-launchable world.
- **Native execution** — *can the lowering be skipped?* `LocalFalconer` declaring `python` means it can hand a raw payload to an interpreter, skipping a shell hop and a layer of quoting.

Eg. with the falconer chain `sh → bash → {perl, python, haskell, …}`:

| Falcon | exports | Local `{sh, bash, python}` | BusyBox `{sh}` | AWS Lambda `{python}` |
| --- | --- | --- | --- | --- |
| `sh` | `{sh}` | ✅ via `sh` | ✅ via `sh` | ❌ |
| `bash` | `{bash, sh}` | ✅ via `bash` | ✅ via `sh` | ❌ |
| `python` | `{python, bash, sh}` | ✅ via `python` (native) | ✅ via `sh` | ✅ via `python` |
| `perl` | `{perl, bash, sh}` | ✅ via `bash` | ✅ via `sh` | ❌ |
| `haskell` | `{haskell, bash, sh}` | ✅ via `bash` | ✅ via `sh` | ❌ |
| *any future shell-launchable falcon* | `{…, sh}` | ✅ | ✅ | ❌ unless it lowers to `python` |


### Yikes! Toolchain validation must belong to the falcon

`sh` support promises launches but.... not toolchains. 
A Haskell falcon lowered to `sh` works only if haskell is on the box!

The falcon must own that check. The Julia falcon verifies `julia`, the Perl falcon verifies `perl`... etc.

But the falcon does not run on the target so the check must be emitted rather than performed.

- Every falcon must emit a validation preamble with each representation
- A falconer may run that preamble as a standalone probe first, where it is cheap.

### What does not change

`Dispatcher` keeps its identifier, its queue, its dedupe, and its metrics. Routing remains as-is (yay).
Splitting execution out of a dispatcher does not move routing, and nothing here should be read as reopening ADR6.

### Scope

This ADR decides the split and the compatibility model, and nothing else.

## Alternatives Considered

- **Keep dispatchers monolithic and accept the duplication. Yikes - bad dev AND user experience.

- **Falconers enumerate the languages they support.** `LocalFalconer` lists
  `{sh, bash, python, perl, haskell, …}` explicitly. Rejected: it restates what the chain
  Every shell falconer in the wild would need editing to support a language it could already
  run.

- **Composition: a falcon holds a `Lowerer` strategy object.** Lowering could then vary
  independently of payload type. Rejected as unearned indirection — there is one lowering per
  level today, and no case where two falcons at the same node want different ones.

- **One falcon per (language, environment) pair.** `SlurmPythonFalcon`, `LambdaPythonFalcon`,
  `LocalBashFalcon`. Rejected: the M × N explosion the split exists to avoid, and it puts
  environment knowledge back inside the executor.

- **Retain the plain names "manager" and "executor".** Rejected in favour of the metaphor, which
  travels better in conversation and carries the directionality of the relationship. The cost is
  recorded below.

### External comparison

- **Airflow** separates *Operators* (what runs — `BashOperator`, `PythonOperator`) from
  *Executors* (where and how — `LocalExecutor`, `CeleryExecutor`, `KubernetesExecutor`)
- **Nextflow** separates a `process` (the work, plus its container and conda directives) from an
  `executor` (`local`, `slurm`, `awsbatch`, `k8s`)
- **Snakemake** separates rules from executor plugins

We differ from Airflow in how we check compatibility. Airflow dynamically checks
Operator/Executor compatibility, so a `BashOperator` scheduled onto a shell-less image fails at
task runtime. We are able to check statically via service validation at the time of writing.

## Trade-offs Accepted

- Every execution pays for a validation that almost always passes.

- `falcon` and `falconer` differ by two characters. That will be hard to tell apart in diffs but will bring joy to the work.

## Consequences

- A new language falcon is additive. A Julia falcon under `bash` runs on every existing
  shell falconer — local, SLURM, Kubernetes, any container with a shell — with no edit to any of
  them, and brings its own `julia` probe, so no falconer learns what Julia is

- Two entry-point groups replace one. `courier.falcons` and `courier.falconers` join the
  other three from {doc}`./0008-entry-point-plugin-discovery`

- Compatibility failures move to config-validation time

- `get_execution_log` becomes the composition point. Everything above it is untouched, so dedupe,
  metrics, tracing, restart and routing behave exactly as today.

- `serial_bash` and `parallel_bash` become collapse candidates

- Falconry vocabulary enters operator-facing surfaces - YAML keys, `courier plugins list`
  output, metric labels, log lines. Expensive to rename once shipped... !
