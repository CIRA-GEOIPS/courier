# ADR-0004: Mutable File / Frozen FrozenFile Type Split

## Status

Accepted

## Context

Files enter the pipeline at the data monitor stage and are enriched with metadata
(source, instrument, domain, etc.) before being serialized and placed on the broker
queue. Once a file crosses a stage boundary it must be immutable so that downstream
consumers receive a stable value.

However, during metadata enrichment within a single stage, the `File` object needs
to be mutated incrementally (e.g., `merge_metadata()` is called once per matching
metadata config).

## Decision

Maintain two parallel types in `src/courier/types/file.py`:

- `File` — mutable dataclass. Owned by a single data-monitor thread during enrichment.
  Carries a `# Mutable because:` comment per the immutability rule.
- `FrozenFile` — `frozen=True` dataclass. Produced by `File.freeze()`. Used for all
  inter-stage messages and set membership in `Job.files`.

Both types implement the same serialization contract (`to_dict`, `from_dict`,
`__str__`, `from_string`) and can be round-tripped through the broker.

## Alternatives Considered

- **Single immutable type with `dataclasses.replace()`**: Cleaner but requires creating
  a new object for each metadata merge step; at scale this produces extra garbage.
- **Pydantic model**: Richer validation, but heavier and conflicts with the existing
  `dataclass` convention used across the types module.

## Trade-offs Accepted

- Two parallel types means two sets of serialization methods to maintain in sync.
- `FrozenFile` is not a subclass of `File`; callers must handle `File | FrozenFile`
  unions explicitly (visible throughout `job_builders.py`).
