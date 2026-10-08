# Types API Reference

The core data types that flow through Courier's pipeline:
`Datum` (mutable) and
`FrozenDatum` (immutable). Each job also carries
the payload it executes, as a [PayloadSpec](#payloadspec).

## Datum

`courier.types.datum.Datum`

Represents a single data file with its associated metadata. `Datum` is a
mutable {py:func}`dataclasses.dataclass` -- it is created by data monitors,
enriched by job builders, and consumed by dispatchers. It is generic over the
type `T` of its optional `data` payload.

```{list-table} Attributes
:header-rows: 1

* - Attribute
  - Type
  - Description
* - `data`
  - `T` | `None`
  - Optional payload carried with the datum. It is serialized by `to_dict`, so it must be JSON-serializable to be published. Defaults to `None`.
* - `file`
  - {py:class}`pathlib.Path` | `str` | `None`
  - Location of the data: a `Path` for filesystem paths, or the URI string verbatim for remote locations (`s3://`, `sftp://`).
* - `hostname`
  - `str` | `None`
  - Hostname where the file resides (defaults to local hostname).
* - `source`
  - `str` | `None`
  - Source identifier (e.g. `"goes16"`, `"himawari9"`).
* - `instrument`
  - `str` | `None`
  - Instrument identifier (e.g. `"abi"`, `"ahi"`).
* - `processing_stage`
  - `str` | `None`
  - Processing stage (e.g. `"l1b"`, `"l2"`).
* - `domain`
  - `str` | `None`
  - Domain or sector (e.g. `"full-disk"`, `"conus"`).
* - `metadata`
  - `dict[str, Any]`
  - Arbitrary key-value pairs from `field_map` entries that do not map to a named `Datum` constructor attribute. Defaults to `{}`.
* - `num_expected`
  - `int`
  - Expected number of files for this dataset. Defaults to `1`.
* - `timestamp`
  - {py:class}`datetime.datetime` | `None`
  - Timestamp extracted from the filename or set manually.
```

```{literalinclude} ../../src/courier/types/datum.py
:language: python
:start-after: "@dataclass"
:end-before: "data: T | None"
:linenos:
```

### Key Methods

```{list-table}
:header-rows: 1

* - Method
  - Description
* - `Datum.to_dict()`
  - Serialize to a `dict` with keys matching the attribute names. The `metadata` dict is copied (not shared) and `timestamp` is ISO-8601 formatted.
* - `Datum.from_dict()`
  - Deserialize from a `dict`. Only recognized keys (`data`, `source`, `instrument`, `processing_stage`, `domain`, `hostname`, `file`, `metadata`, `num_expected`, `timestamp`) are used; extraneous keys are silently ignored. Legacy keys (`platform`, `sensor`, `level`, `sector`) are **not** recognized.
* - `Datum.from_string()`
  - Deserialize from a JSON string via `Datum.from_dict()`.
* - `Datum.freeze()`
  - Convert to an immutable `FrozenDatum`. The `metadata` dict is wrapped with {py:class}`types.MappingProxyType` for true immutability.
* - `Datum.merge_metadata()`
  - Shallow-merge metadata into the file. Only `None` or default fields are overwritten; existing values are preserved. Accepts a `metadata={...}` kwarg that shallow-merges into `self.metadata` (existing keys kept, new keys added).
* - `Datum.with_updates()`
  - Create a new `Datum` with updated fields via {py:func}`dataclasses.replace`.
```

## FrozenDatum

`courier.types.datum.FrozenDatum`

Immutable (`frozen=True`) counterpart of `Datum`.
It is the form carried through the pipeline once a job is built.
All attributes are read-only after construction.

```{list-table} Attributes
:header-rows: 1

* - Attribute
  - Type
  - Description
* - `data`
  - `T` | `None`
  - Optional payload carried with the datum. Excluded from the hash, like `metadata`, so a list or dict payload does not make the datum unhashable.
* - `file`
  - {py:class}`pathlib.Path` | `str` | `None`
  - Location of the data: a `Path` for filesystem paths, or the URI string verbatim for remote locations.
* - `hostname`
  - `str` | `None`
  - Hostname where the file resides.
* - `source`
  - `str` | `None`
  - Source identifier.
* - `instrument`
  - `str` | `None`
  - Instrument identifier.
* - `processing_stage`
  - `str` | `None`
  - Processing stage.
* - `domain`
  - `str` | `None`
  - Domain or sector.
* - `metadata`
  - {py:class}`Mapping[str, Any] <collections.abc.Mapping>`
  - Metadata dictionary. When the `FrozenDatum` is created via `Datum.freeze()`, metadata is wrapped with {py:class}`types.MappingProxyType` for true immutability. When created directly or via `FrozenDatum.from_dict()`, metadata is stored as a plain `dict`. Calling `FrozenDatum.thaw()` unwraps any `MappingProxyType` back to a mutable `dict`.
* - `num_expected`
  - `int`
  - Expected number of files.
* - `timestamp`
  - {py:class}`datetime.datetime` | `None`
  - Timestamp extracted from filename or set manually.
```

```{literalinclude} ../../src/courier/types/datum.py
:language: python
:start-after: "@dataclass(frozen=True)"
:end-before: "# Unhashed like"
:linenos:
```

### Key Methods

```{list-table}
:header-rows: 1

* - Method
  - Description
* - `FrozenDatum.to_dict()`
  - Same serialization as `Datum.to_dict()`.
* - `FrozenDatum.from_dict()`
  - Same deserialization as `Datum.from_dict()`.
* - `FrozenDatum.from_string()`
  - Same JSON deserialization as `Datum.from_string()`.
* - `FrozenDatum.thaw()`
  - Convert back to a mutable `Datum`. The `metadata` field becomes a plain `dict` (the `MappingProxyType` is unwrapped).
* - `FrozenDatum.with_updates()`
  - Create a new `FrozenDatum` with updated fields via {py:func}`dataclasses.replace`.
```

## PayloadSpec

`courier.types.payload.PayloadSpec` is the payload a job carries, as
`job.payload`: the job builder creates it when it emits the job, and the
dispatcher that receives the job executes it. Its fields are listed in
{ref}`payload-wire-format`.

## Breaking Changes

The internal `_file_fields_from_dict()` helper
**no longer recognizes legacy fallback keys**. If your configuration or
serialized data uses legacy keys, update to the canonical attribute names.

Only `data`, `source`, `instrument`, `processing_stage`, `domain`,
`hostname`, `file`, `metadata`, `num_expected`, and `timestamp`
are recognized by `Datum.from_dict()` and
`FrozenDatum.from_dict()`.
