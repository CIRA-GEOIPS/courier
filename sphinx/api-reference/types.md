# Types API Reference

The core data types that flow through Lazy Lemon's pipeline:
:class:`~courier.types.file.File` (mutable) and
:class:`~courier.types.file.FrozenFile` (immutable).

## File

:class:`~courier.types.file.File`

Represents a single data file with its associated metadata. ``File`` is a
mutable :py:func:`dataclasses.dataclass` -- it is created by data monitors,
enriched by job builders, and consumed by dispatchers.

.. list-table:: Attributes
   :header-rows: 1

   * - Attribute
     - Type
     - Description
   * - ``file``
     - :py:class:`pathlib.Path` | ``None``
     - Absolute path to the data file on disk.
   * - ``hostname``
     - ``str`` | ``None``
     - Hostname where the file resides (defaults to local hostname).
   * - ``source``
     - ``str`` | ``None``
     - Source identifier (e.g. ``"goes16"``, ``"himawari9"``).
   * - ``instrument``
     - ``str`` | ``None``
     - Instrument identifier (e.g. ``"abi"``, ``"ahi"``).
   * - ``processing_stage``
     - ``str`` | ``None``
     - Processing stage (e.g. ``"l1b"``, ``"l2"``).
   * - ``domain``
     - ``str`` | ``None``
     - Domain or sector (e.g. ``"full-disk"``, ``"conus"``).
   * - ``metadata``
     - ``dict[str, Any]``
     - Arbitrary key-value pairs from ``field_map`` entries that do not map to a named ``File`` constructor attribute. Defaults to ``{}``.
   * - ``num_expected``
     - ``int``
     - Expected number of files for this dataset. Defaults to ``1``.
   * - ``timestamp``
     - :py:class:`datetime.datetime` | ``None``
     - Timestamp extracted from the filename or set manually.

.. literalinclude:: ../../../src/courier/types/file.py
   :language: python
   :start-after: @dataclass
   :end-before:     file: Path | None = None
   :linenos:

### Key Methods

.. list-table::
   :header-rows: 1

   * - Method
     - Description
   * - :func:`~courier.types.file.File.to_dict`
     - Serialize to a ``dict`` with keys matching the attribute names. The ``metadata`` dict is copied (not shared) and ``timestamp`` is ISO-8601 formatted.
   * - :func:`~courier.types.file.File.from_dict`
     - Deserialize from a ``dict``. Only recognized keys (``source``, ``instrument``, ``processing_stage``, ``domain``, ``hostname``, ``file``, ``metadata``, ``num_expected``, ``timestamp``) are used; extraneous keys are silently ignored. Legacy keys (``platform``, ``sensor``, ``level``, ``sector``) are **not** recognized.
   * - :func:`~courier.types.file.File.from_string`
     - Deserialize from a JSON string via :func:`~courier.types.file.File.from_dict`.
   * - :func:`~courier.types.file.File.freeze`
     - Convert to an immutable :class:`~courier.types.file.FrozenFile`. The ``metadata`` dict is wrapped with :py:class:`types.MappingProxyType` for true immutability.
   * - :func:`~courier.types.file.File.merge_metadata`
     - Shallow-merge metadata into the file. Only ``None`` or default fields are overwritten; existing values are preserved. Accepts a ``metadata={...}`` kwarg that shallow-merges into ``self.metadata`` (existing keys kept, new keys added).
   * - :func:`~courier.types.file.File.with_updates`
     - Create a new ``File`` with updated fields via :py:func:`dataclasses.replace`.

## FrozenFile

:class:`~courier.types.file.FrozenFile`

Immutable (``frozen=True``) counterpart of :class:`~courier.types.file.File`.
It is the form carried through the pipeline once a job is built.
All attributes are read-only after construction.

.. list-table:: Attributes
   :header-rows: 1

   * - Attribute
     - Type
     - Description
   * - ``file``
     - :py:class:`pathlib.Path` | ``None``
     - Absolute path to the data file on disk.
   * - ``hostname``
     - ``str`` | ``None``
     - Hostname where the file resides.
   * - ``source``
     - ``str`` | ``None``
     - Source identifier.
   * - ``instrument``
     - ``str`` | ``None``
     - Instrument identifier.
   * - ``processing_stage``
     - ``str`` | ``None``
     - Processing stage.
   * - ``domain``
     - ``str`` | ``None``
     - Domain or sector.
   * - ``metadata``
     - :py:class:`collections.abc.Mapping`\ ``[str, Any]``
     - Immutable view of the metadata dictionary. Wrapped with :py:class:`types.MappingProxyType` during :func:`~courier.types.file.File.freeze`; calling :func:`~courier.types.file.FrozenFile.thaw` unwraps it back to a mutable ``dict``.
   * - ``num_expected``
     - ``int``
     - Expected number of files.
   * - ``timestamp``
     - :py:class:`datetime.datetime` | ``None``
     - Timestamp extracted from filename or set manually.

.. literalinclude:: ../../../src/courier/types/file.py
   :language: python
   :start-after: @dataclass(frozen=True)
   :end-before:     file: Path | None = None
   :linenos:

### Key Methods

.. list-table::
   :header-rows: 1

   * - Method
     - Description
   * - :func:`~courier.types.file.FrozenFile.to_dict`
     - Same serialization as :func:`~courier.types.file.File.to_dict`.
   * - :func:`~courier.types.file.FrozenFile.from_dict`
     - Same deserialization as :func:`~courier.types.file.File.from_dict`.
   * - :func:`~courier.types.file.FrozenFile.from_string`
     - Same JSON deserialization as :func:`~courier.types.file.File.from_string`.
   * - :func:`~courier.types.file.FrozenFile.thaw`
     - Convert back to a mutable :class:`~courier.types.file.File`. The ``metadata`` field becomes a plain ``dict`` (the ``MappingProxyType`` is unwrapped).
   * - :func:`~courier.types.file.FrozenFile.with_updates`
     - Create a new ``FrozenFile`` with updated fields via :py:func:`dataclasses.replace`.

## Breaking Changes

The internal :func:`_file_fields_from_dict` helper **no longer recognizes
legacy fallback keys**. If your configuration or serialized data uses any
of the following, update to the canonical attribute names:

.. list-table::
   :header-rows: 1

   * - Legacy Key
     - Canonical ``File`` Attribute
   * - ``platform``
     - ``source``
   * - ``sensor``
     - ``instrument``
   * - ``level``
     - ``processing_stage``
   * - ``sector``
     - ``domain``

Only ``source``, ``instrument``, ``processing_stage``, ``domain``,
``hostname``, ``file``, ``metadata``, ``num_expected``, and ``timestamp``
are recognized by :func:`~courier.types.file.File.from_dict` and
:func:`~courier.types.file.FrozenFile.from_dict`.
