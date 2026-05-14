Version 0.2.0 (2026-05-14)
**************************

 * *Feature*: Add ``metadata`` dict to File/FrozenFile for arbitrary field_map data
 * *Enhancement*: Extend ``merge_metadata()`` with ``metadata=`` kwarg for shallow-merge
 * *Enhancement*: Filter supports two-layer lookup (metadata keys + File attributes)
 * *Breaking*: Remove legacy fallback keys (``platform``/``sensor``/``level``/``sector``)

Feature: metadata dict on File and FrozenFile
=============================================

Added a ``metadata: dict[str, Any]`` field to both :class:`~courier.types.file.File`
and :class:`~courier.types.file.FrozenFile`. This dict stores arbitrary key-value pairs
extracted from ``field_map`` entries that do not map directly to ``File`` constructor
attributes (``source``, ``instrument``, ``processing_stage``, ``domain``, ``hostname``,
``file``).

``FrozenFile.metadata`` is wrapped with :py:class:`types.MappingProxyType` via
:func:`~courier.types.file.File.freeze` for true immutability.
:func:`~courier.types.file.FrozenFile.thaw` unwraps it back to a mutable ``dict``.

``merge_metadata()`` extended with ``metadata=`` kwarg
------------------------------------------------------

:func:`~courier.types.file.File.merge_metadata` now accepts a ``metadata={...}``
keyword argument that shallow-merges into ``self.metadata``. Existing keys are
preserved; only new keys are added. This allows layering metadata from
multiple sources without overwriting previously set values.

Two-layer filter lookup
------------------------

The filter in :class:`~courier.plugins.classes.job_builders.filter_and_group.FilterAndGroupJobBuilder`
now performs a **two-layer lookup** for each filter key:

1. ``file.metadata.get(key)`` — metadata dict keys first
2. ``getattr(file, key, None)`` — ``File`` dataclass attributes second

If a key is found in neither layer, a ``WARNING`` is logged and the file is
rejected (the filter returns ``False``).

Legacy fallback keys removed [breaking]
---------------------------------------

:func:`_file_fields_from_dict` no longer recognizes the legacy fallback keys
``platform``, ``sensor``, ``level``, and ``sector``. Only the canonical
``File`` attribute names are used:

.. list-table::
   :header-rows: 1

   * - Legacy Key
     - Canonical Attribute
   * - ``platform``
     - ``source``
   * - ``sensor``
     - ``instrument``
   * - ``level``
     - ``processing_stage``
   * - ``sector``
     - ``domain``

Filter configurations that use the legacy key names must be updated.

::

     modified: src/courier/types/file.py
     modified: src/courier/plugins/classes/job_builders/filter_and_group.py
     modified: src/courier/plugins/classes/data_monitors/kafka_consumer.py
     modified: src/courier/plugins/classes/data_monitors/rabbit_mq_watcher.py
     modified: sphinx/api-reference/types.md
     modified: sphinx/api-reference/plugins.md
     modified: sphinx/getting-started/configuration.md
     modified: RELEASE.md

Version 0.1.0 (2026-04-28)
**************************

 * *Documentation*: Fix readme and pyproject.toml metadata

Documentation
===========

Fix readme and pyproject.toml metadata
-------------------------

Consolidated duplicated sections in README.md into a single clean copy. Updated pyproject.toml to include a real description (was "TODO"), extended Python version support to include 3.14, and fixed the sphinxcontrib-mermaid package name typo.


::

     modified: README.md
     modified: pyproject.toml