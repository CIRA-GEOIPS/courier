"""Schema introspection helpers for the ``courier init`` command.

These pure functions extract structured metadata from plugin classes
and their companion Pydantic Config models, enabling guided interactive
prompting during project initialisation.
"""

from __future__ import annotations

import importlib
import inspect
from types import UnionType
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel

# Suffixes that some plugin class names drop in their companion Config model
# (e.g. MetadataRouterBuilder -> MetadataRouterConfig).
_CONFIG_NAME_STRIPPABLE_SUFFIXES = (
    "Builder",
    "Poller",
    "Watcher",
    "Consumer",
    "Dispatcher",
)


def _resolve_candidate(
    plugin_class: type,
    candidates: list[type[BaseModel]],
) -> type[BaseModel] | None:
    """Pick the best Config model from multiple candidates by name heuristics."""
    plugin_name = plugin_class.__name__

    # (1) Exact match: {PluginClassName}Config
    expected = f"{plugin_name}Config"
    for candidate in candidates:
        if candidate.__name__ == expected:
            return candidate

    # (2) Case-insensitive containment
    plugin_lower = plugin_name.lower()
    for candidate in candidates:
        if plugin_lower in candidate.__name__.lower():
            return candidate

    # (3) Suffix-stripped match
    for suffix in _CONFIG_NAME_STRIPPABLE_SUFFIXES:
        if plugin_name.endswith(suffix):
            bare = plugin_name[: -len(suffix)]
            expected_bare = f"{bare}Config"
            for candidate in candidates:
                if candidate.__name__ == expected_bare:
                    return candidate
            break

    return None


def _module_config_model(plugin_class: type) -> type[BaseModel] | None:
    """Guess *plugin_class*'s config model from the models its module defines."""
    module = importlib.import_module(plugin_class.__module__)
    candidates: list[type[BaseModel]] = []

    for _name, obj in inspect.getmembers(module, inspect.isclass):
        if obj is plugin_class:
            continue
        if not issubclass(obj, BaseModel):
            continue
        if obj.__module__ != plugin_class.__module__:
            continue
        candidates.append(obj)

    if not candidates:
        return None

    if len(candidates) == 1:
        return candidates[0]

    return _resolve_candidate(plugin_class, candidates)


def find_config_model(plugin_class: type) -> type[BaseModel] | None:
    """Find the companion Pydantic Config model for *plugin_class*.

    Scans the plugin's defining module for ``BaseModel`` subclasses,
    preferring those named ``{PluginClassName}Config``.  A ``config_class`` the
    class declares (payloads and dispatchers validate with it) wins over that
    guess unless the guess is a subclass of it -- a declaration inherited from
    a base class is less specific than a model the plugin's own module extends
    it with.

    Returns ``None`` if no Config model is found.
    """
    declared = getattr(plugin_class, "config_class", None)
    guessed = _module_config_model(plugin_class)
    if not (isinstance(declared, type) and issubclass(declared, BaseModel)):
        return guessed
    if guessed is not None and issubclass(guessed, declared):
        return guessed
    return declared


def get_field_metadata(model: type[BaseModel]) -> list[dict[str, Any]]:
    """Extract per-field metadata from a Pydantic model for interactive prompting.

    Each dict contains:

    * ``name`` — field name (``str``)
    * ``type_hint`` — human-readable type string (e.g. ``"str"``, ``"list[str]"``)
    * ``default`` — the default value (a ``default_factory`` is called to
      produce it), or ``...`` sentinel if required
    * ``description`` — field description string (``""`` when absent)
    * ``required`` — ``True`` when the field has no default
    * ``annotation`` - Field type provided by its annotation.
    """
    fields: list[dict[str, Any]] = []

    for field_name, field_info in model.model_fields.items():
        required = field_info.is_required()
        default: Any = ...
        if not required:
            # ``field_info.default`` is PydanticUndefined for a factory default,
            # which the prompt once offered and wrote into the config as the
            # literal string "PydanticUndefined"; ask pydantic to resolve it.
            # ``validated_data`` is the field *values* a factory taking data
            # receives -- none exist before prompting, so pass an empty dict.
            default = field_info.get_default(
                call_default_factory=True,
                validated_data={},
            )

        annotation = field_info.annotation
        if annotation is not None:
            if hasattr(annotation, "_name"):
                type_hint = annotation._name
            else:
                type_hint = str(annotation).replace("typing.", "")
                type_hint = type_hint.replace("<class '", "").replace("'>", "")
        else:
            type_hint = "any"

        description = ""
        if field_info.description:
            description = str(field_info.description)

        fields.append(
            {
                "name": field_name,
                "type_hint": type_hint,
                "default": default,
                "description": description,
                "required": required,
                "annotation": annotation,
            },
        )

    return fields


def get_plugin_description(plugin_class: type) -> str:
    """Extract the first sentence of *plugin_class*'s docstring.

    Returns the text up to the first period or newline, stripped.
    Returns an empty string when the class has no docstring.
    """
    doc = plugin_class.__doc__
    if not doc:
        return ""

    doc = inspect.cleandoc(doc)

    for i, char in enumerate(doc):
        if char == "\n":
            return doc[:i].strip()
        if char == "." and (i + 1 >= len(doc) or doc[i + 1] in (" ", "\n")):
            return doc[: i + 1].strip()

    return doc.strip()


def get_nested_model(type_hint: Any) -> type[BaseModel] | None:
    """Return a nested Pydantic model, if present."""
    if isinstance(type_hint, type) and issubclass(type_hint, BaseModel):
        return type_hint

    if get_origin(type_hint) in (Union, UnionType):
        args = [arg for arg in get_args(type_hint) if arg is not type(None)]

        if len(args) == 1:
            return get_nested_model(args[0])

    return None


def get_list_item_type(annotation: Any) -> Any | None:
    """Get the element annotation for a `list` item."""
    origin = get_origin(annotation)

    if origin in (Union, UnionType):
        non_none = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(non_none) == 1:
            return get_list_item_type(non_none[0])

    if annotation is list or origin is list:
        args = get_args(annotation)
        return args[0] if args else Any

    return None


def is_nullable(type_hint: Any) -> bool:
    """Return whether an annotation accepts None."""

    return get_origin(type_hint) in (Union, UnionType) and type(None) in get_args(
        type_hint
    )
