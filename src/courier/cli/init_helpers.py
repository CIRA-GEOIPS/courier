"""Schema introspection helpers for the ``courier init`` command.

These pure functions extract structured metadata from plugin classes
and their companion Pydantic Config models, enabling guided interactive
prompting during project initialisation.
"""

from __future__ import annotations

import importlib
import inspect
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from typing import Any


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


def _as_model(candidate: object) -> type[BaseModel] | None:
    """Return *candidate* if it is a pydantic model class, else ``None``."""
    if isinstance(candidate, type) and issubclass(candidate, BaseModel):
        return candidate
    return None


def declared_config_model(plugin_class: type) -> type[BaseModel] | None:
    """Return the config model *plugin_class* declares it validates with.

    That is its ``config_class`` class attribute, possibly inherited: payloads
    and dispatchers validate their config block with it. Unlike
    :func:`find_config_model` this never guesses from names.

    Parameters
    ----------
    plugin_class : type
        A plugin class.

    Returns
    -------
    type[BaseModel] or None
        The declared model, or ``None`` when the class declares none.
    """
    return _as_model(getattr(plugin_class, "config_class", None))


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
    preferring those named ``{PluginClassName}Config``.  A model the class
    declares (see :func:`declared_config_model`) wins over that guess unless
    the guess is a subclass of it -- a declaration inherited from a base class
    is less specific than a model the plugin's own module extends it with.

    Returns ``None`` if no Config model is found.
    """
    declared = declared_config_model(plugin_class)
    guessed = _module_config_model(plugin_class)
    if declared is None:
        return guessed
    if guessed is not None and issubclass(guessed, declared):
        return guessed
    return declared


def get_field_metadata(model: type[BaseModel]) -> list[dict[str, Any]]:
    """Extract per-field metadata from a Pydantic model for interactive prompting.

    Each dict contains:

    * ``name`` — field name (``str``)
    * ``type_hint`` — human-readable type string (e.g. ``"str"``, ``"list[str]"``)
    * ``default`` — the default value, or ``...`` sentinel if required or
      produced by a ``default_factory`` (there is no value to show, and
      ``field_info.default`` would be pydantic's ``PydanticUndefined``)
    * ``description`` — field description string (``""`` when absent)
    * ``required`` — ``True`` when the field has no default
    """
    fields: list[dict[str, Any]] = []

    for field_name, field_info in model.model_fields.items():
        required = field_info.is_required()
        # A factory default must not be read from ``field_info.default``: that
        # is PydanticUndefined, which the prompt then offered as the default
        # and wrote into the config as the literal string "PydanticUndefined".
        has_value_default = not required and field_info.default_factory is None
        default = field_info.default if has_value_default else ...

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
