"""Serialized payload specification carried on a Job.

A job builder renders its payload's template once (pass one) and serializes
the result here; the dispatcher hydrates a payload instance from this spec,
resolves the values only it knows (pass two) and executes it.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PayloadSpec(BaseModel):
    """Wire format for the payload a job should execute.

    Attributes
    ----------
    name : str
        Entry-point name of the payload plugin the builder configured; it
        labels the payload metrics even when a dispatcher executes a lower
        representation.
    identifier : str
        Identifier the payload was configured under.
    config : dict
        The payload's serialized config *without* ``script``: a dispatcher
        validates it with the hydrated representation's ``config_class``,
        dropping keys that class does not define.
    script : str or None
        The builder's rendered template, the only copy the job carries;
        ``None`` for a binary-only payload.
    suffix : str
        File suffix for the script the dispatcher writes.
    defer_nonce : str
        Random nonce in the markers pass one left for dispatcher-only values;
        pass two resolves only markers carrying it.
    """

    name: str
    identifier: str
    config: dict[str, Any] = Field(default_factory=dict)
    script: str | None = None
    suffix: str = ".sh"
    defer_nonce: str = ""
