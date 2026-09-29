"""Serialized payload specification carried on a Job.

A payload is the builder-owned, job-attached description of *what* should be
executed.  A job builder holds a nested payload plugin, renders the payload's
template once (pass one), and serializes the result here; the dispatcher
hydrates a payload instance from this spec (pass two) and executes it.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PayloadSpec(BaseModel):
    """Wire format for the payload a job should execute.

    Attributes
    ----------
    name : str
        Entry-point name of the payload class (e.g. ``"bash_payload"``).
    identifier : str
        Identifier the payload was configured under, kept for provenance and
        metric labels.
    config : dict
        Serialized :class:`~courier.interfaces.payloads.PayloadConfig`.  The
        ``file`` field, when present, is a path string and is informational on
        the dispatcher host: ``script`` is authoritative.
    script : str or None
        Output of the job builder's template render (pass one). ``None`` for
        binary-only payloads, which declare their command inline.
    suffix : str
        File suffix to use when the dispatcher materializes ``script``.
    defer_nonce : str
        Random nonce embedded in the deferred-expression markers left by the
        builder's pass-one render.  The dispatcher resolves only markers carrying
        this nonce, so text coming from job data can never be evaluated as a
        template expression.
    """

    name: str
    identifier: str
    config: dict[str, Any] = Field(default_factory=dict)
    script: str | None = None
    suffix: str = ".sh"
    defer_nonce: str = ""
