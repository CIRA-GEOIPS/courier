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
        Entry-point name of the payload plugin the builder configured (e.g.
        ``"bash_payload"``).  A dispatcher may execute it as a lower
        representation; the hydrated payload keeps this name as its
        ``payload_name``, which labels the payload metrics.
    identifier : str
        Identifier the payload was configured under, kept for provenance and
        metric labels.
    config : dict
        Serialized :class:`~courier.interfaces.payloads.PayloadConfig` (or the
        plugin's ``config_class``) *without* its ``script`` field: the raw
        template is not sent, so the job carries its script once, rendered, in
        :attr:`script`.  The dispatcher validates ``config`` with the
        ``config_class`` of the representation it hydrates, dropping keys that
        class does not define, and sets the hydrated config's ``script`` to
        :attr:`script` (a ``config.script`` sent by an older builder is
        replaced the same way).  The ``file`` field, when present, is a path
        string and is informational on the dispatcher host: :attr:`script` is
        authoritative.
    script : str or None
        Output of the job builder's template render (pass one), from the
        payload's ``file`` or inline ``script``; the only copy of the template
        the job carries.  ``None`` for binary-only payloads, which declare
        their command inline.
    suffix : str
        File suffix to use when the dispatcher materializes ``script``.
    defer_nonce : str
        Random nonce embedded in the deferred-expression markers left by the
        builder's pass-one render for dispatcher-only values (see
        :data:`~courier.interfaces.payloads.DISPATCHER_CONTEXT_NAMES`).  The
        dispatcher resolves only markers carrying this nonce whose expression
        is an access path rooted in one of those names, so text coming from
        job data can never be evaluated as a template expression.
    """

    name: str
    identifier: str
    config: dict[str, Any] = Field(default_factory=dict)
    script: str | None = None
    suffix: str = ".sh"
    defer_nonce: str = ""
