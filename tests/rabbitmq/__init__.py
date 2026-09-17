"""Tests that need a real RabbitMQ.

The in-memory transport ignores ``durable``, ``exclusive`` and ``auto_delete``
entirely and never removes a queue when a connection closes, so the central
claim of issue #44 -- that a builder going away used to take its queue and
every buffered file with it -- cannot be expressed there at all.
"""
