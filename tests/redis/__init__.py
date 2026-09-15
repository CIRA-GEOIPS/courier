"""Tests that need a live Redis.

The in-process fake has no Lua runtime, so the server-side union that keeps two
replicas from erasing each other's files silently falls back to a plain write
there. Only a real server exercises the path that makes competing consumers
safe.
"""
