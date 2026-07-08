"""Thin synchronous wrapper around `KeystoneClient`, for scripts/notebooks
that don't want to manage an event loop themselves — mirrors the Rust SDK
having both an async `KeystoneClient` and a sync `blocking::Client`
(`tpt-sdk/src/keystone/blocking.rs`).

Implementation choice (documented, per the task's "pick one and document
which"): each call opens **its own private event loop** via `asyncio.run()`
and tears it down afterward — there is no persistent background loop
thread. That means the underlying TCP connection cannot be kept open
across calls (each `query()`/`query_params()` call connects, runs the
query, and disconnects). This is the simplest-to-reason-about option and
is fine for notebook/script convenience use where each call is
independent, but it is NOT suitable for anything wanting connection reuse,
transactions spanning multiple calls, or LISTEN/NOTIFY — use the async
`KeystoneClient` directly for that.
"""

from __future__ import annotations

import asyncio
from typing import Sequence

from .client import KeystoneClient, ParamValue, QueryResult


class Client:
    """Synchronous Keystone client. See module docstring for the
    per-call-connect tradeoff this makes.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 5432, user: str = "tpt_sdk"):
        self.host = host
        self.port = port
        self.user = user

    def query(self, sql: str) -> QueryResult:
        return asyncio.run(self._query(sql))

    def query_params(self, sql: str, params: Sequence[ParamValue] = ()) -> QueryResult:
        return asyncio.run(self._query_params(sql, params))

    async def _query(self, sql: str) -> QueryResult:
        client = await KeystoneClient(self.host, self.port, self.user).connect()
        try:
            return await client.query(sql)
        finally:
            await client.close()

    async def _query_params(self, sql: str, params: Sequence[ParamValue]) -> QueryResult:
        client = await KeystoneClient(self.host, self.port, self.user).connect()
        try:
            return await client.query_params(sql, params)
        finally:
            await client.close()
