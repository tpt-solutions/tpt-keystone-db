"""tpt-sdk (Python) — client for TPT Keystone, a hand-written
Postgres-wire-compatible database engine (see the repo root `CLAUDE.md` and
`9sdkspec.txt` section 4 for the full platform/spec context).

Speaks the wire protocol directly over `asyncio` TCP sockets
(`tpt_sdk.wire`) — no `psycopg2`/`asyncpg`/other Postgres driver dependency,
matching this project's hand-written-wire-protocol ethos.

    import asyncio
    from tpt_sdk import KeystoneClient

    async def main():
        client = await KeystoneClient(host="localhost", port=5432).connect()
        result = await client.query_params("SELECT * FROM robots WHERE id = $1", [42])
        for row in result.rows:
            print(row.id)
        await client.close()

    asyncio.run(main())

A synchronous wrapper is available as `tpt_sdk.blocking.Client` for
scripts/notebooks that don't want to manage an event loop (see that
module's docstring for the tradeoff it makes: one throwaway event loop and
connection per call).
"""

from .client import ConnectionNotOpen, KeystoneClient, KeystoneError, QueryResult, Row, ServerError

__all__ = [
    "KeystoneClient",
    "QueryResult",
    "Row",
    "KeystoneError",
    "ServerError",
    "ConnectionNotOpen",
]

__version__ = "0.1.0"
