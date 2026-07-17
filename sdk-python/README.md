# tpt-sdk (Python)

Python client for TPT Keystone (`pip install tpt-sdk`). Speaks the Postgres
wire protocol v3 directly over `asyncio` TCP — no `psycopg2`/`asyncpg`/any
Postgres driver dependency; `tpt_sdk/wire.py` is a hand-written client-side
codec mirroring the Rust SDK's `tpt-keystone-sdk/src/keystone/wire.rs`, which mirrors
`tpt-keystone/src/wire` server-side.

```python
import asyncio
from tpt_sdk import KeystoneClient

async def main():
    client = await KeystoneClient(host="localhost", port=5432).connect()
    result = await client.query_params("SELECT * FROM robots WHERE id = $1", [42])
    for row in result.rows:
        print(row.id)
    await client.close()

asyncio.run(main())
```

Optional pandas integration: `pip install tpt-sdk[pandas]`, then
`result.to_pandas()` (or `result.to_pandas(client=client)` for catalog-driven
dtype coercion via `information_schema.columns` instead of pandas' own
sniffing).

A synchronous wrapper (`tpt_sdk.blocking.Client`) is available for scripts
that don't want to manage an event loop; see its docstring for the
one-connection-per-call tradeoff it makes.

See `sdk-python`'s module docstrings (`src/tpt_sdk/__init__.py`,
`client.py`, `wire.py`, `pandas_ext.py`, `blocking.py`) for full scope-cut
documentation.
