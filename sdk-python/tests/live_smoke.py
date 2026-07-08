"""Manual smoke test against a live Keystone instance on 127.0.0.1:5432.
Not a pytest suite (no server fixture exists) — run directly:

    python tests/live_smoke.py
"""

import asyncio
import time

from tpt_sdk import KeystoneClient
from tpt_sdk.blocking import Client


async def main() -> None:
    client = await KeystoneClient(host="127.0.0.1", port=5432).connect()
    try:
        # NOTE: this live instance's DROP TABLE does not actually remove the
        # table from pg_tables (verified separately: DROP TABLE reports
        # success but the table stays listed) - a server-side quirk of this
        # particular build, unrelated to this SDK. Use a unique table name
        # per run instead of DROP+recreate to route around it.
        table = f"tpt_sdk_smoke_robots_{int(time.time())}"
        r = await client.query(
            f"CREATE TABLE {table} (id INT4, name TEXT, weight_kg FLOAT8, active BOOL)"
        )
        print("CREATE TABLE ->", r.command_tag)

        r = await client.query_params(
            f"INSERT INTO {table} (id, name, weight_kg, active) VALUES ($1, $2, $3, $4)",
            [1, "R2D2", 32.5, True],
        )
        print("INSERT (params) ->", r.command_tag)
        await client.query_params(
            f"INSERT INTO {table} (id, name, weight_kg, active) VALUES ($1, $2, $3, $4)",
            [2, "C3PO", 75.0, False],
        )
        await client.query_params(
            f"INSERT INTO {table} (id, name, weight_kg, active) VALUES ($1, $2, $3, $4)",
            [3, "BB8", None, True],
        )

        result = await client.query_params(
            f"SELECT * FROM {table} WHERE id = $1", [1]
        )
        print("Parameterized SELECT columns:", result.columns)
        row = result.rows[0]
        print("row.id =", row.id, "row.name =", row.name, "row.weight_kg =", row.weight_kg)
        assert row.id == 1
        assert row.name == "R2D2"

        multi = await client.query(f"SELECT * FROM {table} ORDER BY id")
        print("Multi-row SELECT: got", len(multi), "rows")
        print("repr():", repr(multi))
        html = multi._repr_html_()
        print("_repr_html_ (first 200 chars):", html[:200])
        assert "<table" in html

        df = await multi.to_pandas_async(client=client, table_hint=table)
        print("to_pandas() dtypes:\n", df.dtypes)
        print(df)
        assert str(df["id"].dtype) == "Int64"
        assert str(df["active"].dtype) == "boolean"

    finally:
        await client.close()


def sync_smoke() -> None:
    # blocking.Client.query() runs its own asyncio.run() internally, so it
    # must be called from outside any running event loop (see blocking.py's
    # docstring) - hence this runs after asyncio.run(main()) has returned,
    # not from inside it.
    sync_client = Client(host="127.0.0.1", port=5432)
    r = sync_client.query("SELECT 1 AS one")
    print("blocking.Client query() ->", r.rows[0].one)
    assert r.rows[0].one == 1

    df2 = r.to_pandas()
    print("sync to_pandas() (no client, untyped):\n", df2)

    print("ALL SMOKE CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
    sync_smoke()
