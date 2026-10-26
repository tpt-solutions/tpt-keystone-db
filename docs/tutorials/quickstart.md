# Quickstart

## Build & run

```sh
cd tpt-keystone
cargo build
cargo run   # starts a single-node writer on 0.0.0.0:55432, local-fs storage under tpt-data/
```

No config is required for a single-node dev instance. `cargo run` defaults
to the `local` storage backend, the writer role, and `tpt-data/` for both the
local disk state and the emulated shared object store.

## Connect

```sh
psql -h localhost -p 55432
```

The engine's default port is `55432`. To listen on the standard `5432`, set
`TPT_PG_ADDR=0.0.0.0:5432` before `cargo run`. `docker compose up` does this
for you.

Authentication is opt-in. While the `_tpt_roles` table is empty (the default),
any connection is accepted with no password. Once a role exists, logins require
SCRAM-SHA-256. Read the status table in
[`docs/security_audit_phase12.md`](../security_audit_phase12.md) before
deploying anywhere reachable.

## Your first table and query

```sql
CREATE TABLE users (id INT4 PRIMARY KEY, name TEXT, signed_up_at INT8);
INSERT INTO users VALUES (1, 'Ada', 1700000000000);
INSERT INTO users VALUES (2, 'Bob', 1700100000000);

SELECT * FROM users WHERE id = 1;
SELECT name FROM users ORDER BY signed_up_at DESC;
```

Restart `cargo run` and re-run the `SELECT` — rows persist across restarts
(Phase 1's milestone: WAL + LSM storage under `tpt-data/`).

## Beyond plain SQL

Every other engine (Meridian/geospatial, Chronos/time-series, Plexus/graph,
Canopy/document, Prism/vector, Flux/streaming, Synapse/Mirror agent layers)
is reachable from this same SQL connection — no separate wire protocol,
port, or client library. See `docs/sql-reference.md` for the full function
list, and `docs/tutorials/hybrid-search.md` for a worked Prism/Canopy
example.

```sql
-- Meridian: spatial index + range query
CREATE TABLE drones (id INT4, pos GEOMETRY);
INSERT INTO drones VALUES (1, 'POINT(174.77 -41.29 50 1700000000000)');
CREATE INDEX ON drones USING SPATIAL (pos);
SELECT id FROM drones WHERE ST_DWithin(pos, ST_MakePoint(174.77, -41.29), 1000);

-- Plexus: graph traversal composed with an ordinary SQL JOIN
CREATE TABLE follows (from_id TEXT, to_id TEXT);
INSERT INTO follows VALUES ('alice', 'bob'), ('bob', 'carol');
CREATE INDEX ON follows USING GRAPH (from_id) WITH (to = 'to_id');
SELECT * FROM graph_bfs('follows', 'from_id', 'alice', 2);
```

## Talking to it from an SDK instead of `psql`

```sh
# CLI
cargo install --path ../tpt-keystone-cli   # or use the tpt-keystone-cli/ crate directly
tpt query "SELECT 1"

# Python
pip install -e sdk-python
```

```python
import asyncio
from tpt_sdk import KeystoneClient

async def main():
    client = await KeystoneClient(host="localhost", port=55432).connect()
    result = await client.query("SELECT 1 AS one")
    print(result.rows)

asyncio.run(main())
```

```ts
// Node / TypeScript: @tpt/sdk-server is an ESM package. Build it first
// (`npm run build` in packages/sdk-server), then import it.
import { KeystoneClient } from "@tpt/sdk-server";
```

See `packages/sdk-server/README` for connection options.

See `docs/sdks.md` for the full list of client libraries and what each one
gives you.
