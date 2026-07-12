# TPT Keystone Cookbook — seven engines, one binary

TPT Keystone ships **seven engines in a single process**: a relational core
(Keystone) plus geospatial (Meridian), vector/AI (Prism), time-series
(Chronos), graph (Plexus), document/JSON (Canopy), and event-streaming (Flux).
Rather than separate storage engines, each extension is a set of SQL index
options, table-valued functions, and operators layered onto the same row
store — which means they **compose inside one query plan**.

This cookbook walks through a single `places` table that exercises all seven
engines at once. The runnable script is [`cookbook.sql`](./cookbook.sql); run it
with:

```bash
# from the repo root, after `docker compose up --build` is healthy:
psql -h localhost -p 5432 -f docs/tutorials/cookbook.sql
```

> No data is persisted between runs in the script as written — re-run it any
> number of times. `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`
> keep it idempotent.

## What each block demonstrates

| # | Engine | Surface used |
|---|--------|--------------|
| 1 | **Keystone** (relational) | `CREATE TABLE`/`INSERT`/`SELECT` with purpose-typed columns (`GEOMETRY`, `VECTOR`, `JSON`, `INT8` epoch-ms) |
| 2 | **Meridian** (geo) | `USING SPATIAL`, `ST_MakePoint`, `ST_Distance`, `ST_DWithin`, `ST_Within`, `ST_GeomFromText` |
| 3 | **Prism** (vector) | `USING VECTOR (…) WITH (metric = 'l2')`, `vector_search('table','col','[…]',k)` |
| 4 | **Chronos** (time-series) | `USING TIME (…) WITH (interval = …, value = …)`, `time_bucket`, `moving_average(…) OVER (ORDER BY ts)` |
| 5 | **Plexus** (graph) | `USING GRAPH (…) WITH (to = …, type = …)`, `graph_neighbors`, `graph_bfs`, `graph_shortest_path`, `MATCH` |
| 6 | **Canopy** (document) | `USING JSONPATH (…) WITH (path = …)`, `->`/`->>`/`@>` operators, `json_path_lookup` |
| 7 | **Flux** (streaming) | `CREATE TOPIC`, automatic per-table CDC (`__cdc_<table>`), `flux_window_tumbling`, `flux_time_travel` |

## Highlights

### Hybrid vector + SQL (Prism + Keystone)
`vector_search` returns the full matched row plus an appended `distance`
column, so it composes with ordinary SQL — here a k-NN search narrowed by a
`region` filter in the same plan:

```sql
SELECT v.name
FROM vector_search('places', 'embedding', '[1.0, 0.0, 0.0]', 5) v
JOIN places p ON p.name = v.name
WHERE p.region = 'oceania'
ORDER BY v.distance;
```

### A graph you can traverse with GQL (Plexus)
Edges are just rows; the `USING GRAPH` index is what makes traversal cheap:

```sql
SELECT b FROM MATCH (a)-[:oceania]->(b) ON ownership(from_id) WHERE a = 'Auckland' RETURN b;
```

### Streaming is automatic (Flux)
You don't opt a table into streaming — every `INSERT`/`UPDATE`/`DELETE` produces
CDC events on `__cdc_<table>`, which you can window or time-travel:

```sql
SELECT window_start, window_end, count FROM flux_window_tumbling('__cdc_places', 3600000);
```

## Notes & scope cuts
- The script is illustrative, not a performance benchmark. Throughput/latency
  figures elsewhere in this repo are unverified.
- Local secondary indexes (geo/vector/graph/time/JSON) are single-node; there
  is no distributed index replication yet.
- The Postgres-wire listener defaults to `55432`; the quickstart `docker-compose.yml`
  relocates it to `5432` via `TPT_PG_ADDR` so standard tooling works unchanged.
