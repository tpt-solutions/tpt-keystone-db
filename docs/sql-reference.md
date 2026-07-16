# SQL Reference

Everything here is hand-parsed by `tpt-keystone/src/sql/`; there is no
`sqlparser-rs` dependency, so syntax not listed here likely isn't
supported. Where a clause is parsed but not enforced, or a feature is a
documented scope cut, this doc says so — see `TODO.md` for full detail per
phase.

## Data types

`INT2`/`INT4`/`INT8` (aliases `SMALLINT`/`INTEGER`/`BIGINT`), `FLOAT4`/
`FLOAT8` (`REAL`/`DOUBLE PRECISION`), `BOOL`, `TEXT`/`VARCHAR`, `BYTEA`,
`JSON`/`JSONB` (same internal representation, stored as text — see
`docs/architecture.md`), `GEOMETRY` (WKT text, `POINT`/`LINESTRING`/
`POLYGON`, optional `Z`/`M` ordinates for altitude/time), `VECTOR` (stored
as `[1,2,3]` text), `SERIAL`/`BIGSERIAL`/`SMALLSERIAL` (shorthand for an
`INT`-family column backed by `CREATE SEQUENCE`+`nextval`).

There is no dedicated `TIMESTAMP`/`INTERVAL` type — timestamps are plain
`INT8` unix-milliseconds, and interval literals (`'1 hour'`, `'30 days'`)
are hand-parsed text, not a first-class type with arithmetic.

## DDL

```sql
CREATE TABLE [IF NOT EXISTS] t (
  col type [DEFAULT expr] [NOT NULL] [UNIQUE] [PRIMARY KEY]
    [REFERENCES other_table(col)],
  ...
  [UNIQUE (col, ...)],
  [FOREIGN KEY (col, ...) REFERENCES other_table(col, ...)]
) [WITH (json_schema_col = '...', json_schema = '...', json_schema_mode = 'strict'|'relaxed'|'off')];

DROP TABLE [IF EXISTS] t;  -- purges rows + per-table secondary indexes
ALTER TABLE t ADD COLUMN col type [DEFAULT expr] [NOT NULL];
ALTER TABLE t DROP COLUMN col;  -- rejected if PK/UNIQUE/FOREIGN KEY/indexed
ALTER TABLE t ALTER COLUMN col SET DEFAULT expr;
ALTER TABLE t ALTER COLUMN col SET/DROP NOT NULL;
CREATE SEQUENCE [IF NOT EXISTS] seq;  -- IF NOT EXISTS is enforced (no-op if exists)
CREATE INDEX ON t (col);                                   -- plain B-Tree
CREATE INDEX ON t USING SPATIAL (col);                     -- Meridian
CREATE INDEX ON t USING TIME(ts_col) WITH (interval = '1 hour', retention = '30 days');  -- Chronos
CREATE INDEX ON t USING GRAPH (from_col) WITH (to = 'to_col', type = 'rel_col');         -- Plexus
CREATE INDEX ON t USING JSONPATH (col) WITH (path = 'user.address.city');               -- Canopy
CREATE INDEX ON t USING GIN (col);       -- or USING FTS — Canopy full-text
CREATE INDEX ON t USING VECTOR (col) WITH (metric = 'l2'|'cosine', m = ..., ef_construction = ..., ef_search = ...);  -- Prism
CREATE TOPIC t WITH (partitions = n, retention = '...', retention_bytes = n);  -- Flux
CREATE FUNCTION name(args) RETURNS type LANGUAGE wasm AS '<base64>';  -- WASM UDF (int8/float8/bool only)
```

`ALTER TABLE ADD/DROP COLUMN` backfill every existing row (non-crash-atomic
pass held under the global LSM lock); `DROP COLUMN` is rejected on
PK/UNIQUE/FOREIGN KEY/indexed columns. `DROP TABLE` purges rows and all
per-table secondary indexes; a reader node may still serve a dropped table
until it refreshes its catalog (a known convergence gap tracked in
`TODO.md`). Composite primary keys only use the first column as the physical
row key (a pre-existing engine limitation).

## DML

```sql
INSERT INTO t [(col, ...)] VALUES (v, ...), ...;
UPDATE t SET col = expr, ... [WHERE ...];
DELETE FROM t [WHERE ...];
BEGIN; ... COMMIT; / ROLLBACK;
COPY t FROM STDIN;   -- text format only, over the simple query protocol
COPY t TO STDOUT;
```

`ON DELETE`/`ON UPDATE` referential actions are parsed but not enforced (no
cascade). `UNIQUE`/`FOREIGN KEY` are enforced via an O(n) table scan per
check (no index acceleration).

## Queries

```sql
SELECT [DISTINCT] expr [AS alias], ...
FROM t [alias] [JOIN t2 ON ...] [, ...]
[WHERE ...]
[GROUP BY ...] [HAVING ...]
[ORDER BY ... [ASC|DESC]]
[LIMIT n] [OFFSET n];

WITH [RECURSIVE] cte AS (SELECT ...) SELECT ... FROM cte;
```

Window functions: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`,
`LAG(expr, n)`, `LEAD(expr, n)`, plus any aggregate as a window function
with `OVER (PARTITION BY ... ORDER BY ... ROWS|RANGE BETWEEN ...)`.

Subqueries: scalar, correlated, `EXISTS`, `IN`, derived tables (`FROM
(SELECT ...) alias`).

## `pg_catalog` / `information_schema`

`pg_tables`, `pg_class`, `pg_namespace`, `pg_attribute`, `pg_type`,
`pg_indexes`, `pg_index`, `pg_constraint`, `pg_sequence`,
`information_schema.tables`/`columns` are all queryable via plain SQL,
including schema-qualified names (`pg_catalog.pg_tables`,
`public.my_table`). Not implemented: the specific joined queries a real
`psql`'s `\d`/`\dt`/`\di` meta-commands issue (they call `format_type()`/
`pg_table_is_visible()`, neither of which exists) — direct `SELECT`s
against these catalog tables work, a literal `psql \d` does not.

## Functions by engine

### Meridian (geospatial)
`ST_MakePoint(x, y[, z[, t]])`, `ST_Point(x, y)`, `ST_GeomFromText(wkt)`,
`ST_AsText(geom)`, `ST_X`/`ST_Y`/`ST_Z`/`ST_T(geom)`, `ST_Distance(a, b)`,
`ST_DWithin(a, b, radius)`, `ST_Within`/`ST_Contains(a, b)`,
`ST_Intersects(a, b)` (bbox-only, not exact polygon/line intersection),
`ST_Length(geom)`, `ST_Area(geom)` (planar shoelace, not geodesic).

A `ST_DWithin(pos, ST_MakePoint(...), r) AND ST_T(pos) BETWEEN t1 AND t2`
WHERE clause on a `SPATIAL`-indexed column is planner-rewritten to an index
scan automatically — no special syntax needed.

### Chronos (time-series)
`time_bucket(interval, ts)`, `moving_average(value, window_size)` (window
function), `interpolate(value)` (window function). `time_bucket(...) =
const` and `BETWEEN` predicates on a `TIME`-indexed column are
planner-rewritten to a range scan automatically.

### Plexus (graph) — all table-valued functions
`graph_neighbors(table, from_col, vertex[, direction])`,
`graph_bfs(table, from_col, vertex, max_depth)`,
`graph_shortest_path(table, from_col, from_vertex, to_vertex)`,
`graph_connected_components(table, from_col)`,
`graph_pagerank(table, from_col[, iterations[, damping]])`,
`graph_triangle_count(table, from_col)`.

### Canopy (JSON/document)
Operators: `->`, `->>`, `#>`, `#>>`, `@>`. Functions: `json_typeof`,
`json_valid`, `json_array_length`, `json_extract_path[_text]`,
`jsonb_set`, `jsonb_build_object`, `jsonb_build_array`, `to_json`.
Table functions: `json_path_lookup(table, column, value)` (equality lookup
against a `JSONPATH` index), `json_text_search(table, column, query)`
(AND-only boolean match against a `GIN`/`FTS` index).

### Prism (vector) / hybrid search
`l2_distance(a, b)`, `cosine_distance(a, b)`, `cosine_similarity(a, b)`,
`dot_product(a, b)` — scalar functions over `VECTOR` text literals.
`vector_search(table, vec_column, '[..]', k[, ef_search])` — HNSW k-NN,
returns the full matched row plus a `distance` column; composes with
ordinary `JOIN`/`WHERE`.
`hybrid_search(table, vec_column, '[..]', fts_column, 'query text', k)` —
fuses a vector k-NN ranking and a Canopy BM25 ranking via Reciprocal Rank
Fusion; returns the matched row plus `vec_distance`, `bm25_score`
(either may be `NULL` if the row only appears in one ranking), and
`fused_score`. See `docs/tutorials/hybrid-search.md` for a worked example.
Requires both a `VECTOR` index on `vec_column` and a `GIN`/`FTS` index on
`fts_column`.

### Flux (event streaming)
`flux_time_travel(table, timestamp_ms)`, `flux_window_tumbling(topic,
window_ms)`, `flux_window_session(topic, gap_ms)`,
`flux_window_sliding(topic, window_size_ms, slide_ms)`. Publish/poll/commit
are `Database` methods (Rust API + the WebSocket bridge), not SQL
statements — every `INSERT`/`UPDATE`/`DELETE` also auto-publishes a CDC
event to an implicit `__cdc_<table>` topic.

### Synapse (agent memory) / Mirror (agent observability)
`synapse_recall_semantic(agent_id, query, k)`,
`synapse_discover_tools(query, k)` — both HNSW k-NN over Synapse's own
tables. `mirror_session_events(session_id)`,
`mirror_agent_metrics(agent_id, t0, t1)` — replay a traced agent session /
answer a latency rollup. Agent lifecycle, task delegation, and audit-chain
verification are Rust `Database`-adjacent APIs (`synapse::actor`,
`synapse::coordination`, `mirror::audit`), not SQL statements.

## Prepared statements & protocol extras

The extended query protocol (Parse/Bind/Describe/Execute/Sync/Close) is
fully supported for parameterized queries (`$1`, `$2`, ...). `DECLARE
CURSOR`/`FETCH`/`MOVE`/`CLOSE` and `LISTEN`/`NOTIFY`/`UNLISTEN` only work
over the simple query protocol.
