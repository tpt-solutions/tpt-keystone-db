-- TPT Keystone — "seven engines, one binary" cookbook
-- ---------------------------------------------------------------------------
-- Run this against a running node (e.g. `docker compose up --build`, then
-- `psql -h localhost -p 5432 -f docs/tutorials/cookbook.sql`). Every engine
-- is just SQL extensions layered onto the same row store, so they compose in
-- a single query plan.
--
-- The seven engines exercised below:
--   1. Keystone  relational (base table + SQL)
--   2. Meridian  geospatial (USING SPATIAL + ST_*)
--   3. Prism     vector / AI (USING VECTOR + vector_search)
--   4. Chronos   time-series (USING TIME + time_bucket / moving_average)
--   5. Plexus    graph (USING GRAPH + graph_* + MATCH)
--   6. Canopy    document / JSON (USING JSONPATH + -> / @> operators)
--   7. Flux      event streaming (CREATE TOPIC + windowing / CDC)

-- ===========================================================================
-- 1. KEYSTONE — relational foundation
-- ===========================================================================
CREATE TABLE IF NOT EXISTS places (
    id      INT4 PRIMARY KEY,
    name    TEXT,
    region  TEXT,
    loc     GEOMETRY,            -- Meridian column
    embedding VECTOR,            -- Prism column
    profile JSON,                -- Canopy column
    ts      INT8,               -- Chronos timestamp (epoch ms)
    value   FLOAT8              -- Chronos metric
);

INSERT INTO places (id, name, region, loc, embedding, profile, ts, value) VALUES
  (1, 'Auckland',   'oceania',  ST_MakePoint(174.76, -36.85),  '[1.0, 0.0, 0.0]', '{"tier":"alpha","owner":"ada"}',   1000, 10.0),
  (2, 'Wellington', 'oceania',  ST_MakePoint(174.78, -41.29),  '[0.9, 0.1, 0.0]',  '{"tier":"beta","owner":"ada"}',   2000, 12.0),
  (3, 'Sydney',     'oceania',  ST_MakePoint(151.21, -33.87),  '[0.0, 1.0, 0.0]',  '{"tier":"alpha","owner":"bob"}',  3000, 9.0),
  (4, 'Tokyo',      'asia',     ST_MakePoint(139.69, 35.69),   '[0.0, 0.0, 1.0]',  '{"tier":"beta","owner":"bob"}',   4000, 15.0),
  (5, 'Singapore',  'asia',     ST_MakePoint(103.82, 1.35),    '[0.2, 0.2, 0.8]',  '{"tier":"alpha","owner":"cleo"}', 5000, 11.0);

-- ===========================================================================
-- 2. MERIDIAN — geospatial
-- ===========================================================================
-- Accelerate spatial predicates with a SPATIAL index.
CREATE INDEX IF NOT EXISTS ON places USING SPATIAL (loc);

-- Distance between two points (degrees here, but the same functions apply
-- to projected/SRID-aware geometries).
SELECT name, ST_Distance(loc, ST_MakePoint(174.76, -36.85)) AS km_from_auckland
FROM places
ORDER BY km_from_auckland;

-- Radius search: everything within ~10 degrees of Auckland.
SELECT name FROM places
WHERE ST_DWithin(loc, ST_MakePoint(174.76, -36.85), 10);

-- Point-in-polygon test.
SELECT name, ST_Within(loc, ST_GeomFromText('POLYGON((100 -50, 200 -50, 200 0, 100 0, 100 -50))')) AS in_asia_box
FROM places;

-- ===========================================================================
-- 3. PRISM — vector / AI
-- ===========================================================================
-- Build an HNSW (or brute-force fallback) index over the embedding column.
CREATE INDEX IF NOT EXISTS ON places USING VECTOR (embedding) WITH (metric = 'l2');

-- k-NN search returns the full matched row plus an appended `distance`.
SELECT name, distance
FROM vector_search('places', 'embedding', '[1.0, 0.0, 0.0]', 3)
ORDER BY distance;

-- Hybrid: vector search composed with a normal SQL filter in one plan.
SELECT v.name
FROM vector_search('places', 'embedding', '[1.0, 0.0, 0.0]', 5) v
JOIN places p ON p.name = v.name
WHERE p.region = 'oceania'
ORDER BY v.distance;

-- ===========================================================================
-- 4. CHRONOS — time-series
-- ===========================================================================
-- Time-bucketed index drives fast time-range scans and rollups.
CREATE INDEX IF NOT EXISTS ON places USING TIME (ts) WITH (interval = '1 hour', value = 'value');

-- Bucket a timestamp down to its interval boundary.
SELECT name, time_bucket('1 hour', ts) AS bucket FROM places ORDER BY name;

-- Rolling average over the series (window function over ORDER BY ts).
SELECT name, ts, value, moving_average(value, 2) OVER (ORDER BY ts) AS smoothed
FROM places ORDER BY ts;

-- ===========================================================================
-- 5. PLEXUS — graph
-- ===========================================================================
-- Model ownership as a graph: from place -> owner (edge type = region).
CREATE TABLE IF NOT EXISTS ownership (
    id      INT4,
    from_id TEXT,
    to_id   TEXT,
    rel     TEXT
);
INSERT INTO ownership (id, from_id, to_id, rel) VALUES
  (1, 'Auckland',   'ada',  'oceania'),
  (2, 'Wellington', 'ada',  'oceania'),
  (3, 'Sydney',     'bob',  'oceania'),
  (4, 'Tokyo',      'bob',  'asia'),
  (5, 'Singapore',  'cleo', 'asia');

CREATE INDEX IF NOT EXISTS ON ownership USING GRAPH (from_id) WITH (to = 'to_id', type = 'rel');

-- Traverse one hop from a start vertex.
SELECT neighbor, rel_type FROM graph_neighbors('ownership', 'from_id', 'Auckland', 'out');

-- Breadth-first search up to a max depth.
SELECT vertex, depth FROM graph_bfs('ownership', 'from_id', 'Auckland', 5) ORDER BY depth;

-- Shortest path between two vertices.
SELECT step, vertex FROM graph_shortest_path('ownership', 'from_id', 'Auckland', 'Tokyo', 'both');

-- GQL-style MATCH statement (Plexus's GQL-subset layer).
SELECT b FROM MATCH (a)-[:oceania]->(b) ON ownership(from_id) WHERE a = 'Auckland' RETURN b;

-- ===========================================================================
-- 6. CANOPY — document / JSON
-- ===========================================================================
-- Path index for deep JSON lookups, plus full-text over the JSON body.
CREATE INDEX IF NOT EXISTS ON places USING JSONPATH (profile) WITH (path = 'owner');

-- JSON operators: -> (extract), ->> (extract as text), @> (contains).
SELECT name, profile->>'tier' AS tier, profile->'owner' AS owner_obj
FROM places
WHERE profile @> '{"tier":"alpha"}';

-- Path-index accelerated lookup of rows by a nested JSON value.
SELECT name FROM json_path_lookup('places', 'profile', 'ada');

-- ===========================================================================
-- 7. FLUX — event streaming
-- ===========================================================================
-- Declare a stream topic. Normal table writes also produce CDC events on
-- `__cdc_<table>` automatically.
CREATE TOPIC events WITH (partitions = '3');

-- Any INSERT/UPDATE/DELETE on a table flows through Flux CDC; windowing
-- functions let you aggregate the live stream. Here we window the CDC stream
-- of `places` into 1-hour tumbling windows (epoch ms width).
SELECT window_start, window_end, count
FROM flux_window_tumbling('__cdc_places', 3600000);

-- Time-travel: replay the state of a table as of a past timestamp.
SELECT row_key FROM flux_time_travel('places', 2500);
