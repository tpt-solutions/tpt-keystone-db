import assert from "node:assert/strict";
import { test } from "node:test";

import { document, geospatial, graph, relational, timeseries, vector } from "./models.js";

test("relational.select builds a parameterized SELECT", () => {
  const q = relational.select("robots", { columns: ["id", "status"], where: "status = $1", params: ["active"], limit: 10 });
  assert.equal(q.sql, 'SELECT "id", "status" FROM "robots" WHERE status = $1 LIMIT 10');
  assert.deepEqual(q.params, ["active"]);
});

test("geospatial.withinDistance uses ST_DWithin/ST_MakePoint", () => {
  const q = geospatial.withinDistance("robots", "location", -122.4194, 37.7749, 500);
  assert.match(q.sql, /ST_DWithin\("location", ST_MakePoint\(-122.4194, 37.7749\), 500\)/);
});

test("timeseries.bucket groups by time_bucket", () => {
  const q = timeseries.bucket("metrics", "ts", "1 hour", { valueColumns: ["value"] });
  assert.match(q.sql, /time_bucket\('1 hour', "ts"\) AS bucket, "value" FROM "metrics"/);
  assert.match(q.sql, /GROUP BY bucket ORDER BY bucket/);
});

test("graph.neighbors calls the graph_neighbors table function", () => {
  const q = graph.neighbors("follows", "from_id", "alice");
  assert.equal(q.sql, "SELECT neighbor, rel_type FROM graph_neighbors($1, $2, $3, $4)");
  assert.deepEqual(q.params, ["follows", "from_id", "alice", "out"]);
});

test("document.setPath builds a jsonb_set UPDATE", () => {
  const q = document.setPath("items", "attrs", "id", 42, ["a", "b"], 2);
  assert.equal(q.sql, 'UPDATE "items" SET "attrs" = jsonb_set("attrs", $1, $2) WHERE "id" = $3');
  assert.deepEqual(q.params, ["{a,b}", "2", 42]);
});

test("vector.orderBySimilarity takes a caller-supplied distance expression", () => {
  const q = vector.orderBySimilarity("docs", "embedding <-> $1", { topK: 5, params: [[0.1, 0.2]] });
  assert.equal(q.sql, 'SELECT * FROM "docs" ORDER BY embedding <-> $1 LIMIT 5');
});
