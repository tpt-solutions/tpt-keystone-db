// Zero-dependency mock of tpt-keystone's Canvas HTTP bridge
// (`POST /query`, `GET /schema`) so the dashboard demo runs without a live
// database. Uses only Node built-ins (http + fs) — no npm install needed.
//
// Usage:
//   node mock-server.mjs            # serves :5435 with seeded data
//   TPT_MOCK_PORT=5435 node mock-server.mjs
//
// The demo points at it via:
//   TPT_HTTP_ADDR_BASE=http://localhost:5435 npm run dev

import { createServer } from "node:http";

const PORT = Number(process.env.TPT_MOCK_PORT || 5435);

// --- seeded data -----------------------------------------------------------
// Each entry is keyed by a substring the demo's SQL is expected to contain.
// We match case-insensitively; the first matching rule wins.
const TABLES = {
  request_latency: {
    columns: ["bucket", "avg_latency_ms"],
    rows: [
      ["10:00", "12.4"],
      ["10:01", "18.1"],
      ["10:02", "9.7"],
      ["10:03", "22.3"],
      ["10:04", "15.0"],
      ["10:05", "11.2"],
    ],
  },
  sensors: {
    columns: ["id", "location"],
    rows: [
      ["s1", "37.77,-122.41"],
      ["s2", "34.05,-118.24"],
      ["s3", "40.71,-74.00"],
      ["s4", "51.50,-0.12"],
      ["s5", "35.68,139.69"],
      ["s6", "48.85,2.35"],
      ["s7", "-33.86,151.20"],
    ],
  },
  nodes: {
    columns: ["id"],
    rows: [["a"], ["b"], ["c"], ["d"], ["e"]],
  },
  edges: {
    columns: ["from_id", "to_id"],
    rows: [["a", "b"], ["b", "c"], ["c", "d"], ["d", "e"], ["e", "a"]],
  },
  vector_search: {
    columns: ["label", "distance"],
    rows: [
      ["cat", "0.12"],
      ["dog", "0.21"],
      ["fox", "0.34"],
      ["owl", "0.41"],
      ["elm", "0.58"],
    ],
  },
  documents: {
    columns: ["id", "doc"],
    rows: [
      ["1", JSON.stringify({ name: "alpha", tags: ["x", "y"], score: 7 })],
      ["2", JSON.stringify({ name: "beta", tags: ["z"], score: 3 })],
    ],
  },
};

const MIRROR_EVENTS = {
  columns: ["evt_offset", "ts", "agent_id", "kind", "detail", "tool_name", "error"],
  rows: [
    ["0", "1000", "agent_1", "step", "planning", "think", ""],
    ["1", "1012", "agent_1", "tool", "search docs", "search", ""],
    ["2", "1030", "agent_1", "step", "draft", "think", ""],
    ["3", "1055", "agent_1", "error", "rate limited", "", "timeout"],
  ],
};

const MIRROR_METRICS = {
  columns: ["agent_id", "latency_ms"],
  rows: [["agent_1", "45.2"], ["agent_2", "12.8"], ["agent_3", "88.1"]],
};

function matchTable(sql) {
  const s = sql.toLowerCase();
  if (s.includes("request_latency")) return TABLES.request_latency;
  if (s.includes("sensors")) return TABLES.sensors;
  if (s.includes("nodes")) return TABLES.nodes;
  if (s.includes("edges")) return TABLES.edges;
  if (s.includes("vector_search")) return TABLES.vector_search;
  if (s.includes("documents")) return TABLES.documents;
  if (s.includes("mirror_session_events")) return MIRROR_EVENTS;
  if (s.includes("mirror_agent_metrics")) return MIRROR_METRICS;
  return { columns: ["note"], rows: [["no mock data for this query"]] };
}

const SCHEMA = {
  tables: [
    { name: "request_latency", columns: [{ name: "bucket", type: "text" }, { name: "avg_latency_ms", type: "float8" }] },
    { name: "sensors", columns: [{ name: "id", type: "text" }, { name: "location", type: "text" }] },
    { name: "nodes", columns: [{ name: "id", type: "text" }] },
    { name: "edges", columns: [{ name: "from_id", type: "text" }, { name: "to_id", type: "text" }] },
    { name: "vector_search", columns: [{ name: "label", type: "text" }, { name: "distance", type: "float8" }] },
    { name: "documents", columns: [{ name: "id", type: "text" }, { name: "doc", type: "jsonb" }] },
  ],
};

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
  });
  res.end(body);
}

const server = createServer((req, res) => {
  if (req.method === "OPTIONS") return sendJson(res, 204, null);
  if (req.method === "GET" && req.url === "/schema") return sendJson(res, 200, SCHEMA);

  if (req.method === "POST" && req.url === "/query") {
    let raw = "";
    req.on("data", (c) => (raw += c));
    req.on("end", () => {
      try {
        const { sql } = JSON.parse(raw || "{}");
        const table = matchTable(sql || "");
        sendJson(res, 200, { columns: table.columns, rows: table.rows });
      } catch (e) {
        sendJson(res, 400, { error: String(e) });
      }
    });
    return;
  }

  sendJson(res, 404, { error: "not found" });
});

server.listen(PORT, () => {
  console.log(`tpt-canvas mock Canvas HTTP bridge on http://localhost:${PORT}`);
  console.log(`  POST /query  -> seeded rows for the demo's SQL`);
  console.log(`  GET  /schema -> table introspection`);
});
