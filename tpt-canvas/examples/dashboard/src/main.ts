// TPT Canvas dashboard demo.
//
// Mounts all six `Canvas.*` components against a live tpt-keystone node's
// Canvas HTTP (default 5435) + Flux WebSocket (default 5434) endpoints. This
// is the crate's first-ever browser verification: every component runs real
// SQL through `use_keystone_query` rather than mock data.
//
// Point at a different node with env vars at dev-server start:
//   TPT_HTTP_ADDR_BASE=http://localhost:5435 TPT_FLUX_WS_ADDR=ws://localhost:5434

import init, {
  CanvasTimeSeries,
  CanvasMap,
  CanvasGraph,
  CanvasVectorSearch,
  CanvasDocument,
  CanvasAgentMonitor,
} from "../pkg/tpt_canvas";

const HTTP_BASE = (import.meta.env.TPT_HTTP_ADDR_BASE as string) || "http://localhost:5435";
const WS_BASE = (import.meta.env.TPT_FLUX_WS_ADDR as string) || "ws://localhost:5434";

function setStatus(msg: string) {
  const el = document.getElementById("status");
  if (el) el.textContent = msg;
}

function mount() {
  // 1. Time series — Chronos rollups published onto an ordinary table.
  new CanvasTimeSeries(
    "ts",
    HTTP_BASE,
    WS_BASE,
    "SELECT bucket, avg_latency_ms FROM request_latency ORDER BY bucket",
    "bucket",
    "avg_latency_ms",
    "",
  );

  // 2. Map — a `lat,lon` text column, rendered as a kernel-density heatmap
  //    plus clustered point markers.
  new CanvasMap(
    "map",
    HTTP_BASE,
    WS_BASE,
    "SELECT id, location FROM sensors WHERE location IS NOT NULL",
    "location",
    "",
    true, // cluster
    true, // heatmap
    (rowJson: string) => console.log("map click:", rowJson),
  );

  // 3. Graph — Plexus vertices + edges as two queries.
  new CanvasGraph(
    "graph",
    HTTP_BASE,
    WS_BASE,
    "SELECT id FROM nodes",
    "SELECT from_id, to_id FROM edges",
    "",
  );

  // 4. Vector search — Prism ANN results ranked by distance.
  new CanvasVectorSearch(
    "vec",
    HTTP_BASE,
    WS_BASE,
    "SELECT label, distance FROM vector_search('...') ORDER BY distance LIMIT 20",
    "distance",
    "",
  );

  // 5. Document — Canopy JSON tree with inline editing (jsonb_set).
  new CanvasDocument(
    "doc",
    HTTP_BASE,
    WS_BASE,
    "SELECT id, doc FROM documents",
    "documents",
    "doc",
    "id",
    "",
  );

  // 6. Agent monitor — Mirror session events + per-agent latency bars.
  new CanvasAgentMonitor(
    "agent",
    "agent",
    HTTP_BASE,
    WS_BASE,
    "SELECT * FROM mirror_session_events('ses_1') ORDER BY evt_offset",
    "SELECT * FROM mirror_agent_metrics('agent_1', 0, 9999999999)",
    "",
  );

  setStatus(`connected to ${HTTP_BASE}`);
}

init()
  .then(mount)
  .catch((e) => setStatus(`wasm init failed: ${e}`));
