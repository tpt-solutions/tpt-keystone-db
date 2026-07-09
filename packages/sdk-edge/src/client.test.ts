import assert from "node:assert/strict";
import { test } from "node:test";

import { EdgeKeystoneClient } from "./client.js";

function mockFetch(responses: Record<string, unknown>): typeof fetch {
  return (async (input: string | URL | Request) => {
    const url = typeof input === "string" ? input : input.toString();
    const path = new URL(url).pathname;
    const body = responses[path];
    return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
  }) as typeof fetch;
}

test("query() zips columns/rows into records", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = mockFetch({
    "/query": { columns: ["id", "name"], rows: [["1", "alice"], ["2", null]] },
  });
  try {
    const client = new EdgeKeystoneClient({ url: "https://db.example.com:5435" });
    const result = await client.query("SELECT id, name FROM users");
    assert.deepEqual(result.rows, [{ id: "1", name: "alice" }, { id: "2", name: null }]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("queryTyped() coerces cells using schema()", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = mockFetch({
    "/query": { columns: ["id", "active"], rows: [["1", "t"]] },
    "/schema": { tables: [{ name: "users", columns: [{ name: "id", type: "int4" }, { name: "active", type: "bool" }] }] },
  });
  try {
    const client = new EdgeKeystoneClient({ url: "https://db.example.com:5435" });
    const result = await client.queryTyped<{ id: number; active: boolean }>("users", "SELECT id, active FROM users");
    assert.deepEqual(result.rows, [{ id: 1, active: true }]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("deriveFluxUrl swaps scheme and port when fluxUrl is not given", () => {
  const client = new EdgeKeystoneClient({ url: "https://db.example.com:5435" });
  assert.equal(client.fluxUrl, "wss://db.example.com:5434/");
});

test("query() throws on error response", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async () =>
    new Response(JSON.stringify({ error: "syntax error" }), { status: 400 })) as typeof fetch;
  try {
    const client = new EdgeKeystoneClient({ url: "https://db.example.com:5435" });
    await assert.rejects(() => client.query("BAD SQL"), /syntax error/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
