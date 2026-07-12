import assert from "node:assert/strict";
import { test } from "node:test";

import {
  InMemoryStorage,
  KeystoneClient,
  type Storage,
} from "./client.js";
import { QueryStore, type QueryState } from "./store.js";

function jsonResponse(result: unknown): Response {
  return new Response(JSON.stringify(result), { status: 200 });
}

test("query hits Canvas and writes the result to the cache", async () => {
  let fetchCount = 0;
  const storage = new InMemoryStorage();
  const client = new KeystoneClient({
    url: "https://db.example.com:5435",
    storage,
    fetchImpl: (async () => {
      fetchCount += 1;
      return jsonResponse({ columns: ["id"], rows: [["1"]] });
    }) as typeof fetch,
  });

  const first = await client.query("SELECT id FROM users", []);
  const second = await client.query("SELECT id FROM users", []);

  assert.equal(fetchCount, 2);
  assert.deepEqual(first.rows, [["1"]]);

  const raw = await storage.getItem(
    `tpt:${JSON.stringify({ q: "SELECT id FROM users", p: [] })}`,
  );
  assert.ok(raw, "cache entry written");
  assert.deepEqual(JSON.parse(raw as string).result, first);
  assert.ok(second !== first); // second is a fresh network fetch
});

test("cache serves when fetch throws (offline fallback)", async () => {
  let fetchCount = 0;
  const client = new KeystoneClient({
    url: "https://db.example.com:5435",
    cacheTtlSeconds: 600,
    fetchImpl: (async () => {
      fetchCount += 1;
      if (fetchCount === 1) {
        return jsonResponse({ columns: ["id"], rows: [["7"]] });
      }
      throw new Error("network down");
    }) as typeof fetch,
  });

  const online = await client.query("SELECT id FROM users", []);
  assert.deepEqual(online.rows, [["7"]]);

  const offline = await client.query("SELECT id FROM users", []);
  assert.deepEqual(offline.rows, [["7"]]);
  assert.equal(fetchCount, 2);
});

test("offline flag uses cache and never calls fetch", async () => {
  let fetchCount = 0;
  const client = new KeystoneClient({
    url: "https://db.example.com:5435",
    cacheTtlSeconds: 600,
    fetchImpl: (async () => {
      fetchCount += 1;
      return jsonResponse({ columns: ["id"], rows: [["9"]] });
    }) as typeof fetch,
  });

  await client.query("SELECT id FROM t", []);
  const offline = await client.query("SELECT id FROM t", [], { offline: true });
  assert.deepEqual(offline.rows, [["9"]]);
  assert.equal(fetchCount, 1);
});

test("offline with no cache throws a clear error", async () => {
  const client = new KeystoneClient({
    url: "https://db.example.com:5435",
    fetchImpl: (async () => {
      throw new Error("network down");
    }) as typeof fetch,
  });

  await assert.rejects(
    client.query("SELECT 1", [], { offline: true }),
    /offline and no fresh cache/,
  );
});

test("stale cache (>ttl) is treated as a miss and forces a network fetch", async () => {
  let fetchCount = 0;
  const storage: Storage = {
    async getItem() {
      return JSON.stringify({
        result: { columns: ["id"], rows: [["old"]] },
        ts: Date.now() - 10_000,
      });
    },
    async setItem() {},
    async removeItem() {},
  };
  const client = new KeystoneClient({
    url: "https://db.example.com:5435",
    storage,
    cacheTtlSeconds: 1,
    fetchImpl: (async () => {
      fetchCount += 1;
      return jsonResponse({ columns: ["id"], rows: [["fresh"]] });
    }) as typeof fetch,
  });

  const result = await client.query("SELECT id FROM t", []);
  assert.deepEqual(result.rows, [["fresh"]]);
  assert.equal(fetchCount, 1);
});

test("invalidate drops the cached entry", async () => {
  let fetched = 0;
  const client = new KeystoneClient({
    url: "https://db.example.com:5435",
    cacheTtlSeconds: 600,
    fetchImpl: (async () => {
      fetched += 1;
      return jsonResponse({ columns: ["id"], rows: [[String(fetched)]] });
    }) as typeof fetch,
  });

  await client.query("SELECT id FROM t", []);
  await client.invalidate("SELECT id FROM t", []);
  await client.query("SELECT id FROM t", []); // refetch
  assert.equal(fetched, 2);
});

test("non-ok Canvas response throws with status", async () => {
  const client = new KeystoneClient({
    url: "https://db.example.com:5435",
    fetchImpl: (async () =>
      new Response("boom", { status: 500 })) as typeof fetch,
  });

  await assert.rejects(client.query("SELECT 1", []), /responded 500/);
});

test("QueryStore notifies subscribers on setState", () => {
  const store = new QueryStore<{ v: number }>();
  const seen: QueryState<{ v: number }>[] = [];
  const unsubscribe = store.subscribe((s) => seen.push(s));
  store.setState({ data: { v: 1 }, loading: false });
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0].data, { v: 1 });
  unsubscribe();
  store.setState({ data: { v: 2 } });
  assert.equal(seen.length, 1); // no notification after unsubscribe
});

test("subscribeFlux throws without fluxUrl", () => {
  const client = new KeystoneClient({ url: "https://db.example.com:5435" });
  assert.throws(
    () => client.subscribeFlux("topic", () => {}),
    /no fluxUrl configured/,
  );
});
