import assert from "node:assert/strict";
import { test } from "node:test";

import { EdgeKeystoneClient } from "./client.js";
import { cachedQuery, invalidateCachedQuery } from "./cache.js";

class InMemoryCache {
  private store = new Map<string, Response>();

  async match(request: Request): Promise<Response | undefined> {
    const hit = this.store.get(request.url);
    return hit ? hit.clone() : undefined;
  }

  async put(request: Request, response: Response): Promise<void> {
    this.store.set(request.url, response.clone());
  }

  async delete(request: Request): Promise<boolean> {
    return this.store.delete(request.url);
  }
}

test("cachedQuery hits Keystone once and serves subsequent calls from cache", async () => {
  let fetchCount = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async () => {
    fetchCount += 1;
    return new Response(JSON.stringify({ columns: ["id"], rows: [["1"]] }), { status: 200 });
  }) as typeof fetch;

  try {
    const client = new EdgeKeystoneClient({ url: "https://db.example.com:5435" });
    const cache = new InMemoryCache() as unknown as Cache;

    const first = await cachedQuery(client, "SELECT id FROM users", [], { ttlSeconds: 60, cache });
    const second = await cachedQuery(client, "SELECT id FROM users", [], { ttlSeconds: 60, cache });

    assert.equal(fetchCount, 1);
    assert.deepEqual(first.rows, second.rows);

    await invalidateCachedQuery(client, "SELECT id FROM users", [], cache);
    await cachedQuery(client, "SELECT id FROM users", [], { ttlSeconds: 60, cache });
    assert.equal(fetchCount, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
