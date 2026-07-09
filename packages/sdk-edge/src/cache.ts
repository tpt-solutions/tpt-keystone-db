// Edge caching for read-only queries, built on the standard Web `Cache` API
// (`caches.open(name)` / Cloudflare Workers' `caches.default`) that
// Cloudflare Workers, Fastly Compute, and Vercel Edge all expose as an
// ambient global — no runtime-specific KV/cache binding required. `/query`
// is a `POST`, which the Cache API won't store under its own method/URL, so
// this synthesizes a stable `GET` request keyed by `sql`+`params` as the
// cache key and stores the raw JSON response body under it with a
// `Cache-Control: max-age=<ttlSeconds>` header the Cache API itself honors
// for expiry — no separate TTL bookkeeping needed.

import type { EdgeKeystoneClient, QueryResult } from "./client.js";

export interface CachedQueryOptions {
  /** How long a cached result stays fresh, in seconds. */
  ttlSeconds: number;
  /** Cache instance to use. Defaults to `caches.default` (Cloudflare) or `caches.open("tpt-edge")`. */
  cache?: Cache;
}

function cacheKey(baseUrl: string, sql: string, params: unknown[]): Request {
  const key = new URL(`${baseUrl}/query`);
  key.searchParams.set("sql", sql);
  key.searchParams.set("params", JSON.stringify(params));
  return new Request(key.toString(), { method: "GET" });
}

async function resolveCache(explicit?: Cache): Promise<Cache> {
  if (explicit) return explicit;
  const globalCaches = (globalThis as { caches?: CacheStorage & { default?: Cache } }).caches;
  if (!globalCaches) {
    throw new Error("cachedQuery: no global `caches` in this runtime — edge caching is unavailable here");
  }
  return globalCaches.default ?? (await globalCaches.open("tpt-edge"));
}

/**
 * Runs `client.query(sql, params)` through the edge Cache API, keyed by
 * `sql`+`params`. On a cache hit within `ttlSeconds`, returns the cached
 * result without contacting Keystone at all.
 */
export async function cachedQuery<T = Record<string, string | null>>(
  client: EdgeKeystoneClient,
  sql: string,
  params: unknown[] = [],
  options: CachedQueryOptions,
): Promise<QueryResult<T>> {
  const cache = await resolveCache(options.cache);
  const request = cacheKey(client.url, sql, params);

  const hit = await cache.match(request);
  if (hit) {
    return (await hit.json()) as QueryResult<T>;
  }

  const result = await client.query<T>(sql, params);
  const response = new Response(JSON.stringify(result), {
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": `max-age=${options.ttlSeconds}`,
    },
  });
  await cache.put(request, response);
  return result;
}

/** Evicts a previously cached `cachedQuery` entry for `sql`+`params`. */
export async function invalidateCachedQuery(
  client: EdgeKeystoneClient,
  sql: string,
  params: unknown[] = [],
  cacheOverride?: Cache,
): Promise<void> {
  const cache = await resolveCache(cacheOverride);
  await cache.delete(cacheKey(client.url, sql, params));
}
