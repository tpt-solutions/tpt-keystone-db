// React Native / Expo client for TPT Keystone's Canvas HTTP/JSON query bridge
// (`wire::http_query.rs`) and Flux WebSocket stream (`wire::websocket.rs`,
// Phase 11). Mirrors `@tpt/sdk-edge` but adds mobile-specific ergonomics:
//
//  - Offline-first: a pluggable `Storage` adapter (AsyncStorage-shaped by
//    default — `getItem`/`setItem`/`removeItem` of string values) caches the
//    last good result per (query, params) so the app renders something on a
//    flaky radio even when `fetch` throws. Online queries refresh the cache.
//  - Flux push: `subscribeFlux` rides a dedicated WebSocket to `fluxUrl`,
//    subscribing to a topic and invoking `onRecord` per pushed record. If the
//    runtime has no global `WebSocket` (rare on RN but possible in a bare
//    Node test), it throws synchronously rather than hanging.
//
// The Canvas bridge is explicitly non-streaming (one JSON response per
// request); Flux is the streaming surface. The client never imports
// `react-native` directly so it stays testable under `node --test` with only
// `fetch`/`WebSocket` globals stubbed.

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
}

export interface KeystoneClientConfig {
  /** Canvas HTTP/JSON bridge base URL, e.g. `https://db.example.com:5435`. */
  url: string;
  /** Flux WebSocket URL, e.g. `wss://db.example.com:5434`. Optional. */
  fluxUrl?: string;
  /** Offline cache. Defaults to an in-memory store (no persistence). */
  storage?: Storage;
  /**
   * Per-query cache TTL in seconds. Results older than this are treated as a
   * cache miss and force a network fetch when online. Default 300s (5 min).
   */
  cacheTtlSeconds?: number;
  /** Extra headers merged into every Canvas request (auth, tracing, ...). */
  headers?: Record<string, string>;
  fetchImpl?: typeof fetch;
}

/** AsyncStorage-shaped contract: all values are UTF-8 strings. */
export interface Storage {
  getItem(key: string): Promise<string | null>;
  setItem(key: string, value: string): Promise<void>;
  removeItem(key: string): Promise<void>;
}

/** In-memory Storage used when no adapter is supplied. Not persisted. */
export class InMemoryStorage implements Storage {
  private store = new Map<string, string>();

  async getItem(key: string): Promise<string | null> {
    return this.store.has(key) ? (this.store.get(key) as string) : null;
  }

  async setItem(key: string, value: string): Promise<void> {
    this.store.set(key, value);
  }

  async removeItem(key: string): Promise<void> {
    this.store.delete(key);
  }
}

interface CacheEntry {
  result: QueryResult;
  /** Epoch ms when the entry was written. */
  ts: number;
}

function cacheKey(query: string, params: unknown[]): string {
  return `tpt:${JSON.stringify({ q: query, p: params })}`;
}

export class KeystoneClient {
  readonly url: string;
  readonly fluxUrl?: string;
  private readonly storage: Storage;
  private readonly cacheTtlMs: number;
  private readonly headers: Record<string, string>;
  private readonly fetchImpl: typeof fetch;

  constructor(config: KeystoneClientConfig) {
    this.url = config.url;
    this.fluxUrl = config.fluxUrl;
    this.storage = config.storage ?? new InMemoryStorage();
    this.cacheTtlMs = (config.cacheTtlSeconds ?? 300) * 1000;
    this.headers = config.headers ?? {};
    this.fetchImpl = config.fetchImpl ?? globalThis.fetch;
  }

  /**
   * Runs `query` against the Canvas bridge. When offline (or the fetch
   * throws), falls back to the last cached result if fresh enough; otherwise
   * rethrows. On success, the result is written to the cache.
   *
   * `offline` lets callers force the offline path (e.g. React Native
   * `NetInfo` reported no connectivity) without waiting for a failed fetch.
   */
  async query(
    query: string,
    params: unknown[] = [],
    opts: { offline?: boolean } = {},
  ): Promise<QueryResult> {
    const key = cacheKey(query, params);

    if (!opts.offline) {
      try {
        const result = await this.fetchQuery(query, params);
        await this.writeCache(key, result);
        return result;
      } catch (error) {
        const cached = await this.readCache(key);
        if (cached) return cached;
        throw error;
      }
    }

    const cached = await this.readCache(key);
    if (cached) return cached;
    throw new Error(
      `KeystoneClient.query: offline and no fresh cache for query "${query}"`,
    );
  }

  /** Forces a network round-trip, bypassing and refreshing the cache. */
  async refresh(query: string, params: unknown[] = []): Promise<QueryResult> {
    const key = cacheKey(query, params);
    const result = await this.fetchQuery(query, params);
    await this.writeCache(key, result);
    return result;
  }

  /** Drops any cached result for a (query, params) pair. */
  async invalidate(query: string, params: unknown[] = []): Promise<void> {
    await this.storage.removeItem(cacheKey(query, params));
  }

  private async fetchQuery(
    query: string,
    params: unknown[],
  ): Promise<QueryResult> {
    const res = await this.fetchImpl(this.url, {
      method: "POST",
      headers: { "content-type": "application/json", ...this.headers },
      body: JSON.stringify({ query, params }),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(
        `KeystoneClient.query: Canvas responded ${res.status}: ${text}`,
      );
    }
    return (await res.json()) as QueryResult;
  }

  private async writeCache(key: string, result: QueryResult): Promise<void> {
    const entry: CacheEntry = { result, ts: Date.now() };
    await this.storage.setItem(key, JSON.stringify(entry));
  }

  private async readCache(key: string): Promise<QueryResult | null> {
    const raw = await this.storage.getItem(key);
    if (!raw) return null;
    try {
      const entry = JSON.parse(raw) as CacheEntry;
      if (Date.now() - entry.ts > this.cacheTtlMs) return null;
      return entry.result;
    } catch {
      return null;
    }
  }

  /**
   * Subscribes to a Flux topic. Returns an unsubscribe function that closes
   * the socket. Throws synchronously if no global `WebSocket` exists.
   */
  subscribeFlux(
    topic: string,
    onRecord: (record: FluxRecord) => void,
    onError?: (error: unknown) => void,
  ): () => void {
    if (!this.fluxUrl) {
      throw new Error("KeystoneClient.subscribeFlux: no fluxUrl configured");
    }
    if (typeof WebSocket === "undefined") {
      throw new Error(
        "KeystoneClient.subscribeFlux: no global WebSocket in this runtime",
      );
    }

    const ws = new WebSocket(this.fluxUrl);
    ws.addEventListener("open", () => {
      ws.send(JSON.stringify({ subscribe: topic }));
    });
    ws.addEventListener("message", (event: MessageEvent) => {
      try {
        onRecord(JSON.parse(String(event.data)) as FluxRecord);
      } catch (error) {
        onError?.(error);
      }
    });
    if (onError) {
      ws.addEventListener("error", (event) => onError(event));
    }
    return () => ws.close();
  }
}

export interface FluxRecord {
  offset: number;
  key: string | null;
  value: string | null;
  ts: number;
}
