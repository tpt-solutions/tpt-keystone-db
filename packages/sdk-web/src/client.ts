// Client for `wire::http_query` (tpt-keystone/src/wire/http_query.rs), the
// hand-rolled HTTP/JSON bridge Canvas/Phase 13 added so a browser — which
// can't open a raw TCP socket to speak the Postgres wire protocol — has a
// way to run SQL against a live `Database`. Two routes on that bridge:
// `POST /query` (body `{"sql", "params"}` -> `{"columns", "rows"}`) and
// `GET /schema` (table/column introspection, also consumed by
// `tpt-canvas`'s `tsgen` binary and this package's `bin/typegen.ts`).
//
// Scope cut carried over honestly from the server side: `/query` always
// decodes every cell as UTF-8 text (or `null`), regardless of the real
// column type — `schema()` is how a caller learns the real type. `query()`
// below returns those raw string/null values; `queryTyped()` additionally
// coerces using a cached `schema()` lookup for callers who want numbers/
// booleans back instead of strings.

import { subscribeFlux, type FluxRecord } from "./flux.js";

export interface KeystoneClientOptions {
  /** Base URL of the Canvas HTTP/JSON query bridge, e.g. "http://localhost:5435". */
  url: string;
  /**
   * WebSocket URL of the Flux real-time bridge (`wire::websocket`), e.g.
   * "ws://localhost:5434". Defaults to `url` with the scheme swapped to
   * ws(s) and the port swapped to 5434 (`TPT_FLUX_WS_ADDR`'s default) —
   * override if either service runs on a non-default port.
   */
  fluxUrl?: string;
  /**
   * Accepted for API-surface parity with the other TPT SDKs. Keystone has
   * no auth layer anywhere yet (see the wire startup handshake, which
   * auto-approves), so this is currently a no-op.
   */
  apiKey?: string;
}

export interface ColumnSchema {
  name: string;
  type: string;
}

export interface TableSchema {
  name: string;
  columns: ColumnSchema[];
}

export interface SchemaInfo {
  tables: TableSchema[];
}

export interface QueryResult<T = Record<string, string | null>> {
  columns: string[];
  rows: T[];
  raw: { columns: string[]; rows: (string | null)[][] };
}

function deriveFluxUrl(url: string): string {
  const parsed = new URL(url);
  parsed.protocol = parsed.protocol === "https:" ? "wss:" : "ws:";
  parsed.port = "5434";
  return parsed.toString();
}

function zipRow(columns: string[], cells: (string | null)[]): Record<string, string | null> {
  const row: Record<string, string | null> = {};
  columns.forEach((col, i) => {
    row[col] = cells[i] ?? null;
  });
  return row;
}

function coerce(value: string | null, type: string): unknown {
  if (value === null) return null;
  switch (type) {
    case "int2":
    case "int4":
    case "int8":
      return Number.parseInt(value, 10);
    case "float4":
    case "float8":
      return Number.parseFloat(value);
    case "bool":
      return value === "t" || value === "true";
    case "json":
      try {
        return JSON.parse(value);
      } catch {
        return value;
      }
    default:
      return value;
  }
}

export class KeystoneClient {
  readonly url: string;
  readonly fluxUrl: string;
  private schemaCache: Promise<SchemaInfo> | null = null;

  constructor(options: KeystoneClientOptions) {
    this.url = options.url.replace(/\/$/, "");
    this.fluxUrl = options.fluxUrl ?? deriveFluxUrl(this.url);
  }

  /** Runs `sql` against `POST /query`, returning rows with raw string/null cells. */
  async query<T = Record<string, string | null>>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    const res = await fetch(`${this.url}/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql, params }),
    });
    const body = (await res.json()) as { columns?: string[]; rows?: (string | null)[][]; error?: string };
    if (!res.ok || body.error) {
      throw new Error(body.error ?? `query failed with status ${res.status}`);
    }
    const columns = body.columns ?? [];
    const rawRows = body.rows ?? [];
    return {
      columns,
      rows: rawRows.map((cells) => zipRow(columns, cells) as unknown as T),
      raw: { columns, rows: rawRows },
    };
  }

  /** Same as `query`, but coerces cells to number/boolean/JSON using a cached `schema()` lookup for `table`. */
  async queryTyped<T = Record<string, unknown>>(table: string, sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    const [result, schema] = await Promise.all([this.query(sql, params), this.schema()]);
    const columnTypes = new Map(schema.tables.find((t) => t.name === table)?.columns.map((c) => [c.name, c.type]) ?? []);
    const rows = result.raw.rows.map((cells) => {
      const row: Record<string, unknown> = {};
      result.columns.forEach((col, i) => {
        const type = columnTypes.get(col);
        row[col] = type ? coerce(cells[i] ?? null, type) : cells[i] ?? null;
      });
      return row as T;
    });
    return { columns: result.columns, rows, raw: result.raw };
  }

  /** Introspects `GET /schema`; cached for the lifetime of the client. */
  schema(): Promise<SchemaInfo> {
    if (!this.schemaCache) {
      this.schemaCache = fetch(`${this.url}/schema`).then((res) => res.json() as Promise<SchemaInfo>);
    }
    return this.schemaCache;
  }

  /** Drops the cached `schema()` result, e.g. after a DDL statement changes the catalog. */
  invalidateSchema(): void {
    this.schemaCache = null;
  }

  /**
   * Subscribes to a Flux topic over `wire::websocket`. One topic per
   * connection (matching the server protocol); returns an unsubscribe
   * function that closes the socket.
   */
  subscribe(topic: string, onRecord: (record: FluxRecord) => void, onError?: (error: unknown) => void): () => void {
    return subscribeFlux(this.fluxUrl, topic, onRecord, onError);
  }
}

export function createKeystoneClient(options: KeystoneClientOptions): KeystoneClient {
  return new KeystoneClient(options);
}
