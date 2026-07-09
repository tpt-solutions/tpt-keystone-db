// Client for `wire::http_query` (tpt-keystone/src/wire/http_query.rs), the
// same Canvas HTTP/JSON bridge `@tpt/sdk-web` talks to. Reused here rather
// than the raw Postgres wire protocol (`@tpt/sdk-server`) because most edge
// runtimes this package targets (Fastly Compute, Vercel Edge, Lambda@Edge)
// cannot open an arbitrary outbound TCP socket, only `fetch`/`WebSocket` —
// the same constraint that shaped `@tpt/sdk-web` for browsers. Cloudflare
// Workers is the one exception (it exposes `connect()` for raw TCP), but
// staying on the HTTP/JSON bridge keeps one code path working everywhere
// this package ships, instead of a Workers-only fast path plus a fallback.
//
// Zero runtime dependencies and no Node built-ins (no `node:*` imports) —
// only `fetch`/`URL`/`WebSocket`/`Cache`, which every target runtime
// provides as ambient globals. Same scope cut carried over from the server
// bridge: `/query` always decodes every cell as UTF-8 text (or `null`);
// `schema()` is how a caller learns the real column type, and `queryTyped()`
// uses it to coerce into numbers/booleans/JSON.

export interface EdgeClientOptions {
  /** Base URL of the Canvas HTTP/JSON query bridge, e.g. "https://db.example.com:5435". */
  url: string;
  /**
   * WebSocket URL of the Flux real-time bridge (`wire::websocket`), e.g.
   * "wss://db.example.com:5434". Defaults to `url` with the scheme swapped
   * to ws(s) and the port swapped to 5434 (`TPT_FLUX_WS_ADDR`'s default).
   */
  fluxUrl?: string;
  /** Accepted for API-surface parity with the other TPT SDKs; currently a no-op (no auth layer yet). */
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

export class EdgeKeystoneClient {
  readonly url: string;
  readonly fluxUrl: string;
  private schemaCache: Promise<SchemaInfo> | null = null;

  constructor(options: EdgeClientOptions) {
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
}

export function createEdgeClient(options: EdgeClientOptions): EdgeKeystoneClient {
  return new EdgeKeystoneClient(options);
}
