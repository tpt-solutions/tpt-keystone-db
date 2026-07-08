// SSR support: server-side helpers to fetch data for a page render, plus
// `schema()`-driven typed coercion symmetrical in spirit to `sdk-web`'s
// `queryTyped` (`packages/sdk-web/src/client.ts`) — but `sdk-web` gets its
// schema from the Canvas HTTP bridge's `GET /schema`, which doesn't exist at
// this layer (this SDK never goes through HTTP). Instead `schema()` here
// queries Keystone's own catalog directly over the wire protocol:
// `information_schema.tables` / `information_schema.columns`, the two
// virtual tables `tpt-keystone/src/executor/catalog.rs` materializes
// (`information_schema_tables`/`information_schema_columns`). Their
// `data_type` column uses long-form Postgres names ("bigint", "double
// precision", "boolean", ...) per `catalog.rs`'s `type_name()` — NOT the
// short `int8`/`float8`/`bool` names the HTTP bridge's `/schema` returns
// (`wire/http_query.rs`'s `column_type_name()`), because those two code
// paths in tpt-keystone independently name types differently. `coerce()`
// below matches the `information_schema` long-form names since that's the
// only introspection surface reachable from a raw wire connection.
//
// No framework-specific SSR adapter (no Next.js/Remix integration) — this
// is a plain awaitable query API meant to be called from a loader/handler
// during SSR, not a framework plugin. That's a deliberate scope cut, not an
// oversight: building an honest per-framework adapter was out of scope for
// the time available.

import type { KeystoneClient, Value } from "./client.js";

export interface ColumnSchema {
  name: string;
  dataType: string;
  ordinalPosition: number;
  nullable: boolean;
}

export interface TableSchema {
  name: string;
  columns: ColumnSchema[];
}

export interface SchemaInfo {
  tables: TableSchema[];
}

function coerce(text: string | null, dataType: string): Value {
  if (text === null) return null;
  switch (dataType) {
    case "bigint":
    case "integer":
    case "smallint":
      return Number.parseInt(text, 10);
    case "double precision":
    case "real":
      return Number.parseFloat(text);
    case "boolean":
      return text === "t" || text === "true";
    case "json":
      try {
        return JSON.parse(text);
      } catch {
        return text;
      }
    default:
      return text;
  }
}

/**
 * Introspects `information_schema.tables`/`information_schema.columns` over
 * the live wire connection. Not cached here (unlike `sdk-web`'s
 * per-instance cache) — callers doing SSR typically want a fresh read per
 * request or to cache it themselves at the call site.
 */
export async function schema(client: KeystoneClient): Promise<SchemaInfo> {
  const tablesResult = await client.query(
    `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`,
  );
  const columnsResult = await client.query(
    `SELECT table_name, column_name, ordinal_position, is_nullable, data_type FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position`,
  );

  const columnsByTable = new Map<string, ColumnSchema[]>();
  for (const row of columnsResult.rows) {
    const [tableName, columnName, ordinal, isNullable, dataType] = row.cells;
    if (tableName === null || columnName === null) continue;
    const list = columnsByTable.get(tableName) ?? [];
    list.push({
      name: columnName,
      dataType: dataType ?? "text",
      ordinalPosition: ordinal ? Number.parseInt(ordinal, 10) : list.length + 1,
      nullable: isNullable === "YES",
    });
    columnsByTable.set(tableName, list);
  }

  const tables: TableSchema[] = tablesResult.rows
    .map((r) => r.cells[0])
    .filter((name): name is string => name !== null)
    .map((name) => ({ name, columns: columnsByTable.get(name) ?? [] }));

  return { tables };
}

/**
 * Runs `sql` and coerces cells to number/boolean/JSON using `table`'s
 * column types from `schema()`, for callers (typically SSR loaders) who
 * want typed values instead of raw wire-text strings. `schemaInfo` can be
 * supplied to reuse a schema fetched once at request/app start; otherwise
 * it's fetched fresh for this call.
 */
export async function queryTyped<T = Record<string, Value>>(
  client: KeystoneClient,
  table: string,
  sql: string,
  params: unknown[] = [],
  schemaInfo?: SchemaInfo,
): Promise<T[]> {
  // Sequential, not `Promise.all`: both calls share one TCP connection, and
  // the wire protocol is strictly request/response (no pipelining/
  // multiplexing here) — running them concurrently interleaves the two
  // requests' bytes on the same socket and desyncs both responses.
  const info = schemaInfo ?? (await schema(client));
  const result = await client.queryParams(sql, params);
  const columnTypes = new Map(info.tables.find((t) => t.name === table)?.columns.map((c) => [c.name, c.dataType]));
  return result.rows.map((row) => {
    const out: Record<string, Value> = {};
    row.columns.forEach((col, i) => {
      const dataType = columnTypes.get(col);
      out[col] = dataType ? coerce(row.cells[i] ?? null, dataType) : (row.cells[i] as Value);
    });
    return out as T;
  });
}

/** Convenience wrapper: `queryTyped` for a query expected to return at most one row, or `undefined`. */
export async function queryOne<T = Record<string, Value>>(
  client: KeystoneClient,
  table: string,
  sql: string,
  params: unknown[] = [],
  schemaInfo?: SchemaInfo,
): Promise<T | undefined> {
  const rows = await queryTyped<T>(client, table, sql, params, schemaInfo);
  return rows[0];
}
