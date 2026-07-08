// Type-safe query builders for Keystone's data models. These are thin SQL
// string builders (`{ sql, params }`), not an ORM — they exist to give
// callers autocomplete and typo-safety over the exact function/table-
// function names each engine actually registers (verified against
// `tpt-keystone/src/executor/eval.rs` and `executor/mod.rs`'s table
// functions), not to hide SQL behind an abstraction. Feed the result
// straight into `KeystoneClient.query`.
//
// Seven data models per the roadmap (`TODO.md`): relational (Keystone
// itself), geospatial (Meridian), time-series (Chronos), graph (Plexus),
// document (Canopy), events (Flux). Vector (Prism) is the seventh — Prism
// is unbuilt (Phase 7, see `TODO.md`), so `vector.orderBySimilarity` below
// takes a caller-supplied distance expression rather than inventing a
// native ANN operator that doesn't exist in `sql::parse`/`executor::eval`
// yet; this is a documented scope cut, not an oversight.

export interface BuiltQuery {
  sql: string;
  params: unknown[];
}

function quoteIdent(ident: string): string {
  return `"${ident.replace(/"/g, '""')}"`;
}

// --- Relational (Keystone) -------------------------------------------------

export interface SelectOptions {
  columns?: string[];
  where?: string;
  params?: unknown[];
  orderBy?: string;
  limit?: number;
  offset?: number;
}

export const relational = {
  select(table: string, options: SelectOptions = {}): BuiltQuery {
    const cols = options.columns?.length ? options.columns.map(quoteIdent).join(", ") : "*";
    let sql = `SELECT ${cols} FROM ${quoteIdent(table)}`;
    if (options.where) sql += ` WHERE ${options.where}`;
    if (options.orderBy) sql += ` ORDER BY ${options.orderBy}`;
    if (options.limit !== undefined) sql += ` LIMIT ${options.limit}`;
    if (options.offset !== undefined) sql += ` OFFSET ${options.offset}`;
    return { sql, params: options.params ?? [] };
  },

  insert(table: string, values: Record<string, unknown>): BuiltQuery {
    const columns = Object.keys(values);
    const placeholders = columns.map((_, i) => `$${i + 1}`);
    return {
      sql: `INSERT INTO ${quoteIdent(table)} (${columns.map(quoteIdent).join(", ")}) VALUES (${placeholders.join(", ")})`,
      params: Object.values(values),
    };
  },
};

// --- Geospatial (Meridian) --------------------------------------------------
// Confirmed against executor/geo_tests.rs: ST_MakePoint, ST_Distance,
// ST_DWithin, ST_Within/ST_Contains, ST_Intersects, ST_GeomFromText.

export const geospatial = {
  point(lng: number, lat: number): string {
    return `ST_MakePoint(${lng}, ${lat})`;
  },

  withinDistance(table: string, column: string, lng: number, lat: number, meters: number, extra?: SelectOptions): BuiltQuery {
    const where = `ST_DWithin(${quoteIdent(column)}, ST_MakePoint(${lng}, ${lat}), ${meters})${extra?.where ? ` AND ${extra.where}` : ""}`;
    return relational.select(table, { ...extra, where });
  },

  distance(fromLng: number, fromLat: number, toLng: number, toLat: number): BuiltQuery {
    return { sql: `SELECT ST_Distance(ST_MakePoint(${fromLng}, ${fromLat}), ST_MakePoint(${toLng}, ${toLat}))`, params: [] };
  },
};

// --- Time-series (Chronos) --------------------------------------------------
// Confirmed against executor/chronos_tests.rs: time_bucket(interval, ts).

export const timeseries = {
  bucket(table: string, tsColumn: string, interval: string, options: SelectOptions & { valueColumns?: string[] } = {}): BuiltQuery {
    const valueCols = options.valueColumns?.length ? `, ${options.valueColumns.map(quoteIdent).join(", ")}` : "";
    let sql = `SELECT time_bucket('${interval}', ${quoteIdent(tsColumn)}) AS bucket${valueCols} FROM ${quoteIdent(table)}`;
    if (options.where) sql += ` WHERE ${options.where}`;
    sql += ` GROUP BY bucket ORDER BY bucket`;
    if (options.limit !== undefined) sql += ` LIMIT ${options.limit}`;
    return { sql, params: options.params ?? [] };
  },
};

// --- Graph (Plexus) ----------------------------------------------------------
// Confirmed against executor/plexus_tests.rs's table-function call syntax.

export type GraphDirection = "out" | "in" | "both";

export const graph = {
  neighbors(table: string, fromColumn: string, vertexKey: string, direction: GraphDirection = "out"): BuiltQuery {
    return { sql: `SELECT neighbor, rel_type FROM graph_neighbors($1, $2, $3, $4)`, params: [table, fromColumn, vertexKey, direction] };
  },

  bfs(table: string, fromColumn: string, startKey: string, maxDepth = 10, direction: GraphDirection = "out"): BuiltQuery {
    return { sql: `SELECT vertex, depth FROM graph_bfs($1, $2, $3, $4, $5)`, params: [table, fromColumn, startKey, maxDepth, direction] };
  },

  shortestPath(table: string, fromColumn: string, startKey: string, endKey: string, direction: GraphDirection = "both"): BuiltQuery {
    return { sql: `SELECT step, vertex FROM graph_shortest_path($1, $2, $3, $4, $5) ORDER BY step`, params: [table, fromColumn, startKey, endKey, direction] };
  },

  pagerank(table: string, fromColumn: string): BuiltQuery {
    return { sql: `SELECT vertex, score FROM graph_pagerank($1, $2)`, params: [table, fromColumn] };
  },
};

// --- Document (Canopy) --------------------------------------------------------
// Confirmed against executor/canopy_tests.rs: jsonb_set(doc, path, value).

export const document = {
  setPath(table: string, column: string, idColumn: string, idValue: unknown, path: string[], value: unknown): BuiltQuery {
    const pathLiteral = `{${path.join(",")}}`;
    return {
      sql: `UPDATE ${quoteIdent(table)} SET ${quoteIdent(column)} = jsonb_set(${quoteIdent(column)}, $1, $2) WHERE ${quoteIdent(idColumn)} = $3`,
      params: [pathLiteral, JSON.stringify(value), idValue],
    };
  },
};

// --- Vector (Prism — unbuilt, Phase 7) ----------------------------------------
// No native ANN operator exists yet; this takes a caller-supplied distance
// expression instead of inventing one, so nothing here claims capability
// the engine doesn't have.

export const vector = {
  orderBySimilarity(table: string, distanceExpr: string, options: SelectOptions & { topK?: number } = {}): BuiltQuery {
    return relational.select(table, { ...options, orderBy: distanceExpr, limit: options.topK ?? options.limit });
  },
};

// --- Events (Flux) -------------------------------------------------------------
// The HTTP/WS surface a browser can reach is push-only subscribe
// (`client.subscribe`, wire::websocket) — the poll/commit cursor API
// (`flux_poll`/`flux_commit`) is only exposed over the Postgres wire
// protocol's extended query flow, out of reach of this SDK by design, so
// there's no builder for it here.

export const events = {
  topic(name: string): string {
    return name;
  },
};
