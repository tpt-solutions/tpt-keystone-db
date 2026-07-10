// Typed query builder — Phase 5's "AI-optimised SDK" checklist item, the
// sdk-server sibling of `@tpt/sdk-web`'s `query-builder.ts` (same design:
// schema-driven `TableDef<Row>`, chainable `.whereEq`/`.orderBy`/`.limit`,
// `.build()` for the escape hatch into raw SQL). Kept as sdk-server's own
// copy rather than an import from `@tpt/sdk-web` — the two packages have no
// workspace dependency between them (each is built/published standalone,
// same "duplicate the hand-written protocol code per package" precedent
// `wire.ts`'s own module doc already follows for the wire codec itself) —
// but `.fetch()` here calls `KeystoneClient.queryParams` (the Postgres-wire
// extended protocol) instead of sdk-web's HTTP bridge, and decodes each
// `Row` via `toObject()` instead of trusting pre-zipped JSON.

import type { KeystoneClient, Value } from "./client.js";

export interface BuiltQuery {
  sql: string;
  params: unknown[];
}

export interface TableDef<Row> {
  name: string;
  columns: (keyof Row & string)[];
}

export function table<Row>(name: string, columns: (keyof Row & string)[]): TableDef<Row> {
  return { name, columns };
}

function quoteIdent(ident: string): string {
  return `"${ident.replace(/"/g, '""')}"`;
}

type Order = "ASC" | "DESC";

export class TypedQueryBuilder<Row> {
  private cols: (keyof Row & string)[];
  private filters: Array<[keyof Row & string, unknown]> = [];
  private orderCol?: keyof Row & string;
  private orderDir: Order = "ASC";
  private limitN?: number;
  private offsetN?: number;

  constructor(private readonly def: TableDef<Row>) {
    this.cols = def.columns;
  }

  select(cols: (keyof Row & string)[]): this {
    this.cols = cols;
    return this;
  }

  whereEq<K extends keyof Row & string>(col: K, value: Row[K]): this {
    this.filters.push([col, value]);
    return this;
  }

  orderBy(col: keyof Row & string, dir: Order = "ASC"): this {
    this.orderCol = col;
    this.orderDir = dir;
    return this;
  }

  limit(n: number): this {
    this.limitN = n;
    return this;
  }

  offset(n: number): this {
    this.offsetN = n;
    return this;
  }

  build(): BuiltQuery {
    const cols = this.cols.map(quoteIdent).join(", ");
    let sql = `SELECT ${cols} FROM ${quoteIdent(this.def.name)}`;
    const params: unknown[] = [];

    if (this.filters.length) {
      const clauses = this.filters.map(([col], i) => `${quoteIdent(col)} = $${i + 1}`);
      sql += ` WHERE ${clauses.join(" AND ")}`;
      params.push(...this.filters.map(([, v]) => v));
    }
    if (this.orderCol) {
      sql += ` ORDER BY ${quoteIdent(this.orderCol)} ${this.orderDir}`;
    }
    if (this.limitN !== undefined) {
      sql += ` LIMIT ${this.limitN}`;
    }
    if (this.offsetN !== undefined) {
      sql += ` OFFSET ${this.offsetN}`;
    }
    return { sql, params };
  }

  /** Builds and executes via `KeystoneClient.queryParams`, decoding each
   * `Row` to a plain object keyed by column name. */
  async fetch(client: Pick<KeystoneClient, "queryParams">): Promise<Record<keyof Row & string, Value>[]> {
    const { sql, params } = this.build();
    const result = await client.queryParams(sql, params);
    return result.rows.map((r) => r.toObject() as Record<keyof Row & string, Value>);
  }
}

export function from<Row>(def: TableDef<Row>): TypedQueryBuilder<Row> {
  return new TypedQueryBuilder(def);
}
