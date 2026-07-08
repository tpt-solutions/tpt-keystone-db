// Core client — "SDK/Server" (Phase 14, TODO.md line ~297). Connects
// directly over TCP to a Keystone node's Postgres-wire listener (default
// port 5432), the same protocol `tpt-keystone/src/wire` speaks server-side
// and `tpt-sdk/src/keystone` speaks client-side in Rust. This is the Node
// equivalent of that Rust client, hand-written the same way (no `pg`/
// `postgres`/`pg-protocol` dependency) — see `wire.ts` for the codec this
// wraps.
//
// No auth exists server-side (the startup handshake auto-approves any
// `user` param), so there is no auth path here either — building one would
// claim capability the engine doesn't have.

import { Conn, type BackendMessage } from "./wire.js";

export class KeystoneError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "KeystoneError";
  }
}

/** A type-erased scalar value, decoded from the wire's text format — mirrors `tpt-sdk`'s Rust `Value` enum. */
export type Value = null | boolean | number | string;

export function valueFromText(s: string | null): Value {
  if (s === null) return null;
  if (s === "t" || s === "true") return true;
  if (s === "f" || s === "false") return false;
  if (/^-?\d+$/.test(s)) {
    const n = Number.parseInt(s, 10);
    if (Number.isSafeInteger(n)) return n;
  }
  const f = Number.parseFloat(s);
  if (!Number.isNaN(f) && /^-?\d+(\.\d+)?(e[+-]?\d+)?$/i.test(s)) return f;
  return s;
}

/** Encodes a JS value as a parameter for the extended query protocol's text format. */
function toParam(v: unknown): Buffer | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "boolean") return Buffer.from(v ? "t" : "f", "utf8");
  if (typeof v === "number") return Buffer.from(String(v), "utf8");
  if (v instanceof Date) return Buffer.from(v.toISOString(), "utf8");
  return Buffer.from(String(v), "utf8");
}

export interface Row {
  /** Raw text-format cells, in column order (`null` for SQL NULL). */
  cells: (string | null)[];
  columns: string[];
  /** Zips columns/cells into a plain object, decoding scalars via `valueFromText`. */
  toObject(): Record<string, Value>;
  get(name: string): string | null | undefined;
}

function makeRow(columns: string[], cells: (string | null)[]): Row {
  return {
    cells,
    columns,
    toObject(): Record<string, Value> {
      const out: Record<string, Value> = {};
      columns.forEach((c, i) => (out[c] = valueFromText(cells[i] ?? null)));
      return out;
    },
    get(name: string): string | null | undefined {
      const i = columns.indexOf(name);
      return i === -1 ? undefined : cells[i];
    },
  };
}

export interface QueryResult {
  columns: string[];
  rows: Row[];
  commandTag: string | null;
}

export interface KeystoneClientOptions {
  /** `"host:port"` of a Keystone node's Postgres-wire listener. Defaults to port 5432 if no port is given. */
  addr: string;
  /** Startup parameters sent verbatim; `user` defaults to `"tpt_sdk_server"`. */
  params?: Record<string, string>;
}

function cellToText(cell: Buffer | null): string | null {
  return cell === null ? null : cell.toString("utf8");
}

export class KeystoneClient {
  private conn: Conn;

  private constructor(conn: Conn) {
    this.conn = conn;
  }

  static async connect(addrOrOptions: string | KeystoneClientOptions): Promise<KeystoneClient> {
    const options: KeystoneClientOptions =
      typeof addrOrOptions === "string" ? { addr: addrOrOptions } : addrOrOptions;
    const params = { user: "tpt_sdk_server", ...options.params };
    const conn = await Conn.connect(options.addr, params);
    return new KeystoneClient(conn);
  }

  /**
   * Runs `sql` over the simple query protocol. Supports multi-statement SQL
   * text but only the last statement's rows are returned (the simple query
   * protocol gives each statement its own CommandComplete; only the final
   * ReadyForQuery ends the exchange) — same semantics as the Rust SDK.
   */
  async query(sql: string): Promise<QueryResult> {
    this.conn.writeQuery(sql);
    await this.conn.flush();
    return this.drainSimple();
  }

  /** Runs a parameterized query over the extended query protocol (Parse/Bind/Describe/Execute/Sync), buffering the full result. */
  async queryParams(sql: string, params: unknown[] = []): Promise<QueryResult> {
    const encoded = params.map(toParam);
    this.conn.writeParse("", sql, []);
    this.conn.writeBind("", "", encoded);
    this.conn.writeDescribePortal("");
    this.conn.writeExecute("", 0);
    this.conn.writeSync();
    await this.conn.flush();
    return this.drainExtended();
  }

  /**
   * Streams query results row by row via the extended query protocol.
   *
   * Scope note (read before relying on this for large result sets): this
   * yields each row as soon as it's decoded off the socket, not once per
   * server round trip — it does NOT use `Execute`'s `max_rows`/
   * `PortalSuspended` batching to fetch more rows only on demand. That
   * would be true batched backpressure, but it isn't viable against this
   * Keystone build: `tpt-keystone/src/wire/session.rs`'s `Execute` handler
   * re-evaluates the whole statement and re-slices to `max_rows` on every
   * call (the `Portal` struct carries no cursor position), so repeated
   * `Execute` calls on one portal would just re-return the same first
   * `max_rows` rows forever rather than advancing. So this method sends one
   * Execute with an unlimited row count and the server streams
   * `DataRow`s down the same TCP connection as it produces them
   * (`session.rs` loops `conn.send(DataRow)` per row before the final
   * flush) — the *client* here decodes and yields each one without
   * buffering the rest, so memory use during iteration is O(1) rows, not
   * O(n), but there's exactly one server round trip, same as `queryParams`.
   */
  async *streamQuery(sql: string, params: unknown[] = []): AsyncGenerator<Row, void, void> {
    const encoded = params.map(toParam);
    this.conn.writeParse("", sql, []);
    this.conn.writeBind("", "", encoded);
    this.conn.writeDescribePortal("");
    this.conn.writeExecute("", 0);
    this.conn.writeSync();
    await this.conn.flush();

    let columns: string[] = [];
    for (;;) {
      const msg = await this.conn.readMessage();
      switch (msg.tag) {
        case "ParseComplete":
        case "BindComplete":
        case "ParameterDescription":
        case "NoData":
        case "PortalSuspended":
          break;
        case "RowDescription":
          columns = msg.fields.map((f) => f.name);
          break;
        case "DataRow":
          yield makeRow(columns, msg.cells.map(cellToText));
          break;
        case "CommandComplete":
          break;
        case "ErrorResponse": {
          await this.drainToReady();
          throw new KeystoneError(msg.message);
        }
        case "ReadyForQuery":
          return;
        default:
          break;
      }
    }
  }

  private async drainSimple(): Promise<QueryResult> {
    let columns: string[] = [];
    let rows: Row[] = [];
    let commandTag: string | null = null;

    for (;;) {
      const msg = await this.conn.readMessage();
      switch (msg.tag) {
        case "RowDescription":
          columns = msg.fields.map((f) => f.name);
          rows = [];
          break;
        case "DataRow":
          rows.push(makeRow(columns, msg.cells.map(cellToText)));
          break;
        case "CommandComplete":
          commandTag = msg.commandTag;
          break;
        case "EmptyQueryResponse":
        case "NoticeResponse":
          break;
        case "ErrorResponse":
          throw new KeystoneError(msg.message);
        case "ReadyForQuery":
          return { columns, rows, commandTag };
        default:
          break;
      }
    }
  }

  private async drainExtended(): Promise<QueryResult> {
    let columns: string[] = [];
    let rows: Row[] = [];
    let commandTag: string | null = null;

    for (;;) {
      const msg = await this.conn.readMessage();
      switch (msg.tag) {
        case "ParseComplete":
        case "BindComplete":
        case "ParameterDescription":
        case "NoData":
        case "PortalSuspended":
          break;
        case "RowDescription":
          columns = msg.fields.map((f) => f.name);
          break;
        case "DataRow":
          rows.push(makeRow(columns, msg.cells.map(cellToText)));
          break;
        case "CommandComplete":
          commandTag = msg.commandTag;
          break;
        case "ErrorResponse":
          await this.drainToReady();
          throw new KeystoneError(msg.message);
        case "ReadyForQuery":
          return { columns, rows, commandTag };
        default:
          break;
      }
    }
  }

  private async drainToReady(): Promise<void> {
    for (;;) {
      const msg: BackendMessage = await this.conn.readMessage();
      if (msg.tag === "ReadyForQuery") return;
    }
  }

  async close(): Promise<void> {
    this.conn.writeTerminate();
    await this.conn.flush();
    this.conn.close();
  }
}

export function createKeystoneClient(addrOrOptions: string | KeystoneClientOptions): Promise<KeystoneClient> {
  return KeystoneClient.connect(addrOrOptions);
}
