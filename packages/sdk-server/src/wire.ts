// Client-side codec for the same hand-written Postgres wire protocol v3
// `tpt-keystone/src/wire/codec.rs` implements server-side. Ported line-for-
// line in spirit from `tpt-sdk/src/keystone/wire.rs` (the Rust SDK's client
// codec for the identical protocol) — frontend (client -> server) messages
// are *encoded* here, backend (server -> client) messages are *decoded*
// here. No `pg`/`postgres`/`pg-protocol` dependency: this is a from-scratch
// TCP socket (`node:net`) + hand-rolled message framing, matching this
// project's whole-codebase rule that wire protocols are hand-written, not
// pulled in from a driver library.
//
// Only the subset of the protocol this SDK needs is implemented: startup,
// the simple query loop, and the extended query subset (Parse/Bind/
// Describe/Execute/Sync) needed for parameterized queries and streaming.
// All formats are text (format code 0) — there is no binary-format support,
// matching the Rust SDK's client codec.

import { connect as netConnect, type Socket } from "node:net";

export interface FieldDescription {
  name: string;
  typeOid: number;
}

export type BackendMessage =
  | { tag: "AuthenticationOk" }
  | { tag: "ParameterStatus"; name: string; value: string }
  | { tag: "BackendKeyData"; pid: number; secret: number }
  | { tag: "ReadyForQuery"; status: number }
  | { tag: "RowDescription"; fields: FieldDescription[] }
  | { tag: "DataRow"; cells: (Buffer | null)[] }
  | { tag: "CommandComplete"; commandTag: string }
  | { tag: "ErrorResponse"; message: string }
  | { tag: "NoticeResponse"; message: string }
  | { tag: "ParseComplete" }
  | { tag: "BindComplete" }
  | { tag: "CloseComplete" }
  | { tag: "ParameterDescription"; types: number[] }
  | { tag: "NoData" }
  | { tag: "PortalSuspended" }
  | { tag: "EmptyQueryResponse" }
  | { tag: "Unknown"; code: number };

/**
 * Wraps a `net.Socket` into a message-framed Postgres wire protocol v3
 * connection. Reads are pulled lazily off the socket's async-iterator
 * interface (`for await` protocol) one message at a time — there is no
 * background loop buffering the whole stream, so `readMessage()` only ever
 * holds as many bytes as the next message needs plus whatever arrived in
 * the same TCP segment.
 */
export class Conn {
  private socket: Socket;
  private iterator: AsyncIterator<Buffer>;
  private pending: Buffer;
  private ended = false;

  private constructor(socket: Socket) {
    this.socket = socket;
    this.iterator = socket[Symbol.asyncIterator]() as AsyncIterator<Buffer>;
    this.pending = Buffer.alloc(0);
  }

  static async connect(addr: string, params: Record<string, string>): Promise<Conn> {
    const [host, portStr] = splitAddr(addr);
    const port = Number.parseInt(portStr, 10);
    const socket = await new Promise<Socket>((resolve, reject) => {
      const s = netConnect({ host, port }, () => resolve(s));
      s.once("error", reject);
    });
    socket.removeAllListeners("error");
    const conn = new Conn(socket);
    conn.writeStartup(params);
    await conn.flush();

    for (;;) {
      const msg = await conn.readMessage();
      if (msg.tag === "AuthenticationOk") continue;
      if (msg.tag === "ReadyForQuery") break;
      if (msg.tag === "ErrorResponse") throw new Error(`startup rejected: ${msg.message}`);
      // ParameterStatus/BackendKeyData etc. — ignored, matching the Rust SDK.
    }
    return conn;
  }

  private writeBuf: Buffer[] = [];

  private writeStartup(params: Record<string, string>): void {
    const parts: Buffer[] = [];
    for (const [k, v] of Object.entries(params)) {
      parts.push(Buffer.from(k, "utf8"), Buffer.from([0]), Buffer.from(v, "utf8"), Buffer.from([0]));
    }
    parts.push(Buffer.from([0]));
    const body = Buffer.concat(parts);
    const versionAndBody = Buffer.concat([i32be(196608), body]);
    this.writeBuf.push(i32be(4 + versionAndBody.length), versionAndBody);
  }

  writeQuery(sql: string): void {
    this.writeMsg(0x51 /* 'Q' */, Buffer.concat([Buffer.from(sql, "utf8"), Buffer.from([0])]));
  }

  writeParse(name: string, sql: string, paramTypes: number[]): void {
    const parts = [
      cstr(name),
      cstr(sql),
      i16be(paramTypes.length),
      ...paramTypes.map((t) => i32be(t)),
    ];
    this.writeMsg(0x50 /* 'P' */, Buffer.concat(parts));
  }

  writeBind(portal: string, stmt: string, params: (Buffer | null)[]): void {
    const parts: Buffer[] = [cstr(portal), cstr(stmt), i16be(1), i16be(0), i16be(params.length)];
    for (const p of params) {
      if (p === null) {
        parts.push(i32be(-1));
      } else {
        parts.push(i32be(p.length), p);
      }
    }
    parts.push(i16be(1), i16be(0)); // all results text format
    this.writeMsg(0x42 /* 'B' */, Buffer.concat(parts));
  }

  writeDescribePortal(name: string): void {
    this.writeMsg(0x44 /* 'D' */, Buffer.concat([Buffer.from("P", "ascii"), cstr(name)]));
  }

  writeExecute(portal: string, maxRows: number): void {
    this.writeMsg(0x45 /* 'E' */, Buffer.concat([cstr(portal), i32be(maxRows)]));
  }

  writeSync(): void {
    this.writeMsg(0x53 /* 'S' */, Buffer.alloc(0));
  }

  writeTerminate(): void {
    this.writeMsg(0x58 /* 'X' */, Buffer.alloc(0));
  }

  private writeMsg(tag: number, body: Buffer): void {
    this.writeBuf.push(Buffer.from([tag]), i32be(4 + body.length), body);
  }

  async flush(): Promise<void> {
    const chunk = Buffer.concat(this.writeBuf);
    this.writeBuf = [];
    if (chunk.length === 0) return;
    await new Promise<void>((resolve, reject) => {
      this.socket.write(chunk, (err) => (err ? reject(err) : resolve()));
    });
  }

  /** Reads and decodes exactly one backend message, blocking until it's fully available. */
  async readMessage(): Promise<BackendMessage> {
    const header = await this.fill(5);
    const tag = header[0];
    const len = header.readInt32BE(1);
    if (len < 4) throw new Error(`invalid message length ${len} for tag ${String.fromCharCode(tag)}`);
    const total = 1 + len;
    const full = await this.fill(total);
    this.consume(total);
    const data = full.subarray(5, total);
    return decodeMessage(tag, data);
  }

  private async fill(n: number): Promise<Buffer> {
    while (this.pending.length < n) {
      if (this.ended) throw new Error("connection closed by server");
      const { value, done } = await this.iterator.next();
      if (done) {
        this.ended = true;
        throw new Error("connection closed by server");
      }
      this.pending = this.pending.length === 0 ? value : Buffer.concat([this.pending, value]);
    }
    return this.pending;
  }

  private consume(n: number): void {
    this.pending = this.pending.subarray(n);
  }

  close(): void {
    this.socket.destroy();
  }
}

function splitAddr(addr: string): [string, string] {
  const idx = addr.lastIndexOf(":");
  if (idx === -1) return [addr, "5432"];
  return [addr.slice(0, idx), addr.slice(idx + 1)];
}

function i32be(n: number): Buffer {
  const b = Buffer.alloc(4);
  b.writeInt32BE(n, 0);
  return b;
}

function i16be(n: number): Buffer {
  const b = Buffer.alloc(2);
  b.writeInt16BE(n, 0);
  return b;
}

function cstr(s: string): Buffer {
  return Buffer.concat([Buffer.from(s, "utf8"), Buffer.from([0])]);
}

function readCstr(data: Buffer, offset: number): [string, number] {
  let end = data.indexOf(0, offset);
  if (end === -1) end = data.length;
  return [data.toString("utf8", offset, end), Math.min(end + 1, data.length)];
}

/** Error/notice fields are `(u8 field_code, cstr value)` pairs terminated by a nul byte; only 'M' (message) is surfaced. */
function parseErrorFields(data: Buffer, offset: number): string {
  let message: string | undefined;
  let o = offset;
  while (o < data.length) {
    const code = data[o];
    o += 1;
    if (code === 0) break;
    const [value, next] = readCstr(data, o);
    o = next;
    if (code === 0x4d /* 'M' */) message = value;
  }
  return message ?? "unknown server error";
}

function decodeMessage(tag: number, data: Buffer): BackendMessage {
  let o = 0;
  switch (tag) {
    case 0x52 /* R */: {
      return { tag: "AuthenticationOk" };
    }
    case 0x53 /* S — ParameterStatus */: {
      const [name, o1] = readCstr(data, o);
      const [value] = readCstr(data, o1);
      return { tag: "ParameterStatus", name, value };
    }
    case 0x4b /* K */: {
      return { tag: "BackendKeyData", pid: data.readInt32BE(0), secret: data.readInt32BE(4) };
    }
    case 0x5a /* Z */: {
      return { tag: "ReadyForQuery", status: data.length > 0 ? data[0] : 0x49 };
    }
    case 0x54 /* T */: {
      const n = data.readInt16BE(o);
      o += 2;
      const fields: FieldDescription[] = [];
      for (let i = 0; i < n; i++) {
        const [name, next] = readCstr(data, o);
        o = next;
        o += 4; // table oid
        o += 2; // col attr
        const typeOid = data.readInt32BE(o);
        o += 4;
        o += 2; // type size
        o += 4; // type modifier
        o += 2; // format
        fields.push({ name, typeOid });
      }
      return { tag: "RowDescription", fields };
    }
    case 0x44 /* D */: {
      const n = data.readInt16BE(o);
      o += 2;
      const cells: (Buffer | null)[] = [];
      for (let i = 0; i < n; i++) {
        const len = data.readInt32BE(o);
        o += 4;
        if (len < 0) {
          cells.push(null);
        } else {
          cells.push(data.subarray(o, o + len));
          o += len;
        }
      }
      return { tag: "DataRow", cells };
    }
    case 0x43 /* C */: {
      const [commandTag] = readCstr(data, o);
      return { tag: "CommandComplete", commandTag };
    }
    case 0x45 /* E */:
      return { tag: "ErrorResponse", message: parseErrorFields(data, o) };
    case 0x4e /* N */:
      return { tag: "NoticeResponse", message: parseErrorFields(data, o) };
    case 0x31 /* 1 */:
      return { tag: "ParseComplete" };
    case 0x32 /* 2 */:
      return { tag: "BindComplete" };
    case 0x33 /* 3 */:
      return { tag: "CloseComplete" };
    case 0x74 /* t */: {
      const n = data.readInt16BE(o);
      o += 2;
      const types: number[] = [];
      for (let i = 0; i < n; i++) {
        types.push(data.readInt32BE(o));
        o += 4;
      }
      return { tag: "ParameterDescription", types };
    }
    case 0x6e /* n */:
      return { tag: "NoData" };
    case 0x73 /* s */:
      return { tag: "PortalSuspended" };
    case 0x49 /* I */:
      return { tag: "EmptyQueryResponse" };
    default:
      return { tag: "Unknown", code: tag };
  }
}
