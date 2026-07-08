// Minimal hand-rolled RFC 6455 frame codec, shared by `ws/client.ts` (this
// SDK acting as a WebSocket *client* consuming Keystone's own Flux bridge)
// and `ws/server.ts` (this SDK acting as a WebSocket *server* re-broadcasting
// to downstream browser tabs). Mirrors the scope of
// `tpt-keystone/src/wire/websocket.rs`'s frame codec: text frames only, no
// fragmentation (a multi-frame message is rejected, not reassembled), no
// permessage-deflate, no ping/pong keepalive — a dead connection is only
// noticed when a write to it fails. No `ws` npm package.

import type { Socket } from "node:net";
import { randomBytes } from "node:crypto";

export const OPCODE_TEXT = 0x1;
export const OPCODE_CLOSE = 0x8;

export type Frame = { opcode: number; payload: Buffer };

/**
 * Encodes one frame. `masked` must be `true` for client->server frames
 * (RFC 6455 requires clients to mask) and `false` for server->client frames
 * (servers must not mask).
 */
export function encodeFrame(opcode: number, payload: Buffer, masked: boolean): Buffer {
  const parts: Buffer[] = [];
  const firstByte = 0x80 | (opcode & 0x0f); // FIN=1
  const len = payload.length;

  let lenByte: Buffer;
  if (len <= 125) {
    lenByte = Buffer.from([len | (masked ? 0x80 : 0)]);
  } else if (len <= 0xffff) {
    lenByte = Buffer.concat([Buffer.from([126 | (masked ? 0x80 : 0)]), u16be(len)]);
  } else {
    lenByte = Buffer.concat([Buffer.from([127 | (masked ? 0x80 : 0)]), u64be(len)]);
  }

  parts.push(Buffer.from([firstByte]), lenByte);

  if (masked) {
    const maskKey = randomBytes(4);
    const masked_ = Buffer.alloc(len);
    for (let i = 0; i < len; i++) masked_[i] = payload[i] ^ maskKey[i % 4];
    parts.push(maskKey, masked_);
  } else {
    parts.push(payload);
  }

  return Buffer.concat(parts);
}

export function encodeTextFrame(text: string, masked: boolean): Buffer {
  return encodeFrame(OPCODE_TEXT, Buffer.from(text, "utf8"), masked);
}

function u16be(n: number): Buffer {
  const b = Buffer.alloc(2);
  b.writeUInt16BE(n, 0);
  return b;
}

function u64be(n: number): Buffer {
  const b = Buffer.alloc(8);
  b.writeBigUInt64BE(BigInt(n), 0);
  return b;
}

/**
 * Reads frames off a socket one at a time via its async-iterator interface,
 * matching the pull-based style `wire.ts`'s `Conn` uses. Works for either
 * role: unmasks the payload whenever the frame's mask bit is set (true for
 * frames this SDK reads as a server; false for frames read as a client
 * talking to Keystone's server, which never masks).
 */
export class FrameReader {
  private iterator: AsyncIterator<Buffer>;
  private pending: Buffer = Buffer.alloc(0);
  private ended = false;

  constructor(socket: Socket, leftover?: Buffer) {
    this.iterator = socket[Symbol.asyncIterator]() as AsyncIterator<Buffer>;
    if (leftover && leftover.length > 0) this.pending = leftover;
  }

  async readFrame(): Promise<Frame> {
    const header = await this.fill(2);
    const fin = (header[0] & 0x80) !== 0;
    if (!fin) throw new Error("fragmented WebSocket messages are not supported");
    const opcode = header[0] & 0x0f;
    const masked = (header[1] & 0x80) !== 0;
    let len = header[1] & 0x7f;
    let headerLen = 2;

    if (len === 126) {
      const ext = await this.fill(4);
      len = ext.readUInt16BE(2);
      headerLen = 4;
    } else if (len === 127) {
      const ext = await this.fill(10);
      len = Number(ext.readBigUInt64BE(2));
      headerLen = 10;
    }

    let maskKey: Buffer | undefined;
    if (masked) {
      const withMask = await this.fill(headerLen + 4);
      maskKey = withMask.subarray(headerLen, headerLen + 4);
      headerLen += 4;
    }

    const full = await this.fill(headerLen + len);
    let payload = full.subarray(headerLen, headerLen + len);
    this.consume(headerLen + len);

    if (maskKey) {
      const unmasked = Buffer.alloc(payload.length);
      for (let i = 0; i < payload.length; i++) unmasked[i] = payload[i] ^ maskKey[i % 4];
      payload = unmasked;
    }

    if (opcode === OPCODE_CLOSE) return { opcode, payload };
    return { opcode, payload };
  }

  private async fill(n: number): Promise<Buffer> {
    while (this.pending.length < n) {
      if (this.ended) throw new Error("connection closed");
      const { value, done } = await this.iterator.next();
      if (done) {
        this.ended = true;
        throw new Error("connection closed");
      }
      this.pending = this.pending.length === 0 ? value : Buffer.concat([this.pending, value]);
    }
    return this.pending;
  }

  private consume(n: number): void {
    this.pending = this.pending.subarray(n);
  }
}
