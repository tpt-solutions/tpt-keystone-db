// RFC 6455 handshake helpers, hand-written per this project's ethos (see
// `tpt-keystone/src/wire/websocket.rs`'s module doc: "hashing itself isn't
// the from-scratch boundary this project draws, only the wire/parsing
// layers are" — same reasoning applies here, `node:crypto`'s SHA-1 is used,
// the HTTP Upgrade parsing/framing is not delegated to any package).

import { createHash, randomBytes } from "node:crypto";

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

export function generateClientKey(): string {
  return randomBytes(16).toString("base64");
}

export function acceptKeyFor(clientKey: string): string {
  return createHash("sha1").update(clientKey + WS_GUID, "utf8").digest("base64");
}

export function buildUpgradeRequest(host: string, path: string, key: string): string {
  return (
    `GET ${path} HTTP/1.1\r\n` +
    `Host: ${host}\r\n` +
    `Upgrade: websocket\r\n` +
    `Connection: Upgrade\r\n` +
    `Sec-WebSocket-Key: ${key}\r\n` +
    `Sec-WebSocket-Version: 13\r\n\r\n`
  );
}

export function buildUpgradeResponse(acceptKey: string): string {
  return (
    `HTTP/1.1 101 Switching Protocols\r\n` +
    `Upgrade: websocket\r\n` +
    `Connection: Upgrade\r\n` +
    `Sec-WebSocket-Accept: ${acceptKey}\r\n\r\n`
  );
}

/** Extracts `Sec-WebSocket-Accept` from a raw HTTP response; throws if the handshake wasn't a 101. */
export function parseUpgradeResponse(raw: string): string {
  const [statusLine, ...headerLines] = raw.split("\r\n");
  if (!statusLine.includes("101")) {
    throw new Error(`WebSocket handshake failed: ${statusLine}`);
  }
  const accept = headerLines
    .find((line) => line.toLowerCase().startsWith("sec-websocket-accept:"))
    ?.split(":")[1]
    ?.trim();
  if (!accept) throw new Error("missing Sec-WebSocket-Accept header in handshake response");
  return accept;
}

/** Extracts `Sec-WebSocket-Key` from a raw HTTP Upgrade request. */
export function parseUpgradeRequest(raw: string): string {
  const key = raw
    .split("\r\n")
    .find((line) => line.toLowerCase().startsWith("sec-websocket-key:"))
    ?.split(":")[1]
    ?.trim();
  if (!key) throw new Error("missing Sec-WebSocket-Key header");
  return key;
}

/** Reads byte-by-byte off `socket` up to the blank line terminating HTTP headers, returning the raw request/response text. */
export async function readHttpHeaders(iterator: AsyncIterator<Buffer>, initial?: Buffer): Promise<{ text: string; leftover: Buffer }> {
  let buf = initial ? Buffer.from(initial) : Buffer.alloc(0);
  for (;;) {
    const terminator = buf.indexOf("\r\n\r\n");
    if (terminator !== -1) {
      const headerEnd = terminator + 4;
      return { text: buf.subarray(0, headerEnd).toString("utf8"), leftover: buf.subarray(headerEnd) };
    }
    if (buf.length > 16_384) throw new Error("HTTP handshake headers too large");
    const { value, done } = await iterator.next();
    if (done) throw new Error("connection closed during WebSocket handshake");
    buf = Buffer.concat([buf, value]);
  }
}
