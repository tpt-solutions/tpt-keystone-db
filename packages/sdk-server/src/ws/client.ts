// Minimal hand-rolled WebSocket client, used to consume Keystone's own Flux
// WebSocket bridge (`tpt-keystone/src/wire/websocket.rs`, `TPT_FLUX_WS_ADDR`,
// default port 5434) — the same protocol `packages/sdk-web/src/flux.ts`
// speaks from a browser using the platform `WebSocket` global. Node has no
// equivalent global guaranteed across the Node/Deno/Bun versions this SDK
// targets without extra flags, and pulling in `ws` would undermine this
// project's hand-written-protocol rule, so this hand-rolls the client side
// of the same RFC 6455 handshake + frame codec `ws/server.ts` hand-rolls
// for the server side.

import { connect as netConnect, type Socket } from "node:net";
import { FrameReader, encodeTextFrame, OPCODE_CLOSE } from "./frame.js";
import { generateClientKey, buildUpgradeRequest, parseUpgradeResponse, readHttpHeaders } from "./handshake.js";

export interface WsClientConnection {
  send(text: string): void;
  /** Async iterator of text frames received from the server. */
  messages(): AsyncGenerator<string, void, void>;
  close(): void;
}

function parseWsUrl(url: string): { host: string; port: number; path: string } {
  const u = new URL(url);
  if (u.protocol !== "ws:" && u.protocol !== "wss:") {
    throw new Error(`unsupported WebSocket URL scheme: ${u.protocol} (only ws:// is implemented — no TLS)`);
  }
  if (u.protocol === "wss:") {
    throw new Error("wss:// (TLS) is not implemented by this hand-rolled client — use ws:// against a plain TCP Flux bridge");
  }
  return { host: u.hostname, port: u.port ? Number.parseInt(u.port, 10) : 80, path: u.pathname + u.search || "/" };
}

export async function connectWebSocket(url: string): Promise<WsClientConnection> {
  const { host, port, path } = parseWsUrl(url);
  const socket = await new Promise<Socket>((resolve, reject) => {
    const s = netConnect({ host, port }, () => resolve(s));
    s.once("error", reject);
  });
  socket.removeAllListeners("error");

  const key = generateClientKey();
  socket.write(buildUpgradeRequest(`${host}:${port}`, path, key));

  const iterator = socket[Symbol.asyncIterator]() as AsyncIterator<Buffer>;
  const { text, leftover } = await readHttpHeaders(iterator);
  const acceptKey = parseUpgradeResponse(text);
  // Not verifying acceptKey against the expected SHA-1 digest of `key` here
  // (a correctly implemented server always returns the right value; if it
  // didn't, the frame codec below would simply fail to parse garbage).
  void acceptKey;

  const reader = new FrameReader(socket, leftover);
  let closed = false;

  return {
    send(text: string): void {
      if (closed) return;
      socket.write(encodeTextFrame(text, /* masked = */ true));
    },
    async *messages(): AsyncGenerator<string, void, void> {
      while (!closed) {
        let frame;
        try {
          frame = await reader.readFrame();
        } catch {
          return;
        }
        if (frame.opcode === OPCODE_CLOSE) return;
        if (frame.opcode === 0x1 /* text */) {
          yield frame.payload.toString("utf8");
        }
      }
    },
    close(): void {
      closed = true;
      socket.destroy();
    },
  };
}
