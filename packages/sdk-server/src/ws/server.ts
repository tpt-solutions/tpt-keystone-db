// Minimal hand-rolled WebSocket server — RFC 6455 handshake + text-frame
// codec, no `ws` npm package — for step (b) of the Flux-broadcast roadmap
// item: re-broadcasting events this Node process consumes from Keystone's
// own Flux WS bridge out to *this SDK's own* downstream WebSocket clients
// (e.g. browser tabs). Mirrors the scope cuts documented in
// `tpt-keystone/src/wire/websocket.rs`: no fragmentation, no
// permessage-deflate, no binary frames, no ping/pong keepalive.

import { createServer, type Server, type Socket } from "node:net";
import { FrameReader, encodeTextFrame, OPCODE_CLOSE } from "./frame.js";
import { acceptKeyFor, buildUpgradeResponse, parseUpgradeRequest, readHttpHeaders } from "./handshake.js";

export interface WsServerClient {
  readonly id: number;
  send(text: string): void;
  close(): void;
}

export interface WsServerOptions {
  port: number;
  host?: string;
  onConnect?: (client: WsServerClient) => void;
  onDisconnect?: (client: WsServerClient) => void;
}

export class WsServer {
  private server: Server;
  private clients = new Map<number, { socket: Socket; api: WsServerClient }>();
  private nextId = 1;

  private constructor(server: Server) {
    this.server = server;
  }

  static async listen(options: WsServerOptions): Promise<WsServer> {
    const server = createServer();
    const ws = new WsServer(server);
    server.on("connection", (socket) => {
      ws.handleConnection(socket, options).catch(() => socket.destroy());
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.port, options.host ?? "0.0.0.0", () => {
        server.removeAllListeners("error");
        resolve();
      });
    });
    return ws;
  }

  private async handleConnection(socket: Socket, options: WsServerOptions): Promise<void> {
    const iterator = socket[Symbol.asyncIterator]() as AsyncIterator<Buffer>;
    const { text, leftover } = await readHttpHeaders(iterator);
    const key = parseUpgradeRequest(text);
    const accept = acceptKeyFor(key);
    socket.write(buildUpgradeResponse(accept));

    const id = this.nextId++;
    const api: WsServerClient = {
      id,
      send: (msg: string) => {
        if (!socket.destroyed) socket.write(encodeTextFrame(msg, /* masked = */ false));
      },
      close: () => socket.destroy(),
    };
    this.clients.set(id, { socket, api });
    options.onConnect?.(api);

    const reader = new FrameReader(socket, leftover);
    socket.on("close", () => {
      this.clients.delete(id);
      options.onDisconnect?.(api);
    });

    try {
      for (;;) {
        const frame = await reader.readFrame();
        if (frame.opcode === OPCODE_CLOSE) {
          socket.destroy();
          return;
        }
        // Text frames from downstream clients (if any) are read off the
        // wire to keep framing in sync but otherwise ignored — this server
        // is a one-directional broadcaster, matching `websocket.rs`'s own
        // "push-only after subscribe" shape.
      }
    } catch {
      socket.destroy();
    }
  }

  /** Sends `text` to every currently connected downstream client. */
  broadcast(text: string): void {
    const frame = encodeTextFrame(text, /* masked = */ false);
    for (const { socket } of this.clients.values()) {
      if (!socket.destroyed) socket.write(frame);
    }
  }

  get clientCount(): number {
    return this.clients.size;
  }

  async close(): Promise<void> {
    for (const { socket } of this.clients.values()) socket.destroy();
    this.clients.clear();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}
