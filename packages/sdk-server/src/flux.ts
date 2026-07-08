// Flux consumer: connects to Keystone's own Flux WebSocket bridge
// (`tpt-keystone/src/wire/websocket.rs`, default `ws://host:5434`) as a
// client and subscribes to one topic, using the exact same wire protocol
// `packages/sdk-web/src/flux.ts` speaks from a browser: send one
// `{"subscribe": "<topic>"}` text frame, then receive one
// `{"offset","key","value","ts"}` text frame per published record from the
// moment of subscription onward (no backlog replay).

import { connectWebSocket, type WsClientConnection } from "./ws/client.js";

export interface FluxRecord {
  offset: number;
  key: string | null;
  value: string;
  ts: number;
}

export interface FluxSubscription {
  /** Async iterator of records pushed for the subscribed topic. */
  records(): AsyncGenerator<FluxRecord, void, void>;
  close(): void;
}

/** Subscribes to `topic` on Keystone's Flux WebSocket bridge at `fluxUrl` (e.g. `"ws://127.0.0.1:5434"`). */
export async function subscribeFlux(fluxUrl: string, topic: string): Promise<FluxSubscription> {
  const conn: WsClientConnection = await connectWebSocket(fluxUrl);
  conn.send(JSON.stringify({ subscribe: topic }));

  return {
    async *records(): AsyncGenerator<FluxRecord, void, void> {
      for await (const text of conn.messages()) {
        try {
          yield JSON.parse(text) as FluxRecord;
        } catch {
          // Malformed frame — skip rather than crash the consumer loop.
        }
      }
    },
    close(): void {
      conn.close();
    },
  };
}
