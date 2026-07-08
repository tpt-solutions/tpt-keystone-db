// Ties `flux.ts` (consume one Flux topic from Keystone) to `ws/server.ts`
// (re-broadcast to this SDK's own downstream WebSocket clients) — the
// "WebSocket server for broadcasting Flux events to clients" roadmap item.
// Typical use: a Node backend process runs this once per topic it wants to
// fan out to browser tabs, so those tabs open one WebSocket to *this*
// server instead of each opening its own connection straight to Keystone's
// Flux bridge.

import { subscribeFlux, type FluxRecord, type FluxSubscription } from "./flux.js";
import { WsServer, type WsServerClient } from "./ws/server.js";

export interface FluxBroadcastOptions {
  /** Keystone's Flux WebSocket bridge, e.g. `"ws://127.0.0.1:5434"`. */
  fluxUrl: string;
  /** Topic to subscribe to on Keystone and re-broadcast downstream. */
  topic: string;
  /** Port this SDK's own WebSocket server listens on for downstream clients. */
  port: number;
  host?: string;
  onConnect?: (client: WsServerClient) => void;
  onDisconnect?: (client: WsServerClient) => void;
  onRecord?: (record: FluxRecord) => void;
}

export class FluxBroadcastServer {
  private constructor(
    private wsServer: WsServer,
    private subscription: FluxSubscription,
    private pump: Promise<void>,
  ) {}

  static async start(options: FluxBroadcastOptions): Promise<FluxBroadcastServer> {
    const subscription = await subscribeFlux(options.fluxUrl, options.topic);
    const wsServer = await WsServer.listen({
      port: options.port,
      host: options.host,
      onConnect: options.onConnect,
      onDisconnect: options.onDisconnect,
    });

    const pump = (async () => {
      try {
        for await (const record of subscription.records()) {
          options.onRecord?.(record);
          wsServer.broadcast(JSON.stringify(record));
        }
      } catch {
        // Upstream Flux connection dropped; downstream clients simply stop
        // receiving further events (no reconnect/backoff implemented here —
        // a scope cut, see README).
      }
    })();

    return new FluxBroadcastServer(wsServer, subscription, pump);
  }

  get downstreamClientCount(): number {
    return this.wsServer.clientCount;
  }

  async close(): Promise<void> {
    this.subscription.close();
    await this.pump;
    await this.wsServer.close();
  }
}
