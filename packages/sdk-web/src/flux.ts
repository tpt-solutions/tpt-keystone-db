// Native WebSocket client for `wire::websocket` (tpt-keystone/src/wire/websocket.rs),
// the hand-rolled RFC 6455 endpoint Flux/Phase 11 exposes for browsers.
// Protocol: send one `{"subscribe": "<topic>"}` text frame, then receive a
// `{"offset","key","value","ts"}` text frame per record published to that
// topic from the moment of subscription onward (no backlog replay — that's
// what `flux_poll`/`flux_commit` over the Postgres wire protocol are for,
// out of reach of a browser client and out of scope here).

export interface FluxRecord {
  offset: number;
  key: string | null;
  value: string | null;
  ts: number;
}

/**
 * Opens a dedicated WebSocket to `fluxUrl`, subscribes to `topic`, and
 * invokes `onRecord` for each pushed record. Returns an unsubscribe
 * function that closes the socket.
 */
export function subscribeFlux(
  fluxUrl: string,
  topic: string,
  onRecord: (record: FluxRecord) => void,
  onError?: (error: unknown) => void,
): () => void {
  const ws = new WebSocket(fluxUrl);

  ws.addEventListener("open", () => {
    ws.send(JSON.stringify({ subscribe: topic }));
  });

  ws.addEventListener("message", (event) => {
    try {
      onRecord(JSON.parse(String(event.data)) as FluxRecord);
    } catch (error) {
      onError?.(error);
    }
  });

  if (onError) {
    ws.addEventListener("error", (event) => onError(event));
  }

  return () => ws.close();
}
