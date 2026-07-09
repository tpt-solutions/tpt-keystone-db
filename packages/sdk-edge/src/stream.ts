// Streaming query results for edge runtimes. The Canvas HTTP/JSON bridge
// (`wire::http_query.rs`) is explicitly non-streaming — it reads the whole
// request via `Content-Length` and writes one complete JSON response, no
// chunked transfer-encoding (see that file's module doc). Real-time
// streaming instead rides Flux (`wire::websocket.rs`, Phase 11): one text
// frame in (`{"subscribe": "<topic>"}`), one text frame out per published
// record, same wire format `@tpt/sdk-web`'s `subscribeFlux` consumes.
//
// Not every edge runtime exposes outbound `WebSocket` (Lambda@Edge does
// not); `subscribeFlux` throws synchronously with a clear message rather
// than hanging if it's missing, instead of silently no-op'ing.

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
  if (typeof WebSocket === "undefined") {
    throw new Error(
      "subscribeFlux: no global WebSocket in this runtime — Flux streaming is unavailable here (e.g. Lambda@Edge); poll query() instead",
    );
  }

  const ws = new WebSocket(fluxUrl);

  ws.addEventListener("open", () => {
    ws.send(JSON.stringify({ subscribe: topic }));
  });

  ws.addEventListener("message", (event: MessageEvent) => {
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
