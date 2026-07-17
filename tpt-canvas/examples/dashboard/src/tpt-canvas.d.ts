/* Minimal hand-written bindings for the tpt-canvas WASM module.
 *
 * Produced by `npm run build:wasm` (wasm-bindgen --target web) into ./pkg.
 * We declare only the constructors this demo uses; see each component's
 * `#[wasm_bindgen]` impl in tpt-canvas/src/components/*.rs for the full
 * surface. Every constructor takes (http_base, ws_base, sql, ...) and mounts
 * itself onto a DOM element by id.
 */

export function default(): Promise<void>;

export class CanvasTimeSeries {
  constructor(
    canvasId: string,
    httpBase: string,
    wsBase: string,
    sql: string,
    xField: string,
    yField: string,
    realtimeTopic: string,
  );
}

export class CanvasMap {
  constructor(
    canvasId: string,
    httpBase: string,
    wsBase: string,
    sql: string,
    locationField: string,
    realtimeTopic: string,
    cluster: boolean,
    heatmap: boolean,
    onClick?: (rowJson: string) => void,
  );
}

export class CanvasGraph {
  constructor(
    canvasId: string,
    httpBase: string,
    wsBase: string,
    nodesSql: string,
    edgesSql: string,
    realtimeTopic: string,
  );
  // Also available: CanvasGraph.new_from_match(...) for a single GQL MATCH query.
}

export class CanvasVectorSearch {
  constructor(
    containerId: string,
    httpBase: string,
    wsBase: string,
    sql: string,
    scoreField: string,
    realtimeTopic: string,
  );
}

export class CanvasDocument {
  constructor(
    containerId: string,
    httpBase: string,
    wsBase: string,
    sql: string,
    table: string,
    column: string,
    pkColumn: string,
    realtimeTopic: string,
  );
}

export class CanvasAgentMonitor {
  constructor(
    canvasId: string,
    containerId: string,
    httpBase: string,
    wsBase: string,
    eventsSql: string,
    metricsSql: string,
    realtimeTopic: string,
  );
}
