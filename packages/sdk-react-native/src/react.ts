// Optional React adapter for `@tpt/sdk-react-native`. `react` is a peer
// dependency (optional, see package.json) so this package ships without
// forcing React into a bare backend bundle. The hooks render an offline-first
// `QueryStore` by polling `KeystoneClient.query`. Flux push is layered on
// separately via `useFlux`.

import { useEffect, useRef, useState } from "react";

import { KeystoneClient, QueryResult } from "./client.js";
import { QueryState, QueryStore } from "./store.js";

function useSyncStore<T>(store: QueryStore<T>): QueryState<T> {
  const [state, setState] = useState<QueryState<T>>(store.getState());
  useEffect(() => store.subscribe(setState), [store]);
  return state;
}

/**
 * Runs `query` on mount and whenever `deps` change, reading from the
 * offline-first cache when the network is unavailable. Returns
 * `{ data, error, loading }`.
 */
export function useKeystoneQuery(
  client: KeystoneClient,
  query: string,
  params: unknown[] = [],
  deps: unknown[] = [],
): QueryState<QueryResult> {
  const storeRef = useRef<QueryStore<QueryResult>>();
  if (!storeRef.current) storeRef.current = new QueryStore<QueryResult>();

  useEffect(() => {
    const store = storeRef.current as QueryStore<QueryResult>;
    let cancelled = false;
    store.setState({ loading: true, error: null });
    client
      .query(query, params)
      .then((data) => {
        if (!cancelled) store.setState({ data, loading: false });
      })
      .catch((error: Error) => {
        if (!cancelled) store.setState({ error, loading: false });
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return useSyncStore(storeRef.current);
}

/**
 * Subscribes to a Flux topic for the component's lifetime. `onRecord` fires
 * for each pushed record. The latest record is held in state for rendering.
 */
export function useFlux(
  client: KeystoneClient,
  topic: string,
  onRecord?: (record: { offset: number; key: string | null; value: string | null; ts: number }) => void,
): { offset: number; key: string | null; value: string | null; ts: number } | null {
  const [last, setLast] = useState<{ offset: number; key: string | null; value: string | null; ts: number } | null>(null);
  const cbRef = useRef(onRecord);
  cbRef.current = onRecord;

  useEffect(() => {
    const unsubscribe = client.subscribeFlux(topic, (record) => {
      setLast(record);
      cbRef.current?.(record);
    });
    return unsubscribe;
  }, [client, topic]);

  return last;
}
