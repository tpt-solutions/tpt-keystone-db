// Framework-agnostic core behind `useKeystoneQuery`/`useKeystoneMutation`.
// Mirrors `tpt-canvas`'s `KeystoneClient::use_keystone_query` (Phase 13):
// "zero-config real-time" becomes an explicit caller-named `realtimeTopic`
// rather than inferring one from the SQL text (same documented scope cut),
// and a topic message triggers a full refetch rather than an incremental
// patch. `react.ts` wraps these in `useSyncExternalStore` for React;
// non-React callers can use `Store.subscribe`/`Store.get` directly.

import type { KeystoneClient, QueryResult } from "./client.js";
import { Store, type Unsubscribe } from "./reactive.js";

export interface QueryState<T> {
  data: QueryResult<T> | null;
  error: string | null;
  isLoading: boolean;
}

export interface UseKeystoneQueryOptions {
  params?: unknown[];
  /** Flux topic to watch; each pushed record triggers a full requery. */
  realtimeTopic?: string;
}

export interface QueryHandle<T> extends Store<QueryState<T>> {
  refetch(): Promise<void>;
  unsubscribeFlux: Unsubscribe;
}

export function useKeystoneQuery<T = Record<string, string | null>>(
  client: KeystoneClient,
  sql: string,
  options: UseKeystoneQueryOptions = {},
): QueryHandle<T> {
  const store = new Store<QueryState<T>>({ data: null, error: null, isLoading: true });

  const refetch = async () => {
    store.update((s) => ({ ...s, isLoading: true }));
    try {
      const data = await client.query<T>(sql, options.params ?? []);
      store.set({ data, error: null, isLoading: false });
    } catch (error) {
      store.set({ data: null, error: (error as Error).message, isLoading: false });
    }
  };

  void refetch();

  const unsubscribeFlux = options.realtimeTopic
    ? client.subscribe(options.realtimeTopic, () => void refetch())
    : () => {};

  return Object.assign(store, { refetch, unsubscribeFlux });
}

export interface MutationState<T> {
  data: QueryResult<T> | null;
  error: string | null;
  isLoading: boolean;
}

export interface MutationHandle<T> extends Store<MutationState<T>> {
  mutate(params?: unknown[]): Promise<QueryResult<T>>;
}

export function useKeystoneMutation<T = Record<string, string | null>>(client: KeystoneClient, sql: string): MutationHandle<T> {
  const store = new Store<MutationState<T>>({ data: null, error: null, isLoading: false });

  const mutate = async (params: unknown[] = []): Promise<QueryResult<T>> => {
    store.set({ data: null, error: null, isLoading: true });
    try {
      const data = await client.query<T>(sql, params);
      store.set({ data, error: null, isLoading: false });
      return data;
    } catch (error) {
      store.set({ data: null, error: (error as Error).message, isLoading: false });
      throw error;
    }
  };

  return Object.assign(store, { mutate });
}
