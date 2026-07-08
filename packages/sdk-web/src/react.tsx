// React adapter over hooks.ts's framework-agnostic Store, via
// useSyncExternalStore (React 18+). Import from "@tpt/sdk-web/react" — the
// core package (index.ts) never imports React, so non-React consumers pay
// nothing for this file existing.

import { useEffect, useMemo } from "react";
import { useSyncExternalStore } from "react";

import type { KeystoneClient } from "./client.js";
import { useKeystoneMutation as useKeystoneMutationCore, useKeystoneQuery as useKeystoneQueryCore, type UseKeystoneQueryOptions } from "./hooks.js";

export function useKeystoneQuery<T = Record<string, string | null>>(
  client: KeystoneClient,
  sql: string,
  options: UseKeystoneQueryOptions = {},
) {
  const paramsKey = JSON.stringify(options.params ?? []);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const handle = useMemo(() => useKeystoneQueryCore<T>(client, sql, options), [client, sql, paramsKey, options.realtimeTopic]);

  useEffect(() => handle.unsubscribeFlux, [handle]);

  const state = useSyncExternalStore(
    (listener) => handle.subscribe(listener),
    () => handle.get(),
    () => handle.get(),
  );

  return { ...state, refetch: handle.refetch };
}

export function useKeystoneMutation<T = Record<string, string | null>>(client: KeystoneClient, sql: string) {
  const handle = useMemo(() => useKeystoneMutationCore<T>(client, sql), [client, sql]);

  const state = useSyncExternalStore(
    (listener) => handle.subscribe(listener),
    () => handle.get(),
    () => handle.get(),
  );

  return { ...state, mutate: handle.mutate };
}
