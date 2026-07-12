import assert from "node:assert/strict";
import { test } from "node:test";

import { QueryStore, type QueryState } from "./store.js";

test("useSyncStore-equivalent: subscribe emits current state immediately", () => {
  // Exercises the same store contract the React hooks rely on, without
  // importing react in a Node test.
  const store = new QueryStore<number>();
  const seen: QueryState<number>[] = [];
  store.subscribe((s) => seen.push(s));
  store.setState({ data: 42, loading: false });

  assert.equal(seen.length, 1);
  assert.equal(seen[0].data, 42);
  assert.equal(store.getState().data, 42);
});
