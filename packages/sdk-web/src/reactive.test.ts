import assert from "node:assert/strict";
import { test } from "node:test";

import { Store } from "./reactive.js";

test("Store notifies subscribers on change and not on identical value", () => {
  const store = new Store(1);
  const seen: number[] = [];
  const unsubscribe = store.subscribe((v) => seen.push(v));

  store.set(2);
  store.set(2);
  store.update((v) => v + 1);

  assert.deepEqual(seen, [2, 3]);
  assert.equal(store.get(), 3);

  unsubscribe();
  store.set(4);
  assert.deepEqual(seen, [2, 3]);
});
