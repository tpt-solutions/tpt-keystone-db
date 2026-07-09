import assert from "node:assert/strict";
import { test } from "node:test";

import { PluginEventBus } from "./plugin-events.js";

test("emit calls every subscribed handler with the payload", () => {
  const bus = new PluginEventBus();
  const seen: string[] = [];
  bus.on<string>("greet", (name) => seen.push(name));
  bus.on<string>("greet", (name) => seen.push(name.toUpperCase()));

  bus.emit("greet", "ada");

  assert.deepEqual(seen, ["ada", "ADA"]);
});

test("off removes a specific handler without affecting others", () => {
  const bus = new PluginEventBus();
  const seen: number[] = [];
  const handler = (n: number) => seen.push(n);
  bus.on<number>("tick", handler);
  bus.on<number>("tick", (n) => seen.push(n * 10));

  bus.off("tick", handler);
  bus.emit("tick", 1);

  assert.deepEqual(seen, [10]);
});

test("the unsubscribe function returned by on() works", () => {
  const bus = new PluginEventBus();
  const seen: number[] = [];
  const unsubscribe = bus.on<number>("tick", (n) => seen.push(n));

  unsubscribe();
  bus.emit("tick", 1);

  assert.deepEqual(seen, []);
});

test("once fires exactly one time", () => {
  const bus = new PluginEventBus();
  const seen: number[] = [];
  bus.once<number>("tick", (n) => seen.push(n));

  bus.emit("tick", 1);
  bus.emit("tick", 2);

  assert.deepEqual(seen, [1]);
});

test("a handler that unsubscribes itself mid-emit doesn't disrupt other handlers", () => {
  const bus = new PluginEventBus();
  const seen: string[] = [];
  const self = (n: number) => {
    seen.push("self");
    bus.off("tick", self);
  };
  bus.on<number>("tick", self);
  bus.on<number>("tick", () => seen.push("other"));

  bus.emit("tick", 1);

  assert.deepEqual(seen, ["self", "other"]);
});

test("clear(event) only clears that event; clear() clears everything", () => {
  const bus = new PluginEventBus();
  const seen: string[] = [];
  bus.on("a", () => seen.push("a"));
  bus.on("b", () => seen.push("b"));

  bus.clear("a");
  bus.emit("a", undefined);
  bus.emit("b", undefined);
  assert.deepEqual(seen, ["b"]);

  bus.clear();
  bus.emit("b", undefined);
  assert.deepEqual(seen, ["b"]);
});
