import assert from "node:assert/strict";
import { test } from "node:test";

import { PluginRegistry, definePlugin } from "./plugin.js";
import type { CanvasPlugin, CanvasPluginContext } from "./plugin.js";
import type { QueryResult } from "./client.js";

const emptyResult: QueryResult = { columns: [], rows: [], raw: { columns: [], rows: [] } };
const surface: CanvasPluginContext = { ctx: {} as CanvasRenderingContext2D, width: 100, height: 50 };

function makePlugin(overrides: Partial<CanvasPlugin> = {}): CanvasPlugin & { mounted: boolean; unmounted: boolean } {
  const state = { mounted: false, unmounted: false };
  return definePlugin({
    name: "heatmap",
    setup(registry) {
      registry.registerComponent({
        name: "Heatmap",
        render: () => {},
      });
    },
    mount() {
      state.mounted = true;
    },
    unmount() {
      state.unmounted = true;
    },
    ...overrides,
  }) as CanvasPlugin & { mounted: boolean; unmounted: boolean };
}

test("install runs setup then mount, and registers components", () => {
  const registry = new PluginRegistry();
  const plugin = makePlugin();

  registry.install(plugin);

  assert.deepEqual(registry.list(), ["Heatmap"]);
  assert.deepEqual(registry.installedPlugins(), ["heatmap"]);
});

test("registerComponent outside of setup throws", () => {
  const registry = new PluginRegistry();
  assert.throws(() => registry.registerComponent({ name: "X", render: () => {} }), /must be called from within a plugin's setup/);
});

test("installing the same plugin twice throws", () => {
  const registry = new PluginRegistry();
  registry.install(makePlugin());
  assert.throws(() => registry.install(makePlugin()), /already installed/);
});

test("mountComponent renders immediately and update() re-renders", () => {
  const registry = new PluginRegistry();
  const calls: unknown[] = [];
  registry.install(
    definePlugin({
      name: "counter",
      setup(r) {
        r.registerComponent<{ label: string }>({
          name: "Counter",
          render: (data, props) => calls.push([data, props]),
        });
      },
    }),
  );

  const handle = registry.mountComponent("Counter", surface, emptyResult, { label: "a" });
  assert.equal(calls.length, 1);

  handle.update(emptyResult, { label: "b" });
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[1], [emptyResult, { label: "b" }]);

  handle.unmount();
  assert.throws(() => handle.update(emptyResult, { label: "c" }), /Cannot update unmounted component/);
  assert.equal(calls.length, 2);

  // Unmounting twice is a no-op, not an error.
  handle.unmount();
});

test("uninstall tears down live component instances and calls unmount", () => {
  const registry = new PluginRegistry();
  let torndown = false;
  registry.install(
    definePlugin({
      name: "gauge",
      setup(r) {
        r.registerComponent({
          name: "Gauge",
          render: () => {},
        });
      },
      unmount() {
        torndown = true;
      },
    }),
  );

  const handle = registry.mountComponent("Gauge", surface, emptyResult, {});
  registry.uninstall("gauge");

  assert.equal(torndown, true);
  assert.deepEqual(registry.list(), []);
  assert.deepEqual(registry.installedPlugins(), []);
  assert.throws(() => handle.update(emptyResult, {}), /Cannot update unmounted component/);
});

test("mountComponent on an unregistered component throws", () => {
  const registry = new PluginRegistry();
  assert.throws(() => registry.mountComponent("Nope", surface, emptyResult, {}), /is not registered/);
});

test("registry.events lets one plugin's setup notify another", () => {
  const registry = new PluginRegistry();
  const received: number[] = [];

  registry.install(
    definePlugin({
      name: "publisher",
      setup(r) {
        r.registerComponent({
          name: "Pub",
          render: () => r.events.emit("pub:tick", 1),
        });
      },
    }),
  );
  registry.install(
    definePlugin({
      name: "subscriber",
      setup(r) {
        r.events.on<number>("pub:tick", (n) => received.push(n));
      },
    }),
  );

  registry.mountComponent("Pub", surface, emptyResult, {});
  assert.deepEqual(received, [1]);
});
