// Plugin API for custom Canvas components (TODO.md Phase 14, "Plugin API
// for custom Canvas components"). `tpt-canvas` itself renders via
// `web_sys::CanvasRenderingContext2d` inside its WASM bundle (not WebGPU —
// see `tpt-canvas/src/lib.rs`'s documented scope cut), so there is no
// shader pipeline on the Rust side for a plugin to hook into. What a
// JS-side plugin *can* do is register a component that receives the same
// `QueryResult` a `<Canvas.*>` component would and draws into a 2D canvas
// context supplied by the host — that's the surface this registry exposes.

import type { QueryResult } from "./client.js";

export interface CanvasPluginContext {
  ctx: CanvasRenderingContext2D;
  width: number;
  height: number;
}

export interface CanvasComponentDefinition<P = Record<string, unknown>> {
  name: string;
  render(data: QueryResult, props: P, canvas: CanvasPluginContext): void;
}

export interface CanvasPlugin {
  name: string;
  setup(registry: PluginRegistry): void;
}

export class PluginRegistry {
  private readonly components = new Map<string, CanvasComponentDefinition<any>>();

  registerComponent<P>(definition: CanvasComponentDefinition<P>): void {
    if (this.components.has(definition.name)) {
      throw new Error(`Canvas component "${definition.name}" is already registered`);
    }
    this.components.set(definition.name, definition);
  }

  get(name: string): CanvasComponentDefinition<any> | undefined {
    return this.components.get(name);
  }

  list(): string[] {
    return [...this.components.keys()];
  }
}

export function definePlugin(plugin: CanvasPlugin): CanvasPlugin {
  return plugin;
}

export function installPlugins(plugins: CanvasPlugin[]): PluginRegistry {
  const registry = new PluginRegistry();
  for (const plugin of plugins) plugin.setup(registry);
  return registry;
}
