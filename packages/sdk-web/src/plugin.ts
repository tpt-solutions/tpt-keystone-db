// Plugin API for custom Canvas components (TODO.md Phase 14, "SDK/Plugin
// (Canvas Extensions)"). `tpt-canvas` itself renders via
// `web_sys::CanvasRenderingContext2d` inside its WASM bundle (not WebGPU —
// see `tpt-canvas/src/lib.rs`'s documented scope cut), so a component's
// default render path draws into a 2D canvas context supplied by the host.
// Components that want real hardware-accelerated rendering can instead (or
// additionally) implement `renderGpu`, which receives a genuine WebGPU
// compute+fragment-shader context (`plugin-gpu.ts`) — see that module's
// header for why this lives one layer above `tpt-canvas` rather than in it.

import type { QueryResult } from "./client.js";
import { PluginEventBus } from "./plugin-events.js";
import type { CanvasGpuContext } from "./plugin-gpu.js";

export interface CanvasPluginContext {
  ctx: CanvasRenderingContext2D;
  width: number;
  height: number;
}

export interface CanvasComponentDefinition<P = Record<string, unknown>> {
  name: string;
  render(data: QueryResult, props: P, canvas: CanvasPluginContext): void;
  /** Optional hardware-accelerated path; see module header. */
  renderGpu?(data: QueryResult, props: P, gpu: CanvasGpuContext): void | Promise<void>;
}

export interface CanvasPlugin {
  name: string;
  /** Called once, synchronously, when the plugin is installed — register components and event listeners here. */
  setup(registry: PluginRegistry): void;
  /** Called once, immediately after `setup`, when the plugin becomes active. */
  mount?(registry: PluginRegistry): void;
  /** Called once when the plugin is uninstalled, after its live component instances have been torn down. */
  unmount?(registry: PluginRegistry): void;
}

export interface MountedComponent<P = Record<string, unknown>> {
  readonly componentName: string;
  /** Re-invokes `render` with new data/props against the same surface. */
  update(data: QueryResult, props?: P): void;
  /** Tears this instance down; safe to call more than once. */
  unmount(): void;
}

interface OwnedComponent {
  pluginName: string;
  definition: CanvasComponentDefinition<any>;
}

export class PluginRegistry {
  private readonly components = new Map<string, OwnedComponent>();
  private readonly installed = new Map<string, CanvasPlugin>();
  private readonly instancesByPlugin = new Map<string, Set<MountedComponent<any>>>();
  /** Shared bus every installed plugin can publish/subscribe on to talk to each other. */
  readonly events = new PluginEventBus();

  /** Currently-installing plugin, so `registerComponent` can record ownership without every call site passing it explicitly. */
  private installingPlugin: string | null = null;

  /** Registers a component definition. Must be called from within a plugin's `setup`. */
  registerComponent<P>(definition: CanvasComponentDefinition<P>): void {
    if (this.components.has(definition.name)) {
      throw new Error(`Canvas component "${definition.name}" is already registered`);
    }
    if (!this.installingPlugin) {
      throw new Error(`registerComponent("${definition.name}") must be called from within a plugin's setup()`);
    }
    this.components.set(definition.name, { pluginName: this.installingPlugin, definition });
  }

  get(name: string): CanvasComponentDefinition<any> | undefined {
    return this.components.get(name)?.definition;
  }

  list(): string[] {
    return [...this.components.keys()];
  }

  /** Plugin lifecycle: register components/listeners (`setup`) then activate (`mount`). Throws if already installed. */
  install(plugin: CanvasPlugin): void {
    if (this.installed.has(plugin.name)) {
      throw new Error(`Plugin "${plugin.name}" is already installed`);
    }
    this.installingPlugin = plugin.name;
    try {
      plugin.setup(this);
    } finally {
      this.installingPlugin = null;
    }
    this.installed.set(plugin.name, plugin);
    plugin.mount?.(this);
  }

  /** Plugin lifecycle: unmounts any live component instances the plugin owns, then calls its `unmount` hook and forgets it. */
  uninstall(pluginName: string): void {
    const plugin = this.installed.get(pluginName);
    if (!plugin) return;

    for (const instance of [...(this.instancesByPlugin.get(pluginName) ?? [])]) {
      instance.unmount();
    }
    this.instancesByPlugin.delete(pluginName);

    for (const [name, owned] of [...this.components]) {
      if (owned.pluginName === pluginName) this.components.delete(name);
    }

    plugin.unmount?.(this);
    this.installed.delete(pluginName);
  }

  installedPlugins(): string[] {
    return [...this.installed.keys()];
  }

  /** Component-instance lifecycle: renders immediately and returns a handle to re-render or unmount this instance. */
  mountComponent<P>(name: string, surface: CanvasPluginContext, data: QueryResult, props: P): MountedComponent<P> {
    const owned = this.components.get(name);
    if (!owned) {
      throw new Error(`Canvas component "${name}" is not registered`);
    }
    const { pluginName, definition } = owned;

    let live = true;
    const instance: MountedComponent<P> = {
      componentName: name,
      update: (nextData, nextProps) => {
        if (!live) throw new Error(`Cannot update unmounted component "${name}"`);
        definition.render(nextData, nextProps ?? props, surface);
      },
      unmount: () => {
        if (!live) return;
        live = false;
        this.instancesByPlugin.get(pluginName)?.delete(instance);
      },
    };

    let set = this.instancesByPlugin.get(pluginName);
    if (!set) {
      set = new Set();
      this.instancesByPlugin.set(pluginName, set);
    }
    set.add(instance);

    definition.render(data, props, surface);
    return instance;
  }
}

export function definePlugin(plugin: CanvasPlugin): CanvasPlugin {
  return plugin;
}

export function installPlugins(plugins: CanvasPlugin[]): PluginRegistry {
  const registry = new PluginRegistry();
  for (const plugin of plugins) registry.install(plugin);
  return registry;
}

export { PluginEventBus } from "./plugin-events.js";
export type { PluginEventHandler } from "./plugin-events.js";
export { CanvasGpuContext, createGpuContext, isWebGpuSupported, GpuBufferUsage, GpuMapMode } from "./plugin-gpu.js";
export type { ComputeBufferSpec, RunComputeOptions, RenderFragmentOptions } from "./plugin-gpu.js";
export { validateManifest, loadManifest } from "./plugin-manifest.js";
export type { PluginManifest } from "./plugin-manifest.js";
