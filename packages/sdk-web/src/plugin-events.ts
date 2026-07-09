// Inter-plugin event system (TODO.md Phase 14, "Inter-plugin event
// system"). A plain synchronous pub/sub bus shared by every plugin
// installed into a `PluginRegistry` (see `plugin.ts`), so one plugin can
// react to another's data without either importing the other directly.

export type PluginEventHandler<T = unknown> = (payload: T) => void;

export class PluginEventBus {
  private readonly handlers = new Map<string, Set<PluginEventHandler<any>>>();

  on<T = unknown>(event: string, handler: PluginEventHandler<T>): () => void {
    let set = this.handlers.get(event);
    if (!set) {
      set = new Set();
      this.handlers.set(event, set);
    }
    set.add(handler as PluginEventHandler<any>);
    return () => this.off(event, handler);
  }

  once<T = unknown>(event: string, handler: PluginEventHandler<T>): () => void {
    const wrapped: PluginEventHandler<T> = (payload) => {
      this.off(event, wrapped);
      handler(payload);
    };
    return this.on(event, wrapped);
  }

  off<T = unknown>(event: string, handler: PluginEventHandler<T>): void {
    this.handlers.get(event)?.delete(handler as PluginEventHandler<any>);
  }

  emit<T = unknown>(event: string, payload: T): void {
    const set = this.handlers.get(event);
    if (!set || set.size === 0) return;
    // Snapshot before iterating: a handler may call `off`/`once` (which
    // calls `off`) on itself mid-emit, and mutating a Set while iterating
    // it would otherwise skip or double-fire neighboring handlers.
    for (const handler of [...set]) handler(payload);
  }

  clear(event?: string): void {
    if (event) this.handlers.delete(event);
    else this.handlers.clear();
  }
}
