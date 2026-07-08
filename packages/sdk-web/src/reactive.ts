// Minimal framework-agnostic observable value, in the spirit of
// `tpt-canvas/src/reactive.rs`'s `Signal` (real dependency notification, no
// batching or cleanup graph — same documented scope cut). Kept separate
// from React so the core hooks in hooks.ts work unmodified under any
// framework (or none) via plain `subscribe`/`get`; `react.ts` is the only
// file that knows React exists.

export type Listener<T> = (value: T) => void;
export type Unsubscribe = () => void;

export class Store<T> {
  private value: T;
  private readonly listeners = new Set<Listener<T>>();

  constructor(initial: T) {
    this.value = initial;
  }

  get(): T {
    return this.value;
  }

  set(next: T): void {
    if (Object.is(next, this.value)) return;
    this.value = next;
    for (const listener of this.listeners) listener(next);
  }

  update(fn: (prev: T) => T): void {
    this.set(fn(this.value));
  }

  subscribe(listener: Listener<T>): Unsubscribe {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
}
