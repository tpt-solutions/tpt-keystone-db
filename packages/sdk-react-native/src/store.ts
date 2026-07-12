// Framework-agnostic reactive store backing the React Native query hooks.
// Kept free of any `react` import so it is unit-testable under `node --test`.
// React adapters live in `./react.ts` as an optional peer dependency.

export interface QueryState<T> {
  data: T | null;
  error: Error | null;
  loading: boolean;
}

type Listener<T> = (state: QueryState<T>) => void;

export class QueryStore<T> {
  private state: QueryState<T> = { data: null, error: null, loading: true };
  private listeners = new Set<Listener<T>>();

  getState(): QueryState<T> {
    return this.state;
  }

  setState(patch: Partial<QueryState<T>>): void {
    this.state = { ...this.state, ...patch };
    for (const listener of this.listeners) listener(this.state);
  }

  subscribe(listener: Listener<T>): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
}
