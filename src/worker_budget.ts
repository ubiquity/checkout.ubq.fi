import type { FetchLike } from "./rpc.ts";

/** Leave headroom beneath the Free plan's 50-subrequest ceiling. */
export class WorkerRequestBudget {
  private used = 0;
  private held = 0;
  private nested = 0;
  constructor(
    private readonly maximum: number,
    private readonly signal?: AbortSignal,
  ) {}
  get remaining(): number {
    return this.maximum - this.used - this.held - this.nested;
  }
  get limit(): number {
    return this.maximum;
  }
  get nestedInvocations(): number {
    return this.nested;
  }
  /** Charge a binding's full nested upper bound before issuing it, preserving one final call. */
  reserveNestedInvocations(calls: number): void {
    if (!Number.isSafeInteger(calls) || calls < 0 || !this.hasCapacity(calls + 2))
      throw new WorkerBudgetError();
    this.nested += calls;
  }
  hasCapacity(calls: number): boolean {
    return !this.signal?.aborted && this.remaining >= calls;
  }
  /** Keep persistence and the HTTP response possible after a search yields. */
  reserveForCompletion(calls: number): () => void {
    if (!this.hasCapacity(calls)) throw new WorkerBudgetError();
    this.held += calls;
    let released = false;
    return () => {
      if (!released) this.held -= calls;
      released = true;
    };
  }
  reserve(): void {
    this.signal?.throwIfAborted();
    if (this.remaining <= 0) throw new WorkerBudgetError();
    this.used++;
  }
  wrap(fetcher: FetchLike): FetchLike {
    return (input, init) => {
      this.reserve();
      const signals = [this.signal, init?.signal].filter((value): value is AbortSignal =>
        Boolean(value),
      );
      return fetcher(input, {
        ...init,
        signal: signals.length ? AbortSignal.any(signals) : undefined,
      });
    };
  }
}

export class WorkerBudgetError extends Error {
  readonly status = 503;
  constructor() {
    super("worker_subrequest_budget_exhausted");
  }
}
