import type { Kv, KvAtomic, KvCommitResult, KvEntry, KvKey, KvPage } from "./kv.ts";
import { adoptAccountAttempt, markAccountIndexComplete } from "./account_store.ts";
import type { InvoiceRecord } from "./payment_types.ts";
import {
  StaleWakeupError,
  wakeTargetKey,
  type EntityJob,
  type EntityJobRunner,
  type WakeTarget,
  type WakeupPort,
} from "./wakeup.ts";

const PREFIX = ["uos_checkout", "v1", "payments"];
export const BUN_DUE_PREFIX = [...PREFIX, "bun_due"];
export const MAX_BUN_TIMER_DELAY = 2 ** 31 - 1;
export const BUN_ATTEMPT_LEASE_MS = 60_000;

export type BunQueuedJob = {
  schemaVersion: 1;
  target: WakeTarget;
  generation: string;
  dueAt: number;
  job?: EntityJob;
  lease?: { token: string; expiresAt: number };
};

type TimerHandle = ReturnType<typeof setTimeout>;
type QueueOptions = {
  kv: Kv;
  runner: () => EntityJobRunner;
  now?: () => number;
  timers?: {
    set(callback: () => void, delay: number): TimerHandle;
    clear(handle: TimerHandle): void;
  };
  onError?: (error: unknown) => void;
};

export function bunJobKey(target: WakeTarget): KvKey {
  return [...PREFIX, "bun_jobs", wakeTargetKey(target)];
}

function dueKey(job: BunQueuedJob): KvKey {
  return [
    ...BUN_DUE_PREFIX,
    String(job.dueAt).padStart(16, "0"),
    job.generation,
    wakeTargetKey(job.target),
  ];
}

/** One durable local queue; only committed work can arm its one-shot timer. */
export class BunDelayedQueue implements WakeupPort {
  private readonly now: () => number;
  private readonly timers: NonNullable<QueueOptions["timers"]>;
  private readonly onError: (error: unknown) => void;
  private readonly pending = new Set<Promise<unknown>>();
  private readonly leases = new Map<string, KvEntry<BunQueuedJob>>();
  private timer: TimerHandle | undefined;
  private timerGeneration = 0;
  private startup: Promise<void> | undefined;
  private refreshing: Promise<void> | undefined;
  private dirty = false;
  private started = false;
  private closed = false;
  private delivering = false;

  constructor(private readonly options: QueueOptions) {
    this.now = options.now ?? Date.now;
    this.timers = options.timers ?? { set: setTimeout, clear: clearTimeout };
    this.onError = options.onError ?? ((error) => console.error("checkout_queue_failed", error));
  }

  start(): Promise<void> {
    if (this.closed) return Promise.reject(new StaleWakeupError());
    if (!this.startup) {
      this.started = true;
      this.startup = this.track(
        this.adoptExisting()
          .then(() => this.refresh())
          .catch((error) => {
            this.startup = undefined;
            throw error;
          }),
      );
    }
    return this.startup;
  }

  commit(
    atomic: KvAtomic,
    requested: readonly WakeTarget[],
    isCurrent?: () => Promise<boolean>,
  ): Promise<KvCommitResult> {
    return this.track(this.enqueue(atomic, requested, isCurrent));
  }

  ensure(target: WakeTarget): Promise<void> {
    return this.track(this.ensureTarget(target).then(() => this.refresh()));
  }

  /** A related request or successful write is also a recovery event. */
  refresh(): Promise<void> {
    if (this.closed || !this.started) return Promise.resolve();
    this.dirty = true;
    if (!this.refreshing) {
      this.refreshing = this.track(
        this.refreshHead().finally(() => {
          this.refreshing = undefined;
        }),
      );
    }
    return this.refreshing;
  }

  async close(): Promise<void> {
    this.closed = true;
    this.started = false;
    this.clearTimer();
    while (this.pending.size) await Promise.allSettled([...this.pending]);
    for (const entry of this.leases.values()) {
      const { lease: _lease, ...queued } = entry.value;
      queued.dueAt = queued.job?.dueAt ?? queued.target.dueAt!;
      await this.options.kv
        .atomic()
        .check(entry)
        .delete(dueKey(entry.value))
        .set(entry.key, queued)
        .set(dueKey(queued), queued)
        .commit();
    }
    this.leases.clear();
  }

  private track<T>(promise: Promise<T>): Promise<T> {
    this.pending.add(promise);
    void promise.then(
      () => this.pending.delete(promise),
      () => this.pending.delete(promise),
    );
    return promise;
  }

  private requireOpen(): void {
    if (this.closed) throw new StaleWakeupError();
  }

  private async enqueue(
    atomic: KvAtomic,
    requested: readonly WakeTarget[],
    isCurrent?: () => Promise<boolean>,
  ): Promise<KvCommitResult> {
    this.requireOpen();
    const targets = [
      ...new Map(requested.map((target) => [wakeTargetKey(target), target])).values(),
    ];
    const previous = await this.options.kv.getMany<BunQueuedJob[]>(targets.map(bunJobKey));
    this.requireOpen();
    targets.forEach((target, index) => {
      const entry = previous[index];
      atomic.check(entry);
      if (entry.value) atomic.delete(dueKey(entry.value));
      if (target.dueAt === null) atomic.delete(entry.key);
      else {
        const queued: BunQueuedJob = {
          schemaVersion: 1,
          target,
          generation: crypto.randomUUID(),
          dueAt: target.dueAt,
        };
        atomic.set(entry.key, queued).set(dueKey(queued), queued);
      }
    });
    if (isCurrent && !(await isCurrent())) throw new StaleWakeupError();
    this.requireOpen();
    const result = await atomic.commit();
    if (result.ok && !this.closed) void this.refresh().catch(this.onError);
    return result;
  }

  private async refreshHead(): Promise<void> {
    while (this.dirty && !this.closed) {
      this.dirty = false;
      this.clearTimer();
      if (this.delivering) return;
      const head = await this.options.kv.listPage<BunQueuedJob>(BUN_DUE_PREFIX, { limit: 1 });
      if (this.closed) return;
      if (this.dirty) continue;
      if (head.entries[0]) this.arm(head.entries[0].value);
    }
  }

  private clearTimer(): void {
    this.timerGeneration++;
    if (this.timer !== undefined) this.timers.clear(this.timer);
    this.timer = undefined;
  }

  private arm(queued: BunQueuedJob): void {
    if (this.closed) return;
    const generation = ++this.timerGeneration;
    const delay = Math.min(MAX_BUN_TIMER_DELAY, Math.max(0, queued.dueAt - this.now()));
    this.timer = this.timers.set(() => {
      if (this.closed || generation !== this.timerGeneration) return;
      this.timer = undefined;
      // Long delays continue toward the same committed deadline, without a sweep.
      if (this.now() < queued.dueAt) {
        void this.track(this.continueDelay(queued, generation)).catch(this.onError);
        return;
      }
      this.delivering = true;
      void this.track(this.deliver(queued)).then(
        () => this.finishDelivery(),
        (error) => {
          this.onError(error);
          this.finishDelivery();
        },
      );
    }, delay);
  }

  private async continueDelay(queued: BunQueuedJob, generation: number): Promise<void> {
    const current = await this.options.kv.get<BunQueuedJob>(bunJobKey(queued.target));
    if (this.closed || generation !== this.timerGeneration) return;
    if (current.value?.generation === queued.generation && current.value.dueAt === queued.dueAt)
      this.arm(queued);
    else await this.refresh();
  }

  private finishDelivery(): void {
    this.delivering = false;
    void this.refresh().catch(this.onError);
  }

  private async deliver(queued: BunQueuedJob): Promise<void> {
    const initial = await this.options.kv.get<BunQueuedJob>(bunJobKey(queued.target));
    if (this.closed || !initial.value || initial.value.generation !== queued.generation) return;
    if (
      initial.value.dueAt > this.now() ||
      (initial.value.lease && initial.value.lease.expiresAt > this.now())
    )
      return;
    const expiresAt = this.now() + BUN_ATTEMPT_LEASE_MS;
    const claimed: BunQueuedJob = {
      ...initial.value,
      dueAt: expiresAt,
      lease: { token: crypto.randomUUID(), expiresAt },
    };
    // The attempt event records its own crash-recovery deadline before external work.
    const result = await this.options.kv
      .atomic()
      .check(initial)
      .delete(dueKey(initial.value))
      .set(initial.key, claimed)
      .set(dueKey(claimed), claimed)
      .commit();
    if (!result.ok) return;
    let entry: KvEntry<BunQueuedJob> = {
      key: initial.key,
      value: claimed,
      versionstamp: result.versionstamp,
    };
    this.leases.set(JSON.stringify(entry.key), entry);
    if (this.closed) return;
    let job = entry.value.job;
    if (!job) {
      job = (await this.options.runner().plan(queued.target, undefined, this.now())) ?? undefined;
      if (!(await this.isCurrent(entry))) return;
      const planned = await this.replace(
        entry,
        job ?? null,
        Boolean(job && job.dueAt <= this.now()),
      );
      if (!planned || !job || job.dueAt > this.now()) return;
      entry = planned;
    }
    if (!(await this.isCurrent(entry))) return;
    const current = () => this.isCurrent(entry);
    const next = await this.options.runner().run(job, current, this.now());
    if (!(await current())) return;
    await this.replace(entry, next);
  }

  private async isCurrent(entry: KvEntry<BunQueuedJob>): Promise<boolean> {
    if (this.closed) return false;
    const current = await this.options.kv.get<BunQueuedJob>(entry.key);
    return (
      !this.closed &&
      current.versionstamp === entry.versionstamp &&
      current.value?.generation === entry.value.generation &&
      current.value.lease?.token === entry.value.lease?.token &&
      Boolean(current.value.lease && current.value.lease.expiresAt > this.now())
    );
  }

  private async replace(
    entry: KvEntry<BunQueuedJob>,
    job: EntityJob | null,
    retainLease = false,
  ): Promise<KvEntry<BunQueuedJob> | null> {
    if (this.closed) return null;
    const atomic = this.options.kv.atomic().check(entry).delete(dueKey(entry.value));
    if (!job) {
      await atomic.delete(entry.key).commit();
      this.leases.delete(JSON.stringify(entry.key));
      return null;
    }
    const queued: BunQueuedJob = {
      schemaVersion: 1,
      target: job.target,
      generation: job.generation,
      dueAt: retainLease ? entry.value.lease!.expiresAt : job.dueAt,
      job,
      ...(retainLease ? { lease: entry.value.lease } : {}),
    };
    const result = await atomic.set(entry.key, queued).set(dueKey(queued), queued).commit();
    if (!result.ok) return null;
    const next = { key: entry.key, value: queued, versionstamp: result.versionstamp };
    if (retainLease) this.leases.set(JSON.stringify(entry.key), next);
    else this.leases.delete(JSON.stringify(entry.key));
    return next;
  }

  private async ensureTarget(target: WakeTarget, source?: KvEntry<{ id: string }>): Promise<void> {
    this.requireOpen();
    const key = [...PREFIX, target.kind === "invoice" ? "invoices" : "events", target.id];
    const canonical = source ?? (await this.options.kv.get<{ id: string }>(key));
    const existing = await this.options.kv.get<BunQueuedJob>(bunJobKey(target));
    this.requireOpen();
    const job = await this.options.runner().plan(target, existing.value?.job, this.now());
    this.requireOpen();
    if (existing.value) {
      if (job && existing.value.job?.sourceVersion === job.sourceVersion) return;
      // A seed already has an earlier known deadline; planning then reads fresh state.
      if (job && !existing.value.job && job.dueAt >= existing.value.dueAt) return;
      if (
        !existing.value.job &&
        existing.value.lease &&
        existing.value.lease.expiresAt > this.now()
      )
        return;
    } else if (!job) return;
    const atomic = this.options.kv.atomic().check(existing, {
      key,
      versionstamp: job?.sourceVersion ?? canonical.versionstamp,
    });
    if (existing.value) atomic.delete(dueKey(existing.value));
    if (!job) atomic.delete(existing.key);
    else {
      const queued: BunQueuedJob = {
        schemaVersion: 1,
        target: job.target,
        generation: job.generation,
        dueAt: job.dueAt,
        job,
      };
      atomic.set(existing.key, queued).set(dueKey(queued), queued);
    }
    this.requireOpen();
    await atomic.commit();
  }

  /** One finite pass per process startup; pages are bounded and never scheduled. */
  private async adoptExisting(): Promise<void> {
    for (const [kind, collection] of [
      ["invoice", "invoices"],
      ["delivery", "events"],
    ] as const) {
      let cursor: string | null = null;
      do {
        const page: KvPage<{ id: string }> = await this.options.kv.listPage<{ id: string }>(
          [...PREFIX, collection],
          {
            limit: 100,
            cursor,
          },
        );
        this.requireOpen();
        for (const source of page.entries) {
          if (kind === "invoice") {
            const adopted = await adoptAccountAttempt(
              this.options.kv,
              source as KvEntry<InvoiceRecord>,
              async () => !this.closed,
            );
            if (!adopted) throw new Error("account_index_adoption_conflict");
            this.requireOpen();
          }
          const target: WakeTarget = {
            kind,
            id: source.value.id,
            dueAt: this.now(),
            reason: kind === "invoice" ? "expiry" : "delivery",
          };
          await this.ensureTarget(target, source);
          this.requireOpen();
        }
        cursor = page.cursor;
      } while (cursor);
      if (kind === "invoice") {
        this.requireOpen();
        await markAccountIndexComplete(this.options.kv);
        this.requireOpen();
      }
    }
  }
}
