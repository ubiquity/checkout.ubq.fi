import { SqliteKv } from "./sqlite_kv.ts";
import { WorkerRequestBudget } from "./worker_budget.ts";
import {
  createWorkerScheduler,
  type SchedulerStorage,
  type SchedulerTransaction,
} from "./worker_scheduler.ts";
import type { EntityJob, EntityJobRunner, WakeTarget } from "./wakeup.ts";

export class MemorySchedulerStorage implements SchedulerStorage {
  values = new Map<string, unknown>();
  alarm: number | null = null;
  private queued: Promise<void> = Promise.resolve();
  async get<T>(key: string): Promise<T | undefined> {
    return structuredClone(this.values.get(key)) as T | undefined;
  }
  async put<T>(key: string, value: T): Promise<void> {
    this.values.set(key, structuredClone(value));
  }
  async getAlarm() {
    return this.alarm;
  }
  async setAlarm(time: number) {
    this.alarm = time;
  }
  async deleteAlarm() {
    this.alarm = null;
  }
  transaction<T>(action: (transaction: SchedulerTransaction) => Promise<T>): Promise<T> {
    const operation = this.queued.then(async () => {
      const values = structuredClone(this.values);
      let alarm = this.alarm;
      const transaction: SchedulerTransaction = {
        get: async <V>(key: string) => structuredClone(values.get(key)) as V | undefined,
        put: async (key, value) => {
          values.set(key, structuredClone(value));
        },
        getAlarm: async () => alarm,
        setAlarm: async (time) => {
          alarm = time;
        },
        deleteAlarm: async () => {
          alarm = null;
        },
      };
      const result = await action(transaction);
      this.values = values;
      this.alarm = alarm;
      return result;
    });
    this.queued = operation.then(
      () => {},
      () => {},
    );
    return operation;
  }
}

export function schedulerFixture() {
  const kv = new SqliteKv();
  let time = 1_000;
  let version = "v1";
  const storage = new MemorySchedulerStorage();
  const fixture = {
    kv,
    storage,
    now: () => time,
    advance: (milliseconds: number) => {
      time += milliseconds;
    },
    version: (value: string) => {
      version = value;
    },
    calls: 0,
    onRun: async (_job: EntityJob, _isCurrent: () => Promise<boolean>): Promise<EntityJob | null> =>
      null,
  };
  const runner: EntityJobRunner = {
    async plan(target, previous) {
      if (target.dueAt === null || version === "terminal") return null;
      if (previous?.sourceVersion === version) return previous;
      return {
        schemaVersion: 1,
        target,
        generation: `${target.id}:${version}`,
        sourceVersion: version,
        dueAt: target.dueAt,
      };
    },
    async run(job, isCurrent) {
      fixture.calls++;
      return fixture.onRun(job, isCurrent);
    },
  };
  const controller = createWorkerScheduler(
    storage,
    (_guard, budget) => ({
      kv,
      runner,
      budget: budget ?? new WorkerRequestBudget(24),
    }),
    fixture.now,
  );
  return Object.assign(fixture, { controller });
}

export const invoiceTarget = (id = "invoice", dueAt: number | null = 1_000): WakeTarget => ({
  kind: "invoice",
  id,
  dueAt,
  reason: dueAt === null ? "cancel" : "expiry",
});
