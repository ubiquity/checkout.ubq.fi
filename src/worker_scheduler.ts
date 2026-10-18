import type { Kv } from "./kv.ts";
import type { WorkerRequestBudget } from "./worker_budget.ts";
import {
  acknowledgeWakeIntent,
  resolveWakeIntent,
  StaleWakeupError,
  wakeIntentKey,
  wakeTargetKey,
  type EntityJob,
  type EntityJobRunner,
  type PreparedWakeIntent,
  type WakeTarget,
  type WakeIntent,
} from "./wakeup.ts";

export const SCHEDULER_STATE_KEY = "checkout_scheduler_v1";
const RUN_LEASE_MS = 25_000;
const PREPARATION_LEASE_MS = 30_000;
type Pending = { target: WakeTarget; adopted: boolean };
export type SchedulerState = {
  revision: number;
  target?: string;
  job: EntityJob | null;
  pending: Record<string, Pending>;
  running?: { id: string; until: number; generation: string };
};

/** The portable subset used by the real SQLite-backed DO and offline tests. */
export type SchedulerTransaction = {
  get<T>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  getAlarm(): Promise<number | null>;
  setAlarm(time: number): Promise<void>;
  deleteAlarm(): Promise<void>;
};
export type SchedulerStorage = SchedulerTransaction & {
  transaction<T>(action: (transaction: SchedulerTransaction) => Promise<T>): Promise<T>;
};
export type SchedulerContext = { kv: Kv; runner: EntityJobRunner; budget: WorkerRequestBudget };

const empty = (): SchedulerState => ({ revision: 0, job: null, pending: {} });

export function createWorkerScheduler(
  storage: SchedulerStorage,
  context: (isCurrent?: () => Promise<boolean>, budget?: WorkerRequestBudget) => SchedulerContext,
  now: () => number = Date.now,
) {
  async function read(): Promise<SchedulerState> {
    return (await storage.get<SchedulerState>(SCHEDULER_STATE_KEY)) ?? empty();
  }

  async function change(action: (state: SchedulerState) => void, arm = true): Promise<void> {
    await storage.transaction(async (transaction) => {
      const state = (await transaction.get<SchedulerState>(SCHEDULER_STATE_KEY)) ?? empty();
      action(state);
      await transaction.put(SCHEDULER_STATE_KEY, state);
      if (!arm) return;
      const deadlines = Object.values(state.pending)
        .map((pending) => pending.target.dueAt)
        .filter((deadline): deadline is number => deadline !== null);
      if (state.job && (!state.running || state.running.until <= now()))
        deadlines.push(state.job.dueAt);
      const deadline = deadlines.length ? Math.min(...deadlines) : null;
      if (deadline === null) await transaction.deleteAlarm();
      else if ((await transaction.getAlarm()) !== deadline) await transaction.setAlarm(deadline);
    });
  }

  async function currentPending(operationId: string): Promise<boolean> {
    return Boolean((await read()).pending[operationId]);
  }

  async function adopt(target: WakeTarget, app: SchedulerContext): Promise<void> {
    const before = await read();
    const name = wakeTargetKey(target);
    if (before.target && before.target !== name) throw new TypeError("scheduler_target_conflict");
    const job = await app.runner.plan(target, before.job, now());
    await change((state) => {
      if (state.revision !== before.revision) throw new StaleWakeupError();
      state.target = name;
      if (!sameJob(state.job, job)) {
        state.job = job;
        state.revision++;
        delete state.running;
      }
    });
  }

  async function pending(app: SchedulerContext): Promise<void> {
    const registrations = Object.entries((await read()).pending).sort(
      (left, right) =>
        Number(left[1].target.dueAt !== null) - Number(right[1].target.dueAt !== null),
    );
    for (const [operationId, registration] of registrations) {
      if (!app.budget.hasCapacity(registration.target.dueAt === null ? 3 : 5)) return;
      const isCurrent = () => currentPending(operationId);
      const observed = await app.kv.get<WakeIntent>(wakeIntentKey(operationId));
      if (!(await isCurrent())) continue;
      if (
        observed.value?.state === "prepared" &&
        registration.target.dueAt !== null &&
        registration.target.dueAt > now()
      )
        continue;
      if (
        observed.value?.state === "prepared" &&
        observed.value.preparedAt + PREPARATION_LEASE_MS > now()
      )
        // A due delivery can race its healthy originating CAS. Retry this same native
        // alarm while preparation is owned, without creating a planning deadline.
        throw new Error("scheduler_intent_preparing");
      const intent =
        observed.value?.state === "prepared"
          ? await resolveWakeIntent(app.kv, operationId, isCurrent)
          : observed.value;
      if (!(await isCurrent())) continue;
      if (!intent) {
        await change((state) => {
          delete state.pending[operationId];
        });
        continue;
      }
      const name = wakeTargetKey(registration.target);
      const target = intent.targets.find((candidate) => wakeTargetKey(candidate) === name);
      if (!target) throw new TypeError("scheduler_intent_target_missing");
      const needsPlan =
        !registration.adopted &&
        !intent.adopted.includes(name) &&
        (target.dueAt !== null || (await read()).job !== null);
      // An expired preparation may have consumed extra CAS reads. Leave its
      // original alarm in place rather than exceed the related-call allowance.
      if (!app.budget.hasCapacity(needsPlan ? 4 : 2)) return;
      if (!registration.adopted && !intent.adopted.includes(name)) {
        // A terminal runner has already durably adopted its own cancellation.
        if (needsPlan) await adopt(target, app);
        await change((state) => {
          if (state.pending[operationId]) state.pending[operationId].adopted = true;
        });
      }
      if (await acknowledgeWakeIntent(app.kv, operationId, target, isCurrent))
        await change((state) => {
          delete state.pending[operationId];
        });
    }
  }

  return {
    async prepare(intent: PreparedWakeIntent, target: WakeTarget): Promise<void> {
      const name = wakeTargetKey(target);
      if (!intent.targets.some((candidate) => wakeTargetKey(candidate) === name))
        throw new TypeError("scheduler_intent_target_missing");
      await change((state) => {
        if (state.target && state.target !== name) throw new TypeError("scheduler_target_conflict");
        if (!state.pending[intent.operationId] && Object.keys(state.pending).length >= 64)
          throw new Error("scheduler_pending_capacity");
        state.target = name;
        state.pending[intent.operationId] ??= { target, adopted: false };
        // Registration alone must not fence the runner that is preparing its own CAS.
      });
    },
    async ensure(target: WakeTarget, budget?: WorkerRequestBudget): Promise<void> {
      const app = context(undefined, budget);
      await adopt(target, app);
      await pending(app);
    },
    async alarm(): Promise<void> {
      const app = context();
      await pending(app);
      const candidate = await read();
      if (!candidate.job || candidate.job.dueAt > now()) {
        await change(() => {});
        return;
      }
      if (!app.budget.hasCapacity(15)) {
        await change(() => {});
        return;
      }
      const runId = crypto.randomUUID();
      const job = candidate.job;
      await change((state) => {
        if (state.revision !== candidate.revision) throw new StaleWakeupError();
        if (state.running && state.running.until > now()) throw new Error("scheduler_job_owned");
        state.running = { id: runId, until: now() + RUN_LEASE_MS, generation: job.generation };
      }, false);
      const isCurrent = async () => {
        const state = await read();
        return (
          state.job?.generation === job.generation &&
          state.running?.id === runId &&
          state.running.until > now()
        );
      };
      const releaseCompletion = app.budget.reserveForCompletion(3);
      try {
        const running = context(isCurrent, app.budget);
        let next: EntityJob | null;
        try {
          next = await running.runner.run(job, isCurrent, now());
        } finally {
          releaseCompletion();
        }
        await change((state) => {
          if (
            state.running?.id !== runId ||
            state.job?.generation !== job.generation ||
            state.running.until <= now()
          )
            throw new StaleWakeupError();
          state.job = next;
          state.revision++;
          delete state.running;
        });
        await pending(app);
      } catch (error) {
        releaseCompletion();
        await change((state) => {
          if (state.running?.id === runId) delete state.running;
        }, false);
        // Native alarms retry this same event; never invent a planning/recovery timer.
        throw error;
      }
    },
  };
}

function sameJob(left: EntityJob | null, right: EntityJob | null): boolean {
  if (!left || !right) return left === right;
  return (
    left.generation === right.generation &&
    left.sourceVersion === right.sourceVersion &&
    left.dueAt === right.dueAt &&
    left.target.reason === right.target.reason &&
    left.expiry?.toBlock === right.expiry?.toBlock &&
    left.expiry?.searchedThrough === right.expiry?.searchedThrough &&
    left.expiry?.failures === right.expiry?.failures
  );
}
