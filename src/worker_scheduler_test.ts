import { test } from "bun:test";
import { deepStrictEqual as equal, ok, rejects } from "node:assert/strict";
import {
  createIntentWakeupPort,
  wakeIntentKey,
  type PreparedWakeIntent,
  type WakeIntent,
  type WakeTarget,
} from "./wakeup.ts";
import {
  createWorkerScheduler,
  SCHEDULER_STATE_KEY,
  type SchedulerState,
} from "./worker_scheduler.ts";
import { WorkerRequestBudget } from "./worker_budget.ts";
import { invoiceTarget, schedulerFixture } from "./worker_scheduler_test_fixture.ts";

async function prepared(
  app: ReturnType<typeof schedulerFixture>,
  target: WakeTarget,
): Promise<PreparedWakeIntent> {
  const operationId = crypto.randomUUID();
  const key = wakeIntentKey(operationId);
  const intent: WakeIntent = {
    schemaVersion: 1,
    operationId,
    state: "prepared",
    targets: [target],
    adopted: [],
    preparedAt: app.now(),
  };
  const result = await app.kv.set(key, intent);
  return { ...intent, key, versionstamp: result.versionstamp };
}

test("preparation arms only the originating deadline and cancellation creates no alarm", async () => {
  const app = schedulerFixture();
  try {
    const target = invoiceTarget("invoice", 8_000);
    await app.controller.prepare(await prepared(app, target), target);
    equal(app.storage.alarm, 8_000);
    const empty = schedulerFixture();
    try {
      const cancel = invoiceTarget("cancelled", null);
      await empty.controller.prepare(await prepared(empty, cancel), cancel);
      equal(empty.storage.alarm, null);
    } finally {
      empty.kv.close();
    }
  } finally {
    app.kv.close();
  }
});

test("an immediately due preparation retries its original alarm until healthy CAS commits", async () => {
  const app = schedulerFixture();
  try {
    const target = invoiceTarget();
    const intent = await prepared(app, target);
    await app.controller.prepare(intent, target);
    await rejects(() => app.controller.alarm(), /scheduler_intent_preparing/);
    equal((await app.kv.get<WakeIntent>(intent.key)).value?.state, "prepared");
    equal(app.calls, 0);
    await app.kv
      .atomic()
      .check({ key: intent.key, versionstamp: intent.versionstamp })
      .set(intent.key, { ...intent, state: "committed" })
      .commit();
    await app.controller.alarm();
    equal(app.calls, 1);
    equal(app.storage.alarm, null);
    equal((await app.kv.get(intent.key)).value, null);
    await app.controller.alarm();
    equal(app.calls, 1);
  } finally {
    app.kv.close();
  }
});

test("abandoned preparation fences a late originating commit without a recovery timer", async () => {
  const app = schedulerFixture();
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  try {
    const target = invoiceTarget();
    const port = createIntentWakeupPort({
      kv: app.kv,
      now: app.now,
      transport: {
        async prepare(intent) {
          await app.controller.prepare(intent, target);
          entered.resolve();
          await release.promise;
        },
      },
    });
    const commit = port.commit(app.kv.atomic().set(["domain"], "must-not-commit"), [target]);
    await entered.promise;
    app.advance(31_000);
    await app.controller.alarm();
    release.resolve();
    equal((await commit).ok, false);
    equal((await app.kv.get(["domain"])).value, null);
    equal(app.storage.alarm, null);
  } finally {
    release.resolve();
    app.kv.close();
  }
});

test("multi-target adoption acknowledges only after durable plan and preserves the other target", async () => {
  const app = schedulerFixture();
  const other = schedulerFixture();
  try {
    const first = invoiceTarget();
    const second: WakeTarget = { kind: "delivery", id: "event", dueAt: 1_000, reason: "delivery" };
    let operationId = "";
    const port = createIntentWakeupPort({
      kv: app.kv,
      now: app.now,
      transport: {
        async prepare(intent) {
          operationId = intent.operationId;
          await app.controller.prepare(intent, first);
          // Both targets share authoritative KV, as in the Worker service binding.
          other.controller = createWorkerScheduler(
            other.storage,
            () => ({
              kv: app.kv,
              runner: {
                plan: async (target) => ({
                  schemaVersion: 1,
                  target,
                  generation: "delivery:v1",
                  sourceVersion: "v1",
                  dueAt: 1_000,
                }),
                run: async () => null,
              },
              budget: new WorkerRequestBudget(24),
            }),
            app.now,
          );
          await other.controller.prepare(intent, second);
        },
      },
    });
    ok((await port.commit(app.kv.atomic().set(["domain"], "committed"), [first, second])).ok);
    await app.controller.alarm();
    equal((await app.kv.get<WakeIntent>(wakeIntentKey(operationId))).value?.adopted, [
      "invoice:invoice",
    ]);
    await other.controller.alarm();
    equal((await app.kv.get(wakeIntentKey(operationId))).value, null);
    equal(other.storage.alarm, null);
  } finally {
    app.kv.close();
    other.kv.close();
  }
});

test("preparing the running handler's own mutation does not invalidate its generation", async () => {
  const app = schedulerFixture();
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let isCurrent: (() => Promise<boolean>) | undefined;
  try {
    app.onRun = async (_job, guard) => {
      isCurrent = guard;
      entered.resolve();
      await release.promise;
      return null;
    };
    await app.controller.ensure(invoiceTarget());
    const running = app.controller.alarm();
    await entered.promise;
    const target = invoiceTarget("invoice", 9_000);
    await app.controller.prepare(await prepared(app, target), target);
    equal(await isCurrent!(), true);
    release.resolve();
    await running;
    equal(app.storage.alarm, 9_000);
  } finally {
    release.resolve();
    app.kv.close();
  }
});

test("committed replacement fences a blocked old runner before it can rearm", async () => {
  const app = schedulerFixture();
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  try {
    app.onRun = async (job) => {
      entered.resolve();
      await release.promise;
      return { ...job, dueAt: 99_000 };
    };
    await app.controller.ensure(invoiceTarget());
    const running = app.controller.alarm();
    await entered.promise;
    app.version("v2");
    await app.controller.ensure(invoiceTarget("invoice", 8_000));
    release.resolve();
    await rejects(() => running, /stale_wakeup/);
    const state = await app.storage.get<SchedulerState>(SCHEDULER_STATE_KEY);
    equal(state?.job?.sourceVersion, "v2");
    equal(state?.job?.dueAt, 8_000);
    equal(app.storage.alarm, 8_000);
  } finally {
    release.resolve();
    app.kv.close();
  }
});

test("a runner's terminal cancellation is adopted and ACKed before becoming idle", async () => {
  const app = schedulerFixture();
  let operationId = "";
  try {
    app.onRun = async () => {
      const target = invoiceTarget("invoice", null);
      const port = createIntentWakeupPort({
        kv: app.kv,
        now: app.now,
        transport: {
          async prepare(intent) {
            operationId = intent.operationId;
            await app.controller.prepare(intent, target);
          },
        },
      });
      ok((await port.commit(app.kv.atomic().set(["domain"], "terminal"), [target])).ok);
      return null;
    };
    await app.controller.ensure(invoiceTarget());
    await app.controller.alarm();
    equal((await app.storage.get<SchedulerState>(SCHEDULER_STATE_KEY))?.pending, {});
    equal((await app.kv.get(wakeIntentKey(operationId))).value, null);
    equal(app.storage.alarm, null);
  } finally {
    app.kv.close();
  }
});

test("a related terminal event retires committed future intent and its original alarm", async () => {
  const app = schedulerFixture();
  try {
    const future = invoiceTarget("invoice", 90_000);
    const intent = await prepared(app, future);
    await app.controller.prepare(intent, future);
    await app.kv
      .atomic()
      .check({ key: intent.key, versionstamp: intent.versionstamp })
      .set(intent.key, { ...intent, state: "committed" })
      .commit();
    app.version("terminal");
    await app.controller.ensure(invoiceTarget("invoice", null));
    equal((await app.storage.get<SchedulerState>(SCHEDULER_STATE_KEY))?.pending, {});
    equal((await app.kv.get(intent.key)).value, null);
    equal(app.storage.alarm, null);
  } finally {
    app.kv.close();
  }
});
