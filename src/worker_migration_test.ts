import { test } from "bun:test";
import { deepStrictEqual as equal, rejects } from "node:assert/strict";
import { ensureWorkerMigration, MIGRATION_STATE_KEY } from "./worker_migration.ts";
import { MemorySchedulerStorage } from "./worker_scheduler_test_fixture.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { WorkerRequestBudget } from "./worker_budget.ts";
import type { WakeTarget } from "./wakeup.ts";

const prefix = ["uos_checkout", "v1", "payments"];
function fixture() {
  const kv = new SqliteKv();
  const storage = new MemorySchedulerStorage();
  const seen: string[] = [];
  const adopt = async (target: WakeTarget) => {
    seen.push(`${target.kind}:${target.id}`);
  };
  return {
    kv,
    storage,
    seen,
    run: (replacement = adopt) =>
      ensureWorkerMigration({
        kv,
        storage,
        budget: new WorkerRequestBudget(24),
        adopt: replacement,
        now: () => 1_000,
      }),
  };
}
async function invoice(kv: SqliteKv, id: string, status = "awaiting_payment") {
  await kv.set([...prefix, "invoices", id], {
    id,
    status,
    expiresAt: new Date(9_000).toISOString(),
  });
}

test("request migration checkpoints each item and never installs a migration alarm", async () => {
  const app = fixture();
  try {
    for (let index = 0; index < 5; index++) await invoice(app.kv, `invoice-${index}`);
    await invoice(app.kv, "terminal", "finalized");
    await app.kv.set([...prefix, "outbox", 12_000, "event"], "event");
    equal(await app.run(), "pending");
    equal(app.seen, ["invoice:invoice-0", "invoice:invoice-1"]);
    equal(app.storage.alarm, null);
    await finish(app);
    equal(
      new Set(app.seen),
      new Set([0, 1, 2, 3, 4].map((id) => `invoice:invoice-${id}`).concat("delivery:event")),
    );
    equal(app.storage.alarm, null);
    const calls = app.seen.length;
    equal(await app.run(), "complete");
    equal(app.seen.length, calls);
    equal((await app.storage.get<{ phase: string }>(MIGRATION_STATE_KEY))?.phase, "complete");
  } finally {
    app.kv.close();
  }
});

test("interrupted page resumes its retained item rather than skipping it", async () => {
  const app = fixture();
  try {
    for (let index = 0; index < 4; index++) await invoice(app.kv, `invoice-${index}`);
    let calls = 0;
    await rejects(
      () =>
        app.run(async (target) => {
          if (++calls === 2) throw new Error("interrupted_enqueue");
          app.seen.push(`${target.kind}:${target.id}`);
        }),
      /interrupted_enqueue/,
    );
    equal(app.seen, ["invoice:invoice-0"]);
    await app.run();
    equal(app.seen, ["invoice:invoice-0", "invoice:invoice-1", "invoice:invoice-2"]);
    equal(app.storage.alarm, null);
  } finally {
    app.kv.close();
  }
});

test("migration filters empty raw pages and does not revive expired/reverted search", async () => {
  const app = fixture();
  try {
    await invoice(app.kv, "expired", "expired");
    await invoice(app.kv, "reverted", "reverted");
    await invoice(app.kv, "paid", "settled");
    await finish(app);
    equal(app.seen, ["invoice:paid"]);
    equal(app.storage.alarm, null);
  } finally {
    app.kv.close();
  }
});

async function finish(app: ReturnType<typeof fixture>) {
  for (let attempt = 0; attempt < 50; attempt++) {
    if ((await app.run()) === "complete") return;
  }
  throw new Error("migration did not complete within bounded request events");
}

test("the final working slice stays pending until a later entry-complete request", async () => {
  const app = fixture();
  try {
    equal(await app.run(), "pending");
    equal(await app.run(), "pending");
    equal(await app.run(), "pending");
    equal((await app.storage.get<{ phase: string }>(MIGRATION_STATE_KEY))?.phase, "complete");
    equal(await app.run(), "complete");
    equal(app.seen, []);
    equal(app.storage.alarm, null);
  } finally {
    app.kv.close();
  }
});
