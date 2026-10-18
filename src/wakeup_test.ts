import { test } from "bun:test";
import { deepStrictEqual as equal, ok, rejects } from "node:assert/strict";
import { SqliteKv } from "./sqlite_kv.ts";
import {
  acknowledgeWakeIntent,
  createIntentWakeupPort,
  resolveWakeIntent,
  StaleWakeupError,
  wakeIntentKey,
  type PreparedWakeIntent,
  type WakeIntent,
  type WakeTarget,
} from "./wakeup.ts";
import { WorkerBudgetError, WorkerRequestBudget } from "./worker_budget.ts";

const invoice: WakeTarget = { kind: "invoice", id: "invoice", dueAt: 1000, reason: "expiry" };
const delivery: WakeTarget = { kind: "delivery", id: "event", dueAt: 2000, reason: "delivery" };
const domainKey = ["uos_checkout", "v1", "payments", "invoices", "invoice"];

test("an invoice and delivery are prearmed before the fenced domain CAS and retire only after both adopt", async () => {
  const kv = new SqliteKv();
  try {
    let prepared!: PreparedWakeIntent;
    const port = createIntentWakeupPort({
      kv,
      transport: {
        prepare: async (intent) => {
          prepared = intent;
          equal((await kv.get(domainKey)).value, null);
          equal((await kv.get<WakeIntent>(intent.key)).value?.state, "prepared");
        },
      },
    });
    ok(
      (
        await port.commit(kv.atomic().set(domainKey, { status: "confirming" }), [
          invoice,
          delivery,
          invoice,
        ])
      ).ok,
    );
    equal((await kv.get(domainKey)).value, { status: "confirming" });
    equal((await resolveWakeIntent(kv, prepared.operationId))?.state, "committed");
    ok(await acknowledgeWakeIntent(kv, prepared.operationId, invoice));
    equal((await kv.get<WakeIntent>(prepared.key)).value?.adopted, ["invoice:invoice"]);
    ok(await acknowledgeWakeIntent(kv, prepared.operationId, invoice));
    ok((await kv.get(prepared.key)).value);
    ok(await acknowledgeWakeIntent(kv, prepared.operationId, delivery));
    equal((await kv.get(prepared.key)).value, null);
    ok(await acknowledgeWakeIntent(kv, prepared.operationId, delivery));
  } finally {
    kv.close();
  }
});

test("recovery cancellation fences a late domain commit instead of creating orphan work", async () => {
  const kv = new SqliteKv();
  try {
    const port = createIntentWakeupPort({
      kv,
      transport: {
        prepare: async (intent) => {
          equal(await resolveWakeIntent(kv, intent.operationId), null);
        },
      },
    });
    equal((await port.commit(kv.atomic().set(domainKey, "late"), [invoice])).ok, false);
    equal((await kv.get(domainKey)).value, null);
  } finally {
    kv.close();
  }
});

test("a committed intent survives lost confirmation and rejects an unrelated target ACK", async () => {
  const kv = new SqliteKv();
  try {
    let prepared!: PreparedWakeIntent;
    const port = createIntentWakeupPort({
      kv,
      transport: {
        prepare: async (intent) => {
          prepared = intent;
        },
      },
    });
    ok((await port.commit(kv.atomic().set(domainKey, "committed"), [invoice])).ok);
    equal((await resolveWakeIntent(kv, prepared.operationId))?.state, "committed");
    equal(await acknowledgeWakeIntent(kv, prepared.operationId, delivery), false);
    ok((await kv.get(prepared.key)).value);
    ok(await acknowledgeWakeIntent(kv, prepared.operationId, invoice));
    equal((await kv.get(prepared.key)).value, null);
  } finally {
    kv.close();
  }
});

test("a failed prearm or stale continuation cannot write domain state", async () => {
  const kv = new SqliteKv();
  try {
    let current = true;
    let prepared!: PreparedWakeIntent;
    const port = createIntentWakeupPort({
      kv,
      isCurrent: async () => current,
      transport: {
        prepare: async (intent) => {
          prepared = intent;
          current = false;
        },
      },
    });
    await rejects(
      () => port.commit(kv.atomic().set(domainKey, "stale"), [invoice]),
      StaleWakeupError,
    );
    equal((await kv.get(domainKey)).value, null);
    equal((await kv.get<WakeIntent>(prepared.key)).value?.state, "prepared");
    await rejects(
      () => resolveWakeIntent(kv, prepared.operationId, async () => false),
      StaleWakeupError,
    );
    ok((await kv.get(prepared.key)).value);
    equal(await resolveWakeIntent(kv, prepared.operationId), null);
    const failed = createIntentWakeupPort({
      kv,
      transport: {
        prepare: async () => {
          throw new Error("prearm_failed");
        },
      },
    });
    await rejects(
      () => failed.commit(kv.atomic().set(domainKey, "failed"), [invoice]),
      /prearm_failed/,
    );
    equal((await kv.get(domainKey)).value, null);
  } finally {
    kv.close();
  }
});

test("insufficient prearm budget yields before any intent or domain write", async () => {
  const kv = new SqliteKv();
  try {
    let calls = 0;
    const port = createIntentWakeupPort({
      kv,
      budget: new WorkerRequestBudget(3),
      transport: {
        prepare: async () => {
          calls++;
        },
      },
    });
    await rejects(
      () => port.commit(kv.atomic().set(domainKey, "over-budget"), [invoice, delivery]),
      WorkerBudgetError,
    );
    equal(calls, 0);
    equal((await kv.get(domainKey)).value, null);
    equal(
      (await kv.listPage(["uos_checkout", "v1", "payments", "wake_intents"], { limit: 10 }))
        .entries,
      [],
    );
    equal((await kv.get(wakeIntentKey("absent"))).value, null);
  } finally {
    kv.close();
  }
});
