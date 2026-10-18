import type { Kv } from "./kv.ts";
import { isOpenInvoiceStatus, type InvoiceRecord } from "./payment_types.ts";
import type { SchedulerStorage } from "./worker_scheduler.ts";
import type { WakeTarget } from "./wakeup.ts";
import type { WorkerRequestBudget } from "./worker_budget.ts";
import { adoptAccountIndexSlice } from "./account_store.ts";

export const MIGRATION_OBJECT_NAME = "migration:checkout-events-v2";
export const MIGRATION_STATE_KEY = "checkout_migration_v2";
type Migration = {
  phase: "invoices" | "accounts" | "outbox" | "complete";
  cursor: string | null;
  page?: { targets: WakeTarget[]; next: string | null; offset: number };
  owner?: { id: string; until: number };
};

/** One bounded slice per ordinary request. This function never arms an alarm. */
export async function ensureWorkerMigration(options: {
  storage: SchedulerStorage;
  kv: Kv;
  budget: WorkerRequestBudget;
  adopt: (target: WakeTarget) => Promise<void>;
  now?: () => number;
}): Promise<"pending" | "complete"> {
  const { storage, kv, budget, adopt } = options;
  const now = options.now ?? Date.now;
  const id = crypto.randomUUID();
  let admitted = false;
  let complete = false;
  await storage.transaction(async (transaction) => {
    const state = (await transaction.get<Migration>(MIGRATION_STATE_KEY)) ?? {
      phase: "invoices",
      cursor: null,
    };
    complete = state.phase === "complete";
    if (complete || (state.owner && state.owner.until > now())) return;
    state.owner = { id, until: now() + 20_000 };
    await transaction.put(MIGRATION_STATE_KEY, state);
    admitted = true;
  });
  if (complete) return "complete";
  if (!admitted) return "pending";
  const current = async () => {
    const state = await storage.get<Migration>(MIGRATION_STATE_KEY);
    if (state?.owner?.id !== id || state.owner.until <= now()) return null;
    return state;
  };
  async function save(state: Migration) {
    await storage.transaction(async (transaction) => {
      const owned = await transaction.get<Migration>(MIGRATION_STATE_KEY);
      if (owned?.owner?.id !== id || owned.owner.until <= now()) throw new Error("stale_migration");
      await transaction.put(MIGRATION_STATE_KEY, state);
    });
  }
  try {
    let state = await current();
    if (!state) return "pending";
    if (state.phase === "accounts") {
      const done = await adoptAccountIndexSlice(kv, {
        budget,
        isCurrent: async () => Boolean(await current()),
      });
      if (done && (await current())) {
        state.phase = "outbox";
        state.cursor = null;
        await save(state);
      }
      return "pending";
    }
    if (!state.page) {
      const prefix = ["uos_checkout", "v1", "payments", state.phase];
      const page = await kv.listPage<unknown>(prefix, { limit: 8, cursor: state.cursor });
      if (!(await current())) return "pending";
      const targets: WakeTarget[] = [];
      for (const entry of page.entries) {
        if (state.phase === "invoices") {
          const invoice = entry.value as InvoiceRecord;
          if (typeof invoice?.id !== "string" || !isOpenInvoiceStatus(invoice.status)) continue;
          const dueAt = Date.parse(invoice.expiresAt);
          if (Number.isFinite(dueAt))
            targets.push({ kind: "invoice", id: invoice.id, dueAt, reason: "expiry" });
        } else {
          const dueAt = entry.key[entry.key.length - 2];
          if (typeof entry.value === "string" && typeof dueAt === "number")
            targets.push({ kind: "delivery", id: entry.value, dueAt, reason: "delivery" });
        }
      }
      state.page = { targets, next: page.cursor, offset: 0 };
      await save(state);
    }
    for (let processed = 0; processed < 2; processed++) {
      const page = state.page;
      if (!page || page.offset >= page.targets.length) break;
      if (!budget.hasCapacity(2) || !(await current())) return "pending";
      await adopt(page.targets[page.offset]);
      state = (await current())!;
      if (!state) return "pending";
      state.page!.offset++;
      await save(state);
    }
    if (state.page!.offset === state.page!.targets.length) {
      const next = state.page!.next;
      delete state.page;
      state.cursor = next;
      if (next === null) {
        state.phase = state.phase === "invoices" ? "accounts" : "complete";
        state.cursor = null;
      }
      await save(state);
    }
    // A slice that performed work cannot also admit a heavy caller. The next
    // request observes the durable complete marker without nested migration IO.
    return "pending";
  } finally {
    await storage.transaction(async (transaction) => {
      const state = await transaction.get<Migration>(MIGRATION_STATE_KEY);
      if (state?.owner?.id === id) {
        delete state.owner;
        await transaction.put(MIGRATION_STATE_KEY, state);
      }
    });
  }
}
