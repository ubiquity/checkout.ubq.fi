import { deliverOutboxEvent, type CheckoutServices } from "../main.ts";
import type { KvEntryMaybe } from "./kv.ts";
import {
  expiryDiscoveryStateKey,
  hasCurrentPayment,
  type DiscoveryState,
} from "./payment_discovery.ts";
import { invoiceKey, paymentKey } from "./payment_service.ts";
import type { InvoiceRecord, OutboxEventRecord, PaymentRecord } from "./payment_types.ts";
import { chainHeads } from "./rpc.ts";
import {
  confirmationMilestones,
  type EntityJob,
  type EntityJobRunner,
  type WakeTarget,
} from "./wakeup.ts";
import { WorkerBudgetError, type WorkerRequestBudget } from "./worker_budget.ts";

const PREFIX = ["uos_checkout", "v1", "payments"];
const EXPIRY_WINDOW_MS = 5 * 60_000;
const MAX_EXPIRY_FAILURES = 3;
export type ExpiryPass = DiscoveryState & {
  schemaVersion: 1;
  startedAt: number;
  status: "pending" | "complete" | "partial" | "paid";
  failures: number;
  toBlock?: bigint;
  attemptFrom?: bigint;
  completedAt?: number;
  lastError?: string;
};

export function invoiceWakeTarget(
  invoice: InvoiceRecord,
  payment: PaymentRecord | null,
  now = Date.now(),
): WakeTarget {
  const target: WakeTarget = { kind: "invoice", id: invoice.id, dueAt: null, reason: "cancel" };
  if (invoice.status === "awaiting_payment")
    return { ...target, dueAt: Date.parse(invoice.expiresAt), reason: "expiry" };
  if (
    !payment ||
    !hasCurrentPayment(invoice) ||
    invoice.requiresManualReview ||
    !["confirming", "provisionally_paid", "settled"].includes(invoice.status)
  )
    return target;
  const times = confirmationMilestones(invoice.chainId, Date.parse(payment.blockTimestamp));
  const expected = invoice.status === "settled" ? times.finalizedAt : times.safeAt;
  return { ...target, dueAt: Math.max(now, expected), reason: "confirmation" };
}

export function deliveryWakeTarget(
  event: OutboxEventRecord & { nextAttemptAt?: string },
): WakeTarget {
  return {
    kind: "delivery",
    id: event.id,
    dueAt: event.deliveredAt ? null : Date.parse(event.nextAttemptAt ?? event.createdAt),
    reason: event.deliveredAt ? "cancel" : "delivery",
  };
}

export function createEntityJobRunner(
  services: CheckoutServices,
  options: { budget?: WorkerRequestBudget; now?: () => number } = {},
): EntityJobRunner {
  const clock = options.now ?? Date.now;
  const room = (calls: number) => options.budget?.hasCapacity(calls) ?? true;
  const job = (
    target: WakeTarget,
    sourceVersion: string,
    expiry?: EntityJob["expiry"],
  ): EntityJob => ({
    schemaVersion: 1,
    target,
    generation: crypto.randomUUID(),
    sourceVersion,
    dueAt: target.dueAt!,
    ...(expiry ? { expiry } : {}),
  });
  const continuation = (current: EntityJob, now: number, pass?: ExpiryPass): EntityJob => ({
    ...current,
    generation: crypto.randomUUID(),
    dueAt: now,
    ...(pass?.toBlock === undefined
      ? {}
      : {
          expiry: {
            toBlock: pass.toBlock,
            searchedThrough: pass.searchedThrough,
            failures: pass.failures,
          },
        }),
  });

  async function plan(
    target: WakeTarget,
    previous?: EntityJob | null,
    now = clock(),
  ): Promise<EntityJob | null> {
    if (target.kind === "delivery") {
      const entry = await services.kv.get<OutboxEventRecord & { nextAttemptAt?: string }>([
        ...PREFIX,
        "events",
        target.id,
      ]);
      if (!entry.value || entry.value.deliveredAt) return null;
      const next = deliveryWakeTarget(entry.value);
      return previous?.sourceVersion === entry.versionstamp
        ? previous
        : job(next, entry.versionstamp!);
    }
    const [entry, pass] = await services.kv.getMany<[InvoiceRecord, ExpiryPass]>([
      invoiceKey(target.id),
      expiryDiscoveryStateKey(target.id),
    ]);
    const invoice = entry.value;
    if (!invoice) return null;
    if (!hasCurrentPayment(invoice) && ["awaiting_payment", "expired"].includes(invoice.status)) {
      if (pass.value && pass.value.status !== "pending") return null;
      if (
        invoice.status === "expired" &&
        target.reason !== "expiry" &&
        !previous?.expiry &&
        !pass.value
      )
        return null;
      const next: WakeTarget = {
        kind: "invoice",
        id: invoice.id,
        reason: "expiry",
        dueAt: Date.parse(invoice.expiresAt),
      };
      if (previous?.sourceVersion === entry.versionstamp) return previous;
      return job(
        next,
        entry.versionstamp!,
        pass.value?.toBlock === undefined
          ? undefined
          : {
              toBlock: pass.value.toBlock,
              searchedThrough: pass.value.searchedThrough,
              failures: pass.value.failures,
            },
      );
    }
    const payment = invoice.paymentId
      ? (await services.kv.get<PaymentRecord>(paymentKey(invoice.paymentId))).value
      : null;
    const next = invoiceWakeTarget(invoice, payment, now);
    if (next.dueAt === null) return null;
    return previous?.sourceVersion === entry.versionstamp
      ? previous
      : job(next, entry.versionstamp!);
  }

  async function writePass(
    id: string,
    changes: Partial<ExpiryPass>,
    current: () => Promise<boolean>,
  ) {
    if (!(await current())) return null;
    const entry = await services.kv.get<ExpiryPass>(expiryDiscoveryStateKey(id));
    if (!(await current())) return null;
    const next = { ...entry.value, ...changes } as ExpiryPass;
    return (await services.kv.atomic().check(entry).set(entry.key, next).commit()).ok ? next : null;
  }

  async function finishExpiry(
    currentJob: EntityJob,
    state: ExpiryPass,
    status: "complete" | "partial",
    isCurrent: () => Promise<boolean>,
    now: number,
  ) {
    if (!room(10)) return continuation(currentJob, now, state);
    if (!(await isCurrent())) return null;
    await services.payment.expireInvoice(currentJob.target.id);
    if (!(await isCurrent())) return null;
    const [invoice, pass] = await services.kv.getMany<[InvoiceRecord, ExpiryPass]>([
      invoiceKey(currentJob.target.id),
      expiryDiscoveryStateKey(currentJob.target.id),
    ]);
    if (!invoice.value || !pass.value || !(await isCurrent())) return null;
    const paid = hasCurrentPayment(invoice.value);
    let atomic = services.kv
      .atomic()
      .check(invoice, pass)
      .set(pass.key, {
        ...pass.value,
        status: paid ? "paid" : status,
        completedAt: now,
      } satisfies ExpiryPass);
    if (!paid && invoice.value.status === "expired")
      atomic = atomic.delete([
        ...PREFIX,
        "invoices_by_chain",
        invoice.value.chainId,
        invoice.value.id,
      ]);
    if (!(await atomic.commit()).ok) return continuation(currentJob, now, state);
    return paid ? plan(currentJob.target, undefined, now) : null;
  }

  async function runExpiry(
    currentJob: EntityJob,
    entry: KvEntryMaybe<InvoiceRecord>,
    state: ExpiryPass | null,
    isCurrent: () => Promise<boolean>,
    now: number,
  ): Promise<EntityJob | null> {
    const invoice = entry.value!;
    const key = expiryDiscoveryStateKey(invoice.id);
    if (state && state.status !== "pending") return null;
    if (!state) {
      state = { schemaVersion: 1, startedAt: now, status: "pending", failures: 0 };
      if (
        !(await isCurrent()) ||
        !(await services.kv.atomic().check({ key, versionstamp: null }).set(key, state).commit()).ok
      )
        return continuation(currentJob, now);
    }
    const progressed =
      state.searchedThrough !== undefined &&
      state.attemptFrom !== undefined &&
      state.searchedThrough > state.attemptFrom;
    if (progressed) state.failures = 0;
    if (
      state.toBlock !== undefined &&
      (state.toBlock < invoice.minimumBlock || (state.searchedThrough ?? -1n) >= state.toBlock)
    )
      return finishExpiry(currentJob, state, "complete", isCurrent, now);
    if (state.failures >= MAX_EXPIRY_FAILURES || now >= state.startedAt + EXPIRY_WINDOW_MS)
      return finishExpiry(currentJob, state, "partial", isCurrent, now);
    if (!room(15)) return continuation(currentJob, now, state);
    state = await writePass(
      invoice.id,
      {
        failures: state.failures + 1,
        attemptFrom: state.searchedThrough ?? invoice.minimumBlock - 1n,
      },
      isCurrent,
    );
    if (!state) return continuation(currentJob, now);
    try {
      if (state.toBlock === undefined) {
        if (!(await isCurrent())) return null;
        const heads = await chainHeads(services.rpc, invoice.chainId);
        if (!(await isCurrent())) return null;
        const saved = await writePass(
          invoice.id,
          { toBlock: heads.latestBlock, failures: 0 },
          isCurrent,
        );
        return continuation(currentJob, now, saved ?? state);
      }
      if (!(await isCurrent())) return null;
      const found = await services.payment.discoverPayment(invoice.id, {
        expiryToBlock: state.toBlock,
        isCurrent,
      });
      if (!(await isCurrent())) return null;
      if (found.discovery?.found) {
        // The claim atomically prearmed the known-payment job; expiry cleanup marked its pass paid.
        return null;
      }
      const through = found.discovery?.searchedToBlock;
      if (through !== undefined && BigInt(through) > (state.searchedThrough ?? -1n) && room(2)) {
        const saved = await writePass(invoice.id, { failures: 0 }, isCurrent);
        return continuation(currentJob, now, saved ?? state);
      }
      return continuation(currentJob, now + 1000, state);
    } catch (error) {
      if (!(await isCurrent())) return null;
      if (!(error instanceof WorkerBudgetError) && room(2))
        await writePass(
          invoice.id,
          { lastError: error instanceof Error ? error.message.slice(0, 512) : "expiry_rpc_failed" },
          isCurrent,
        );
      return continuation(currentJob, now + 1000, state);
    }
  }

  async function run(
    currentJob: EntityJob,
    isCurrent: () => Promise<boolean>,
    now = clock(),
  ): Promise<EntityJob | null> {
    if (!(await isCurrent())) return null;
    if (currentJob.dueAt > now) return currentJob;
    if (currentJob.target.kind === "delivery") {
      await deliverOutboxEvent(services, currentJob.target.id, {
        isCurrent,
        deadline: Date.now() + 20_000,
      });
      return (await isCurrent()) ? plan(currentJob.target, undefined, now) : null;
    }
    const [entry, pass] = await services.kv.getMany<[InvoiceRecord, ExpiryPass]>([
      invoiceKey(currentJob.target.id),
      expiryDiscoveryStateKey(currentJob.target.id),
    ]);
    if (!entry.value || !(await isCurrent())) return null;
    if (
      hasCurrentPayment(entry.value) &&
      !entry.value.requiresManualReview &&
      ["confirming", "provisionally_paid", "settled"].includes(entry.value.status)
    ) {
      if (
        currentJob.target.reason !== "confirmation" ||
        currentJob.sourceVersion !== entry.versionstamp
      )
        return plan(currentJob.target, undefined, now);
      const payment = (await services.kv.get<PaymentRecord>(paymentKey(entry.value.paymentId!)))
        .value;
      if (!payment || !(await isCurrent())) return null;
      try {
        await services.payment.refreshInvoice(entry.value.id);
      } catch (error) {
        if (error instanceof WorkerBudgetError) throw error;
      }
      if (!(await isCurrent())) return null;
      const refreshed = await services.payment.getInvoiceRecord(entry.value.id);
      if (!(await isCurrent())) return null;
      if (refreshed.status !== entry.value.status || refreshed.paymentId !== entry.value.paymentId)
        return plan(currentJob.target, undefined, now);
      const times = confirmationMilestones(entry.value.chainId, Date.parse(payment.blockTimestamp));
      const next = [times.safeAt, times.finalizedAt].find(
        (at) => at > Math.max(now, currentJob.dueAt),
      );
      return next === undefined
        ? null
        : {
            ...currentJob,
            generation: crypto.randomUUID(),
            dueAt: next,
            target: { ...currentJob.target, dueAt: next },
          };
    }
    if (
      ["awaiting_payment", "expired"].includes(entry.value.status) &&
      currentJob.target.reason === "expiry"
    )
      return runExpiry(currentJob, entry, pass.value, isCurrent, now);
    return null;
  }
  return { plan, run };
}
