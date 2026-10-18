import { test } from "bun:test";
import { deepStrictEqual as equal, ok, rejects } from "node:assert/strict";
import type { Kv, KvAtomic, KvKey } from "./kv.ts";
import type { InvoiceRecord, PaymentRecord } from "./payment_types.ts";
import { createPaymentService } from "./payment_service.ts";
import { createPersistentJwsSigner } from "./jws.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { RuntimeRpc } from "./account_runtime_test_fixture.ts";
import { attemptStateKey, reservationKey, type AttemptState } from "./account_store.ts";
import { WorkerRequestBudget } from "./worker_budget.ts";
import type { JsonRpcCall } from "./rpc.ts";
import { observationKey, type TransferReview } from "./account_runtime.ts";

const prefix = ["uos_checkout", "v1", "payments"];
const treasury = `0x${"44".repeat(20)}`;
const coordinates = (log: ReturnType<RuntimeRpc["add"]>) => ({
  transactionHash: log.transactionHash,
  logIndex: String(log.logIndex),
  observedBlockNumber: String(log.blockNumber),
  observedBlockHash: log.blockHash,
});

for (const keepReorgedLogs of [false, true]) {
  test(`reorg-cleared earlier attempt replays retained matching (contradicted log retained=${keepReorgedLogs})`, async () => {
    const app = await setup();
    try {
      const earlier = await app.create("earlier");
      const later = await app.create("later");
      const original = app.rpc.add(await app.service.getInvoiceRecord(earlier.id), 200n);
      const laterLog = app.rpc.add(await app.service.getInvoiceRecord(later.id), 210n);
      app.rpc.latest = 300n;
      await app.service.claimPayment(earlier.id, coordinates(original));
      await app.service.claimPayment(later.id, coordinates(laterLog));
      app.rpc.safe = 250n;
      await app.service.refreshInvoice(later.id);
      app.rpc.reorged.add(original.transactionHash);
      app.rpc.keepReorgedLogs = keepReorgedLogs;
      const replacement = app.rpc.add(await app.service.getInvoiceRecord(earlier.id), 220n);

      const first = await app.service.discoverPayment(later.id);
      const afterRefresh = await app.service.getInvoiceRecord(earlier.id);
      ok((await app.kv.get(reservationKey(afterRefresh))).value);
      ok(
        (await app.kv.get<AttemptState>(attemptStateKey(earlier.id))).value?.resolution !==
          "invalidated",
      );
      if (!afterRefresh.paymentId) equal(first.accountHunt?.pending, true);
      // The replacement remains canonically verifiable even if a later log query omits it.
      app.rpc.hidden.add(replacement.transactionHash);
      for (let attempt = 0; attempt < 3; attempt++) {
        const invoice = await app.service.getInvoiceRecord(earlier.id);
        if (invoice.paymentId) break;
        await app.service.discoverPayment(later.id);
      }
      const recovered = await app.service.getInvoiceRecord(earlier.id);
      ok(recovered.paymentId);
      const payment = await app.kv.get<PaymentRecord>([
        ...prefix,
        "payment_records",
        recovered.paymentId,
      ]);
      equal(payment.value?.transactionHash, replacement.transactionHash);
      ok((await app.kv.get(reservationKey(recovered))).value);
      if (keepReorgedLogs) {
        const quarantined = await app.kv.get<TransferReview>(observationKey(8453, original));
        equal(quarantined.value?.canonicalContradiction, "non_canonical_block");
        equal(quarantined.value?.log.transactionHash, original.transactionHash);
        equal(quarantined.value?.log.blockHash, original.blockHash);
        ok(quarantined.value?.provenance.includes("account_hunt_canonical_contradiction"));
      }
    } finally {
      app.kv.close();
    }
  });
}

test("fork coverage cannot release an unpaid attempt until a clean related-event range pass", async () => {
  const app = await setup();
  try {
    const unpaid = await app.create("unpaid");
    const reorged = await app.create("reorged");
    const later = await app.create("later");
    const old = app.rpc.add(await app.service.getInvoiceRecord(reorged.id), 200n);
    const paid = app.rpc.add(await app.service.getInvoiceRecord(later.id), 210n);
    app.rpc.latest = 300n;
    await app.service.claimPayment(reorged.id, coordinates(old));
    await app.service.claimPayment(later.id, coordinates(paid));
    app.rpc.safe = 250n;
    await app.service.refreshInvoice(later.id);
    app.rpc.reorged.add(old.transactionHash);
    app.rpc.keepReorgedLogs = true;
    const replacement = app.rpc.add(await app.service.getInvoiceRecord(reorged.id), 220n);
    await app.service.discoverPayment(later.id);
    const contradicted = await app.service.discoverPayment(later.id);
    const replacementInvoice = await app.service.getInvoiceRecord(reorged.id);
    const payment = await app.kv.get<PaymentRecord>([
      ...prefix,
      "payment_records",
      replacementInvoice.paymentId!,
    ]);
    equal(payment.value?.transactionHash, replacement.transactionHash);
    const unpaidRecord = await app.service.getInvoiceRecord(unpaid.id);
    equal(
      (await app.kv.get<AttemptState>(attemptStateKey(unpaid.id))).value?.resolution,
      "unresolved",
    );
    ok((await app.kv.get(reservationKey(unpaidRecord))).value);
    equal(contradicted.accountHunt?.pending, true);
    equal(contradicted.accountHunt?.reachedHead, false);
    const repeatedFork = await app.service.discoverPayment(later.id);
    equal(repeatedFork.accountHunt?.pending, true);
    equal(repeatedFork.accountHunt?.reachedHead, false);
    ok((await app.kv.get(reservationKey(unpaidRecord))).value);
    app.rpc.keepReorgedLogs = false;
    const clean = await app.service.discoverPayment(later.id);
    equal(clean.accountHunt?.reachedHead, true);
    equal(clean.accountHunt?.pending, false);
    equal(
      (await app.kv.get<AttemptState>(attemptStateKey(unpaid.id))).value?.resolution,
      "invalidated",
    );
    equal((await app.kv.get(reservationKey(unpaidRecord))).value, null);
    equal(
      (await app.kv.get<TransferReview>(observationKey(8453, old))).value?.canonicalContradiction,
      "non_canonical_block",
    );
  } finally {
    app.kv.close();
  }
});

for (const failure of ["receipt", "block", "provider"] as const) {
  test(`inconclusive ${failure} evidence on the credited trigger protects earlier ownership`, async () => {
    const app = await setup();
    try {
      const earlier = await app.create("earlier");
      const later = await app.create("later");
      const laterLog = app.rpc.add(await app.service.getInvoiceRecord(later.id), 210n);
      app.rpc.latest = 300n;
      await app.service.claimPayment(later.id, coordinates(laterLog));
      app.rpc.safe = 250n;
      await app.service.refreshInvoice(later.id);
      app.rpc.unavailable.set(laterLog.transactionHash, failure);
      if (failure === "provider") {
        await rejects(() => app.service.discoverPayment(later.id), /rpc_upstream_error:receipt/);
      } else {
        const result = await app.service.discoverPayment(later.id);
        equal(result.invoice.id, later.id);
        equal(result.invoice.status, "provisionally_paid");
        equal(result.accountHunt?.pending, true);
      }
      const original = await app.service.getInvoiceRecord(earlier.id);
      equal(
        (await app.kv.get<AttemptState>(attemptStateKey(earlier.id))).value?.resolution,
        "unresolved",
      );
      equal(
        (await app.kv.get<{ invoiceId: string }>(reservationKey(original))).value?.invoiceId,
        earlier.id,
      );
      app.rpc.unavailable.clear();
      await app.service.discoverPayment(later.id);
      equal(
        (await app.kv.get<AttemptState>(attemptStateKey(earlier.id))).value?.resolution,
        "invalidated",
      );
    } finally {
      app.kv.close();
    }
  });
}

test("signed extra transfer is retained once without replacing primary credit or event identity", async () => {
  const app = await setup();
  try {
    const made = await app.create("paid");
    const original = app.rpc.add(await app.service.getInvoiceRecord(made.id), 200n);
    app.rpc.latest = 300n;
    await app.service.claimPayment(made.id, coordinates(original));
    app.rpc.safe = 250n;
    const accepted = await app.service.refreshInvoice(made.id);
    const originalEvents = await app.service.listOutbox();
    ok(accepted.eventJws);
    const extra = app.rpc.add(await app.service.getInvoiceRecord(made.id), 220n);
    const context = { deadline: Date.now() + 20_000, isCurrent: async () => true };
    const first = await app.service.processChainTransfer(
      { ...extra, chainId: 8453, removed: false },
      context,
    );
    equal(first.terminal, true);
    await app.service.processChainTransfer({ ...extra, chainId: 8453, removed: false }, context);
    const invoice = await app.service.getInvoiceRecord(made.id);
    equal(invoice.paymentId, accepted.invoice.paymentId);
    equal(invoice.requiresManualReview, true);
    const duplicates = [];
    for await (const entry of app.kv.list<PaymentRecord>({
      prefix: [...prefix, "payment_records"],
    }))
      if (entry.value.transactionHash === extra.transactionHash) duplicates.push(entry.value);
    equal(duplicates.length, 1);
    equal(duplicates[0].phase, "duplicate");
    equal(
      (await app.service.listOutbox()).map((event) => event.id),
      originalEvents.map((event) => event.id),
    );
    equal((await app.service.listOutbox())[0].compactJws, accepted.eventJws);
  } finally {
    app.kv.close();
  }
});

for (const mode of ["existing", "fresh", "changed_version"] as const) {
  test(`one response-signing slot handles ${mode} event without exhaustion or stale reuse`, async () => {
    const app = await setup();
    try {
      const made = await app.create("paid");
      const original = app.rpc.add(await app.service.getInvoiceRecord(made.id), 200n);
      app.rpc.latest = 300n;
      await app.service.claimPayment(made.id, coordinates(original));
      app.rpc.safe = 250n;
      if (mode === "existing") {
        await app.service.refreshInvoice(made.id);
        await app.service.refreshInvoice(made.id);
      }
      const budget = new WorkerRequestBudget(200);
      let reachedResponse = false;
      const metered = responseBoundaryKv(
        app.kv,
        budget,
        () => {
          reachedResponse = true;
        },
        mode === "changed_version",
      );
      const service = createPaymentService({
        kv: metered,
        rpc: {
          batch: (chain, calls) => {
            budget.reserve();
            return app.rpc.batch(chain, calls);
          },
        },
        signer: createPersistentJwsSigner(metered),
        discovery: { budget },
        clock: app.clock,
      });
      const result = await service.discoverPayment(made.id);
      equal(reachedResponse, true);
      equal(result.invoice.id, made.id);
      equal(result.invoice.status, mode === "existing" ? "finalized" : "provisionally_paid");
      equal(result.discovery?.found, true);
      if (mode === "fresh") equal(result.eventJws, (await app.service.listOutbox())[0].compactJws);
      else equal(result.eventJws, undefined);
      equal(budget.remaining, 1);
    } finally {
      app.kv.close();
    }
  });
}

test("wallet report preserves its verified fresh event when the account hunt has one slot", async () => {
  const app = await setup();
  try {
    const made = await app.create("wallet");
    const original = app.rpc.add(await app.service.getInvoiceRecord(made.id), 200n);
    app.rpc.latest = 300n;
    await app.service.claimPayment(made.id, coordinates(original));
    app.rpc.safe = 250n;
    const budget = new WorkerRequestBudget(200);
    let boundary = false;
    const metered = responseBoundaryKv(
      app.kv,
      budget,
      () => {
        boundary = true;
      },
      false,
      true,
    );
    const service = createPaymentService({
      kv: metered,
      rpc: {
        batch: (chain, calls) => {
          budget.reserve();
          return app.rpc.batch(chain, calls);
        },
      },
      signer: createPersistentJwsSigner(metered),
      discovery: { budget },
      clock: app.clock,
    });
    const result = await service.reportPayment(made.id, coordinates(original));
    equal(boundary, true);
    equal(result.invoice.status, "provisionally_paid");
    equal(result.accountHunt?.pending, true);
    equal(result.eventJws, (await app.service.listOutbox())[0].compactJws);
    ok(result.eventJws);
    equal(budget.remaining, 1);
  } finally {
    app.kv.close();
  }
});

class ReviewRpc extends RuntimeRpc {
  reorged = new Set<string>();
  keepReorgedLogs = false;
  hidden = new Set<string>();
  unavailable = new Map<string, "receipt" | "block" | "provider">();
  override async batch(chain: number, calls: readonly JsonRpcCall[]) {
    const results = await super.batch(chain, calls);
    const transaction = calls.find((call) => call.id === "receipt")?.params[0];
    const failure = typeof transaction === "string" ? this.unavailable.get(transaction) : undefined;
    return results.map((response, index) => {
      const call = calls[index];
      if (call.method === "eth_getLogs" && Array.isArray(response.result))
        return {
          ...response,
          result: response.result.filter(
            (log) =>
              (!this.reorged.has(log.transactionHash) || this.keepReorgedLogs) &&
              !this.hidden.has(log.transactionHash),
          ),
        };
      if (failure === "provider" && call.id === "receipt")
        return {
          jsonrpc: "2.0",
          id: call.id,
          error: { code: -32000, message: "offline provider unavailable" },
        };
      if (
        (failure === "receipt" && call.id === "receipt") ||
        (failure === "block" && call.id === "event_block")
      )
        return { ...response, result: null };
      if (
        typeof transaction === "string" &&
        this.reorged.has(transaction) &&
        call.id === "event_block"
      )
        return {
          ...response,
          result: { ...(response.result as object), hash: `0x${"ee".repeat(32)}` },
        };
      return response;
    });
  }
}

async function setup() {
  const kv = new SqliteKv();
  const rpc = new ReviewRpc();
  const clock = { now: () => new Date("2026-10-18T00:00:00Z") };
  let serial = 0;
  const service = createPaymentService({
    kv,
    rpc,
    clock,
    signer: createPersistentJwsSigner(kv, clock),
    idGenerator: { next: () => `review-${++serial}` },
    discountStart: () => 1n,
  });
  await service.configureChain(8453, { enabled: true, treasuryAddress: treasury });
  await service.publishCatalogVersion({ merchantId: "merchant" }, "prices", {
    currency: "USD",
    products: [
      {
        productId: "credits",
        label: "Credits",
        kind: "credit_pack",
        priceCents: 100,
        enabled: true,
      },
    ],
  });
  return {
    kv,
    rpc,
    clock,
    service,
    create: (idempotencyKey: string) =>
      service.createInvoice(
        { merchantId: "merchant", idempotencyKey },
        {
          customerReference: "account",
          pricingVersionId: "prices",
          productId: "credits",
          chainId: 8453,
        },
      ),
  };
}

function responseBoundaryKv(
  kv: Kv,
  budget: WorkerRequestBudget,
  onBoundary: () => void,
  changeVersion = false,
  walletBoundary = false,
): Kv {
  let changed = false;
  return new Proxy(kv, {
    get(target, property) {
      if (property === "atomic")
        return () => {
          const base = target.atomic();
          let final = false;
          let freshInvoice: { key: KvKey; value: InvoiceRecord } | undefined;
          const atomic: KvAtomic = {
            check(...checks) {
              base.check(...checks);
              return this;
            },
            set(key, value, options) {
              base.set(key, value, options);
              const run = value as { complete?: boolean; lease?: unknown };
              final ||=
                key.includes("account_runs") && run.complete === true && run.lease === undefined;
              if (
                key.includes("invoices") &&
                (value as InvoiceRecord).status === "provisionally_paid"
              )
                freshInvoice = { key, value: value as InvoiceRecord };
              return this;
            },
            delete(key) {
              base.delete(key);
              return this;
            },
            async commit() {
              budget.reserve();
              const committed = await base.commit();
              if (committed.ok && freshInvoice && changeVersion && !changed) {
                // An independent writer changes the invoice version while all public phase fields recur.
                await target.set(freshInvoice.key, freshInvoice.value);
                changed = true;
              }
              if (committed.ok && freshInvoice && walletBoundary) {
                while (budget.remaining > 2) budget.reserve();
                onBoundary();
              }
              if (committed.ok && final) {
                while (budget.remaining > 1) budget.reserve();
                onBoundary();
              }
              return committed;
            },
          };
          return atomic;
        };
      const value = Reflect.get(target, property);
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => {
        budget.reserve();
        return value.apply(target, args);
      };
    },
  });
}
