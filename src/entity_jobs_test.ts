import { test } from "bun:test";
import { deepStrictEqual as equal, ok } from "node:assert/strict";
import { createCheckoutServices } from "../main.ts";
import { createPaymentService } from "./payment_service.ts";
import { createEntityJobRunner, type ExpiryPass } from "./entity_jobs.ts";
import { expiryDiscoveryStateKey, discoveryStateKey } from "./payment_discovery.ts";
import { CHAIN_CONFIGURATIONS, type InvoiceRecord, type PaymentRecord } from "./payment_types.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { RpcError, type JsonRpcCall, type RpcClient } from "./rpc.ts";
import type { EntityJob, WakeTarget } from "./wakeup.ts";

const prefix = ["uos_checkout", "v1", "payments"];
const hash = (n: number) => `0x${n.toString(16).padStart(64, "0")}`;

test("expiry scans one full fixed snapshot in bounded chunks and never consumes manual progress", async () => {
  const app = await fixture();
  try {
    await app.kv.set(discoveryStateKey("invoice"), { searchedThrough: 999999n });
    let job = await app.runner.plan(app.target);
    ok(job);
    equal(job.dueAt, app.expiresAt);
    equal(await app.runner.run(job, async () => true), job);
    equal(app.rpc.calls, 0);
    app.setNow(app.expiresAt);
    job = await app.runner.run(job, async () => true);
    ok(job);
    equal((await app.kv.get<ExpiryPass>(expiryDiscoveryStateKey("invoice"))).value?.toBlock, 5000n);
    app.rpc.head = 10000n;
    for (let turn = 0; job && turn < 12; turn++) {
      app.setNow(Math.max(app.now(), job.dueAt));
      job = await app.runner.run(job, async () => true);
    }
    equal(job, null);
    equal((await app.services.payment.getInvoice("invoice")).status, "expired");
    const pass = (await app.kv.get<ExpiryPass>(expiryDiscoveryStateKey("invoice"))).value;
    equal(pass?.status, "complete");
    equal(pass?.searchedThrough, 5000n);
    equal(app.rpc.ranges, [
      [100n, 1099n],
      [1100n, 2099n],
      [2100n, 3099n],
      [3100n, 4099n],
      [4100n, 5000n],
    ]);
    equal((await app.kv.get(discoveryStateKey("invoice"))).value, { searchedThrough: 999999n });
    equal(await app.runner.plan(app.target), null);
    equal((await app.kv.get([...prefix, "invoices_by_chain", 8453, "invoice"])).value, null);
  } finally {
    app.kv.close();
  }
});

test("late expiry starts its own bounded window and three transient failures terminate it honestly", async () => {
  const app = await fixture();
  try {
    app.setNow(app.expiresAt + 3600000);
    const started = app.now();
    app.rpc.fail = true;
    let job = await app.runner.plan(app.target);
    for (let turn = 0; job && turn < 8; turn++) {
      app.setNow(Math.max(app.now(), job.dueAt));
      job = await app.runner.run(job, async () => true);
    }
    equal(job, null);
    equal(app.rpc.calls, 3);
    const pass = (await app.kv.get<ExpiryPass>(expiryDiscoveryStateKey("invoice"))).value;
    equal(pass?.startedAt, started);
    equal(pass?.status, "partial");
    equal(pass?.failures, 3);
    equal((await app.services.payment.getInvoice("invoice")).status, "expired");
  } finally {
    app.kv.close();
  }
});

test("lazy expiration preserves an already captured expiry pass", async () => {
  const app = await fixture();
  try {
    app.setNow(app.expiresAt);
    let job = await app.runner.plan(app.target);
    ok(job);
    job = await app.runner.run(job, async () => true);
    ok(job);
    await app.services.payment.expireInvoice("invoice");
    const adopted = await app.runner.plan({ ...app.target, dueAt: null, reason: "cancel" }, job);
    ok(adopted);
    for (let turn = 0, next: EntityJob | null = adopted; turn < 12; turn++) {
      if (!next) break;
      app.setNow(Math.max(app.now(), next.dueAt));
      next = await app.runner.run(next, async () => true);
    }
    equal(
      (await app.kv.get<ExpiryPass>(expiryDiscoveryStateKey("invoice"))).value?.status,
      "complete",
    );
  } finally {
    app.kv.close();
  }
});

test("known-payment milestones never search for new payments and stop when finality remains late", async () => {
  const app = await fixture();
  try {
    const payment: PaymentRecord = {
      schemaVersion: 1,
      id: "payment",
      invoiceId: "invoice",
      chainId: 8453,
      transactionHash: hash(7),
      logIndex: 0,
      payer: `0x${"33".repeat(20)}`,
      tokenAddress: app.invoice.tokenAddress,
      treasuryAddress: app.invoice.treasuryAddress,
      amountUnits: app.invoice.paidAmountUnits,
      blockNumber: 110n,
      blockHash: hash(110),
      blockTimestamp: new Date(app.now()).toISOString(),
      phase: "confirming",
      firstObservedAt: new Date(app.now()).toISOString(),
      updatedAt: new Date(app.now()).toISOString(),
    };
    await app.kv.set([...prefix, "payment_records", "payment"], payment);
    await app.kv.set([...prefix, "invoices", "invoice"], {
      ...app.invoice,
      paymentId: "payment",
      status: "confirming",
    });
    const start = app.now();
    let job = await app.runner.plan(app.target);
    ok(job);
    equal(job.dueAt, start + 120000);
    app.setNow(job.dueAt);
    job = await app.runner.run(job, async () => true);
    ok(job);
    equal(job.dueAt, start + 1200000);
    app.setNow(job.dueAt);
    job = await app.runner.run(job, async () => true);
    equal(job, null);
    equal(app.rpc.ranges, []);
    equal((await app.services.payment.getInvoice("invoice")).status, "confirming");
  } finally {
    app.kv.close();
  }
});

async function fixture() {
  const kv = new SqliteKv();
  let instant = Date.parse("2026-10-18T00:00:00Z");
  const rpc = new JobRpc();
  const invoice: InvoiceRecord = {
    schemaVersion: 1,
    id: "invoice",
    merchant: "merchant",
    idempotencyKey: "create",
    customerReference: "customer",
    pricingVersionId: "prices",
    catalogDigest: "digest",
    productId: "pack",
    productKind: "credit_pack",
    chainId: 8453,
    networkName: "Base",
    tokenAddress: CHAIN_CONFIGURATIONS[8453].tokenAddress.toLowerCase(),
    tokenSymbol: "USDC",
    tokenDecimals: 6,
    treasuryAddress: `0x${"44".repeat(20)}`,
    listAmountUnits: 5000000n,
    paidAmountUnits: 4999999n,
    discountUnits: 1n,
    minimumBlock: 100n,
    status: "awaiting_payment",
    createdAt: new Date(instant).toISOString(),
    expiresAt: new Date(instant + 60000).toISOString(),
    updatedAt: new Date(instant).toISOString(),
  };
  await kv.set([...prefix, "invoices", "invoice"], invoice);
  await kv.set([...prefix, "invoices_by_chain", 8453, "invoice"], "invoice");
  const services = createCheckoutServices(kv, { rpc });
  services.payment = createPaymentService({
    kv,
    rpc,
    clock: { now: () => new Date(instant) },
    signer: {
      sign: async () => "offline-signature",
      jwks: async () => ({ keys: [] }),
      kid: async () => "offline",
    },
  });
  const target: WakeTarget = {
    kind: "invoice",
    id: "invoice",
    dueAt: Date.parse(invoice.expiresAt),
    reason: "expiry",
  };
  return {
    kv,
    rpc,
    invoice,
    target,
    services,
    runner: createEntityJobRunner(services, { now: () => instant }),
    expiresAt: Date.parse(invoice.expiresAt),
    now: () => instant,
    setNow: (value: number) => {
      instant = value;
    },
  };
}

class JobRpc implements RpcClient {
  calls = 0;
  head = 5000n;
  fail = false;
  ranges: bigint[][] = [];
  async batch(_chain: 1 | 10 | 8453, calls: readonly JsonRpcCall[]) {
    this.calls++;
    if (this.fail) throw new RpcError("offline_rpc_failure");
    return calls.map((call) => {
      let result: unknown = null;
      if (call.id === "transfer_logs") {
        const range = call.params[0] as { fromBlock: string; toBlock: string };
        this.ranges.push([BigInt(range.fromBlock), BigInt(range.toBlock)]);
        result = [];
      } else if (call.id === "latest" || call.id === "event_block") {
        const n = call.id === "latest" ? this.head : BigInt(String(call.params[0]));
        result = { number: `0x${n.toString(16)}`, hash: hash(Number(n)), timestamp: "0x6ad3d000" };
      }
      return { jsonrpc: "2.0" as const, id: call.id, result };
    });
  }
}
