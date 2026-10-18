import { test } from "bun:test";
import { deepStrictEqual as equal, ok } from "node:assert/strict";
import { createPaymentService } from "./payment_service.ts";
import { createPersistentJwsSigner } from "./jws.ts";
import {
  discoverInvoicePayment,
  discoveryStateKey,
  invoiceMatchesTransfer,
  type DiscoveryState,
} from "./payment_discovery.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { WorkerRequestBudget } from "./worker_budget.ts";
import { createIntentWakeupPort, type WakeupPort } from "./wakeup.ts";
import { UbqKv } from "./ubq_kv.ts";
import { decodeKvValue, encodeKvValue } from "./kv_codec.ts";
import type { Kv, KvCheck, KvKey } from "./kv.ts";
import type { InvoiceRecord } from "./payment_types.ts";
import {
  TRANSFER_TOPIC,
  type DiscoveredTransferLog,
  type JsonRpcCall,
  type JsonRpcBatchItem,
  type RpcClient,
} from "./rpc.ts";

const prefix = ["uos_checkout", "v1", "payments"];
const treasury = `0x${"44".repeat(20)}`;
const hash = (value: number) => `0x${value.toString(16).padStart(64, "0")}`;
const quantity = (value: bigint) => `0x${value.toString(16)}`;
const topic = (address: string) => `0x${address.slice(2).padStart(64, "0")}`;

test("discovery resumes bounded chunks after a service restart without changing reconciliation cursors", async () => {
  const app = await setup();
  try {
    const globalKey = [...prefix, "reconciliation_cursors", 8453];
    const batchKey = [...prefix, "invoice_reconciliation_cursors", 8453, app.invoice.id];
    await app.kv.set(globalKey, { blockNumber: 7000n });
    await app.kv.set(batchKey, { blockNumber: 8000n });
    app.rpc.latest = 5000n;
    const first = await app.service.discoverPayment(app.invoice.id);
    equal(first.discovery, { found: false, searchedToBlock: "1099" });
    const second = await app.restart().discoverPayment(app.invoice.id);
    equal(second.discovery, { found: false, searchedToBlock: "1971" });
    equal(app.rpc.ranges, [
      [100n, 1099n],
      [972n, 1971n],
    ]);
    equal((await app.kv.get(globalKey)).value, { blockNumber: 7000n });
    equal((await app.kv.get(batchKey)).value, { blockNumber: 8000n });
  } finally {
    app.kv.close();
  }
});

test("a busy discovery lease returns only committed progress without making RPC calls", async () => {
  const app = await setup();
  try {
    const state: DiscoveryState = {
      searchedThrough: 1099n,
      leaseId: "busy",
      leaseUntil: app.now() + 30_000,
    };
    await app.kv.set(discoveryStateKey(app.invoice.id), state);
    const before = app.rpc.calls;
    equal((await app.service.discoverPayment(app.invoice.id)).discovery, {
      found: false,
      searchedToBlock: "1099",
    });
    equal(app.rpc.calls, before);
    equal((await app.kv.get(discoveryStateKey(app.invoice.id))).value, state);
  } finally {
    app.kv.close();
  }
});

test("matching logs with unavailable receipts retain the retry position beyond the overlap", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 5000n;
    await app.service.discoverPayment(app.invoice.id);
    const transfer = app.rpc.add(app.invoice, 1150n);
    app.rpc.hidden.add(transfer.transactionHash);
    equal((await app.service.discoverPayment(app.invoice.id)).discovery, {
      found: false,
      searchedToBlock: "1099",
    });
    app.rpc.hidden.clear();
    const found = await app.service.discoverPayment(app.invoice.id);
    equal(found.discovery?.found, true);
    ok(found.invoice.paymentId);
    equal((await app.kv.get(discoveryStateKey(app.invoice.id))).value, null);
    const replay = await app.service.discoverPayment(app.invoice.id);
    equal(replay.invoice.paymentId, found.invoice.paymentId);
    equal(replay.discovery?.found, true);
    equal((await app.service.listOutbox()).length, 0);
  } finally {
    app.kv.close();
  }
});

test("discovery uses coherent safe or latest heads and one shared exact match predicate", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 200n;
    app.rpc.safe = 120n;
    app.rpc.finalized = 110n;
    equal((await app.service.discoverPayment(app.invoice.id)).discovery?.searchedToBlock, "120");
    app.rpc.safe = null;
    app.rpc.finalized = 150n;
    equal((await app.service.discoverPayment(app.invoice.id)).discovery?.searchedToBlock, "150");
    equal(
      (await app.kv.get<DiscoveryState>(discoveryStateKey(app.invoice.id))).value?.searchedThrough,
      150n,
    );
    app.rpc.finalized = null;
    equal((await app.service.discoverPayment(app.invoice.id)).discovery?.searchedToBlock, "200");
    const log = app.rpc.add(app.invoice, 150n);
    equal(invoiceMatchesTransfer(app.invoice, log), true);
    for (const changed of [
      { ...log, blockNumber: 99n },
      { ...log, tokenAddress: treasury },
      { ...log, recipientAddress: log.payerAddress },
      { ...log, amountUnits: log.amountUnits + 1n },
    ])
      equal(invoiceMatchesTransfer(app.invoice, changed), false);
    equal(
      invoiceMatchesTransfer({ ...app.invoice, paymentId: "paid", status: "confirming" }, log),
      false,
    );
    equal(
      invoiceMatchesTransfer({ ...app.invoice, paymentId: "paid", status: "reverted" }, log),
      true,
    );
  } finally {
    app.kv.close();
  }
});

for (const replacement of [false, true]) {
  test(`a concurrent claim and reorg derives discovery from the returned state (replacement=${replacement})`, async () => {
    const app = await setup();
    try {
      app.rpc.latest = 111n;
      const original = app.rpc.add(app.invoice, 110n);
      if (replacement) app.rpc.add(app.invoice, 111n);
      await app.kv.set(discoveryStateKey(app.invoice.id), { searchedThrough: 100n });
      const gate = deferred();
      const entered = deferred();
      app.rpc.beforeLogs = async () => {
        entered.resolve();
        await gate.promise;
      };
      const pending = app.service.discoverPayment(app.invoice.id);
      await entered.promise;
      await app.service.claimPayment(app.invoice.id, coordinates(original));
      app.rpc.reorged.add(original.transactionHash);
      gate.resolve();
      const result = await pending;
      equal(result.discovery?.found, replacement);
      equal(Boolean(result.invoice.paymentId), replacement);
      equal(result.eventJws, undefined);
      if (!replacement) equal(result.invoice.status, "awaiting_payment");
      equal((await app.kv.get(discoveryStateKey(app.invoice.id))).value, null);
    } finally {
      app.kv.close();
    }
  });
}

test("an expired scanner cannot clear or overwrite a successor's lease", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 5000n;
    const gate = deferred();
    const entered = deferred();
    app.rpc.beforeLogs = async () => {
      entered.resolve();
      await gate.promise;
    };
    const pending = app.service.discoverPayment(app.invoice.id);
    await entered.promise;
    app.advance(31_000);
    const successor: DiscoveryState = {
      searchedThrough: 2000n,
      leaseId: "successor",
      leaseUntil: app.now() + 30_000,
    };
    await app.kv.set(discoveryStateKey(app.invoice.id), successor);
    gate.resolve();
    equal((await pending).discovery, { found: false, searchedToBlock: "2000" });
    equal((await app.kv.get(discoveryStateKey(app.invoice.id))).value, successor);
  } finally {
    app.kv.close();
  }
});

test("same-transaction reinclusion can move the log index without replaying an old acceptance", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 115n;
    const original = app.rpc.add(app.invoice, 110n);
    await app.service.claimPayment(app.invoice.id, coordinates(original));
    app.rpc.safe = 110n;
    const accepted = await app.service.refreshInvoice(app.invoice.id);
    ok(accepted.eventJws);
    app.rpc.reorged.add(original.transactionHash);
    equal((await app.service.refreshInvoice(app.invoice.id)).invoice.status, "reverted");
    app.rpc.reorged.clear();
    original.blockNumber = 120n;
    original.blockHash = hash(1120);
    original.logIndex = 7;
    app.rpc.latest = 120n;
    app.rpc.safe = null;
    const recovered = await app.service.discoverPayment(app.invoice.id);
    equal(recovered.discovery?.found, true);
    equal(recovered.invoice.status, "confirming");
    equal(recovered.invoice.paymentId, accepted.invoice.paymentId);
    equal(recovered.eventJws, undefined);
    equal(
      (await app.kv.get([...prefix, "payment_locators", 8453, original.transactionHash, 0])).value,
      null,
    );
    const replay = await app.service.claimPayment(app.invoice.id, coordinates(original));
    equal(replay.eventJws, undefined);
  } finally {
    app.kv.close();
  }
});

test("an unrelated duplicate does not prevent discovery from reincluding the original payment", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 115n;
    const original = app.rpc.add(app.invoice, 110n);
    await app.service.claimPayment(app.invoice.id, coordinates(original));
    app.rpc.reorged.add(original.transactionHash);
    await app.service.refreshInvoice(app.invoice.id);
    app.rpc.reorged.clear();
    original.blockNumber = 120n;
    original.blockHash = hash(1120);
    original.logIndex = 7;
    app.rpc.add(app.invoice, 119n);
    app.rpc.latest = 120n;
    const found = await app.service.discoverPayment(app.invoice.id);
    equal(found.discovery?.found, true);
    equal(found.invoice.status, "confirming");
    equal(found.eventJws, undefined);
  } finally {
    app.kv.close();
  }
});

test("the Worker budget retains completion headroom for partial results and later progress", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 5000n;
    const budget = new WorkerRequestBudget(28);
    // Authentication and the merchant ownership read share the same invocation.
    for (let index = 0; index < 4; index++) budget.reserve();
    const storage = countedKv(app.kv, budget);
    const rpc: RpcClient = {
      batch: (chain, calls) => {
        budget.reserve();
        return app.rpc.batch(chain, calls);
      },
    };
    const service = app.restart(storage, rpc, budget);
    equal((await service.discoverPayment(app.invoice.id)).discovery, {
      found: false,
      searchedToBlock: "1099",
    });
    ok(budget.remaining >= 0);
    const low = new WorkerRequestBudget(28);
    // Leave less than the admission minimum after the initial storage read.
    for (let index = 0; index < 16; index++) low.reserve();
    const lowRpc: RpcClient = {
      batch: (chain, calls) => {
        low.reserve();
        return app.rpc.batch(chain, calls);
      },
    };
    const before = app.rpc.calls;
    const yielded = await app
      .restart(countedKv(app.kv, low), lowRpc, low)
      .discoverPayment(app.invoice.id);
    equal(yielded.discovery, { found: false, searchedToBlock: "1099" });
    equal(app.rpc.calls, before);
    ok(low.remaining >= 0);
    const found = app.rpc.add(app.invoice, 1150n);
    app.rpc.hidden.add(found.transactionHash);
    const retryBudget = new WorkerRequestBudget(28);
    const retryRpc: RpcClient = {
      batch: (chain, calls) => {
        retryBudget.reserve();
        return app.rpc.batch(chain, calls);
      },
    };
    const pending = await app
      .restart(countedKv(app.kv, retryBudget), retryRpc, retryBudget)
      .discoverPayment(app.invoice.id);
    equal(pending.discovery, { found: false, searchedToBlock: "1099" });
    ok(retryBudget.remaining >= 0);
    app.rpc.hidden.clear();
    const foundBudget = new WorkerRequestBudget(28);
    for (let index = 0; index < 4; index++) foundBudget.reserve();
    const foundRpc: RpcClient = {
      batch: (chain, calls) => {
        foundBudget.reserve();
        return app.rpc.batch(chain, calls);
      },
    };
    const paid = await app
      .restart(countedKv(app.kv, foundBudget), foundRpc, foundBudget)
      .discoverPayment(app.invoice.id);
    equal(paid.discovery?.found, true);
    equal(paid.invoice.status, "confirming");
    equal(paid.eventJws, undefined);
    ok(foundBudget.remaining > 0);
  } finally {
    app.kv.close();
  }
});

for (const maximum of [12, 16]) {
  test(`optional head retry preserves mandatory payment writes with budget ${maximum}`, async () => {
    const app = await setup();
    try {
      app.rpc.latest = 115n;
      const transfer = app.rpc.add(app.invoice, 110n);
      const budget = new WorkerRequestBudget(maximum);
      const requests: string[][] = [];
      const rpc: RpcClient = {
        async batch(chain, calls) {
          budget.reserve();
          requests.push(calls.map(({ id }) => id));
          const responses = await app.rpc.batch(chain, calls);
          return calls.some(({ id }) => id === "receipt")
            ? responses.map((item) =>
                item.id === "safe" || item.id === "finalized"
                  ? { jsonrpc: "2.0", id: item.id, error: { code: -32000, message: "unavailable" } }
                  : item,
              )
            : responses;
        },
      };
      const result = await app
        .restart(countedKv(app.kv, budget), rpc, budget)
        .claimPayment(app.invoice.id, coordinates(transfer));
      equal(result.invoice.status, "confirming");
      equal(result.eventJws, undefined);
      equal(requests.length, maximum === 12 ? 1 : 2);
      if (maximum === 16) equal(requests[1], ["safe", "finalized"]);
      ok(budget.remaining > 0);
      equal((await app.service.getInvoice(app.invoice.id)).paymentId, result.invoice.paymentId);
    } finally {
      app.kv.close();
    }
  });
}

test("real service-adapter calls keep intent prearming and a zero-search-budget response within24", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 5000n;
    app.rpc.add(app.invoice, 1150n);
    await app.kv.set(discoveryStateKey(app.invoice.id), { searchedThrough: 1099n });
    const budget = new WorkerRequestBudget(28);
    for (let index = 0; index < 6; index++) budget.reserve(); // four auth calls, ownership, migration RPC
    let storageCalls = 0,
      prepareCalls = 0,
      rpcCalls = 0;
    let sawCompletionBoundary = false;
    const wire = new UbqKv({
      url: "https://kv.invalid",
      token: "offline",
      fetchImpl: async (url, init) => {
        budget.reserve();
        storageCalls++;
        if (budget.remaining === 0) sawCompletionBoundary = true;
        const body = JSON.parse(String(init.body)) as {
          key: KvKey;
          keys: KvKey[];
          checks: KvCheck[];
          mutations: { type: "set" | "delete"; key: KvKey; value?: unknown }[];
        };
        const path = new URL(url).pathname;
        if (path === "/kv/get") {
          const entry = await app.kv.get(body.key);
          return Response.json({
            ok: true,
            entry:
              entry.versionstamp === null
                ? null
                : { ...entry, value: encodeKvValue(decodeKvValue(entry.value)) },
          });
        }
        if (path === "/kv/getMany") {
          const entries = await app.kv.getMany(body.keys);
          return Response.json({
            ok: true,
            entries: entries.map((entry) =>
              entry.versionstamp === null
                ? null
                : { ...entry, value: encodeKvValue(decodeKvValue(entry.value)) },
            ),
          });
        }
        if (path === "/kv/atomic") {
          const atomic = app.kv.atomic().check(...body.checks);
          for (const mutation of body.mutations) {
            if (mutation.type === "set") atomic.set(mutation.key, mutation.value);
            else atomic.delete(mutation.key);
          }
          return Response.json(await atomic.commit());
        }
        throw new Error(`unexpected_offline_path:${path}`);
      },
    });
    const rpc: RpcClient = {
      batch: (chain, calls) => {
        budget.reserve();
        rpcCalls++;
        return app.rpc.batch(chain, calls);
      },
    };
    const wakeups = createIntentWakeupPort({
      kv: wire,
      budget,
      transport: {
        prepare: async (intent) => {
          for (const _target of intent.targets) {
            budget.reserve();
            prepareCalls++;
          }
        },
      },
    });
    const result = await app.restart(wire, rpc, budget, wakeups).discoverPayment(app.invoice.id);
    equal(result.discovery?.found, true);
    equal(result.invoice.status, "confirming");
    equal(result.eventJws, undefined);
    equal(sawCompletionBoundary, false);
    ok(budget.remaining > 0);
    equal(prepareCalls, 1);
    console.log(
      `intent discovery calls: storage=${storageCalls},rpc=${rpcCalls},prearm=${prepareCalls},prior=6,total=${28 - budget.remaining},ceiling=28`,
    );
  } finally {
    app.kv.close();
  }
});

test("chain push uses the reserved invoice treasury snapshot and records one confirmation phase", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 115n;
    const transfer = app.rpc.add(app.invoice, 110n);
    await app.service.configureChain(8453, {
      enabled: false,
      treasuryAddress: `0x${"55".repeat(20)}`,
    });
    const event = { ...transfer, chainId: 8453 as const, removed: false };
    const context = { deadline: Date.now() + 20000, isCurrent: async () => true };
    equal((await app.service.processChainTransfer(event, context)).terminal, true);
    equal((await app.service.getInvoice(app.invoice.id)).status, "confirming");
    equal((await app.service.listOutbox()).length, 0);
    equal((await app.service.processChainTransfer(event, context)).disposition, "ignored");
    const publicReplay = await app.service.claimPayment(app.invoice.id, coordinates(transfer));
    equal(publicReplay.invoice.status, "confirming");
    equal(publicReplay.eventJws, undefined);
    app.rpc.finalized = 110n;
    const credited = await app.service.refreshInvoice(app.invoice.id);
    equal(credited.invoice.status, "provisionally_paid");
    ok(credited.eventJws);
  } finally {
    app.kv.close();
  }
});

test("out-of-order reinclusion uses two receipt phases and ignores the older removed hint", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 115n;
    const transfer = app.rpc.add(app.invoice, 110n);
    const original = { ...transfer, chainId: 8453 as const, removed: true };
    await app.service.claimPayment(app.invoice.id, coordinates(transfer));
    app.rpc.safe = 110n;
    const accepted = await app.service.refreshInvoice(app.invoice.id);
    transfer.blockNumber = 120n;
    transfer.blockHash = hash(1120);
    transfer.logIndex = 7;
    app.rpc.latest = 125n;
    app.rpc.safe = null;
    const event = { ...transfer, chainId: 8453 as const, removed: false };
    const context = { deadline: Date.now() + 20000, isCurrent: async () => true };
    equal((await app.service.processChainTransfer(event, context)).terminal, false);
    equal((await app.service.getInvoice(app.invoice.id)).status, "reverted");
    equal((await app.service.processChainTransfer(event, context)).terminal, true);
    const current = await app.service.getInvoice(app.invoice.id);
    equal(current.status, "confirming");
    equal(current.paymentId, accepted.invoice.paymentId);
    equal((await app.service.processChainTransfer(original, context)).disposition, "ignored");
    equal((await app.service.getInvoice(app.invoice.id)).status, "confirming");
  } finally {
    app.kv.close();
  }
});

test("an exact removed hint canonically reverts a finalized payment without a background monitor", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 115n;
    const transfer = app.rpc.add(app.invoice, 110n);
    await app.service.claimPayment(app.invoice.id, coordinates(transfer));
    app.rpc.safe = 110n;
    app.rpc.finalized = 110n;
    await app.service.refreshInvoice(app.invoice.id);
    await app.service.refreshInvoice(app.invoice.id);
    await app.service.refreshInvoice(app.invoice.id);
    equal((await app.service.getInvoice(app.invoice.id)).status, "finalized");
    app.rpc.reorged.add(transfer.transactionHash);
    const result = await app.service.processChainTransfer(
      { ...transfer, chainId: 8453, removed: true },
      { deadline: Date.now() + 20000, isCurrent: async () => true },
    );
    equal(result.terminal, true);
    equal((await app.service.getInvoice(app.invoice.id)).status, "reverted");
  } finally {
    app.kv.close();
  }
});

test("receipt ownership is checked in the payment CAS after canonical RPC returns", async () => {
  const app = await setup();
  try {
    app.rpc.latest = 115n;
    const transfer = app.rpc.add(app.invoice, 110n);
    const key = ["uos_checkout", "v1", "chain_events", "receipts", "test"];
    const stamp = await app.kv.set(key, "first");
    const batch = app.rpc.batch.bind(app.rpc);
    app.rpc.batch = async (chain, calls) => {
      const result = await batch(chain, calls);
      if (calls.some((call) => call.id === "receipt")) await app.kv.set(key, "successor");
      return result;
    };
    const result = await app.service.processChainTransfer(
      { ...transfer, chainId: 8453, removed: false },
      {
        deadline: Date.now() + 20000,
        isCurrent: async () => true,
        fence: { key, versionstamp: stamp.versionstamp },
      },
    );
    equal(result.terminal, false);
    equal((await app.service.getInvoice(app.invoice.id)).paymentId, undefined);
    equal((await app.kv.get(key)).value, "successor");
  } finally {
    app.kv.close();
  }
});

async function setup() {
  const kv = new SqliteKv();
  const rpc = new DiscoveryRpc();
  let instant = Date.parse("2026-10-18T00:00:00Z");
  let sequence = 0;
  const clock = { now: () => new Date(instant) };
  const restart = (
    storage: Kv = kv,
    client: RpcClient = rpc,
    budget?: WorkerRequestBudget,
    wakeups?: WakeupPort,
  ) => {
    const base = createPaymentService({
      kv: storage,
      rpc: client,
      clock,
      signer: createPersistentJwsSigner(storage, clock),
      idGenerator: { next: () => `discovery-${++sequence}` },
      discountStart: () => 1n,
      discovery: { budget },
      wakeups,
    });
    // These unit cases exercise the retained per-invoice backup algorithm directly.
    // HTTP account-hunt behavior has endpoint and workerd acceptance elsewhere.
    return {
      ...base,
      discoverPayment: (id: unknown) =>
        discoverInvoicePayment(
          {
            kv: storage,
            rpc: client,
            clock,
            claim: (invoiceId, input) => base.claimPayment(invoiceId, input),
            existingEvent: async (invoice) =>
              ["provisionally_paid", "settled", "finalized", "reverted"].includes(invoice.status)
                ? (await base.listOutbox()).find((event) => event.invoiceId === invoice.id)
                    ?.compactJws
                : undefined,
          },
          id,
          { budget },
        ),
    };
  };

  const service = restart();
  await service.configureChain(8453, { enabled: true, treasuryAddress: treasury });
  await service.publishCatalogVersion({ merchantId: "merchant" }, "prices", {
    currency: "USD",
    products: [
      { productId: "pack", label: "Pack", kind: "credit_pack", priceCents: 500, enabled: true },
    ],
  });
  const created = await service.createInvoice(
    { merchantId: "merchant", idempotencyKey: "create" },
    {
      customerReference: "customer",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    },
  );
  const invoice = await service.getInvoiceRecord(created.id);
  await service.jwks();
  return {
    kv,
    rpc,
    service,
    invoice,
    restart,
    now: () => instant,
    advance: (milliseconds: number) => {
      instant += milliseconds;
    },
  };
}

type Transfer = DiscoveredTransferLog & { timestamp: number };
class DiscoveryRpc implements RpcClient {
  latest = 99n;
  safe: bigint | null = null;
  finalized: bigint | null = null;
  calls = 0;
  ranges: bigint[][] = [];
  transfers: Transfer[] = [];
  hidden = new Set<string>();
  reorged = new Set<string>();
  beforeLogs?: () => Promise<void>;
  add(invoice: InvoiceRecord, blockNumber: bigint): Transfer {
    const transfer: Transfer = {
      transactionHash: hash(this.transfers.length + 1),
      logIndex: 0,
      blockNumber,
      blockHash: hash(Number(blockNumber) + 1000),
      tokenAddress: invoice.tokenAddress,
      recipientAddress: invoice.treasuryAddress,
      payerAddress: `0x${"33".repeat(20)}`,
      amountUnits: invoice.paidAmountUnits,
      timestamp: Date.parse(invoice.createdAt) / 1000 + 1,
    };
    this.transfers.push(transfer);
    return transfer;
  }
  async batch(_chain: 1 | 10 | 8453, calls: readonly JsonRpcCall[]): Promise<JsonRpcBatchItem[]> {
    this.calls++;
    const receiptCall = calls.find((call) => call.id === "receipt");
    const transfer = this.transfers.find(
      (candidate) => candidate.transactionHash === receiptCall?.params[0],
    );
    const block = (
      number: bigint,
      blockHash = hash(Number(number) + 1000),
      timestamp = 1792281601,
    ) => ({ number: quantity(number), hash: blockHash, timestamp: quantity(BigInt(timestamp)) });
    const log = (value: Transfer) => ({
      address: value.tokenAddress,
      transactionHash: value.transactionHash,
      logIndex: quantity(BigInt(value.logIndex)),
      blockNumber: quantity(value.blockNumber),
      blockHash: value.blockHash,
      removed: false,
      topics: [TRANSFER_TOPIC, topic(value.payerAddress), topic(value.recipientAddress)],
      data: `0x${value.amountUnits.toString(16).padStart(64, "0")}`,
    });
    const values: Record<string, unknown> = {
      latest: block(this.latest),
      safe: this.safe === null ? null : block(this.safe),
      finalized: this.finalized === null ? null : block(this.finalized),
      receipt:
        !transfer || this.hidden.has(transfer.transactionHash)
          ? null
          : {
              transactionHash: transfer.transactionHash,
              status: "0x1",
              blockNumber: quantity(transfer.blockNumber),
              blockHash: transfer.blockHash,
              logs: [log(transfer)],
            },
      event_block: transfer
        ? block(
            transfer.blockNumber,
            this.reorged.has(transfer.transactionHash) ? hash(99999) : transfer.blockHash,
            transfer.timestamp,
          )
        : null,
    };
    const scan = calls.find((call) => call.method === "eth_getLogs");
    if (scan) {
      const filter = scan.params[0] as { fromBlock: string; toBlock: string };
      const from = BigInt(filter.fromBlock),
        to = BigInt(filter.toBlock);
      this.ranges.push([from, to]);
      values.transfer_logs = this.transfers
        .filter((candidate) => candidate.blockNumber >= from && candidate.blockNumber <= to)
        .sort((a, b) => Number(a.blockNumber - b.blockNumber))
        .map(log);
      await this.beforeLogs?.();
    }
    return calls.map((call) => ({
      jsonrpc: "2.0",
      id: call.id,
      result:
        call.method === "eth_getLogs"
          ? (() => {
              const range = call.params[0] as { fromBlock: string; toBlock: string };
              return this.transfers
                .filter(
                  (candidate) =>
                    candidate.blockNumber >= BigInt(range.fromBlock) &&
                    candidate.blockNumber <= BigInt(range.toBlock),
                )
                .map(log);
            })()
          : values[call.id],
    }));
  }
}

function coordinates(transfer: Transfer) {
  return {
    transactionHash: transfer.transactionHash,
    logIndex: transfer.logIndex.toString(),
    observedBlockNumber: transfer.blockNumber.toString(),
    observedBlockHash: transfer.blockHash,
  };
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function countedKv(kv: Kv, budget: WorkerRequestBudget): Kv {
  return {
    listPage: (prefix, options) => {
      budget.reserve();
      return kv.listPage(prefix, options);
    },
    get: (key) => {
      budget.reserve();
      return kv.get(key);
    },
    getMany: (keys) => {
      budget.reserve();
      return kv.getMany(keys);
    },
    set: (key, value, options) => {
      budget.reserve();
      return kv.set(key, value, options);
    },
    delete: (key) => {
      budget.reserve();
      return kv.delete(key);
    },
    list: (selector, options) => {
      budget.reserve();
      return kv.list(selector, options);
    },
    atomic: () => {
      const atomic = kv.atomic();
      const commit = atomic.commit.bind(atomic);
      atomic.commit = () => {
        budget.reserve();
        return commit();
      };
      return atomic;
    },
    close: () => {},
  };
}
