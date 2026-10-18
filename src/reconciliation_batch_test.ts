import { test } from "bun:test";
import { deepStrictEqual as equal, ok, rejects } from "node:assert/strict";
import { createCheckoutServices } from "../main.ts";
import { maintenanceChainAt, reconcileInvoiceBatch } from "./reconciliation_batch.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { WorkerBudgetError, WorkerRequestBudget } from "./worker_budget.ts";
import type {
  Kv,
  KvAtomic,
  KvEntries,
  KvEntryMaybe,
  KvKey,
  KvListOptions,
  KvListSelector,
} from "./kv.ts";
import type { JsonRpcBatchItem, JsonRpcCall, RpcClient } from "./rpc.ts";
import type { SupportedChainId } from "./payment_types.ts";

const prefix = ["uos_checkout", "v1", "payments"];
const scanKey = (id: string) => [...prefix, "invoice_reconciliation_cursors", 8453, id];

test("one-minute maintenance gives each supported chain one turn every three minutes", () => {
  equal(
    [0, 60_000, 120_000, 180_000, 240_000, 300_000].map(maintenanceChainAt),
    [1, 10, 8453, 1, 10, 8453],
  );
});

test("a backlog larger than the invocation budget reaches later invoices after exhaustion", async () => {
  const app = await setup(48);
  try {
    const first = [...app.ids].sort()[0];
    await rejects(() => app.run(5), WorkerBudgetError);
    equal((await app.db.get(scanKey(first))).value, null);
    const seen: string[] = [];
    for (let index = 0; index < app.ids.length; index++) {
      const result = await app.run(24);
      ok(result);
      seen.push(result.invoiceId);
    }
    equal(new Set(seen), new Set(app.ids));
    equal(seen.at(-1), first);
    for (const id of app.ids) ok((await app.db.get(scanKey(id))).value);
  } finally {
    app.db.close();
  }
});

test("scan ranges stay bounded, advance after wrap, and retain their cursor on failure", async () => {
  const app = await setup(1);
  try {
    const first = await app.run(24);
    ok(first?.fromBlock !== undefined && first.toBlock !== undefined);
    equal(first.toBlock - first.fromBlock + 1n, 1000n);
    const before = await app.db.get(scanKey(app.ids[0]));
    await rejects(() => app.run(5), WorkerBudgetError);
    equal(await app.db.get(scanKey(app.ids[0])), before);
    const next = await app.run(24);
    ok(next?.toBlock !== undefined && next.fromBlock !== undefined);
    ok(next.toBlock > first.toBlock);
    equal(next.fromBlock, first.toBlock + 1n - 128n);
    equal(next.toBlock - next.fromBlock + 1n, 1000n);
  } finally {
    app.db.close();
  }
});

test("concurrent maintenance selection uses CAS and selects distinct pending invoices", async () => {
  const app = await setup(3);
  try {
    const results = await Promise.all([app.run(24), app.run(24)]);
    ok(results[0] && results[1]);
    ok(results[0].invoiceId !== results[1].invoiceId);
  } finally {
    app.db.close();
  }
});

async function setup(count: number) {
  const db = new SqliteKv();
  let head = 100n;
  const rpc: RpcClient = {
    async batch(
      _chainId: SupportedChainId,
      calls: readonly JsonRpcCall[],
    ): Promise<JsonRpcBatchItem[]> {
      return calls.map((call) => ({
        id: call.id,
        jsonrpc: "2.0",
        result:
          call.method === "eth_getLogs"
            ? []
            : {
                number: `0x${head.toString(16)}`,
                hash: `0x${"ab".repeat(32)}`,
                timestamp: `0x${Math.floor(Date.now() / 1000).toString(16)}`,
              },
      }));
    },
  };
  const services = createCheckoutServices(db, { rpc });
  await services.payment.configureChain(8453, {
    enabled: true,
    treasuryAddress: "0x1111111111111111111111111111111111111111",
  });
  await services.payment.publishCatalogVersion({ merchantId: "merchant-a" }, "batch-v1", {
    currency: "USD",
    products: [
      {
        productId: "pack:batch",
        label: "Batch",
        kind: "credit_pack",
        priceCents: 500,
        enabled: true,
      },
    ],
  });
  const ids: string[] = [];
  for (let index = 0; index < count; index++) {
    const invoice = await services.payment.createInvoice(
      { merchantId: "merchant-a", idempotencyKey: `attempt-${index}` },
      {
        customerReference: `customer-${index}`,
        pricingVersionId: "batch-v1",
        productId: "pack:batch",
        chainId: 8453,
      },
    );
    ids.push(invoice.id);
  }
  head = 20_000n;
  return {
    db,
    ids,
    run(maximum: number) {
      const budget = new WorkerRequestBudget(maximum);
      const storage = budgetedKv(db, budget);
      const guardedRpc: RpcClient = {
        batch(chainId, calls) {
          budget.reserve();
          return rpc.batch(chainId, calls);
        },
      };
      return reconcileInvoiceBatch(createCheckoutServices(storage, { rpc: guardedRpc }), 8453);
    },
  };
}

function budgetedKv(kv: Kv, budget: WorkerRequestBudget): Kv {
  return {
    listPage: (prefix, options) => {
      budget.reserve();
      return kv.listPage(prefix, options);
    },
    get<T>(key: KvKey): Promise<KvEntryMaybe<T>> {
      budget.reserve();
      return kv.get<T>(key);
    },
    getMany<T extends readonly unknown[]>(keys: readonly KvKey[]): Promise<KvEntries<T>> {
      budget.reserve();
      return kv.getMany<T>(keys);
    },
    set(key, value, options) {
      budget.reserve();
      return kv.set(key, value, options);
    },
    delete(key) {
      budget.reserve();
      return kv.delete(key);
    },
    async *list<T>(selector: KvListSelector, options?: KvListOptions) {
      budget.reserve();
      yield* kv.list<T>(selector, options);
    },
    atomic() {
      const operation = kv.atomic();
      const wrapper: KvAtomic = {
        check(...entries) {
          operation.check(...entries);
          return wrapper;
        },
        set(key, value, options) {
          operation.set(key, value, options);
          return wrapper;
        },
        delete(key) {
          operation.delete(key);
          return wrapper;
        },
        commit() {
          budget.reserve();
          return operation.commit();
        },
      };
      return wrapper;
    },
    close() {},
  };
}
