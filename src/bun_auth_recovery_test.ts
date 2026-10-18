import { test } from "bun:test";
import { deepStrictEqual as equal, ok } from "node:assert/strict";
import { createHmac } from "node:crypto";
import { startCheckoutServer } from "../bun.ts";
import { BUN_DUE_PREFIX } from "./bun_queue.ts";
import type { KvAtomic, KvEntries, KvKey, KvPage, KvPageOptions } from "./kv.ts";
import { CHAIN_CONFIGURATIONS, type InvoiceRecord } from "./payment_types.ts";
import { RuntimeRpc } from "./account_runtime_test_fixture.ts";
import { treasuryHistoryKey } from "./account_store.ts";
import { TRANSFER_TOPIC } from "./rpc.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { WorkerRequestBudget } from "./worker_budget.ts";

const SIGNING_KEY = "local-auth-recovery-fixture";
const SUBSCRIPTION = "auth-recovery";
const INVOICE_ID = "auth-recovery-invoice";
const MERCHANT = "auth-recovery-merchant";
const PROXY_HEADERS = { host: "checkout.ubq.fi", "x-forwarded-proto": "https" };
type Timer = ReturnType<typeof setTimeout>;

class ObservedKv extends SqliteKv {
  readonly operations: { operation: string; key: KvKey }[] = [];
  override async get<T>(key: KvKey) {
    this.operations.push({ operation: "get", key });
    return super.get<T>(key);
  }
  override async getMany<T extends readonly unknown[]>(
    keys: readonly KvKey[],
  ): Promise<KvEntries<T>> {
    this.operations.push(...keys.map((key) => ({ operation: "getMany", key })));
    return super.getMany<T>(keys);
  }
  override async listPage<T>(prefix: KvKey, options: KvPageOptions): Promise<KvPage<T>> {
    this.operations.push({ operation: "listPage", key: prefix });
    return super.listPage<T>(prefix, options);
  }
  override atomic(): KvAtomic {
    const atomic = super.atomic();
    const operations = this.operations;
    const keys: KvKey[] = [];
    return {
      check(...entries) {
        atomic.check(...entries);
        return this;
      },
      set(key, value, options) {
        keys.push(key);
        atomic.set(key, value, options);
        return this;
      },
      delete(key) {
        keys.push(key);
        atomic.delete(key);
        return this;
      },
      async commit() {
        operations.push(...keys.map((key) => ({ operation: "commit", key })));
        return atomic.commit();
      },
    };
  }
  get queueOperations() {
    return this.operations.filter(({ key }) => key[3] === "bun_jobs" || key[3] === "bun_due");
  }
}

function payload(createdAt = new Date().toISOString()): string {
  return JSON.stringify({
    webhookId: SUBSCRIPTION,
    id: "notice",
    type: "ADDRESS_ACTIVITY",
    createdAt,
    event: { network: "BASE_MAINNET", activity: [] },
  });
}
const signature = (body: string) => createHmac("sha256", SIGNING_KEY).update(body).digest("hex");

async function serverFixture() {
  const kv = new ObservedKv();
  const now = new Date().toISOString();
  const invoice: InvoiceRecord = {
    schemaVersion: 1,
    id: INVOICE_ID,
    merchant: MERCHANT,
    idempotencyKey: "fixture",
    customerReference: "account",
    pricingVersionId: "prices",
    catalogDigest: "fixture",
    productId: "credits",
    productKind: "credit_pack",
    chainId: 8453,
    networkName: "Base",
    tokenAddress: CHAIN_CONFIGURATIONS[8453].tokenAddress,
    tokenSymbol: "USDC",
    tokenDecimals: 6,
    treasuryAddress: `0x${"44".repeat(20)}`,
    listAmountUnits: 1_000_000n,
    paidAmountUnits: 999_999n,
    discountUnits: 1n,
    minimumBlock: 1n,
    status: "awaiting_payment",
    createdAt: now,
    updatedAt: now,
    expiresAt: new Date(Date.now() + 3_600_000).toISOString(),
  };
  await kv.set(["uos_checkout", "v1", "payments", "invoices", invoice.id], invoice);
  await kv.set(["uos_checkout", "v1", "merchant_auth", "merchants", MERCHANT], {
    id: MERCHANT,
    name: "Local fixture",
    settlementWebhookUrl: "https://merchant.example/events",
    createdAt: now,
    updatedAt: now,
  });
  const handles = new Set<Timer>();
  const nativeSetTimeout = globalThis.setTimeout;
  let app: ReturnType<typeof startCheckoutServer>;
  // Only this queue retains the captured function; restore the global before awaiting startup.
  globalThis.setTimeout = ((callback: () => void, delay?: number) => {
    const handle = nativeSetTimeout(callback, delay);
    handles.add(handle);
    return handle;
  }) as typeof setTimeout;
  try {
    app = startCheckoutServer({ kv, hostname: "127.0.0.1", port: 0 });
  } finally {
    globalThis.setTimeout = nativeSetTimeout;
  }
  try {
    app.services.chainWebhookKeys = JSON.stringify({
      [SUBSCRIPTION]: { network: "BASE_MAINNET", signingKey: SIGNING_KEY },
    });
    await app.ready;
    const { token } = await app.services.merchantAuth.createKey({
      merchantId: MERCHANT,
      label: "read",
      scopes: ["invoice:read"],
    });
    await app.queue.refresh();
    ok(handles.size > 0, "startup must have armed the committed future invoice job");
    const refresh = app.queue.refresh.bind(app.queue);
    let refreshes = 0;
    app.queue.refresh = async () => {
      refreshes++;
      await refresh();
    };
    const loseCallbacks = () => {
      for (const handle of handles) clearTimeout(handle);
      handles.clear();
    };
    const reset = () => {
      kv.operations.length = 0;
      refreshes = 0;
    };
    return {
      app,
      kv,
      handles,
      token,
      loseCallbacks,
      reset,
      get refreshes() {
        return refreshes;
      },
      request(path: string, init: RequestInit = {}) {
        return fetch(`http://127.0.0.1:${app.server.port}${path}`, {
          ...init,
          headers: { ...PROXY_HEADERS, ...Object.fromEntries(new Headers(init.headers)) },
        });
      },
      async close() {
        await app.close();
        loseCallbacks();
      },
    };
  } catch (error) {
    await app.close();
    throw error;
  }
}

test("rejected chain webhooks do no storage or queue rearming after Bun startup", async () => {
  const fixture = await serverFixture();
  try {
    const fresh = payload();
    const malformed = "{";
    const stale = payload(new Date(Date.now() - 20 * 60_000).toISOString());
    const cases: { name: string; body: string; signature?: string; status: number }[] = [
      { name: "unsigned", body: fresh, signature: undefined, status: 401 },
      { name: "invalid HMAC", body: fresh, signature: "0".repeat(64), status: 401 },
      {
        name: "malformed signed JSON",
        body: malformed,
        signature: signature(malformed),
        status: 400,
      },
      { name: "stale signed notice", body: stale, signature: signature(stale), status: 401 },
    ];
    for (const item of cases) {
      fixture.loseCallbacks();
      fixture.reset();
      const response = await fixture.request("/api/v1/chain-events/alchemy", {
        method: "POST",
        body: item.body,
        headers: {
          "content-type": "application/json",
          ...(item.signature ? { "x-alchemy-signature": item.signature } : {}),
        },
      });
      await response.text();
      equal(response.status, item.status, item.name);
      equal(fixture.kv.operations, [], `${item.name}: rejected webhook must not touch KV`);
      equal(fixture.refreshes, 0, item.name);
      equal(fixture.handles.size, 0, `${item.name}: rejected webhook must not restore a callback`);
    }
  } finally {
    await fixture.close();
  }
});

test("rejected invoice requests, preflight and public successes do not recover the Bun queue", async () => {
  const fixture = await serverFixture();
  try {
    const cases: { name: string; path: string; init: RequestInit; status: number }[] = [
      { name: "missing bearer", path: `/api/v1/invoices/${INVOICE_ID}`, init: {}, status: 401 },
      {
        name: "invalid bearer",
        path: `/api/v1/invoices/${INVOICE_ID}`,
        init: { headers: { authorization: "Bearer uos_checkout_invalid" } },
        status: 401,
      },
      {
        name: "wrong scope",
        path: `/api/v1/invoices/${INVOICE_ID}/claims`,
        init: { method: "POST", headers: { authorization: `Bearer ${fixture.token}` }, body: "{}" },
        status: 403,
      },
      {
        name: "invoice preflight",
        path: "/api/v1/invoices",
        init: {
          method: "OPTIONS",
          headers: { origin: "https://merchant.example", "access-control-request-method": "POST" },
        },
        status: 404,
      },
      {
        name: "chain preflight",
        path: "/api/v1/chain-events/alchemy",
        init: { method: "OPTIONS" },
        status: 404,
      },
      { name: "public health", path: "/api/health", init: {}, status: 200 },
      { name: "public session", path: "/api/v1/admin/session", init: {}, status: 200 },
    ];
    for (const item of cases) {
      fixture.loseCallbacks();
      fixture.reset();
      const response = await fixture.request(item.path, item.init);
      await response.text();
      equal(response.status, item.status, item.name);
      // Authentication may read its own key records; rejected requests must never read queue state.
      equal(fixture.kv.queueOperations, [], item.name);
      equal(fixture.refreshes, 0, item.name);
      equal(fixture.handles.size, 0, item.name);
    }
  } finally {
    await fixture.close();
  }
});

test("an authenticated merchant GET restores a lost callback through its trusted related-event hook", async () => {
  const fixture = await serverFixture();
  try {
    const before = (await fixture.kv.listPage(BUN_DUE_PREFIX, { limit: 1 })).entries[0];
    ok(before);
    fixture.loseCallbacks();
    fixture.reset();
    const response = await fixture.request(`/api/v1/invoices/${INVOICE_ID}`, {
      headers: { authorization: `Bearer ${fixture.token}` },
    });
    equal(response.status, 200);
    equal((await response.json()).id, INVOICE_ID);
    equal(fixture.refreshes, 1, "only the authenticated relatedInvoiceEvent may recover the queue");
    ok(fixture.kv.queueOperations.some(({ operation }) => operation === "listPage"));
    equal(fixture.handles.size, 1, "authenticated GET must arm the retained durable job");
    const after = (await fixture.kv.listPage(BUN_DUE_PREFIX, { limit: 1 })).entries[0];
    equal(after, before, "recovery must preserve the committed job and its generation");
  } finally {
    await fixture.close();
  }
});

test("a valid signed notice with no related transfer does not sweep unrelated Bun jobs", async () => {
  const fixture = await serverFixture();
  try {
    fixture.loseCallbacks();
    fixture.reset();
    const body = payload();
    const response = await fixture.request("/api/v1/chain-events/alchemy", {
      method: "POST",
      body,
      headers: { "content-type": "application/json", "x-alchemy-signature": signature(body) },
    });
    equal(response.status, 200);
    await response.text();
    ok(fixture.kv.operations.length > 0, "valid delivery must persist its receipt");
    equal(fixture.kv.queueOperations, []);
    equal(fixture.refreshes, 0);
    equal(fixture.handles.size, 0);
  } finally {
    await fixture.close();
  }
});

async function recordedTransferFixture() {
  const fixture = await serverFixture();
  try {
    const rpc = new RuntimeRpc();
    fixture.app.services.rpc.batch = rpc.batch.bind(rpc);
    await fixture.app.services.payment.configureChain(8453, {
      enabled: true,
      treasuryAddress: `0x${"44".repeat(20)}`,
    });
    await fixture.app.services.payment.publishCatalogVersion(
      { merchantId: MERCHANT },
      "related-prices",
      {
        currency: "USD",
        products: [
          {
            productId: "related-credits",
            label: "Credits",
            kind: "credit_pack",
            priceCents: 200,
            enabled: true,
          },
        ],
      },
    );
    const created = await fixture.app.services.payment.createInvoice(
      { merchantId: MERCHANT, idempotencyKey: "related-attempt" },
      {
        customerReference: "related-account",
        pricingVersionId: "related-prices",
        productId: "related-credits",
        chainId: 8453,
      },
    );
    const invoice = await fixture.app.services.payment.getInvoiceRecord(created.id);
    const transfer = rpc.add(invoice, 100n);
    rpc.latest = 100n;
    transfer.timestamp = Math.floor(Date.now() / 1000);
    const claimed = await fixture.app.services.payment.claimPayment(created.id, {
      transactionHash: transfer.transactionHash,
      logIndex: "0",
      observedBlockNumber: "100",
      observedBlockHash: transfer.blockHash,
    });
    equal(claimed.invoice.status, "confirming");
    ok(claimed.invoice.paymentId);
    equal(
      (
        await fixture.kv.get(
          treasuryHistoryKey(8453, invoice.tokenAddress, invoice.treasuryAddress),
        )
      ).value,
      true,
    );
    await fixture.app.queue.refresh();
    fixture.loseCallbacks();
    fixture.reset();
    const topic = (address: string) => `0x${address.slice(2).padStart(64, "0")}`;
    const body = JSON.stringify({
      webhookId: SUBSCRIPTION,
      id: "related-notice",
      type: "ADDRESS_ACTIVITY",
      createdAt: new Date().toISOString(),
      event: {
        network: "BASE_MAINNET",
        activity: [
          {
            category: "token",
            log: {
              address: transfer.tokenAddress,
              transactionHash: transfer.transactionHash,
              logIndex: "0x0",
              blockNumber: "0x64",
              blockHash: transfer.blockHash,
              topics: [
                TRANSFER_TOPIC,
                topic(transfer.payerAddress),
                topic(transfer.recipientAddress),
              ],
              data: `0x${transfer.amountUnits.toString(16).padStart(64, "0")}`,
              removed: false,
            },
          },
        ],
      },
    });
    return { ...fixture, body, transfer, invoiceId: created.id };
  } catch (error) {
    await fixture.close();
    throw error;
  }
}

test("an authenticated related signed notice restores an unchanged inclusion's lost callback", async () => {
  const fixture = await recordedTransferFixture();
  try {
    const before = (await fixture.kv.listPage(BUN_DUE_PREFIX, { limit: 1 })).entries[0];
    fixture.reset();
    const response = await fixture.request("/api/v1/chain-events/alchemy", {
      method: "POST",
      body: fixture.body,
      headers: {
        "content-type": "application/json",
        "x-alchemy-signature": signature(fixture.body),
      },
    });
    equal(response.status, 200);
    await response.text();
    ok(
      fixture.handles.size > 0,
      "exact related inclusion must restore its retained confirmation callback",
    );
    const after = (await fixture.kv.listPage(BUN_DUE_PREFIX, { limit: 1 })).entries[0];
    equal(after, before, "related recovery preserves the committed job generation");
    equal(
      (await fixture.app.services.payment.getInvoiceRecord(fixture.invoiceId)).status,
      "confirming",
    );
  } finally {
    await fixture.close();
  }
});

test("a related notice retains retryable receipt work when queue recovery is unavailable", async () => {
  const fixture = await recordedTransferFixture();
  const ensure = fixture.app.queue.ensure.bind(fixture.app.queue);
  try {
    fixture.app.queue.ensure = async () => {
      throw new Error("queue_temporarily_unavailable");
    };
    const request = () =>
      fixture.request("/api/v1/chain-events/alchemy", {
        method: "POST",
        body: fixture.body,
        headers: {
          "content-type": "application/json",
          "x-alchemy-signature": signature(fixture.body),
        },
      });
    const pending = await request();
    equal(pending.status, 503, "failed recovery must not consume a related notice as complete");
    await pending.text();
    equal(fixture.handles.size, 0);
    fixture.app.queue.ensure = ensure;
    const retried = await request();
    equal(retried.status, 200);
    await retried.text();
    ok(fixture.handles.size > 0);
  } finally {
    fixture.app.queue.ensure = ensure;
    await fixture.close();
  }
});

test("a related inclusion defers instead of acknowledging when recovery has insufficient capacity", async () => {
  const fixture = await recordedTransferFixture();
  try {
    const result = await fixture.app.services.payment.processChainTransfer(
      { ...fixture.transfer, chainId: 8453, removed: false },
      {
        budget: new WorkerRequestBudget(10),
        deadline: Date.now() + 20_000,
        isCurrent: async () => true,
      },
    );
    equal(
      result.invoiceId,
      fixture.invoiceId,
      "fixture notice must attribute to the recorded invoice",
    );
    equal(result.terminal, false);
    equal(fixture.handles.size, 0);
  } finally {
    await fixture.close();
  }
});
