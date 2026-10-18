import { test } from "bun:test";
import { RuntimeRpc } from "./src/account_runtime_test_fixture.ts";
import { SqliteKv } from "./src/sqlite_kv.ts";
import { fetchPublicAsset } from "./src/bun_runtime.ts";
import {
  type CheckoutServices,
  createCheckoutServices,
  createHandler,
  deliverOutbox,
  withPeerAddress,
} from "./main.ts";
import { LOCAL_PEER_HEADER } from "./src/admin_auth.ts";
import type { JsonRpcBatchItem, JsonRpcCall, RpcClient } from "./src/rpc.ts";
import type { Kv } from "./src/kv.ts";
import {
  chainEventReceiptKey,
  pendingChainTransfersKey,
  processChainEventDelivery,
  type ChainEventReceipt,
} from "./src/chain_ingress.ts";
import type { VerifiedChainEventDelivery } from "./src/chain_events.ts";
import { WorkerRequestBudget } from "./src/worker_budget.ts";

const TREASURY = "0x1111111111111111111111111111111111111111";

test("local operator configures Checkout without a bootstrap password", async () => {
  await withApp(async ({ request }) => {
    const session = await request("/api/v1/admin/session");
    assertEquals(session.status, 200);
    const sessionBody = await session.json();
    assertEquals(sessionBody.authenticated, true);
    assertEquals(sessionBody.member.role, "owner");

    const rejected = await request("/api/v1/admin/chains/8453", {
      method: "PUT",
      body: { enabled: true, treasuryAddress: TREASURY },
    });
    assertEquals(rejected.status, 403);

    const configured = await request("/api/v1/admin/chains/8453", {
      method: "PUT",
      adminMutation: true,
      body: { enabled: true, treasuryAddress: TREASURY },
    });
    assertEquals(configured.status, 200);

    const merchant = await request("/api/v1/admin/merchants/linkedin-scraper-2025", {
      method: "PUT",
      adminMutation: true,
      body: {
        name: "LinkedIn Sales Agent",
        settlementWebhookUrl: "https://webhook.example/api/billing/crypto-settlements",
      },
    });
    assertEquals(merchant.status, 200);

    const key = await request("/api/v1/admin/merchant-keys", {
      method: "POST",
      adminMutation: true,
      body: { merchantId: "linkedin-scraper-2025", label: "Local integration" },
    });
    assertEquals(key.status, 201);
    const keyBody = await key.json();
    assert(String(keyBody.token).startsWith("uos_checkout_"));
    assertEquals("tokenHash" in keyBody.key, false);

    const invited = await request("/api/v1/admin/members/operator", {
      method: "PUT",
      adminMutation: true,
      body: { role: "viewer" },
    });
    assertEquals(invited.status, 200);
    assertEquals((await invited.json()).invitation.handle, "operator");
    const access = await request("/api/v1/admin/members");
    const accessBody = await access.json();
    assertEquals(accessBody.invitations.length, 1);

    const overview = await request("/api/v1/admin/overview");
    const overviewBody = await overview.json();
    assertEquals(overviewBody.chains.length, 3);
    assertEquals(
      overviewBody.chains.find((chain: { chainId: number }) => chain.chainId === 8453).enabled,
      true,
    );
    assertEquals("ok" in overviewBody, false);
  });
});

test("merchant API publishes an immutable catalog and creates an idempotent exact invoice", async () => {
  await withApp(async ({ request }) => {
    await request("/api/v1/admin/chains/8453", {
      method: "PUT",
      adminMutation: true,
      body: { enabled: true, treasuryAddress: TREASURY },
    });

    const catalog = await request("/api/v1/catalog/versions/pricing-2026-07-21", {
      method: "PUT",
      body: {
        currency: "USD",
        products: [
          {
            productId: "pack:starter",
            label: "Starter",
            kind: "credit_pack",
            priceCents: 2_000,
            enabled: true,
          },
        ],
      },
    });
    assertEquals(catalog.status, 200);

    const input = {
      customerReference: "user-123",
      pricingVersionId: "pricing-2026-07-21",
      productId: "pack:starter",
      chainId: 8453,
    };
    const first = await request("/api/v1/invoices", {
      method: "POST",
      headers: { "idempotency-key": "invoice-attempt-1" },
      body: input,
    });
    assertEquals(first.status, 201);
    const invoice = await first.json();
    assertEquals(invoice.chainId, 8453);
    assertEquals(invoice.treasuryAddress, TREASURY);
    assert(BigInt(invoice.paidAmountUnits) < BigInt(invoice.listAmountUnits));
    assertEquals("customerReference" in invoice, false);
    assertEquals("idempotencyKey" in invoice, false);

    const retry = await request("/api/v1/invoices", {
      method: "POST",
      headers: { "idempotency-key": "invoice-attempt-1" },
      body: input,
    });
    assertEquals((await retry.json()).id, invoice.id);

    const loaded = await request(`/api/v1/invoices/${invoice.id}`);
    assertEquals(loaded.status, 200);
    assertEquals((await loaded.json()).id, invoice.id);
  });
});

test("reading an invoice never performs expiration writes", async () => {
  let reads = 0;
  let expirations = 0;
  const services = {
    merchantAuth: {
      authenticate: () =>
        Promise.resolve({
          merchant: { id: "merchant" },
        }),
    },
    payment: {
      getInvoiceRecord: () => Promise.resolve({ merchant: "merchant" }),
      getInvoice: () => {
        reads++;
        return Promise.resolve({ id: "invoice", status: "awaiting_payment" });
      },
      expireInvoice: () => {
        expirations++;
        return Promise.resolve({ id: "invoice", status: "expired" });
      },
    },
  } as unknown as CheckoutServices;
  const response = await createHandler(services)(
    new Request("https://checkout.ubq.fi/api/v1/invoices/invoice"),
  );
  assertEquals(response.status, 200);
  assertEquals((await response.json()).status, "awaiting_payment");
  assertEquals(reads, 1);
  assertEquals(expirations, 0);
});

test("claims accept an empty discovery object or exactly four coordinates after authorization", async () => {
  await withApp(async ({ request, handler }) => {
    await request("/api/v1/admin/chains/8453", {
      method: "PUT",
      adminMutation: true,
      body: { enabled: true, treasuryAddress: TREASURY },
    });
    await request("/api/v1/catalog/versions/claim-prices", {
      method: "PUT",
      body: {
        currency: "USD",
        products: [
          { productId: "pack", label: "Pack", kind: "credit_pack", priceCents: 500, enabled: true },
        ],
      },
    });
    const created = await request("/api/v1/invoices", {
      method: "POST",
      headers: { "idempotency-key": "discovery" },
      body: {
        customerReference: "claim-customer",
        pricingVersionId: "claim-prices",
        productId: "pack",
        chainId: 8453,
      },
    });
    assertEquals(created.status, 201);
    const invoice = await created.json();
    const path = `/api/v1/invoices/${invoice.id}/claims`;
    const found = await request(path, { method: "POST", body: {} });
    assertEquals(found.status, 200);
    assertEquals((await found.json()).discovery, { found: false, searchedToBlock: "100" });
    const coordinates = {
      transactionHash: "invalid",
      logIndex: "0",
      observedBlockNumber: "100",
      observedBlockHash: `0x${"ab".repeat(32)}`,
    };
    for (const body of [
      { transactionHash: "invalid" },
      { ...coordinates, extra: true },
      { unrelated: true },
    ]) {
      const rejected = await request(path, { method: "POST", body });
      assertEquals(rejected.status, 400);
      assertEquals(await rejected.json(), { error: "invalid_claim_input" });
    }
    const coordinateClaim = await request(path, { method: "POST", body: coordinates });
    assertEquals(coordinateClaim.status, 400);
    assertEquals(await coordinateClaim.json(), { error: "invalid_transaction_hash" });
    const unauthorized = await handler(
      new Request(`https://checkout.ubq.fi${path}`, { method: "POST", body: '{"extra":true}' }),
    );
    assertEquals(unauthorized.status, 401);
  });
});

test("unsigned chain ingress fails before any storage or payment callback", async () => {
  const kv = new SqliteKv();
  try {
    let reads = 0,
      mutations = 0;
    const get = kv.get.bind(kv);
    kv.get = (key) => {
      reads++;
      return get(key);
    };
    const services = createCheckoutServices(kv, {
      chainWebhookKeys: JSON.stringify({
        hook: { network: "BASE_MAINNET", signingKey: "offline" },
      }),
    });
    services.payment.processChainTransfer = async () => {
      mutations++;
      return { terminal: true, disposition: "ignored" };
    };
    const response = await createHandler(services)(
      new Request("https://checkout.ubq.fi/api/v1/chain-events/alchemy", {
        method: "POST",
        body: JSON.stringify({ webhookId: "hook" }),
      }),
    );
    assertEquals(response.status, 401);
    assertEquals(reads, 0);
    assertEquals(mutations, 0);
  } finally {
    kv.close();
  }
});

for (const alreadyClaimed of [true, false])
  test(`authenticated related GET handles pending chain receipt within24 calls (claimed=${alreadyClaimed})`, async () => {
    const store = new SqliteKv();
    try {
      const seed = createCheckoutServices(store, {
        rpc: new StaticRpc(),
        resolveDns: async () => ["93.184.216.34"],
      });
      await seed.merchantAuth.upsertMerchant({
        id: "related",
        name: "Related",
        settlementWebhookUrl: "https://merchant.example/events",
      });
      const { token } = await seed.merchantAuth.createKey({
        merchantId: "related",
        label: "Related",
      });
      await seed.payment.configureChain(8453, { enabled: true, treasuryAddress: TREASURY });
      await seed.payment.publishCatalogVersion({ merchantId: "related" }, "related-prices", {
        currency: "USD",
        products: [
          { productId: "pack", label: "Pack", kind: "credit_pack", priceCents: 500, enabled: true },
        ],
      });
      const created = await seed.payment.createInvoice(
        { merchantId: "related", idempotencyKey: "related" },
        {
          customerReference: "customer",
          pricingVersionId: "related-prices",
          productId: "pack",
          chainId: 8453,
        },
      );
      const invoice = await seed.payment.getInvoiceRecord(created.id);
      const prefix = ["uos_checkout", "v1", "payments"];
      const transfer = {
        chainId: 8453 as const,
        transactionHash: `0x${"11".repeat(32)}`,
        logIndex: 7,
        blockNumber: invoice.minimumBlock,
        blockHash: `0x${"ab".repeat(32)}`,
        tokenAddress: invoice.tokenAddress,
        payerAddress: `0x${"33".repeat(20)}`,
        recipientAddress: invoice.treasuryAddress,
        amountUnits: invoice.paidAmountUnits,
        removed: false,
      };
      const instant = new Date().toISOString();
      if (alreadyClaimed)
        await store
          .atomic()
          .set([...prefix, "invoices", invoice.id], {
            ...invoice,
            status: "confirming",
            paymentId: "related-payment",
          })
          .set([...prefix, "payment_locators", 8453, transfer.transactionHash, transfer.logIndex], {
            invoiceId: invoice.id,
            paymentId: "related-payment",
            duplicate: false,
          })
          .set([...prefix, "payment_records", "related-payment"], {
            schemaVersion: 1,
            id: "related-payment",
            invoiceId: invoice.id,
            chainId: 8453,
            transactionHash: transfer.transactionHash,
            logIndex: 7,
            payer: transfer.payerAddress,
            tokenAddress: transfer.tokenAddress,
            treasuryAddress: transfer.recipientAddress,
            amountUnits: transfer.amountUnits,
            blockNumber: transfer.blockNumber,
            blockHash: transfer.blockHash,
            blockTimestamp: instant,
            phase: "confirming",
            firstObservedAt: instant,
            updatedAt: instant,
          })
          .commit();
      const delivery: VerifiedChainEventDelivery = {
        provider: "alchemy",
        subscriptionId: "hook",
        eventId: "related-event",
        deliveryId: "related-delivery",
        createdAt: instant,
        bodySha256: "a".repeat(64),
        transfers: [transfer],
      };
      await processChainEventDelivery(delivery, {
        kv: store,
        minimumTransferCalls: 0,
        processTransfer: async () => ({ terminal: false }),
      });
      assertEquals(
        (await store.get<ChainEventReceipt>(chainEventReceiptKey(delivery))).value?.state,
        "pending",
      );
      const budget = new WorkerRequestBudget(24);
      budget.reserve(); // The completed migration object's one HTTP binding call.
      const kv: Kv = {
        get: (key) => {
          budget.reserve();
          return store.get(key);
        },
        getMany: (keys) => {
          budget.reserve();
          return store.getMany(keys);
        },
        set: (key, value, options) => {
          budget.reserve();
          return store.set(key, value, options);
        },
        delete: (key) => {
          budget.reserve();
          return store.delete(key);
        },
        list: () => {
          throw new Error("related recovery must not list");
        },
        listPage: () => {
          throw new Error("related recovery must not page");
        },
        atomic: () => {
          const atomic = store.atomic();
          const commit = atomic.commit.bind(atomic);
          atomic.commit = () => {
            budget.reserve();
            return commit();
          };
          return atomic;
        },
        close: () => {},
      };
      let rpcCalls = 0;
      const services = createCheckoutServices(kv, {
        budget,
        chainWebhookKeys: "{}",
        rpc: {
          batch: async () => {
            rpcCalls++;
            throw new Error("already-claimed cleanup must not query RPC");
          },
        },
      });
      const response = await createHandler(services)(
        new Request(`https://checkout.ubq.fi/api/v1/invoices/${invoice.id}`, {
          headers: { authorization: `Bearer ${token}` },
        }),
      );
      assertEquals(response.status, 200);
      console.log(`related authenticated GET calls=${24 - budget.remaining}/24,rpc=${rpcCalls}`);
      assertEquals(
        (await store.get<ChainEventReceipt>(chainEventReceiptKey(delivery))).value?.state,
        "pending",
      );
      const pendingReferences = (await store.get(pendingChainTransfersKey(transfer))).value;
      assert(Array.isArray(pendingReferences) && pendingReferences.length === 1);
      assertEquals(rpcCalls, 0);
      assert(budget.remaining >= 0);
    } finally {
      store.close();
    }
  });

test("public origins cannot forge loopback trust", async () => {
  await withApp(async ({ handler }) => {
    const forged = await handler(
      new Request("https://checkout.ubq.fi/api/v1/invoices", {
        method: "POST",
        headers: {
          [LOCAL_PEER_HEADER]: "127.0.0.1",
          "content-type": "application/json",
          "idempotency-key": "forged",
        },
        body: JSON.stringify({}),
      }),
    );
    assertEquals(forged.status, 401);

    const original = new Request("http://localhost:8000/api/health", {
      headers: { [LOCAL_PEER_HEADER]: "127.0.0.1" },
    });
    const stripped = withPeerAddress(original, "203.0.113.8");
    assertEquals(stripped.headers.has(LOCAL_PEER_HEADER), false);
  });
});

test("health, JWKS, and branded static assets are public", async () => {
  await withApp(async ({ request }) => {
    const health = await request("/api/health");
    assertEquals(await health.json(), { status: "ready" });

    const jwks = await request("/.well-known/jwks.json");
    const jwksBody = await jwks.json();
    assertEquals(jwksBody.keys.length, 1);
    assertEquals(jwksBody.keys[0].alg, "RS256");

    const index = await request("/");
    assert((await index.text()).includes("Ubiquity Checkout"));
    assert((index.headers.get("content-security-policy") ?? "").includes("frame-ancestors 'none'"));
  });
});

test("webhook delivery paginates, isolates failures, forbids redirects, and carries a timeout", async () => {
  const failed: string[] = [];
  const delivered: string[] = [];
  const attempts: string[] = [];
  const events = Array.from({ length: 101 }, (_, index) => {
    const id = index === 0 ? "poison" : `healthy-${index}`;
    return { id, payload: { merchantId: id } };
  });
  const services = {
    payment: {
      listOutbox: (limit: number) => Promise.resolve(events.splice(0, limit)),
      markOutboxFailed: (id: string) => {
        failed.push(id);
        return Promise.resolve();
      },
      markOutboxDelivered: (id: string) => {
        delivered.push(id);
        return Promise.resolve();
      },
    },
    merchantAuth: {
      getMerchant: (id: string) =>
        Promise.resolve({
          id,
          settlementWebhookUrl: `https://${id}.example/hook`,
        }),
      resolveWebhookTarget: (url: string) =>
        Promise.resolve({
          url: new URL(url),
          addresses: ["93.184.216.34"],
        }),
    },
    webhookSender: ((_target, _body, signal) => {
      assert(signal instanceof AbortSignal);
      const merchantId = attempts.length === 0 ? "poison" : `healthy-${attempts.length}`;
      attempts.push(merchantId);
      if (attempts.length === 1) return Promise.reject(new Error("poison webhook"));
      return Promise.resolve({ status: 204, ok: true });
    }) as CheckoutServices["webhookSender"],
  } as unknown as CheckoutServices;

  await deliverOutbox(services);
  assertEquals(failed, ["poison"]);
  assertEquals(delivered.length, 100);
  assertEquals(delivered.at(-1), "healthy-100");
  assertEquals(attempts.length, 101);
});

async function withApp(
  run: (value: {
    request: RequestFn;
    handler: (request: Request) => Promise<Response>;
    services: ReturnType<typeof createCheckoutServices>;
  }) => Promise<void>,
  rpc: RpcClient = new StaticRpc(),
): Promise<void> {
  const kv = new SqliteKv(":memory:");
  const services = createCheckoutServices(kv, {
    rpc,
    assetFetcher: fetchPublicAsset,
    resolveDns: () => Promise.resolve(["93.184.216.34"]),
  });
  const handler = createHandler(services);
  try {
    await run({
      handler,
      services,
      request: async (pathname, init = {}) => {
        const headers = new Headers(init.headers);
        if (init.body !== undefined) headers.set("content-type", "application/json");
        if (init.adminMutation) headers.set("x-uos-requested-with", "checkout-admin");
        const request = new Request(`http://localhost:8000${pathname}`, {
          method: init.method ?? "GET",
          headers,
          body: init.body === undefined ? undefined : JSON.stringify(init.body),
        });
        return await handler(withPeerAddress(request, "127.0.0.1"));
      },
    });
  } finally {
    kv.close();
  }
}

type RequestFn = (
  path: string,
  init?: {
    method?: string;
    headers?: HeadersInit;
    body?: unknown;
    adminMutation?: boolean;
  },
) => Promise<Response>;

class StaticRpc implements RpcClient {
  batch(_chainId: 1 | 10 | 8453, calls: readonly JsonRpcCall[]): Promise<JsonRpcBatchItem[]> {
    return Promise.resolve(
      calls.map((call) => ({
        jsonrpc: "2.0",
        id: call.id,
        result: {
          number: "0x64",
          hash: `0x${"ab".repeat(32)}`,
          timestamp: "0x669fb640",
        },
      })),
    );
  }
}

function assert(value: unknown): asserts value {
  if (!value) throw new Error("Expected value to be truthy.");
}

function assertEquals(actual: unknown, expected: unknown): void {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}.`);
  }
}

test("HTTP JSON rejects oversized streamed bodies before payment work", async () => {
  await withApp(async ({ request }) => {
    const response = await request("/api/v1/catalog/versions/oversized", {
      method: "PUT",
      body: { padding: "x".repeat(128 * 1024) },
    });
    assertEquals(response.status, 413);
    assertEquals((await response.json()).error, "request_body_too_large");
  });
});

test("authenticated account check spans attempts, credits canonical matches and returns the requested anchor", async () => {
  const rpc = new RuntimeRpc();
  await withApp(async ({ request, services }) => {
    await request("/api/v1/admin/chains/8453", {
      method: "PUT",
      adminMutation: true,
      body: { enabled: true, treasuryAddress: TREASURY },
    });
    await request("/api/v1/catalog/versions/account-price", {
      method: "PUT",
      body: {
        currency: "USD",
        products: [
          { productId: "pack", label: "Pack", kind: "credit_pack", priceCents: 100, enabled: true },
        ],
      },
    });
    const create = async (id: string) =>
      await (
        await request("/api/v1/invoices", {
          method: "POST",
          headers: { "idempotency-key": id },
          body: {
            customerReference: "account",
            pricingVersionId: "account-price",
            productId: "pack",
            chainId: 8453,
          },
        })
      ).json();
    const old = await create("old");
    const latest = await create("latest");
    assert(old.id !== latest.id);
    rpc.add(await services.payment.getInvoiceRecord(latest.id), 200n);
    rpc.latest = 300n;
    rpc.safe = 250n;
    const response = await request(`/api/v1/invoices/${old.id}/claims`, {
      method: "POST",
      body: {},
    });
    assertEquals(response.status, 200);
    const result = await response.json();
    assertEquals(result.invoice.id, old.id);
    assertEquals(result.discovery.found, false);
    assertEquals(result.discovery.searchedToBlock, "300");
    assertEquals(result.accountHunt.reachedHead, true);
    assertEquals(
      result.accountHunt.attempts.find((value: { invoiceId: string }) => value.invoiceId === old.id)
        .outcome,
      "invalidated",
    );
    assertEquals(
      result.accountHunt.attempts.find(
        (value: { invoiceId: string }) => value.invoiceId === latest.id,
      ).outcome,
      "credited",
    );
  }, rpc);
});
