import { readFile } from "node:fs/promises";
import { AUTH_HUB_ORIGIN, LOCAL_PEER_HEADER } from "../src/admin_auth.ts";
import { createRequire } from "node:module";
import type { SigningKeyRecord } from "../src/jws.ts";
import { deepStrictEqual as equal, ok } from "node:assert/strict";
import { dirname, join, resolve } from "node:path";
import { wakeIntentKey, type WakeTarget, type PreparedWakeIntent } from "../src/wakeup.ts";
import type { SchedulerState } from "../src/worker_scheduler.ts";
import { createCheckoutServices } from "../main.ts";
import { UbqKv } from "../src/ubq_kv.ts";
import { verifyCompactJws } from "../src/jws.ts";
import { fixtureScript } from "./worker_fixture.ts";
import { accountHash } from "../src/account_attempts.ts";

// The published Miniflare 5 beta declarations reference unpublished internal packages.
// Keep this supported Node control-plane boundary narrow; production Worker types
// are generated and checked separately, and this complete API path runs below.
type HarnessWorker = {
  fetch(input: string, init?: RequestInit): Promise<Response>;
};
type SchedulerStub = {
  inspect(): Promise<{
    state?: SchedulerState;
    migration?: { phase: string; cursor: string | null; page?: unknown };
    alarm: number | null;
    sqlite: boolean;
    incarnation: string;
  }>;
  arm(time: number): Promise<void>;
  evict(): Promise<void>;
  adopt(target: WakeTarget): Promise<void>;
  prepare(intent: PreparedWakeIntent, target: WakeTarget): Promise<void>;
  ensure(): Promise<"pending" | "complete">;
  budgetLedger(): Promise<
    {
      id: number;
      calls: number;
      storage: number;
      rpc: number;
      other: number;
      nested?: number;
      remaining: number;
      limit: number;
      denied: { need: number; remaining: number }[];
    }[]
  >;
};
type HarnessRuntime = {
  getWorker(name: string): Promise<HarnessWorker>;
  getDurableObjectNamespace(
    binding: string,
    worker: string,
  ): Promise<{ getByName(name: string): SchedulerStub }>;
  dispose(): Promise<void>;
};
type MiniflareModule = {
  Miniflare: new (options: Record<string, unknown>) => HarnessRuntime;
  convertV4MiniflareOptions(options: Record<string, unknown>): Record<string, unknown>;
};
const loaded: unknown = createRequire(resolve("package.json"))("miniflare");
function isMiniflareModule(value: unknown): value is MiniflareModule {
  return Boolean(
    value &&
    typeof value === "object" &&
    "Miniflare" in value &&
    typeof value.Miniflare === "function" &&
    "convertV4MiniflareOptions" in value &&
    typeof value.convertV4MiniflareOptions === "function",
  );
}
if (!isMiniflareModule(loaded)) throw new Error("miniflare_control_plane_missing");
const { Miniflare, convertV4MiniflareOptions } = loaded;
const path = process.argv[2];
if (!path) throw new Error("worker_bundle_required");
const signingKey = "offline-chain-signing-key";
const productionScript = await readFile(path, "utf8");
// These RPC helpers exist only in this generated harness script, never the production bundle.
const testScript =
  productionScript +
  `
const harnessBudgetRecords=[];
const harnessBudgets=[];
const harnessBudgetIds=new WeakMap();
const harnessBudgetRecord=budget=>{let value=harnessBudgetIds.get(budget);if(!value){value={id:harnessBudgetRecords.length,calls:0,storage:0,rpc:0,other:0,denied:[]};harnessBudgetIds.set(budget,value);harnessBudgetRecords.push(value);harnessBudgets.push(budget);}return value;};
const harnessNativeReserve=WorkerRequestBudget.prototype.reserve;
WorkerRequestBudget.prototype.reserve=function(){harnessNativeReserve.call(this);harnessBudgetRecord(this).calls++;};
const harnessNativeCapacity=WorkerRequestBudget.prototype.hasCapacity;
WorkerRequestBudget.prototype.hasCapacity=function(need){const result=harnessNativeCapacity.call(this,need);if(!result)harnessBudgetRecord(this).denied.push({need,remaining:this.remaining});return result;};
const harnessNativeNested=WorkerRequestBudget.prototype.reserveNestedInvocations;
WorkerRequestBudget.prototype.reserveNestedInvocations=function(count){harnessNativeNested.call(this,count);const record=harnessBudgetRecord(this);record.nested=(record.nested??0)+count;};
const harnessNativeWrap=WorkerRequestBudget.prototype.wrap;
WorkerRequestBudget.prototype.wrap=function(fetcher){const wrapped=harnessNativeWrap.call(this,fetcher);return(input,init)=>{const result=wrapped(input,init);const url=new URL(typeof input==="string"?input:input.url??input.href);const entry=harnessBudgetRecord(this);if(url.hostname==="kv.ubq.fi")entry.storage++;else if(url.hostname==="rpc.ubq.fi")entry.rpc++;else entry.other++;return result;};};
export class HarnessCheckoutScheduler extends CheckoutScheduler {
  incarnation=crypto.randomUUID();
  async inspect() { return {state:await this.ctx.storage.get("checkout_scheduler_v1"), migration:await this.ctx.storage.get("checkout_migration_v2"), alarm:await this.ctx.storage.getAlarm(), sqlite:typeof this.ctx.storage.sql.exec === "function",incarnation:this.incarnation}; }
  async arm(time) { await this.ctx.storage.setAlarm(time); }
  async evict() { this.ctx.abort("offline instance restart"); }
  async budgetLedger() { return harnessBudgetRecords.map((record,index)=>({...record,remaining:harnessBudgets[index].remaining,limit:harnessBudgets[index].limit})); }
}
`;
const mf = new Miniflare(
  convertV4MiniflareOptions({
    durableObjectsPersist: join(dirname(path), "durable-state"),
    workers: [
      {
        name: "checkout",
        modules: true,
        script: testScript,
        durableObjects: {
          CHECKOUT_SCHEDULER: { className: "HarnessCheckoutScheduler", useSQLite: true },
        },
        compatibilityDate: "2026-10-18",
        compatibilityFlags: ["global_fetch_strictly_public", "no_cots_on_external_fetch"],
        bindings: {
          CHECKOUT_KV_SERVICE_TOKEN: "offline-service-token",
          CHECKOUT_ALCHEMY_WEBHOOK_KEYS: JSON.stringify({
            wh_offline: { signingKey, network: "BASE_MAINNET" },
          }),
          UOS_VERSION: {
            id: "offline-worker-version",
            tag: "offline",
            timestamp: "2026-10-18T00:00:00.000Z",
          },
        },
        serviceBindings: { UOS_KV: "fixture" },
        outboundService: "fixture",
        assets: { directory: resolve("public"), binding: "ASSETS", run_worker_first: true },
      },
      {
        name: "unconfigured",
        modules: true,
        script: productionScript,
        compatibilityDate: "2026-10-18",
        compatibilityFlags: ["global_fetch_strictly_public", "no_cots_on_external_fetch"],
        bindings: {
          CHECKOUT_KV_SERVICE_TOKEN: "offline-service-token",
          UOS_VERSION: {
            id: "offline-unconfigured",
            tag: "offline",
            timestamp: "2026-10-18T00:00:00.000Z",
          },
        },
        serviceBindings: { UOS_KV: "fixture" },
        outboundService: "fixture",
      },
      {
        name: "fixture",
        modules: true,
        script: fixtureScript,
        compatibilityDate: "2026-10-18",
      },
    ],
  }),
);
try {
  const store = await mf.getWorker("fixture");
  const worker = await mf.getWorker("checkout");
  const kv = new UbqKv({
    url: "https://kv.invalid",
    token: "offline-service-token",
    fetchImpl: async (input, init) => {
      const response = await store.fetch(input, {
        method: init.method,
        headers: Object.fromEntries(new Headers(init.headers)),
        body: typeof init.body === "string" ? init.body : undefined,
      });
      return new Response(await response.arrayBuffer(), {
        status: response.status,
        headers: Object.fromEntries(response.headers),
      });
    },
  });
  const seed = createCheckoutServices(kv, {
    resolveDns: async () => ["93.184.216.34"],
    fetch: async (input, init) =>
      store.fetch(String(input), {
        method: init?.method,
        headers: Object.fromEntries(new Headers(init?.headers)),
        body: typeof init?.body === "string" ? init.body : undefined,
      }),
  });
  const objects = await mf.getDurableObjectNamespace("CHECKOUT_SCHEDULER", "checkout");
  const stats = async () =>
    (await (await store.fetch("https://fixture.invalid/stats")).json()) as {
      writes: number;
      kvCalls: number;
      rpcCalls: number;
      methods: Record<string, number>;
    };
  const eventually = async (check: () => Promise<boolean>, message: string) => {
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
      if (await check()) return;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error(message);
  };
  // An invalid signature must not bootstrap migration or touch KV.
  const beforeInvalid = await stats();
  const unconfigured = await mf.getWorker("unconfigured");
  equal(
    (
      await unconfigured.fetch("https://checkout.ubq.fi/api/v1/chain-events/alchemy", {
        method: "POST",
        body: "{}",
      })
    ).status,
    503,
  );
  equal(
    (await stats()).kvCalls,
    beforeInvalid.kvCalls,
    "missing HMAC secret bootstrapped or read KV",
  );
  equal(
    (
      await worker.fetch("https://checkout.ubq.fi/api/v1/chain-events/alchemy", {
        method: "POST",
        headers: { "content-type": "application/json", "x-alchemy-signature": "0".repeat(64) },
        body: JSON.stringify({ webhookId: "wh_offline" }),
      })
    ).status,
    401,
  );
  equal((await stats()).kvCalls, beforeInvalid.kvCalls);
  await seed.payment.configureChain(8453, {
    enabled: true,
    treasuryAddress: "0x1111111111111111111111111111111111111111",
  });
  await seed.payment.publishCatalogVersion({ merchantId: "merchant-a" }, "worker-v1", {
    currency: "USD",
    products: [
      {
        productId: "pack:worker",
        label: "Worker",
        kind: "credit_pack",
        priceCents: 500,
        enabled: true,
      },
    ],
  });
  await seed.merchantAuth.upsertMerchant({
    id: "merchant-a",
    name: "Offline merchant",
    settlementWebhookUrl: "https://merchant.example/events",
  });
  const { token } = await seed.merchantAuth.createKey({
    merchantId: "merchant-a",
    label: "Offline fixture",
  });
  const jwks = await seed.payment.jwks();
  const activeKey = ["uos_checkout", "v1", "jws", "active"];
  const legacySigning = (await kv.get<SigningKeyRecord>(activeKey)).value;
  await store.fetch("https://kv.invalid/kv/set", {
    method: "POST",
    headers: { authorization: "Bearer offline-service-token", "content-type": "application/json" },
    body: JSON.stringify({ key: activeKey, value: legacySigning }),
  });
  const legacy = await seed.payment.createInvoice(
    { merchantId: "merchant-a", idempotencyKey: "legacy-invoice" },
    {
      customerReference: "legacy-customer",
      pricingVersionId: "worker-v1",
      productId: "pack:worker",
      chainId: 8453,
    },
  );
  const writesBeforeMigrationGate = (await stats()).writes;
  const migrationGate = await worker.fetch("https://checkout.ubq.fi/api/v1/invoices", {
    method: "POST",
    headers: {
      authorization: "Bearer " + token,
      "content-type": "application/json",
      "idempotency-key": "gated-attempt",
    },
    body: JSON.stringify({
      customerReference: "gated-customer",
      pricingVersionId: "worker-v1",
      productId: "pack:worker",
      chainId: 8453,
    }),
  });
  equal(migrationGate.status, 503);
  equal(migrationGate.headers.get("x-checkout-events-migration"), "pending");
  equal(await migrationGate.json(), { error: "migration_pending" });
  equal(
    (await stats()).writes,
    writesBeforeMigrationGate,
    "pending migration reached the domain handler",
  );
  let migrationComplete = false;
  let finalSliceBlocked = false;
  for (let slice = 0; slice < 50; slice++) {
    const state = (await objects.getByName("migration:checkout-events-v2").inspect()).migration;
    if (state?.phase === "outbox" && !state.page) {
      const preview = await kv.listPage(["uos_checkout", "v1", "payments", "outbox"], {
        limit: 8,
        cursor: state.cursor,
      });
      if (preview.cursor === null && preview.entries.length === 0) {
        const writes = (await stats()).writes;
        const finalRequest = await worker.fetch("https://checkout.ubq.fi/api/v1/invoices", {
          method: "POST",
          headers: {
            authorization: "Bearer " + token,
            "content-type": "application/json",
            "idempotency-key": "final-gated-attempt",
          },
          body: "{}",
        });
        equal(finalRequest.status, 503);
        equal(finalRequest.headers.get("x-checkout-events-migration"), "pending");
        equal((await stats()).writes, writes, "final migration slice entered domain handler");
        equal(
          (await objects.getByName("migration:checkout-events-v2").inspect()).migration?.phase,
          "complete",
        );
        const calls = (await stats()).kvCalls;
        const admitted = await worker.fetch("https://checkout.ubq.fi/api/health");
        equal(admitted.status, 200);
        equal(admitted.headers.get("x-checkout-events-migration"), "complete");
        equal((await stats()).kvCalls, calls, "entry-complete request performed migration KV IO");
        finalSliceBlocked = true;
        migrationComplete = true;
        break;
      }
    }
    const response = await worker.fetch("https://checkout.ubq.fi/api/health");
    equal(response.status, 200);
    if (response.headers.get("x-checkout-events-migration") === "complete") {
      migrationComplete = true;
      break;
    }
  }
  ok(migrationComplete, "request-driven migration did not complete");
  ok(finalSliceBlocked, "final working migration slice was not gated");
  const legacyName = "invoice:" + legacy.id;
  const legacyBefore = await objects.getByName(legacyName).inspect();
  equal(legacyBefore.sqlite, true);
  equal(legacyBefore.alarm, Date.parse(legacy.expiresAt));
  try {
    await objects.getByName(legacyName).evict();
  } catch {
    // Native abort closes this test-only RPC response and destroys the live instance.
  }
  const legacyAfter = await objects.getByName(legacyName).inspect();
  ok(legacyAfter.incarnation !== legacyBefore.incarnation, "DO constructor was not rerun");
  equal(legacyAfter.state?.job?.generation, legacyBefore.state?.job?.generation);
  equal(legacyAfter.alarm, legacyBefore.alarm);
  equal((await objects.getByName("migration:checkout-events-v2").inspect()).alarm, null);
  const pagingPrefix = ["uos_checkout", "v1", "paging_probe"];
  for (let index = 0; index < 6; index++) await kv.set([...pagingPrefix, index], index);
  const firstPage = await kv.listPage<number>(pagingPrefix, { limit: 2 });
  equal(
    firstPage.entries.map((entry) => entry.value),
    [0, 1],
  );
  await kv.delete([...pagingPrefix, 0]);
  const secondPage = await kv.listPage<number>(pagingPrefix, {
    limit: 2,
    cursor: firstPage.cursor,
  });
  equal(
    secondPage.entries.map((entry) => entry.value),
    [2, 3],
    "key cursor skipped entries after an earlier deletion",
  );
  for (let index = 0; index < 6; index++) await kv.delete([...pagingPrefix, index]);
  const beforeReads = await stats();
  const health = await worker.fetch("https://checkout.ubq.fi/api/health");
  equal(await health.json(), { status: "ready" });
  equal(health.headers.get("x-uos-runtime"), "cloudflare-workers");
  equal(health.headers.get("x-uos-version"), "offline-worker-version");
  for (const origin of ["https://checkout.ubq.fi", "http://localhost:8787"]) {
    const session = await worker.fetch(`${origin}/api/v1/admin/session`, {
      headers: { [LOCAL_PEER_HEADER]: "127.0.0.1", "cf-connecting-ip": "127.0.0.1" },
    });
    equal(((await session.json()) as { authenticated: boolean }).authenticated, false);
    equal(
      (
        await worker.fetch(`${origin}/api/v1/admin/overview`, {
          headers: { [LOCAL_PEER_HEADER]: "127.0.0.1" },
        })
      ).status,
      401,
    );
  }
  const index = await worker.fetch("https://checkout.ubq.fi/");
  equal(index.status, 200);
  ok((await index.text()).includes("Ubiquity Checkout"));
  ok(index.headers.get("content-security-policy")?.includes("frame-ancestors 'none'"));
  for (const asset of ["/styles.css", "/app.js", "/index.html"]) {
    const response = await worker.fetch(`https://checkout.ubq.fi${asset}`);
    equal(response.status, 200);
    ok((await response.text()).length > 100);
  }
  const head = await worker.fetch("https://checkout.ubq.fi/app.js", { method: "HEAD" });
  equal(head.status, 200);
  equal(await head.text(), "");
  equal((await worker.fetch("https://checkout.ubq.fi/unknown")).status, 404);
  equal(
    (await worker.fetch("https://checkout.ubq.fi/deliver", { method: "POST", body: "{}" })).status,
    404,
  );
  const login = await worker.fetch("https://checkout.ubq.fi/api/auth/login", {
    redirect: "manual",
  });
  equal(login.status, 302);
  ok(login.headers.get("location")?.startsWith(AUTH_HUB_ORIGIN + "/"));
  equal(
    (await worker.fetch("https://checkout.ubq.fi/api/v1/invoices", { method: "POST", body: "{}" }))
      .status,
    401,
  );
  equal(await (await worker.fetch("https://checkout.ubq.fi/.well-known/jwks.json")).json(), jwks);
  equal(await (await worker.fetch("https://checkout.ubq.fi/.well-known/jwks.json")).json(), jwks);
  const afterReads = (await (await store.fetch("https://fixture.invalid/stats")).json()) as {
    writes: number;
  };
  equal(
    afterReads.writes,
    beforeReads.writes,
    "candidate GET checks must not write production-shaped storage",
  );
  const create = () =>
    worker.fetch("https://checkout.ubq.fi/api/v1/invoices", {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
        "idempotency-key": "offline-attempt",
      },
      body: JSON.stringify({
        customerReference: "customer-a",
        pricingVersionId: "worker-v1",
        productId: "pack:worker",
        chainId: 8453,
      }),
    });
  const created = await create();
  equal(created.status, 201);
  const invoice = (await created.json()) as {
    id: string;
    paidAmountUnits: string;
    listAmountUnits: string;
    tokenAddress: string;
    treasuryAddress: string;
    minimumBlock: string;
  };
  ok(BigInt(invoice.paidAmountUnits) < BigInt(invoice.listAmountUnits));
  equal(((await (await create()).json()) as { id: string }).id, invoice.id);
  const read = await worker.fetch(`https://checkout.ubq.fi/api/v1/invoices/${invoice.id}`, {
    headers: { authorization: `Bearer ${token}` },
  });
  equal(((await read.json()) as { id: string }).id, invoice.id);

  const counts = async () =>
    (await (await store.fetch("https://fixture.invalid/stats")).json()) as {
      rpcCalls: number;
      kvCalls: number;
    };
  const discover = () =>
    worker.fetch(`https://checkout.ubq.fi/api/v1/invoices/${invoice.id}/claims`, {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: "{}",
    });
  const beforeDiscovery = await counts();
  const pending = await discover();
  equal(pending.status, 200);
  equal(
    ((await pending.json()) as { discovery: { found: boolean; searchedToBlock: string } })
      .discovery,
    { found: false, searchedToBlock: "100" },
  );
  const afterPending = await counts();
  const pendingCalls =
    afterPending.rpcCalls +
    afterPending.kvCalls -
    beforeDiscovery.rpcCalls -
    beforeDiscovery.kvCalls;
  ok(pendingCalls <= 30, `pending discovery used ${pendingCalls} subrequests`);

  const transactionHash = "0x" + "11".repeat(32);
  await store.fetch("https://fixture.invalid/rpc-fixture", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      transactionHash,
      tokenAddress: invoice.tokenAddress,
      treasuryAddress: invoice.treasuryAddress,
      amountUnits: invoice.paidAmountUnits,
      blockNumber: invoice.minimumBlock,
      unavailableHeadTags: true,
    }),
  });
  const budgetsBefore = new Set(
    (await objects.getByName(legacyName).budgetLedger()).map((entry) => entry.id),
  );
  const discovered = await discover();
  equal(discovered.status, 200);
  const discovery = (await discovered.json()) as {
    invoice: { status: string };
    eventJws: string;
    discovery: { found: boolean };
  };
  console.log(
    "workerd manual discovery budget",
    JSON.stringify(
      (await objects.getByName(legacyName).budgetLedger()).filter(
        (entry) => !budgetsBefore.has(entry.id),
      ),
    ),
  );
  equal(discovery.discovery.found, true);
  equal(discovery.invoice.status, "confirming");
  equal(discovery.eventJws, undefined);
  const afterFound = await counts();
  const foundCalls =
    afterFound.rpcCalls + afterFound.kvCalls - afterPending.rpcCalls - afterPending.kvCalls;
  // This aggregate includes concurrently triggered DO delivery, not one invocation.
  ok(foundCalls > 0);
  console.log(
    `workerd: pending discovery=${pendingCalls}/24; aggregate discovery and concurrent delivery=${foundCalls}`,
  );
  const claim = () =>
    worker.fetch(`https://checkout.ubq.fi/api/v1/invoices/${invoice.id}/claims`, {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify({
        transactionHash,
        logIndex: "7",
        observedBlockNumber: invoice.minimumBlock,
        observedBlockHash: "0x" + "ab".repeat(32),
      }),
    });
  await store.fetch("https://fixture.invalid/rpc-fixture", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      transactionHash,
      tokenAddress: invoice.tokenAddress,
      treasuryAddress: invoice.treasuryAddress,
      amountUnits: invoice.paidAmountUnits,
      blockNumber: invoice.minimumBlock,
      safeBlock: invoice.minimumBlock,
    }),
  });
  const claimed = await claim();
  if (claimed.status !== 200) {
    const error = await claimed.text();
    throw new Error(`payment_claim_failed:${error}`);
  }
  const accepted = (await claimed.json()) as {
    invoice: { id: string; status: string };
    eventJws: string;
  };
  equal(accepted.invoice.status, "provisionally_paid");
  const event = await verifyCompactJws(accepted.eventJws, jwks);
  equal((event.payload as { type: string; invoiceId: string }).type, "payment.accepted.v1");
  equal((event.payload as { invoiceId: string }).invoiceId, invoice.id);
  await store.fetch("https://fixture.invalid/rpc-fixture", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      transactionHash,
      tokenAddress: invoice.tokenAddress,
      treasuryAddress: invoice.treasuryAddress,
      amountUnits: invoice.paidAmountUnits,
      blockNumber: invoice.minimumBlock,
      unavailableHeadTags: true,
    }),
  });
  const replay = await claim();
  equal(replay.status, 200);
  equal(
    ((await replay.json()) as { invoice: { status: string } }).invoice.status,
    "provisionally_paid",
  );
  const persisted = await kv.get<{ status: string; paidAmountUnits: bigint }>([
    "uos_checkout",
    "v1",
    "payments",
    "invoices",
    invoice.id,
  ]);
  equal(persisted.value?.status, "provisionally_paid");
  equal(persisted.value?.paidAmountUnits, BigInt(invoice.paidAmountUnits));
  const payments = ["uos_checkout", "v1", "payments"];
  const nextAttemptAt = new Date(Date.now() - 1000).toISOString();
  await kv.set([...payments, "events", "offline-event"], {
    schemaVersion: 1,
    id: "offline-event",
    invoiceId: invoice.id,
    paymentId: "offline-payment",
    type: "payment.accepted.v1",
    payload: { merchantId: "merchant-a", type: "payment.accepted.v1" },
    compactJws: "regenerated-at-delivery",
    createdAt: nextAttemptAt,
    deliveryAttempts: 0,
    nextAttemptAt,
  });
  await kv.set(
    [...payments, "outbox", new Date(nextAttemptAt).getTime(), "offline-event"],
    "offline-event",
  );
  await objects.getByName("delivery:offline-event").adopt({
    kind: "delivery",
    id: "offline-event",
    dueAt: Date.parse(nextAttemptAt),
    reason: "delivery",
  });
  await eventually(
    async () =>
      Boolean(
        (await kv.get<{ deliveredAt?: string }>([...payments, "events", "offline-event"])).value
          ?.deliveredAt,
      ),
    "native delivery alarm did not finish",
  );
  const deliveryLedger = (await (
    await store.fetch("https://fixture.invalid/relay-ledger")
  ).json()) as { url: string; eventJws: string }[];
  const verifiedLedger = await Promise.all(
    deliveryLedger.map(async (item) => ({
      item,
      verified: await verifyCompactJws(item.eventJws, jwks),
    })),
  );
  const recordedDelivery = verifiedLedger.find(
    ({ verified }) => (verified.payload as { jti: string }).jti === "offline-event",
  );
  ok(recordedDelivery, "offline event was not sent by native alarm");
  equal(recordedDelivery.item.url, "https://merchant.example/events");
  ok(
    (await kv.get<{ deliveredAt: string }>([...payments, "events", "offline-event"])).value
      ?.deliveredAt,
  );
  equal(
    (await kv.get([...payments, "outbox", new Date(nextAttemptAt).getTime(), "offline-event"]))
      .value,
    null,
  );
  ok(
    ((await (await store.fetch("https://fixture.invalid/stats")).json()) as { rpcCalls: number })
      .rpcCalls > 0,
  );
  const deliveryIds = async () => {
    const ledger = (await (await store.fetch("https://fixture.invalid/relay-ledger")).json()) as {
      eventJws: string;
    }[];
    return Promise.all(
      ledger.map(
        async (item) => (await verifyCompactJws(item.eventJws, jwks)).payload as { jti: string },
      ),
    );
  };
  await eventually(async () => {
    const state = await objects.getByName("delivery:offline-event").inspect();
    return (
      state.alarm === null &&
      state.state?.job === null &&
      Object.keys(state.state.pending).length === 0
    );
  }, "terminal delivery left a pending cancellation intent");
  const deliveredBefore = (await deliveryIds()).filter(
    (event) => event.jti === "offline-event",
  ).length;
  await objects.getByName("delivery:offline-event").arm(Date.now() + 5);
  await new Promise((resolve) => setTimeout(resolve, 100));
  equal(
    (await deliveryIds()).filter((event) => event.jti === "offline-event").length,
    deliveredBefore,
    "delivered duplicate alarm sent again",
  );
  await seed.payment.markManualReview(invoice.id);
  await objects
    .getByName("invoice:" + invoice.id)
    .adopt({ kind: "invoice", id: invoice.id, dueAt: null, reason: "cancel" });

  const pushCreate = await worker.fetch("https://checkout.ubq.fi/api/v1/invoices", {
    method: "POST",
    headers: {
      authorization: "Bearer " + token,
      "content-type": "application/json",
      "idempotency-key": "push-invoice",
    },
    body: JSON.stringify({
      customerReference: "push-customer",
      pricingVersionId: "worker-v1",
      productId: "pack:worker",
      chainId: 8453,
    }),
  });
  equal(pushCreate.status, 201);
  const push = (await pushCreate.json()) as typeof invoice;
  const pushTransfer = {
    transactionHash: "0x" + "44".repeat(32),
    tokenAddress: push.tokenAddress,
    treasuryAddress: push.treasuryAddress,
    amountUnits: push.paidAmountUnits,
    blockNumber: push.minimumBlock,
    safeBlock: push.minimumBlock,
    includedAt: Math.floor(Date.now() / 1000) - 117,
  };
  const configure = async (transfer: Record<string, unknown>) => {
    await store.fetch("https://fixture.invalid/rpc-fixture", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(transfer),
    });
  };
  await configure(pushTransfer);
  const log = {
    address: push.tokenAddress,
    transactionHash: pushTransfer.transactionHash,
    blockNumber: "0x" + BigInt(push.minimumBlock).toString(16),
    blockHash: "0x" + "ab".repeat(32),
    logIndex: "0x7",
    removed: false,
    topics: [
      "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
      "0x" + "0".repeat(24) + "33".repeat(20),
      "0x" + "0".repeat(24) + push.treasuryAddress.slice(2),
    ],
    data: "0x" + BigInt(push.paidAmountUnits).toString(16).padStart(64, "0"),
  };
  const signed = async (id: string, item: typeof log, createdAt?: string) => {
    const body = JSON.stringify({
      webhookId: "wh_offline",
      id,
      createdAt: createdAt ?? new Date().toISOString().slice(0, -1) + "123456Z",
      type: "ADDRESS_ACTIVITY",
      event: { network: "BASE_MAINNET", activity: [{ category: "erc20", log: item }] },
    });
    const key = await crypto.subtle.importKey(
      "raw",
      new TextEncoder().encode(signingKey),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"],
    );
    const bytes = new Uint8Array(
      await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(body)),
    );
    return { body, signature: Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("") };
  };
  const submit = (input: { body: string; signature: string }) =>
    worker.fetch("https://checkout.ubq.fi/api/v1/chain-events/alchemy", {
      method: "POST",
      headers: { "content-type": "application/json", "x-alchemy-signature": input.signature },
      body: input.body,
    });
  const searchesBeforePush = (await stats()).methods.eth_getLogs ?? 0;
  const pushDelivery = await signed("push_accepted", log);
  const racedPush = await Promise.all([submit(pushDelivery), submit(pushDelivery)]);
  ok(racedPush.every((response) => response.status === 200 || response.status === 503));
  let pushResponse = racedPush.find((response) => response.status === 200) ?? racedPush[0];
  for (const response of racedPush) if (response !== pushResponse) await response.body?.cancel();
  for (let attempt = 0; pushResponse.status === 503 && attempt < 5; attempt++)
    pushResponse = await submit(pushDelivery);
  equal(pushResponse.status, 200, await pushResponse.text());
  equal(
    (await kv.get<{ status: string }>([...payments, "invoices", push.id])).value?.status,
    "confirming",
  );
  await eventually(
    async () =>
      (await kv.get<string>([...payments, "events_by_invoice", push.id, "payment.accepted.v1"]))
        .value !== null,
    "known-payment confirmation alarm did not accept payment",
  );
  const pushAcceptedId = (
    await kv.get<string>([...payments, "events_by_invoice", push.id, "payment.accepted.v1"])
  ).value!;
  const pushAccepted = (
    await kv.get<{ compactJws: string }>([...payments, "events", pushAcceptedId])
  ).value!;
  const pushAcceptedClaims = (await verifyCompactJws(pushAccepted.compactJws, jwks)).payload as {
    type: string;
    jti: string;
    invoiceId: string;
  };
  equal(pushAcceptedClaims.type, "payment.accepted.v1");
  equal(pushAcceptedClaims.invoiceId, push.id);
  equal(pushAcceptedClaims.jti, pushAcceptedId);
  ok(
    ["provisionally_paid", "settled", "finalized"].includes(
      (await kv.get<{ status: string }>([...payments, "invoices", push.id])).value!.status,
    ),
  );
  equal(
    (await stats()).methods.eth_getLogs ?? 0,
    searchesBeforePush,
    "primary chain ingress searched for a payment",
  );
  const beforeReplay = await stats();
  equal((await submit(pushDelivery)).status, 200);
  equal(
    (await stats()).rpcCalls,
    beforeReplay.rpcCalls,
    "completed transport replay executed canonical RPC",
  );
  equal(
    (await submit(await signed("push_accepted", { ...log, data: "0x" + "0".repeat(63) + "1" })))
      .status,
    409,
  );
  equal(
    (
      await submit(
        await signed("push_stale", log, new Date(Date.now() - 60 * 60_000).toISOString()),
      )
    ).status,
    401,
  );
  // A different authenticated ID with the same transfer is still domain-idempotent.
  const semanticDelivery = await signed("push_duplicate", log);
  let semanticReplay = await submit(semanticDelivery);
  if (semanticReplay.status === 503) {
    const receipt = (
      await kv.get<{ state: string; nextIndex: number; accountPrepared: number; leaseId?: string }>(
        [
          "uos_checkout",
          "v1",
          "chain_events",
          "receipts",
          "alchemy",
          "wh_offline",
          "push_duplicate",
        ],
      )
    ).value;
    equal(receipt?.state, "pending");
    equal(receipt?.nextIndex, 0);
    equal(receipt?.accountPrepared, 1);
    equal(receipt?.leaseId, undefined);
    const source = await seed.payment.getInvoiceRecord(push.id);
    const owner = await accountHash([source.merchant, source.customerReference]);
    ok((await kv.get<{ complete: boolean }>([...payments, "account_runs", owner])).value?.complete);
  }
  if (semanticReplay.status === 503) semanticReplay = await submit(semanticDelivery);

  equal(semanticReplay.status, 200, await semanticReplay.text());
  equal(
    (await kv.get<string>([...payments, "events_by_invoice", push.id, "payment.accepted.v1"]))
      .value,
    pushAcceptedId,
  );
  await configure({ ...pushTransfer, canonicalHash: "0x" + "cd".repeat(32) });
  const removed = await signed("push_removed", { ...log, removed: true });
  let removal = await submit(removed);
  for (let attempt = 0; removal.status === 503 && attempt < 5; attempt++)
    removal = await submit(removed);
  equal(removal.status, 200, await removal.text());
  equal(
    (await kv.get<{ status: string }>([...payments, "invoices", push.id])).value?.status,
    "reverted",
  );
  const newBlock = BigInt(push.minimumBlock) + 5n;
  const newLog = {
    ...log,
    blockNumber: "0x" + newBlock.toString(16),
    blockHash: "0x" + "cd".repeat(32),
    logIndex: "0x8",
  };
  await configure({
    ...pushTransfer,
    blockNumber: newBlock.toString(),
    blockHash: newLog.blockHash,
    logIndex: 8,
  });
  const reincluded = await signed("push_reincluded", newLog);
  let reinclusion = await submit(reincluded);
  for (let attempt = 0; reinclusion.status === 503 && attempt < 5; attempt++)
    reinclusion = await submit(reincluded);
  equal(reinclusion.status, 200, await reinclusion.text());
  const beforeOld = await kv.get<{ status: string; paymentId: string }>([
    ...payments,
    "invoices",
    push.id,
  ]);
  equal((await submit(await signed("push_stale_removed", { ...log, removed: true }))).status, 200);
  const afterOld = await kv.get<{ status: string; paymentId: string }>([
    ...payments,
    "invoices",
    push.id,
  ]);
  equal(afterOld.value?.paymentId, beforeOld.value?.paymentId);
  equal(afterOld.value?.status, beforeOld.value?.status);
  await seed.payment.markManualReview(push.id);
  await objects
    .getByName("invoice:" + push.id)
    .adopt({ kind: "invoice", id: push.id, dueAt: null, reason: "cancel" });
  await seed.payment.markManualReview(legacy.id);
  await objects
    .getByName(legacyName)
    .adopt({ kind: "invoice", id: legacy.id, dueAt: null, reason: "cancel" });
  equal((await objects.getByName(legacyName).inspect()).alarm, null);
  // Each optional repair is bounded to 7 nested calls, so distinct committed
  // operations may need another related event instead of a cleanup timer.
  for (let attempt = 0; attempt < 4; attempt++) {
    const state = await objects.getByName("invoice:" + push.id).inspect();
    if (state.alarm === null && Object.keys(state.state?.pending ?? {}).length === 0) break;
    const related = await worker.fetch(`https://checkout.ubq.fi/api/v1/invoices/${push.id}`, {
      headers: { authorization: "Bearer " + token },
    });
    equal(related.status, 200);
  }
  equal((await objects.getByName("invoice:" + push.id).inspect()).alarm, null);
  const pendingOperations = new Set<string>();
  let observedPending = false;
  for (const id of [legacy.id, invoice.id, push.id]) {
    const target = objects.getByName("invoice:" + id);
    let state = await target.inspect();
    if (Object.keys(state.state?.pending ?? {}).length) {
      observedPending = true;
      const before = JSON.stringify(state.state?.pending);
      const health = await worker.fetch("https://checkout.ubq.fi/api/health");
      equal(health.status, 200);
      equal(
        JSON.stringify((await target.inspect()).state?.pending),
        before,
        "unrelated health swept entity bookkeeping",
      );
    }
    for (
      let attempt = 0;
      Object.keys(state.state?.pending ?? {}).length && attempt < 64;
      attempt++
    ) {
      for (const operation of Object.keys(state.state!.pending)) pendingOperations.add(operation);
      const before = JSON.stringify(state.state?.pending);
      const related = await worker.fetch(`https://checkout.ubq.fi/api/v1/invoices/${id}`, {
        headers: { authorization: "Bearer " + token },
      });
      equal(related.status, 200);
      const next = await target.inspect();
      ok(
        next.alarm === null || JSON.stringify(next.state?.pending) !== before,
        "related event did not advance finite bookkeeping",
      );
      state = next;
    }
    equal(state.alarm, null, JSON.stringify({ id, state }));
    equal(state.state?.job, null);
    equal(state.state?.pending, {});
  }
  ok(observedPending, "fixture did not exercise related-event intent retirement");
  await eventually(async () => {
    for await (const entry of kv.list({ prefix: [...payments, "outbox"] })) {
      if (entry) return false;
    }
    return true;
  }, "merchant outbox did not quiesce");
  for (const operation of pendingOperations)
    equal((await kv.get(wakeIntentKey(operation))).value, null);
  const ledger = await objects.getByName("invoice:" + push.id).budgetLedger();
  for (const invocation of ledger) {
    ok(invocation.calls + (invocation.nested ?? 0) + 1 <= 32, JSON.stringify(invocation));
    ok(invocation.calls <= 50, JSON.stringify(invocation));
    if (invocation.limit === 30 && invocation.rpc > 0)
      ok(invocation.remaining > 0, JSON.stringify(invocation));
  }
  console.log(
    "workerd invocation bounds",
    JSON.stringify(
      ledger
        .filter((invocation) => invocation.limit === 30)
        .map(({ id, calls, storage, rpc, nested, remaining }) => ({
          id,
          calls,
          storage,
          rpc,
          nested,
          remaining,
        })),
    ),
  );
  console.log(
    "workerd: signed Alchemy HMAC rejection, exact transfer claim/replay, removed/reincluded/stale notices, event-only SQLite alarms and migration passed",
  );
  console.log(
    "workerd: routes, socket-marker stripping, assets/CSP/HEAD, Auth Hub redirect, authenticated merchant invoice/idempotency, empty-body discovery within budget, canonical payment transition and signed replay, persistent JWKS, D1 protocol, SQLite DO eviction/migration and native alarm/JWS delivery passed; all upstreams isolated",
  );
} finally {
  await mf.dispose();
}
