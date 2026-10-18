import { expect, test } from "bun:test";
import { MAX_ALCHEMY_BODY_BYTES, verifyAlchemyWebhookDelivery } from "./alchemy_webhook.ts";
import {
  chainEventReceiptKey,
  handleAlchemyChainWebhook,
  MAX_PENDING_CHAIN_REFERENCES,
  pendingChainTransfersKey,
  processChainEventDelivery,
  recoverPendingChainTransfers,
  type ChainEventReceipt,
  type ChainIngressOptions,
} from "./chain_ingress.ts";
import type { Kv, KvAtomic } from "./kv.ts";
import { CHAIN_CONFIGURATIONS } from "./payment_types.ts";
import { TRANSFER_TOPIC } from "./rpc.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { WorkerRequestBudget } from "./worker_budget.ts";

const NOW = new Date("2026-10-18T04:00:00.000Z");
const KEY = "fake_signing_key";
const ID = "wh_fake";
const KEYS = JSON.stringify({ [ID]: { network: "BASE_MAINNET", signingKey: KEY } });

test("HTTP200 follows durable completion and an authenticated duplicate has no callback effects", async () => {
  await withStore(async (kv) => {
    const signed = await fixture(3);
    const calls: number[] = [];
    const options = settings(kv, async (event) => {
      calls.push(event.logIndex);
      return { terminal: true };
    });
    expect((await handleAlchemyChainWebhook(request(signed), options)).status).toBe(200);
    const retry = await handleAlchemyChainWebhook(request(signed), options);
    expect(retry.status).toBe(200);
    expect(await retry.json()).toMatchObject({ complete: true, replay: true });
    expect(calls).toEqual([0, 1, 2]);
    const receipt = (await kv.get<ChainEventReceipt>(chainEventReceiptKey(signed.delivery))).value!;
    expect(receipt).toMatchObject({ state: "complete", nextIndex: 3, transfers: [] });
    expect(receipt.leaseId).toBeUndefined();
    expect((await kv.get(pendingChainTransfersKey(signed.delivery.transfers[0]))).value).toBeNull();
  });
});

test("unsigned, tampered, unknown-key, stale and mismatched-network requests cause zero KV calls", async () => {
  await withStore(async (store) => {
    const signed = await fixture();
    const measured = measuredKv(store);
    const options = settings(measured.kv);
    const inputs = [
      new Request("https://checkout.example/events", { method: "POST", body: signed.body }),
      request({ ...signed, signature: "0".repeat(64) }),
      request({
        ...signed,
        body: new TextEncoder().encode(`${new TextDecoder().decode(signed.body)} `),
      }),
      request(await fixture(1, { webhookId: "wh_unknown" })),
      request(await fixture(1, { network: "ETH_MAINNET" })),
      request(await fixture(1, { createdAt: "2026-10-17T04:00:00.000Z" })),
    ];
    const statuses = [401, 401, 401, 401, 400, 401];
    for (const [index, input] of inputs.entries())
      expect((await handleAlchemyChainWebhook(input, options)).status).toBe(statuses[index]);
    for (const webhookKeys of [
      undefined,
      "{}",
      "null",
      "{",
      JSON.stringify({ [ID]: { signingKey: KEY, network: "__proto__" } }),
    ]) {
      expect(
        (await handleAlchemyChainWebhook(request(signed), { ...options, webhookKeys })).status,
      ).toBe(503);
    }
    expect(measured.calls()).toBe(0);
  });
});

test("streaming ingress needs no Content-Length and cancels oversized bodies before storage", async () => {
  await withStore(async (store) => {
    const signed = await fixture();
    const measured = measuredKv(store);
    const validStream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(signed.body.subarray(0, 20));
        controller.enqueue(signed.body.subarray(20));
        controller.close();
      },
    });
    expect(
      (
        await handleAlchemyChainWebhook(
          new Request("https://checkout.example/events", {
            method: "POST",
            headers: { "x-alchemy-signature": signed.signature },
            body: validStream,
          }),
          settings(measured.kv),
        )
      ).status,
    ).toBe(200);
    const before = measured.calls();
    let cancelled = false;
    const large = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(MAX_ALCHEMY_BODY_BYTES));
        controller.enqueue(new Uint8Array(1));
      },
      cancel() {
        cancelled = true;
      },
    });
    expect(
      (
        await handleAlchemyChainWebhook(
          new Request("https://checkout.example/events", { method: "POST", body: large }),
          settings(measured.kv),
        )
      ).status,
    ).toBe(413);
    expect(cancelled).toBe(true);
    expect(
      (
        await handleAlchemyChainWebhook(
          new Request("https://checkout.example/events", {
            method: "POST",
            headers: { "content-length": String(MAX_ALCHEMY_BODY_BYTES + 1) },
            body: "x",
          }),
          settings(measured.kv),
        )
      ).status,
    ).toBe(413);
    expect(measured.calls()).toBe(before);
  });
});

test("incomplete and transient-failure deliveries resume exactly their terminal prefix", async () => {
  for (const throws of [false, true])
    await withStore(async (kv) => {
      const signed = await fixture(3);
      const calls: number[] = [];
      let blocked = true;
      const options = settings(kv, async (event) => {
        calls.push(event.logIndex);
        if (event.logIndex === 1 && blocked) {
          if (throws) throw new Error("receipt not visible");
          return { terminal: false };
        }
        return { terminal: true };
      });
      expect((await handleAlchemyChainWebhook(request(signed), options)).status).toBe(503);
      const pending = (await kv.get<ChainEventReceipt>(chainEventReceiptKey(signed.delivery)))
        .value!;
      expect(pending.nextIndex).toBe(1);
      expect(pending.transfers).toHaveLength(3);
      expect(pending.leaseId).toBeUndefined();
      const refs = (
        await kv.get<Array<{ index: number }>>(
          pendingChainTransfersKey(signed.delivery.transfers[1]),
        )
      ).value!;
      expect(refs.map((ref) => ref.index)).toEqual([1]);
      blocked = false;
      expect((await handleAlchemyChainWebhook(request(signed), options)).status).toBe(200);
      expect(calls).toEqual([0, 1, 1, 2]);
    });
});

test("same identity with changed signed bytes is a409 conflict", async () => {
  await withStore(async (kv) => {
    const original = await fixture();
    let calls = 0;
    const options = settings(kv, async () => {
      calls++;
      return { terminal: true };
    });
    expect((await handleAlchemyChainWebhook(request(original), options)).status).toBe(200);
    const changed = await fixture(2);
    expect((await handleAlchemyChainWebhook(request(changed), options)).status).toBe(409);
    expect(calls).toBe(1);
  });
});

test("active lease prevents parallel processing; expired owner cannot checkpoint over its successor", async () => {
  await withStore(async (kv) => {
    const signed = await fixture();
    let now = NOW;
    let enter!: () => void;
    let release!: () => void;
    const entered = new Promise<void>((resolve) => {
      enter = resolve;
    });
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    let invocations = 0;
    let effects = 0;
    const options = {
      ...settings(kv, async (_event, context) => {
        invocations++;
        if (invocations === 1) {
          enter();
          await held;
        }
        if (!(await context.isCurrent())) return { terminal: false };
        effects++;
        return { terminal: true };
      }),
      clock: { now: () => now },
    };
    const first = processChainEventDelivery(signed.delivery, options);
    await entered;
    expect((await handleAlchemyChainWebhook(request(signed), options)).status).toBe(503);
    now = new Date(NOW.getTime() + 31_000);
    expect((await processChainEventDelivery(signed.delivery, options)).complete).toBe(true);
    release();
    expect((await first).complete).toBe(false);
    expect(effects).toBe(1);
    expect(
      (await kv.get<ChainEventReceipt>(chainEventReceiptKey(signed.delivery))).value?.state,
    ).toBe("complete");
  });
});

test("four reserved calls checkpoint the completed item and release ownership under a tight budget", async () => {
  await withStore(async (store) => {
    const signed = await fixture(3);
    const calls: number[] = [];
    for (let index = 0; index < 3; index++) {
      const budget = new WorkerRequestBudget(10);
      const measured = measuredKv(store, budget);
      const result = await processChainEventDelivery(signed.delivery, {
        ...settings(measured.kv, async (event) => {
          budget.reserve();
          calls.push(event.logIndex);
          return { terminal: true };
        }),
        budget,
        minimumTransferCalls: 1,
      });
      expect(result.processed).toBe(1);
      expect(result.complete).toBe(index === 2);
      expect(measured.calls() + 1).toBeLessThanOrEqual(10);
      expect(
        (await store.get<ChainEventReceipt>(chainEventReceiptKey(signed.delivery))).value
          ?.nextIndex,
      ).toBe(index + 1);
    }
    expect(calls).toEqual([0, 1, 2]);
  });
});

test("crash after domain commit before checkpoint safely replays through domain idempotence", async () => {
  await withStore(async (store) => {
    const signed = await fixture();
    const measured = measuredKv(store, undefined, 3);
    const applied = new Set<string>();
    let attempts = 0;
    const options = settings(measured.kv, async (event) => {
      attempts++;
      applied.add(event.transactionHash);
      return { terminal: true };
    });
    expect((await processChainEventDelivery(signed.delivery, options)).complete).toBe(false);
    expect(
      (await store.get<ChainEventReceipt>(chainEventReceiptKey(signed.delivery))).value?.nextIndex,
    ).toBe(0);
    expect((await processChainEventDelivery(signed.delivery, options)).complete).toBe(true);
    expect(attempts).toBe(2);
    expect(applied.size).toBe(1);
  });
});

test("related recovery visits only the indexed matching item, leaving the unindexed suffix for retry", async () => {
  await withStore(async (store) => {
    const signed = await fixture(2);
    const measured = measuredKv(store);
    const seen: number[] = [];
    let ready = false;
    const options = settings(measured.kv, async (event) => {
      seen.push(event.logIndex);
      return { terminal: ready };
    });
    expect((await processChainEventDelivery(signed.delivery, options)).complete).toBe(false);
    expect(
      (await recoverPendingChainTransfers(signed.delivery.transfers[1], options)).processed,
    ).toBe(0);
    ready = true;
    const recovered = await recoverPendingChainTransfers(signed.delivery.transfers[0], options);
    expect(recovered).toEqual({ complete: false, processed: 1 });
    expect(seen).toEqual([0, 0]);
    expect(
      (await store.get(pendingChainTransfersKey(signed.delivery.transfers[1]))).value,
    ).toBeNull();
    expect((await processChainEventDelivery(signed.delivery, options)).complete).toBe(true);
    expect(seen).toEqual([0, 0, 1]);
  });
});

test("full pending-reference tuple preserves every existing reference and retains the new receipt", async () => {
  await withStore(async (kv) => {
    const signed = await fixture();
    const refs = Array.from({ length: MAX_PENDING_CHAIN_REFERENCES }, (_, index) => ({
      provider: "alchemy",
      subscriptionId: ID,
      eventId: `other_${index}`,
      bodySha256: "0".repeat(64),
      index: 0,
    }));
    const key = pendingChainTransfersKey(signed.delivery.transfers[0]);
    await kv.set(key, refs);
    let calls = 0;
    const result = await processChainEventDelivery(
      signed.delivery,
      settings(kv, async () => {
        calls++;
        return { terminal: true };
      }),
    );
    expect(result.complete).toBe(false);
    expect(calls).toBe(0);
    expect((await kv.get(key)).value).toEqual(refs);
    expect(
      (await kv.get<ChainEventReceipt>(chainEventReceiptKey(signed.delivery))).value,
    ).toMatchObject({ state: "pending", nextIndex: 0 });
  });
});

test("expired deadline does not invoke callback or admit a new lease", async () => {
  await withStore(async (kv) => {
    const signed = await fixture();
    let calls = 0;
    const result = await processChainEventDelivery(signed.delivery, {
      ...settings(kv, async () => {
        calls++;
        return { terminal: true };
      }),
      deadline: 0,
    });
    expect(result.complete).toBe(false);
    expect(calls).toBe(0);
    expect((await kv.get(chainEventReceiptKey(signed.delivery))).value).toBeNull();
  });
});

function settings(
  kv: Kv,
  processTransfer: ChainIngressOptions["processTransfer"] = async () => ({ terminal: true }),
) {
  return {
    kv,
    webhookKeys: KEYS,
    processTransfer,
    minimumTransferCalls: 0,
    clock: { now: () => NOW },
  };
}
async function fixture(
  count = 1,
  override: { webhookId?: string; network?: string; createdAt?: string } = {},
) {
  const payload = {
    webhookId: override.webhookId ?? ID,
    id: "whevt_fake",
    createdAt: override.createdAt ?? NOW.toISOString(),
    type: "ADDRESS_ACTIVITY",
    event: {
      network: override.network ?? "BASE_MAINNET",
      activity: Array.from({ length: count }, (_, index) => ({
        category: "token",
        log: {
          address: CHAIN_CONFIGURATIONS[8453].tokenAddress,
          topics: [
            TRANSFER_TOPIC,
            `0x${"0".repeat(24)}${"11".repeat(20)}`,
            `0x${"0".repeat(24)}${"22".repeat(20)}`,
          ],
          data: `0x${(100n + BigInt(index)).toString(16).padStart(64, "0")}`,
          blockNumber: "0x123",
          blockHash: `0x${"33".repeat(32)}`,
          transactionHash: `0x${index.toString(16).padStart(64, "0")}`,
          logIndex: `0x${index.toString(16)}`,
          removed: false,
        },
      })),
    },
  };
  const body = new TextEncoder().encode(JSON.stringify(payload));
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(KEY),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = Array.from(
    new Uint8Array(await crypto.subtle.sign("HMAC", key, body)),
    (byte) => byte.toString(16).padStart(2, "0"),
  ).join("");
  // Invalid HTTP cases still use authentic signed fake deliveries; their shape binds to the trusted key only at ingress.
  const validPayload = {
    ...payload,
    webhookId: ID,
    createdAt: NOW.toISOString(),
    event: { ...payload.event, network: "BASE_MAINNET" },
  };
  const validBody = new TextEncoder().encode(JSON.stringify(validPayload));
  const validSignature = Array.from(
    new Uint8Array(await crypto.subtle.sign("HMAC", key, validBody)),
    (byte) => byte.toString(16).padStart(2, "0"),
  ).join("");
  const delivery = await verifyAlchemyWebhookDelivery(validBody, validSignature, {
    signingKey: KEY,
    webhookId: ID,
    chainId: 8453,
    now: NOW,
  });
  return { body, signature, delivery };
}
function request(signed: { body: Uint8Array<ArrayBuffer>; signature: string }) {
  return new Request("https://checkout.example/events", {
    method: "POST",
    headers: { "content-type": "application/json", "x-alchemy-signature": signed.signature },
    body: signed.body,
  });
}
async function withStore(run: (kv: SqliteKv) => Promise<void>) {
  const kv = new SqliteKv(":memory:");
  try {
    await run(kv);
  } finally {
    kv.close();
  }
}
function measuredKv(store: Kv, budget?: WorkerRequestBudget, failAtomic?: number) {
  let calls = 0;
  let atomics = 0;
  const count = () => {
    budget?.reserve();
    calls++;
  };
  const kv: Kv = {
    listPage: (prefix, options) => {
      count();
      return store.listPage(prefix, options);
    },
    get: (key) => {
      count();
      return store.get(key);
    },
    getMany: (keys) => {
      count();
      return store.getMany(keys);
    },
    set: (key, value, options) => {
      count();
      return store.set(key, value, options);
    },
    delete: (key) => {
      count();
      return store.delete(key);
    },
    list: () => {
      throw new Error("global listing is forbidden");
    },
    close: () => {},
    atomic: () => {
      const atomic = store.atomic();
      const wrapper: KvAtomic = {
        check(...entries) {
          atomic.check(...entries);
          return wrapper;
        },
        set(key, value, options) {
          atomic.set(key, value, options);
          return wrapper;
        },
        delete(key) {
          atomic.delete(key);
          return wrapper;
        },
        async commit() {
          count();
          atomics++;
          if (atomics === failAtomic) return { ok: false, versionstamp: null };
          return atomic.commit();
        },
      };
      return wrapper;
    },
  };
  return { kv, calls: () => calls };
}
