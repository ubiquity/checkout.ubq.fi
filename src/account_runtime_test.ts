import { test } from "bun:test";
import { deepStrictEqual as equal, ok, rejects } from "node:assert/strict";
import type { KvAtomic } from "./kv.ts";
import { createPaymentService } from "./payment_service.ts";
import { createPersistentJwsSigner } from "./jws.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { attemptStateKey, reservationKey, listAccountManifestPage } from "./account_store.ts";
import { accountKeyForInvoice } from "./account_attempts.ts";
import { observationKey, type TransferReview } from "./account_runtime.ts";
import { RuntimeRpc } from "./account_runtime_test_fixture.ts";
import { WorkerRequestBudget, WorkerBudgetError } from "./worker_budget.ts";
import { UbqKv } from "./ubq_kv.ts";
import { encodeKvValue, decodeKvValue } from "./kv_codec.ts";
import {
  ACCOUNT_INDEX_STATE_KEY,
  storedInvoiceKey,
  prepareAccountAttempt,
} from "./account_store.ts";

const treasury = `0x${"44".repeat(20)}`;

test("exhaustion after the payment CAS preserves its observation and resumes without duplicate acceptance", async () => {
  const app = await setup();
  try {
    const made = await app.create("crash-boundary");
    app.rpc.add(await app.service.getInvoiceRecord(made.id), 200n);
    app.rpc.latest = 300n;
    let blocked = false;
    const fault = new Proxy(app.kv, {
      get(target, property) {
        if (property !== "atomic") {
          const value = Reflect.get(target, property);
          return typeof value === "function" ? value.bind(target) : value;
        }
        return () => {
          const base = target.atomic();
          let domain = false;
          let checkpoint = false;
          const atomic: KvAtomic = {
            check(...entries) {
              base.check(...entries);
              return this;
            },
            set(key, value, options) {
              base.set(key, value, options);
              const record = value as { paymentId?: string; offset?: number };
              domain ||= key.includes("invoices") && !!record?.paymentId;
              checkpoint ||= key.includes("account_runs") && (record?.offset ?? 0) > 0;
              return this;
            },
            delete(key) {
              base.delete(key);
              return this;
            },
            async commit() {
              if (blocked && checkpoint) throw new WorkerBudgetError();
              const committed = await base.commit();
              if (committed.ok && domain) blocked = true;
              return committed;
            },
          };
          return atomic;
        };
      },
    });
    const crashing = createPaymentService({
      kv: fault,
      rpc: app.rpc,
      signer: createPersistentJwsSigner(fault),
    });
    await rejects(() => crashing.discoverPayment(made.id), WorkerBudgetError);
    const paymentId = (await app.service.getInvoiceRecord(made.id)).paymentId;
    ok(paymentId);
    const runs = [];
    for await (const entry of app.kv.list<{
      lease?: { id: string; until: number };
      offset: number;
      result: { observations: unknown[] };
    }>({ prefix: ["uos_checkout", "v1", "payments", "account_runs"] }))
      runs.push(entry);
    equal(runs.length, 1);
    equal(runs[0].value.offset, 0);
    ok(runs[0].value.result.observations.length > 0);
    // The next related event owns the lease after the previous invocation's deadline.
    await app.kv.set(runs[0].key, {
      ...runs[0].value,
      lease: { ...runs[0].value.lease!, until: Date.now() - 1 },
    });
    blocked = false;
    app.rpc.safe = 250n;
    const recovered = await app.service.discoverPayment(made.id);
    equal(recovered.invoice.paymentId, paymentId);
    equal(recovered.discovery?.found, true);
    await app.service.discoverPayment(made.id);
    equal(
      (await app.service.listOutbox()).filter((event) => event.type === "payment.accepted.v1")
        .length,
      1,
    );
  } finally {
    app.kv.close();
  }
});

test("low-RPC multiple-target invocation allowances share one conservative envelope", () => {
  for (const maximum of [24, 28, 30]) {
    const budget = new WorkerRequestBudget(maximum);
    let actual = 0;
    for (let target = 0; target < 8; target++) {
      try {
        budget.reserveNestedInvocations(7);
        budget.reserve();
        actual++;
      } catch (error) {
        ok(error instanceof WorkerBudgetError);
        break;
      }
    }
    while (budget.hasCapacity(2)) {
      budget.reserve();
      actual++;
    }
    ok(actual + budget.nestedInvocations + 2 <= 32);
    ok(actual < 50);
    ok(budget.remaining > 0);
    ok(budget.nestedInvocations >= 14);
  }
});

test("production service manifests enumerate one account and ownership history without any prefix query", async () => {
  const app = await setup();
  try {
    const first = await app.create("service-one");
    await app.create("other-account", "foreign");
    const { wire, paths } = wireFixture(app.kv);
    app.rpc.add(await app.service.getInvoiceRecord(first.id), 200n);
    app.rpc.latest = 300n;
    const service = createPaymentService({
      kv: wire,
      rpc: app.rpc,
      signer: createPersistentJwsSigner(wire),
    });
    const result = await service.discoverPayment(first.id);
    equal(result.discovery?.found, true);
    equal(
      result.accountHunt?.attempts.map((attempt) => attempt.invoiceId),
      [first.id],
    );
    ok(!paths.includes("/kv/list"));
    ok(paths.includes("/kv/getMany"));
  } finally {
    app.kv.close();
  }
});

test("legacy invalidation uses original creation time rather than migration ID visitation", async () => {
  const app = await setup();
  try {
    const template = await app.create("template");
    const record = await app.service.getInvoiceRecord(template.id);
    const older = {
      ...record,
      id: "z-older",
      customerReference: "legacy",
      paidAmountUnits: 999_997n,
      createdAt: "2026-10-17T00:00:00.000Z",
    };
    const newer = {
      ...record,
      id: "a-newer",
      customerReference: "legacy",
      paidAmountUnits: 999_996n,
      createdAt: "2026-10-17T01:00:00.000Z",
    };
    for (const invoice of [newer, older])
      await app.kv
        .atomic()
        .set(storedInvoiceKey(invoice.id), invoice)
        .set(reservationKey(invoice), { invoiceId: invoice.id, reservedAt: invoice.createdAt })
        .commit();
    await app.kv.delete(ACCOUNT_INDEX_STATE_KEY);
    app.rpc.add(newer, 200n);
    app.rpc.latest = 300n;
    app.rpc.safe = 250n;
    const result = await app.service.discoverPayment(older.id);
    equal(
      result.accountHunt?.attempts.find((attempt) => attempt.invoiceId === older.id)?.outcome,
      "invalidated",
    );
    equal(
      (await app.kv.get<{ sequence?: number }>(attemptStateKey(newer.id))).value?.sequence,
      undefined,
    );
    equal(
      (await app.kv.get<{ sequence?: number }>(attemptStateKey(older.id))).value?.sequence,
      undefined,
    );
  } finally {
    app.kv.close();
  }
});

test("an earlier current-confirming payment is protected when the later attempt reaches safe", async () => {
  const app = await setup();
  try {
    const first = await app.create("confirming-first");
    const second = await app.create("paid-later");
    app.rpc.add(await app.service.getInvoiceRecord(first.id), 250n);
    app.rpc.add(await app.service.getInvoiceRecord(second.id), 200n);
    app.rpc.latest = 300n;
    app.rpc.safe = 200n;
    const result = await app.service.discoverPayment(second.id);
    equal(
      result.accountHunt?.attempts.find((attempt) => attempt.invoiceId === first.id)?.outcome,
      "confirming",
    );
    equal(
      result.accountHunt?.attempts.find((attempt) => attempt.invoiceId === second.id)?.outcome,
      "credited",
    );
    ok((await app.kv.get(reservationKey(await app.service.getInvoiceRecord(first.id)))).value);
  } finally {
    app.kv.close();
  }
});

test("runtime resolves distinct same-time attempts, releases only earlier unpaid ownership, and anchors the requested invoice", async () => {
  const app = await setup();
  try {
    const first = await app.create("first");
    const second = await app.create("second");
    ok(first.id !== second.id);
    ok(first.paidAmountUnits !== second.paidAmountUnits);
    equal((await app.create("second")).id, second.id);
    const secondRecord = await app.service.getInvoiceRecord(second.id);
    app.rpc.add(secondRecord, 200n);
    app.rpc.latest = 300n;
    app.rpc.safe = 250n;
    const result = await app.service.discoverPayment(first.id);
    equal(result.invoice.id, first.id);
    equal(result.discovery?.found, false);
    equal(result.accountHunt?.reachedHead, true);
    equal(
      result.accountHunt?.attempts.find((attempt) => attempt.invoiceId === first.id)?.outcome,
      "invalidated",
    );
    equal(
      result.accountHunt?.attempts.find((attempt) => attempt.invoiceId === second.id)?.outcome,
      "credited",
    );
    const firstRecord = await app.service.getInvoiceRecord(first.id);
    equal((await app.kv.get(reservationKey(firstRecord))).value, null);
    equal(
      (await app.kv.get<{ resolution: string }>(attemptStateKey(first.id))).value?.resolution,
      "invalidated",
    );
    const replay = await app.service.discoverPayment(first.id);
    equal(
      replay.accountHunt?.attempts.find((attempt) => attempt.invoiceId === first.id)?.outcome,
      "invalidated",
    );
  } finally {
    app.kv.close();
  }
});

test("late expired attempts remain candidates and earliest confirming payments survive a later credited attempt", async () => {
  const app = await setup();
  try {
    const first = await app.create("expired");
    app.advance(61 * 60_000);
    await app.service.expireInvoice(first.id);
    const older = await app.service.getInvoiceRecord(first.id);
    app.rpc.add(older, 250n);
    app.rpc.latest = 300n;
    const late = await app.service.discoverPayment(first.id);
    equal(late.invoice.status, "confirming");
    equal(late.discovery?.found, true);
    const second = await app.create("new");
    app.rpc.add(await app.service.getInvoiceRecord(second.id), 302n);
    app.rpc.latest = 310n;
    app.rpc.safe = 303n;
    await app.service.discoverPayment(second.id);
    ok((await app.service.getInvoiceRecord(first.id)).paymentId);
    ok((await app.kv.get(reservationKey(older))).value);
    equal(
      (await app.kv.get<{ resolution: string }>(attemptStateKey(first.id))).value?.resolution,
      "unresolved",
    );
  } finally {
    app.kv.close();
  }
});

test("released amount reuse retains historical owners and blocks late cross-account attribution on wallet and hunts", async () => {
  const app = await setup();
  try {
    const first = await app.create("a-old");
    const second = await app.create("a-new");
    app.rpc.add(await app.service.getInvoiceRecord(second.id), 200n);
    app.rpc.latest = 300n;
    app.rpc.safe = 250n;
    await app.service.discoverPayment(second.id);
    const reused = await app.create("b-new", "different-account");
    equal(reused.paidAmountUnits, first.paidAmountUnits);
    const late = app.rpc.add(await app.service.getInvoiceRecord(first.id), 302n);
    app.rpc.latest = 310n;
    app.rpc.safe = 305n;
    await app.service.reportPayment(reused.id, {
      transactionHash: late.transactionHash,
      logIndex: "0",
      observedBlockNumber: "302",
      observedBlockHash: late.blockHash,
    });
    equal((await app.service.getInvoiceRecord(reused.id)).paymentId, undefined);
    const review = (await app.kv.get<TransferReview>(observationKey(8453, late))).value;
    equal(review?.classification, "ambiguous");
    equal(review?.owners.length, 2);
    const page = await listAccountManifestPage(
      app.kv,
      accountKeyForInvoice(await app.service.getInvoiceRecord(reused.id), "merchant"),
      { limit: 500 },
    );
    equal(
      page.entries.map((entry) => entry.value.invoiceId),
      [reused.id],
    );
  } finally {
    app.kv.close();
  }
});

test("signed unknown outgoing recipients are ignored while authorized unmatched treasury transfers are retained", async () => {
  const app = await setup();
  try {
    const made = await app.create("one");
    const base = await app.service.getInvoiceRecord(made.id);
    const log = app.rpc.add({ ...base, paidAmountUnits: 77n }, 200n);
    const context = { deadline: Date.now() + 20_000, isCurrent: async () => true };
    const outgoing = {
      ...log,
      chainId: 8453 as const,
      recipientAddress: `0x${"88".repeat(20)}`,
      removed: false,
    };
    const ignored = await app.service.processChainTransfer(outgoing, context);
    equal(ignored.terminal, true);
    equal((await app.kv.get(observationKey(8453, outgoing))).value, null);
    const retained = await app.service.processChainTransfer(
      { ...log, chainId: 8453, removed: false },
      context,
    );
    equal(retained.terminal, true);
    equal(
      (await app.kv.get<TransferReview>(observationKey(8453, log))).value?.classification,
      "unowned",
    );
  } finally {
    app.kv.close();
  }
});

async function setup() {
  const kv = new SqliteKv();
  const rpc = new RuntimeRpc();
  let now = Date.parse("2026-10-18T00:00:00Z");
  const clock = { now: () => new Date(now) };
  let serial = 0;
  const service = createPaymentService({
    kv,
    rpc,
    signer: createPersistentJwsSigner(kv, clock),
    clock,
    idGenerator: { next: () => `runtime-${++serial}` },
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
    service,
    advance: (milliseconds: number) => {
      now += milliseconds;
    },
    create: (idempotencyKey: string, customerReference = "account") =>
      service.createInvoice(
        { merchantId: "merchant", idempotencyKey },
        { customerReference, pricingVersionId: "prices", productId: "credits", chainId: 8453 },
      ),
  };
}

function wireFixture(store: SqliteKv) {
  const paths: string[] = [];
  const wire = new UbqKv({
    url: "https://offline.invalid",
    token: "fake",
    fetchImpl: async (url, init) => {
      const path = new URL(url).pathname;
      paths.push(path);
      const body = JSON.parse(String(init.body));
      const entry = async (key: readonly (string | number | boolean)[]) => {
        const value = await store.get(key);
        return value.value === null ? null : { ...value, value: encodeKvValue(value.value) };
      };
      if (path === "/kv/get") return Response.json({ ok: true, entry: await entry(body.key) });
      if (path === "/kv/getMany")
        return Response.json({ ok: true, entries: await Promise.all(body.keys.map(entry)) });
      if (path === "/kv/atomic") {
        const atomic = store.atomic().check(...body.checks);
        for (const change of body.mutations) {
          if (change.type === "set") atomic.set(change.key, decodeKvValue(change.value));
          else atomic.delete(change.key);
        }
        return Response.json(await atomic.commit());
      }
      throw new Error(`unexpected_prefix_or_wire_call:${path}`);
    },
  });

  return { wire, paths };
}

test("exact account manifest pages cross the64-entry boundary without reading unrelated keyspace", async () => {
  const app = await setup();
  try {
    const initial = await app.create("manifest-template");
    const template = await app.service.getInvoiceRecord(initial.id);
    for (let index = 0; index < 70; index++) {
      const invoice = {
        ...template,
        id: `legacy-page-${index}`,
        customerReference: "paged-account",
        paidAmountUnits: template.paidAmountUnits - BigInt(index + 10),
        discountUnits: template.discountUnits + BigInt(index + 10),
      };
      const prepared = await prepareAccountAttempt(app.kv, invoice);
      ok(
        (
          await prepared
            .apply(
              app.kv
                .atomic()
                .check({ key: storedInvoiceKey(invoice.id), versionstamp: null })
                .set(storedInvoiceKey(invoice.id), invoice)
                .set(reservationKey(invoice), {
                  invoiceId: invoice.id,
                  reservedAt: invoice.createdAt,
                }),
            )
            .commit()
        ).ok,
      );
    }
    const { wire, paths } = wireFixture(app.kv);
    const first = await listAccountManifestPage(wire, ["merchant", "paged-account"], {
      limit: 500,
    });
    equal(first.entries.length, 64);
    ok(first.cursor);
    const second = await listAccountManifestPage(wire, ["merchant", "paged-account"], {
      limit: 500,
      cursor: first.cursor,
    });
    equal(second.entries.length, 6);
    equal(second.cursor, null);
    equal(
      new Set([...first.entries, ...second.entries].map((entry) => entry.value.invoiceId)).size,
      70,
    );
    ok(
      [...first.entries, ...second.entries].every((entry) => entry.value.invoiceId !== initial.id),
    );
    ok(paths.every((path) => path === "/kv/get" || path === "/kv/getMany"));
  } finally {
    app.kv.close();
  }
});
