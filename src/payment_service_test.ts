import { WorkerBudgetError } from "./worker_budget.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { inspect } from "node:util";
import { test } from "bun:test";
import type { Kv } from "./kv.ts";
import { createPersistentJwsSigner, verifyCompactJws } from "./jws.ts";
import {
  createPaymentService,
  type InvoiceCreationContext,
  type PaymentService,
} from "./payment_service.ts";
import { type InvoiceRecord, PaymentDomainError } from "./payment_types.ts";
import { type JsonRpcBatchItem, type JsonRpcCall, type RpcClient, TRANSFER_TOPIC } from "./rpc.ts";

const MERCHANT = "linkedin-scraper-2025";
const TREASURY = `0x${"44".repeat(20)}`;
const PAYER = `0x${"33".repeat(20)}`;

test("catalog versions are canonical, merchant-scoped, immutable, and idempotent", async () => {
  await withService(async ({ service }) => {
    const unsorted = catalog([product("second", 200), product("first", 100)]);
    const first = await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices-1",
      unsorted,
    );
    const reordered = await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices-1",
      catalog([product("first", 100), product("second", 200)]),
    );
    assertEquals(first.digest, reordered.digest);
    assertEquals(
      first.projection.products.map((entry) => entry.productId),
      ["first", "second"],
    );
    assertEquals(first.canonicalJson, JSON.stringify(first.projection));

    await assertDomainError(
      () =>
        service.publishCatalogVersion(
          { merchantId: MERCHANT },
          "prices-1",
          catalog([product("first", 101), product("second", 200)]),
        ),
      "catalog_version_conflict",
    );
    const otherMerchant = await service.publishCatalogVersion(
      { merchantId: "another-merchant" },
      "prices-1",
      catalog([product("first", 500)]),
    );
    assert(otherMerchant.digest !== first.digest);
  });
});

test("invoice allocation resolves overlapping amount bands and preserves expired slots", async () => {
  const starts = [1n, 10_001n, 1n];
  await withService(
    async ({ service, clock }) => {
      await service.publishCatalogVersion(
        { merchantId: MERCHANT },
        "prices-100",
        catalog([product("pack-100", 100)]),
      );
      await service.publishCatalogVersion(
        { merchantId: MERCHANT },
        "prices-101",
        catalog([product("pack-101", 101)]),
      );
      const first = await service.createInvoice(context("first"), {
        customerReference: "customer-a",
        pricingVersionId: "prices-100",
        productId: "pack-100",
        chainId: 8453,
      });
      const overlapping = await service.createInvoice(context("second"), {
        customerReference: "customer-b",
        pricingVersionId: "prices-101",
        productId: "pack-101",
        chainId: 8453,
      });
      assertEquals(first.paidAmountUnits, "999999");
      assertEquals(overlapping.paidAmountUnits, "999998");
      assertEquals(overlapping.discountUnits, "10002");

      clock.advance(61 * 60 * 1000);
      assertEquals((await service.expireInvoice(first.id)).status, "expired");
      const afterExpiry = await service.createInvoice(context("third"), {
        customerReference: "customer-c",
        pricingVersionId: "prices-100",
        productId: "pack-100",
        chainId: 8453,
      });
      assertEquals(afterExpiry.paidAmountUnits, "999997");
      assertEquals(afterExpiry.discountUnits, "3");
    },
    { discountStart: () => starts.shift() ?? 1n },
  );
});

test("finalized amount reservations become reusable only after the quarantine window", async () => {
  await withService(async ({ service, rpc, clock }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    const first = await service.createInvoice(context("first-finalized"), {
      customerReference: "first-finalized",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    rpc.addTransfer(await service.getInvoiceRecord(first.id), {
      transactionHash: hash(1),
      blockNumber: 100n,
      includedAt: clock.now(),
    });
    rpc.setHeads(103n, null, null);
    await service.claimPayment(first.id, claim(hash(1), 100n));
    rpc.setHeads(103n, 100n, null);
    await service.refreshInvoice(first.id);
    await service.refreshInvoice(first.id);
    rpc.setHeads(103n, 100n, 100n);
    assertEquals((await service.refreshInvoice(first.id)).invoice.status, "finalized");

    const quarantined = await service.createInvoice(context("during-quarantine"), {
      customerReference: "during-quarantine",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    assertEquals(quarantined.discountUnits, "2");

    clock.advance(24 * 60 * 60 * 1000 + 1);
    const reused = await service.createInvoice(context("after-quarantine"), {
      customerReference: "after-quarantine",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    assertEquals(reused.discountUnits, "1");
    assertEquals(reused.paidAmountUnits, first.paidAmountUnits);
    assert(BigInt(reused.minimumBlock) > 100n);
    await assertDomainError(
      () => service.claimPayment(reused.id, claim(hash(1), 100n)),
      "payment_locator_already_claimed",
    );
  });
});

test("concurrent creation replays one key and gives fresh keys distinct attempts", async () => {
  await withService(async ({ service }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    const creationContext = context("same-key");
    const input = {
      customerReference: "customer-concurrent",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    };
    const invoices = await Promise.all(
      Array.from({ length: 12 }, () => service.createInvoice(creationContext, input)),
    );
    assertEquals(new Set(invoices.map((invoice) => invoice.id)).size, 1);
    assertEquals(new Set(invoices.map((invoice) => invoice.paidAmountUnits)).size, 1);

    const secondKey = await service.createInvoice(context("another-key"), input);
    assert(secondKey.id !== invoices[0].id);
    assert(secondKey.paidAmountUnits !== invoices[0].paidAmountUnits);
    await assertDomainError(
      () => service.createInvoice(creationContext, { ...input, customerReference: "changed" }),
      "idempotency_key_conflict",
    );
  });
});

test("admin overviews merge disabled chains and never expose invoice binding secrets", async () => {
  await withService(async ({ service }) => {
    const chains = await service.listChainConfigurations();
    assertEquals(
      chains.map((chain) => [chain.chainId, chain.enabled]),
      [
        [1, false],
        [10, false],
        [8453, true],
      ],
    );
    const updated = await service.configureChain(8453, {
      enabled: true,
      treasuryAddress: TREASURY,
    });
    assertEquals(updated.revision, 2);
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    await service.createInvoice(context("overview"), {
      customerReference: "secret-customer-reference",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    const [invoice] = await service.listInvoices();
    assertEquals("customerReference" in invoice, false);
    assertEquals("merchant" in invoice, false);
    assertEquals("idempotencyKey" in invoice, false);
    assertEquals(typeof invoice.paidAmountUnits, "string");
  });
});

test("expired unresolved attempts accept both late and pre-expiry canonical inclusion", async () => {
  await withService(async ({ service, rpc, clock }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    const late = await service.createInvoice(context("late"), {
      customerReference: "late-customer",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    const lateRecord = await service.getInvoiceRecord(late.id);
    rpc.addTransfer(lateRecord, {
      transactionHash: hash(10),
      blockNumber: 100n,
      includedAt: new Date(Date.parse(late.expiresAt) + 1),
    });
    rpc.setHeads(103n, null, null);
    clock.advance(2 * 60 * 60 * 1000);
    assertEquals((await service.expireInvoice(late.id)).status, "expired");
    const lateClaim = await service.claimPayment(late.id, claim(hash(10), 100n));
    assertEquals(lateClaim.invoice.status, "confirming");
    assertEquals(lateClaim.eventJws, undefined);

    const onTime = await service.createInvoice(context("on-time"), {
      customerReference: "on-time-customer",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    const onTimeRecord = await service.getInvoiceRecord(onTime.id);
    rpc.addTransfer(onTimeRecord, {
      transactionHash: hash(11),
      blockNumber: 104n,
      includedAt: new Date(Date.parse(onTime.expiresAt) - 1),
    });
    rpc.setHeads(107n, null, null);
    clock.advance(2 * 60 * 60 * 1000);
    assertEquals((await service.expireInvoice(onTime.id)).status, "expired");
    const onTimeClaim = await service.claimPayment(onTime.id, claim(hash(11), 104n));
    assertEquals(onTimeClaim.invoice.status, "confirming");
    assertEquals(onTimeClaim.eventJws, undefined);
    rpc.setHeads(107n, 104n, null);
    assert((await service.refreshInvoice(onTime.id)).eventJws);
    const idempotentOpen = await service.createInvoice(context("on-time-retry"), {
      customerReference: "on-time-customer",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    assert(idempotentOpen.id !== onTime.id);
  });
});

test("latest depth stays confirming, safe accepts once, and actual heads settle and finalize", async () => {
  await withService(async ({ service, rpc, kv }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    const invoice = await service.createInvoice(context("lifecycle"), {
      customerReference: "customer-lifecycle",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    const record = await service.getInvoiceRecord(invoice.id);
    rpc.addTransfer(record, {
      transactionHash: hash(20),
      blockNumber: 100n,
      includedAt: new Date("2026-07-21T12:10:00.000Z"),
    });
    rpc.setHeads(102n, null, null);
    const confirming = await service.claimPayment(invoice.id, claim(hash(20), 100n));
    assertEquals(confirming.invoice.status, "confirming");
    assertEquals(confirming.eventJws, undefined);

    const confirmingVersion = (
      await kv.get(["uos_checkout", "v1", "payments", "invoices", invoice.id])
    ).versionstamp;
    rpc.setHeads(103n, null, null);
    const sealed = await service.refreshInvoice(invoice.id);
    assertEquals(sealed.invoice.status, "confirming");
    assertEquals(sealed.eventJws, undefined);
    assertEquals(
      (await kv.get(["uos_checkout", "v1", "payments", "invoices", invoice.id])).versionstamp,
      confirmingVersion,
    );
    assertEquals((await service.listOutbox()).length, 0);
    rpc.setHeads(103n, 100n, null);
    const accepted = await service.refreshInvoice(invoice.id);
    assertEquals(accepted.invoice.status, "provisionally_paid");
    assert(accepted.eventJws);
    const acceptedPayload = (await verifyCompactJws(accepted.eventJws!, await service.jwks()))
      .payload as Record<string, unknown>;
    assertEquals(acceptedPayload.type, "payment.accepted.v1");
    assertEquals(acceptedPayload.aud, MERCHANT);
    assertEquals(acceptedPayload.merchantId, MERCHANT);
    assertEquals(acceptedPayload.customerReference, "customer-lifecycle");
    assertEquals(acceptedPayload.logIndex, "0");
    assertEquals(acceptedPayload.blockNumber, "100");
    assertEquals(acceptedPayload.paidAmountUnits, invoice.paidAmountUnits);
    assertEquals("reason" in acceptedPayload, false);
    assertEquals(Object.keys(acceptedPayload).sort(), [
      "aud",
      "blockHash",
      "blockNumber",
      "catalogDigest",
      "chainId",
      "customerReference",
      "discountUnits",
      "exp",
      "iat",
      "includedAt",
      "invoiceId",
      "iss",
      "jti",
      "listAmountUnits",
      "logIndex",
      "merchantId",
      "occurredAt",
      "paidAmountUnits",
      "payer",
      "pricingVersionId",
      "productId",
      "tokenAddress",
      "tokenDecimals",
      "treasuryAddress",
      "txHash",
      "type",
    ]);

    rpc.setHeads(101n, null, null);
    assertEquals((await service.refreshInvoice(invoice.id)).invoice.status, "provisionally_paid");
    const recoveredHeads = (
      await kv.get<{ latestBlock: bigint }>(["uos_checkout", "v1", "payments", "chain_heads", 8453])
    ).value;
    assertEquals(recoveredHeads?.latestBlock, 101n);
    rpc.setHeads(101n, 100n, null);
    const settled = await service.refreshInvoice(invoice.id);
    assertEquals(settled.invoice.status, "settled");
    assert(settled.eventJws);
    rpc.setHeads(100n, 100n, 100n);
    assertEquals((await service.refreshInvoice(invoice.id)).invoice.status, "finalized");
    assertEquals((await service.listOutbox()).length, 2);
  });
});

test("reorg before provisional returns to awaiting; reorg after provisional emits quarantine event", async () => {
  await withService(async ({ service, rpc }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    const before = await service.createInvoice(context("before-reorg"), {
      customerReference: "before-reorg",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    rpc.addTransfer(await service.getInvoiceRecord(before.id), {
      transactionHash: hash(30),
      blockNumber: 100n,
      includedAt: new Date("2026-07-21T12:05:00.000Z"),
    });
    rpc.setHeads(102n, null, null);
    assertEquals(
      (await service.claimPayment(before.id, claim(hash(30), 100n))).invoice.status,
      "confirming",
    );
    rpc.reorgTransfer(hash(30));
    assertEquals((await service.refreshInvoice(before.id)).invoice.status, "awaiting_payment");

    const after = await service.createInvoice(context("after-reorg"), {
      customerReference: "after-reorg",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    rpc.addTransfer(await service.getInvoiceRecord(after.id), {
      transactionHash: hash(31),
      blockNumber: 103n,
      includedAt: new Date("2026-07-21T12:06:00.000Z"),
    });
    rpc.setHeads(106n, null, null);
    await service.claimPayment(after.id, claim(hash(31), 103n));
    rpc.setHeads(106n, 103n, null);
    const accepted = await service.refreshInvoice(after.id);
    assertEquals(accepted.invoice.status, "provisionally_paid");
    rpc.hideReceipt(hash(31));
    const incomplete = await service.refreshInvoice(after.id);
    assertEquals(incomplete.invoice.status, "provisionally_paid");
    assertEquals(incomplete.eventJws, undefined);
    rpc.reorgTransfer(hash(31));
    const reverted = await service.refreshInvoice(after.id);
    assertEquals(reverted.invoice.status, "reverted");
    assert(reverted.eventJws);
    const payload = (await verifyCompactJws(reverted.eventJws!, await service.jwks()))
      .payload as Record<string, unknown>;
    assertEquals(payload.type, "payment.reverted.v1");
    assertEquals(payload.reason, "non_canonical_block");
  });
});

test("claim replay and a second exact transfer never emit a second acceptance", async () => {
  await withService(async ({ service, rpc }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    const invoice = await service.createInvoice(context("replay"), {
      customerReference: "replay-customer",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    const record = await service.getInvoiceRecord(invoice.id);
    rpc.addTransfer(record, {
      transactionHash: hash(40),
      blockNumber: 100n,
      includedAt: new Date("2026-07-21T12:05:00.000Z"),
    });
    rpc.setHeads(103n, null, null);
    const seen = await service.claimPayment(invoice.id, claim(hash(40), 100n));
    assertEquals(seen.invoice.status, "confirming");
    assertEquals(seen.eventJws, undefined);
    rpc.setHeads(103n, null, 100n);
    const first = await service.refreshInvoice(invoice.id);
    assert(first.eventJws);
    rpc.setHeads(103n, null, null);
    const replay = await service.claimPayment(invoice.id, claim(hash(40), 100n));
    assert(replay.eventJws);
    const firstPayload = (await verifyCompactJws(first.eventJws!, await service.jwks()))
      .payload as Record<string, unknown>;
    const replayPayload = (await verifyCompactJws(replay.eventJws!, await service.jwks()))
      .payload as Record<string, unknown>;
    assertEquals(replayPayload.jti, firstPayload.jti);

    rpc.addTransfer(record, {
      transactionHash: hash(41),
      blockNumber: 101n,
      includedAt: new Date("2026-07-21T12:06:00.000Z"),
    });
    const duplicate = await service.claimPayment(invoice.id, claim(hash(41), 101n));
    assertEquals(duplicate.invoice.status, "provisionally_paid");
    assertEquals(duplicate.invoice.requiresManualReview, true);
    assertEquals((await service.listOutbox()).length, 1);
    rpc.setHeads(104n, 101n, 100n);
    assertEquals((await service.refreshInvoice(invoice.id)).invoice.status, "settled");
  });
});

test("a reverted locator can reappear canonically and emit a new acceptance lifecycle", async () => {
  await withService(async ({ service, rpc }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    const invoice = await service.createInvoice(context("canonical-reappearance"), {
      customerReference: "canonical-reappearance",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    const transactionHash = hash(42);
    rpc.addTransfer(await service.getInvoiceRecord(invoice.id), {
      transactionHash,
      blockNumber: 100n,
      includedAt: new Date("2026-07-21T12:05:00.000Z"),
    });
    rpc.setHeads(103n, null, null);
    await service.claimPayment(invoice.id, claim(transactionHash, 100n));
    rpc.setHeads(103n, 100n, null);
    const first = await service.refreshInvoice(invoice.id);
    const firstClaims = (await verifyCompactJws(first.eventJws!, await service.jwks()))
      .payload as Record<string, unknown>;

    rpc.reorgTransfer(transactionHash);
    const reverted = await service.refreshInvoice(invoice.id);
    assertEquals(reverted.invoice.status, "reverted");

    rpc.reincludeTransfer(transactionHash, 101n);
    rpc.setHeads(104n, 101n, null);
    const watch = await service.reconcileHourly(8453, 104n);
    assertEquals(
      watch.invoices.map((candidate) => candidate.id),
      [invoice.id],
    );
    assertEquals((await service.getInvoice(invoice.id)).status, "provisionally_paid");
    const reaccepted = await service.claimPayment(invoice.id, claim(transactionHash, 101n));
    const reacceptedClaims = (await verifyCompactJws(reaccepted.eventJws!, await service.jwks()))
      .payload as Record<string, unknown>;
    assert(reacceptedClaims.jti !== firstClaims.jti);
    assertEquals(reacceptedClaims.invoiceId, firstClaims.invoiceId);
    assertEquals(reacceptedClaims.txHash, firstClaims.txHash);
    assertEquals(reacceptedClaims.logIndex, firstClaims.logIndex);
    assertEquals(reacceptedClaims.blockNumber, "101");

    rpc.setHeads(104n, 101n, 100n);
    assertEquals((await service.refreshInvoice(invoice.id)).invoice.status, "settled");
  });
});

test("reconciliation cursor advances only after a complete worker range", async () => {
  await withService(async ({ service, kv }) => {
    let attempts = 0;
    await assertRejects(() =>
      service.reconcileChainRange(8453, 200n, () => {
        attempts++;
        return Promise.reject(new WorkerBudgetError());
      }),
    );
    assertEquals(
      (await kv.get(["uos_checkout", "v1", "payments", "reconciliation_cursors", 8453])).value,
      null,
    );
    const watch = await service.reconcileChainRange(8453, 200n, () => {
      attempts++;
      return Promise.resolve();
    });
    assertEquals(watch.toBlock, 200n);
    const next = await service.reconcileChainRange(8453, 205n, (range) => {
      assertEquals(range.fromBlock, 205n);
      return Promise.resolve();
    });
    assertEquals(next.fromBlock, 205n);
    assertEquals(attempts, 2);
  });
});

test("hourly reconciliation discovers matching treasury transfers and claims them", async () => {
  await withService(async ({ service, rpc }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    const invoice = await service.createInvoice(context("hourly"), {
      customerReference: "hourly-customer",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    rpc.addTransfer(await service.getInvoiceRecord(invoice.id), {
      transactionHash: hash(50),
      blockNumber: 100n,
      includedAt: new Date("2026-07-21T12:10:00.000Z"),
    });
    rpc.setHeads(103n, null, null);
    const range = await service.reconcileHourly(8453, 103n);
    assertEquals(range.fromBlock, 100n);
    assertEquals((await service.getInvoice(invoice.id)).status, "confirming");
    assertEquals((await service.listOutbox()).length, 0);
  });
});

test("hourly reconciliation scans only through the coherent safe head", async () => {
  await withService(async ({ service, rpc }) => {
    rpc.setHeads(200n, 190n, 180n);
    const watch = await service.reconcileHourly(8453);
    assertEquals(watch.fromBlock, 190n);
    assertEquals(watch.toBlock, 190n);
  });
});

test("outbox retries after JWS expiry keep stable event identity and isolate poison entries", async () => {
  await withService(async ({ service, rpc, clock }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    for (const [index, transactionHash] of [hash(60), hash(61)].entries()) {
      const invoice = await service.createInvoice(context(`outbox-${index}`), {
        customerReference: `outbox-${index}`,
        pricingVersionId: "prices",
        productId: "pack",
        chainId: 8453,
      });
      const blockNumber = index === 0 ? 100n : 104n;
      rpc.addTransfer(await service.getInvoiceRecord(invoice.id), {
        transactionHash,
        blockNumber,
        includedAt: clock.now(),
      });
      rpc.setHeads(blockNumber + 3n, null, null);
      await service.claimPayment(invoice.id, claim(transactionHash, blockNumber));
      rpc.setHeads(blockNumber + 3n, blockNumber, null);
      await service.refreshInvoice(invoice.id);
    }

    const initial = await service.listOutbox();
    assertEquals(initial.length, 2);
    const originalClaims = (await verifyCompactJws(initial[0].compactJws, await service.jwks()))
      .payload as Record<string, unknown>;
    await service.markOutboxFailed(initial[0].id, "merchant outage");
    assertEquals(
      (await service.listOutbox()).map((event) => event.id),
      [initial[1].id],
    );

    clock.advance(25 * 60 * 60 * 1000);
    const retried = (await service.listOutbox()).find((event) => event.id === initial[0].id);
    assert(retried);
    const retryClaims = (await verifyCompactJws(retried.compactJws, await service.jwks()))
      .payload as Record<string, unknown>;
    assertEquals(retryClaims.jti, originalClaims.jti);
    assert(Number(retryClaims.iat) > Number(originalClaims.iat));
    assert(Number(retryClaims.exp) > Number(originalClaims.exp));
    assertEquals(retried.deliveryAttempts, 1);
  });
});

test("reconciliation overlaps the cursor and finds a transfer introduced behind it", async () => {
  await withService(async ({ service, rpc }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    const invoice = await service.createInvoice(context("overlap"), {
      customerReference: "overlap",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    await service.reconcileHourly(8453, 103n);
    rpc.addTransfer(await service.getInvoiceRecord(invoice.id), {
      transactionHash: hash(62),
      blockNumber: 102n,
      includedAt: new Date("2026-07-21T12:10:00.000Z"),
    });
    rpc.setHeads(105n, 104n, 103n);
    const watch = await service.reconcileHourly(8453, 105n);
    assertEquals(watch.fromBlock, 100n);
    assertEquals((await service.getInvoice(invoice.id)).status, "provisionally_paid");
  });
});

test("disabled chains drain outstanding invoices and terminal invoices leave the active index", async () => {
  await withService(async ({ service, rpc, kv }) => {
    await service.publishCatalogVersion(
      { merchantId: MERCHANT },
      "prices",
      catalog([product("pack", 100)]),
    );
    const invoice = await service.createInvoice(context("disabled-drain"), {
      customerReference: "disabled-drain",
      pricingVersionId: "prices",
      productId: "pack",
      chainId: 8453,
    });
    await service.configureChain(8453, { enabled: false, treasuryAddress: TREASURY });
    await assertDomainError(
      () =>
        service.createInvoice(context("disabled-new"), {
          customerReference: "disabled-new",
          pricingVersionId: "prices",
          productId: "pack",
          chainId: 8453,
        }),
      "chain_disabled",
    );
    rpc.addTransfer(await service.getInvoiceRecord(invoice.id), {
      transactionHash: hash(63),
      blockNumber: 100n,
      includedAt: new Date("2026-07-21T12:10:00.000Z"),
    });
    rpc.setHeads(103n, 100n, 100n);
    await service.reconcileHourly(8453, 103n);
    await service.refreshInvoice(invoice.id);
    await service.refreshInvoice(invoice.id);
    await service.refreshInvoice(invoice.id);
    assertEquals((await service.getInvoice(invoice.id)).status, "finalized");
    const active = [];
    for await (const entry of kv.list({
      prefix: ["uos_checkout", "v1", "payments", "invoices_by_chain", 8453],
    }))
      active.push(entry);
    assertEquals(active.length, 0);
  });
});

test("treasury rejects zero and token addresses and events target the invoice merchant", async () => {
  await withService(async ({ service, rpc }) => {
    await assertDomainError(
      () =>
        service.configureChain(8453, {
          enabled: true,
          treasuryAddress: "0x0000000000000000000000000000000000000000",
        }),
      "invalid_treasury_address",
    );
    await assertDomainError(
      () =>
        service.configureChain(8453, {
          enabled: true,
          treasuryAddress: "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        }),
      "invalid_treasury_address",
    );
    const merchant = "another-merchant";
    await service.publishCatalogVersion(
      { merchantId: merchant },
      "prices",
      catalog([product("pack", 100)]),
    );
    const invoice = await service.createInvoice(
      { merchantId: merchant, idempotencyKey: "aud" },
      {
        customerReference: "audience",
        pricingVersionId: "prices",
        productId: "pack",
        chainId: 8453,
      },
    );
    rpc.addTransfer(await service.getInvoiceRecord(invoice.id), {
      transactionHash: hash(64),
      blockNumber: 100n,
      includedAt: new Date("2026-07-21T12:10:00.000Z"),
    });
    rpc.setHeads(103n, null, null);
    await service.claimPayment(invoice.id, claim(hash(64), 100n));
    rpc.setHeads(103n, 100n, null);
    const accepted = await service.refreshInvoice(invoice.id);
    const payload = (await verifyCompactJws(accepted.eventJws!, await service.jwks()))
      .payload as Record<string, unknown>;
    assertEquals(payload.aud, merchant);
    assertEquals(payload.merchantId, merchant);
  });
});

async function withService(
  fn: (context: {
    service: PaymentService;
    rpc: MockRpc;
    clock: MutableClock;
    kv: Kv;
  }) => Promise<void>,
  options: { discountStart?: () => bigint } = {},
): Promise<void> {
  const kv = new SqliteKv(":memory:");
  const clock = new MutableClock("2026-07-21T12:00:00.000Z");
  const rpc = new MockRpc();
  let id = 0;
  const signer = createPersistentJwsSigner(kv, clock);
  const service = createPaymentService({
    kv,
    rpc,
    signer,
    clock,
    idGenerator: { next: () => `test-${++id}` },
    discountStart: options.discountStart ?? (() => 1n),
  });
  await service.configureChain(8453, { enabled: true, treasuryAddress: TREASURY });
  try {
    await fn({ service, rpc, clock, kv });
  } finally {
    kv.close();
  }
}

class MutableClock {
  #now: number;
  constructor(value: string) {
    this.#now = Date.parse(value);
  }
  now(): Date {
    return new Date(this.#now);
  }
  advance(milliseconds: number): void {
    this.#now += milliseconds;
  }
}

type MockTransfer = {
  transactionHash: string;
  logIndex: number;
  blockNumber: bigint;
  blockHash: string;
  includedAt: Date;
  tokenAddress: string;
  treasuryAddress: string;
  amountUnits: bigint;
};

class MockRpc implements RpcClient {
  #transfers = new Map<string, MockTransfer>();
  #missingReceipts = new Set<string>();
  #canonicalOverrides = new Map<string, string>();
  #latest = 99n;
  #safe: bigint | null = null;
  #finalized: bigint | null = null;

  setHeads(latest: bigint, safe: bigint | null, finalized: bigint | null): void {
    this.#latest = latest;
    this.#safe = safe;
    this.#finalized = finalized;
  }

  addTransfer(
    invoice: InvoiceRecord,
    input: {
      transactionHash: string;
      blockNumber: bigint;
      includedAt: Date;
      logIndex?: number;
    },
  ): void {
    this.#transfers.set(input.transactionHash.toLowerCase(), {
      transactionHash: input.transactionHash.toLowerCase(),
      logIndex: input.logIndex ?? 0,
      blockNumber: input.blockNumber,
      blockHash: hash(Number(input.blockNumber) + 1000),
      includedAt: input.includedAt,
      tokenAddress: invoice.tokenAddress,
      treasuryAddress: invoice.treasuryAddress,
      amountUnits: invoice.paidAmountUnits,
    });
  }

  hideReceipt(transactionHash: string): void {
    this.#missingReceipts.add(transactionHash.toLowerCase());
  }

  reorgTransfer(transactionHash: string): void {
    this.#canonicalOverrides.set(transactionHash.toLowerCase(), hash(90_000));
  }

  reincludeTransfer(transactionHash: string, blockNumber: bigint): void {
    const key = transactionHash.toLowerCase();
    const transfer = this.#transfers.get(key);
    if (!transfer) throw new Error("missing mock transfer");
    transfer.blockNumber = blockNumber;
    transfer.blockHash = hash(Number(blockNumber) + 5_000);
    this.#canonicalOverrides.delete(key);
    this.#missingReceipts.delete(key);
  }

  batch(_chainId: 1 | 10 | 8453, calls: readonly JsonRpcCall[]): Promise<JsonRpcBatchItem[]> {
    if (calls.length === 1 && calls[0].id === "transfer_logs") {
      const filter = calls[0].params[0] as {
        fromBlock: string;
        toBlock: string;
        topics: [string, null, string];
      };
      const fromBlock = BigInt(filter.fromBlock);
      const toBlock = BigInt(filter.toBlock);
      const logs = [...this.#transfers.values()]
        .filter(
          (transfer) =>
            transfer.blockNumber >= fromBlock &&
            transfer.blockNumber <= toBlock &&
            addressTopic(transfer.treasuryAddress) === filter.topics[2],
        )
        .map(rpcLog);
      return Promise.resolve([{ jsonrpc: "2.0", id: "transfer_logs", result: logs }]);
    }
    const receiptCall = calls.find((call) => call.id === "receipt");
    const transactionHash = receiptCall ? String(receiptCall.params[0]).toLowerCase() : "";
    const transfer = this.#transfers.get(transactionHash);
    const results: Record<string, unknown> = {
      receipt: transfer && !this.#missingReceipts.has(transactionHash) ? receipt(transfer) : null,
      event_block: transfer
        ? block(
            transfer.blockNumber,
            this.#canonicalOverrides.get(transactionHash) ?? transfer.blockHash,
            transfer.includedAt,
          )
        : null,
      latest: block(this.#latest, hash(Number(this.#latest) + 2000)),
      safe: this.#safe === null ? null : block(this.#safe, hash(Number(this.#safe) + 3000)),
      finalized:
        this.#finalized === null
          ? null
          : block(this.#finalized, hash(Number(this.#finalized) + 4000)),
    };
    return Promise.resolve(
      calls.map((call) => ({ jsonrpc: "2.0", id: call.id, result: results[call.id] })),
    );
  }
}

function receipt(transfer: MockTransfer) {
  return {
    transactionHash: transfer.transactionHash,
    blockNumber: quantity(transfer.blockNumber),
    blockHash: transfer.blockHash,
    status: "0x1",
    logs: [rpcLog(transfer)],
  };
}

function rpcLog(transfer: MockTransfer) {
  return {
    address: transfer.tokenAddress,
    transactionHash: transfer.transactionHash,
    blockNumber: quantity(transfer.blockNumber),
    blockHash: transfer.blockHash,
    logIndex: quantity(BigInt(transfer.logIndex)),
    removed: false,
    topics: [TRANSFER_TOPIC, addressTopic(PAYER), addressTopic(transfer.treasuryAddress)],
    data: word(transfer.amountUnits),
  };
}

function block(number: bigint, blockHash: string, timestamp = new Date("2026-07-21T12:00:00Z")) {
  return {
    number: quantity(number),
    hash: blockHash,
    timestamp: quantity(BigInt(Math.floor(timestamp.getTime() / 1000))),
  };
}

function context(key: string): InvoiceCreationContext {
  return { merchantId: MERCHANT, idempotencyKey: key };
}

function catalog(products: ReturnType<typeof product>[]) {
  return { currency: "USD" as const, products };
}

function product(productId: string, priceCents: number) {
  return {
    productId,
    label: productId,
    kind: "credit_pack" as const,
    priceCents,
    enabled: true,
  };
}

function claim(transactionHash: string, blockNumber: bigint) {
  return {
    transactionHash,
    logIndex: "0",
    observedBlockNumber: blockNumber.toString(),
    observedBlockHash: hash(Number(blockNumber) + 9999),
  };
}

function hash(seed: number): string {
  return `0x${seed.toString(16).padStart(64, "0")}`;
}

function quantity(value: bigint): string {
  return `0x${value.toString(16)}`;
}

function addressTopic(address: string): string {
  return `0x${"0".repeat(24)}${address.slice(2).toLowerCase()}`;
}

function word(value: bigint): string {
  return `0x${value.toString(16).padStart(64, "0")}`;
}

async function assertDomainError(fn: () => Promise<unknown>, code: string): Promise<void> {
  try {
    await fn();
  } catch (error) {
    if (error instanceof PaymentDomainError && error.code === code) return;
    throw new Error(`Expected ${code}, got ${String(error)}`, { cause: error });
  }
  throw new Error(`Expected rejection ${code}`);
}

async function assertRejects(fn: () => Promise<unknown>): Promise<void> {
  try {
    await fn();
  } catch {
    return;
  }
  throw new Error("Expected promise rejection");
}

function assert(value: unknown): asserts value {
  if (!value) throw new Error("Expected value to be truthy");
}

function assertEquals(actual: unknown, expected: unknown): void {
  if (!deepEqual(actual, expected)) {
    throw new Error(`Expected ${inspect(expected)}, got ${inspect(actual)}`);
  }
}

function deepEqual(actual: unknown, expected: unknown): boolean {
  if (typeof actual === "bigint" || typeof expected === "bigint") return actual === expected;
  return JSON.stringify(actual) === JSON.stringify(expected);
}
