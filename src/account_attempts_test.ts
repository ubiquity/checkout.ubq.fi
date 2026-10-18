import { test } from "bun:test";
import { deepStrictEqual as equal, ok, rejects, throws } from "node:assert/strict";
import {
  accountAttemptDescriptor,
  accountHash,
  accountKeyForInvoice,
  attemptOwnershipDescriptor,
  legacyAttemptResolution,
  listAccountAttemptPage,
  listAmountOwnershipPage,
} from "./account_attempts.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { UbqKv } from "./ubq_kv.ts";
import { encodeKvValue } from "./kv_codec.ts";
import type { KvKey } from "./kv.ts";
import type { InvoiceRecord, InvoiceStatus } from "./payment_types.ts";

test("account identity binds the authenticated merchant and preserves opaque account references", async () => {
  const invoice = attempt();
  const account = accountKeyForInvoice(invoice, "merchant");
  equal(account, ["merchant", "workspace:reference"]);
  ok(Object.isFrozen(account));
  throws(() => accountKeyForInvoice(invoice, "other"), /account_merchant_mismatch/);
  await rejects(() => accountAttemptDescriptor(invoice, "other"), /account_merchant_mismatch/);
  const hash = await accountHash(account);
  equal(hash, await accountHash(["merchant", "workspace:reference"]));
  ok(/^[0-9a-f]{64}$/.test(hash));
  for (const other of [
    ["other", "workspace:reference"],
    ["merchant", "workspace:other"],
    ["merchant", " workspace:reference"],
    ["merchant:workspace", "reference"],
  ] as const)
    ok(hash !== (await accountHash(other)));
  ok((await accountHash(["a", "bc"])) !== (await accountHash(["ab", "c"])));
  await rejects(() => accountHash(["merchant", ""]), /invalid_account_key/);
});

test("legacy UI expiry and confirming or reverted payments do not resolve an attempt", () => {
  for (const status of [
    "awaiting_payment",
    "expired",
    "confirming",
    "reverted",
    "late_payment",
    "manual_review",
  ] as InvoiceStatus[])
    equal(legacyAttemptResolution({ status }), "unresolved");
  for (const status of ["provisionally_paid", "settled", "finalized"] as const) {
    equal(legacyAttemptResolution({ status }), "credited");
  }
});

test("account index pages are chronological with deterministic variable-length ID ties", async () => {
  const kv = new SqliteKv();
  try {
    const snapshots = [
      attempt({ id: "z", createdAt: "2026-10-18T00:00:00.001Z" }),
      ...["a0", "b", "aa", "a", "a\0"].map((id) => attempt({ id })),
      attempt({ id: "older", createdAt: "2026-10-17T23:59:59.999Z" }),
    ];
    for (const invoice of snapshots) {
      const item = await accountAttemptDescriptor(invoice, invoice.merchant);
      await kv.set(item.key, item.value);
    }
    const foreign = await accountAttemptDescriptor(attempt({ merchant: "other" }), "other");
    await kv.set(foreign.key, foreign.value);
    const otherAccount = await accountAttemptDescriptor(
      attempt({ customerReference: "other" }),
      "merchant",
    );
    await kv.set(otherAccount.key, otherAccount.value);
    const ids: string[] = [];
    let cursor: string | null = null;
    do {
      const page = await listAccountAttemptPage(kv, ["merchant", "workspace:reference"], {
        limit: 2,
        cursor,
      });
      ok(page.entries.length <= 2);
      ids.push(...page.entries.map((entry) => entry.value.invoiceId));
      cursor = page.cursor;
    } while (cursor !== null);
    equal(ids, ["older", "a", "a\0", "a0", "aa", "b", "z"]);
  } finally {
    kv.close();
  }
});

test("descriptors are immutable and idempotent without writing or overwriting shared history", async () => {
  const invoice = attempt();
  const index = await accountAttemptDescriptor(invoice, invoice.merchant);
  const owner = await attemptOwnershipDescriptor(invoice, invoice.merchant);
  equal(index, await accountAttemptDescriptor({ ...invoice }, invoice.merchant));
  equal(
    owner,
    await attemptOwnershipDescriptor({ ...invoice, status: "expired" }, invoice.merchant),
  );
  for (const item of [index, owner]) {
    ok(Object.isFrozen(item));
    ok(Object.isFrozen(item.key));
    ok(Object.isFrozen(item.value));
  }
  equal(invoice.tokenAddress, `0x${"AB".repeat(20)}`);
  equal(owner.value.tokenAddress, invoice.tokenAddress.toLowerCase());
  equal(owner.value.treasuryAddress, invoice.treasuryAddress.toLowerCase());
  equal(owner.value.amountUnits, invoice.paidAmountUnits);
  equal(owner.value.minimumBlock, invoice.minimumBlock);
  await rejects(
    () => accountAttemptDescriptor(attempt({ createdAt: "invalid" }), "merchant"),
    /invalid_attempt_reference/,
  );
});

test("global exact-amount history retains multiple accounts, merchants and legacy owners", async () => {
  const kv = new SqliteKv();
  try {
    const original = attempt();
    const invoices = [
      original,
      attempt({ id: "legacy", status: "finalized" }),
      attempt({ customerReference: "other" }),
      attempt({ merchant: "another" }),
    ];
    for (const invoice of invoices) {
      const item = await attemptOwnershipDescriptor(invoice, invoice.merchant);
      await kv.set(item.key, item.value);
    }
    const fingerprint = {
      chainId: original.chainId,
      tokenAddress: original.tokenAddress,
      treasuryAddress: original.treasuryAddress,
      amountUnits: original.paidAmountUnits,
    };
    const found: string[] = [];
    let cursor: string | null = null;
    do {
      const page = await listAmountOwnershipPage(kv, fingerprint, { limit: 1, cursor });
      found.push(
        ...page.entries.map((entry) => `${entry.value.accountHash}:${entry.value.invoiceId}`),
      );
      cursor = page.cursor;
    } while (cursor !== null);
    equal(found.length, 4);
    equal(new Set(found).size, 4);
    for (const changed of [
      { ...fingerprint, chainId: 1 as const },
      { ...fingerprint, amountUnits: fingerprint.amountUnits + 1n },
      { ...fingerprint, tokenAddress: `0x${"12".repeat(20)}` },
      { ...fingerprint, treasuryAddress: `0x${"34".repeat(20)}` },
    ])
      equal((await listAmountOwnershipPage(kv, changed, { limit: 1 })).entries, []);
  } finally {
    kv.close();
  }
});

test("a backend without the narrow index forwards empty raw pages without extra reads", async () => {
  const invoice = attempt();
  const index = await accountAttemptDescriptor(invoice, "merchant");
  const history = await attemptOwnershipDescriptor(invoice, "merchant");
  for (const kind of ["account", "ownership"] as const) {
    const item = kind === "account" ? index : history;
    const calls: { prefix: KvKey; cursor?: string; limit: number }[] = [];
    const kv = new UbqKv({
      url: "https://kv.invalid",
      token: "offline",
      fetchImpl: async (_url, init) => {
        const body = JSON.parse(String(init.body));
        calls.push(body);
        return Response.json({
          ok: true,
          entries: body.cursor
            ? [
                {
                  key: item.key,
                  value: encodeKvValue(item.value),
                  versionstamp: "1",
                },
              ]
            : [],
          cursor: body.cursor ? null : "opaque-raw-page",
        });
      },
    });
    const page = (cursor?: string | null) =>
      kind === "account"
        ? listAccountAttemptPage(kv, ["merchant", "workspace:reference"], { limit: 1, cursor })
        : listAmountOwnershipPage(kv, history.value, { limit: 1, cursor });
    const first = await page();
    equal(first.entries, []);
    ok(first.cursor);
    equal(calls.length, 1);
    equal(calls[0].prefix, ["uos_checkout", "v1"]);
    equal(calls[0].limit, 1);
    const second = await page(first.cursor);
    equal(calls.length, 2);
    equal(calls[1].cursor, "opaque-raw-page");
    equal(
      second.entries.map((entry) => entry.value),
      [item.value],
    );
    equal(second.cursor, null);
  }
});

test("index references with foreign ownership or forged chronological keys cannot cross accounts", async () => {
  const kv = new SqliteKv();
  try {
    const invoice = attempt();
    const index = await accountAttemptDescriptor(invoice, "merchant");
    await kv.set(index.key, {
      ...index.value,
      accountHash: await accountHash(["foreign", "owner"]),
    });
    await kv.set([...index.key.slice(0, -1), "forged"], index.value);
    equal(
      (await listAccountAttemptPage(kv, ["merchant", "workspace:reference"], { limit: 10 }))
        .entries,
      [],
    );
    await rejects(
      () => listAccountAttemptPage(kv, ["merchant", "workspace:reference"], { limit: 501 }),
      /invalid_kv_page_limit/,
    );
  } finally {
    kv.close();
  }
});

function attempt(changes: Partial<InvoiceRecord> = {}): InvoiceRecord {
  return {
    schemaVersion: 1,
    id: "invoice",
    merchant: "merchant",
    idempotencyKey: "idempotent",
    customerReference: "workspace:reference",
    pricingVersionId: "price",
    catalogDigest: "digest",
    productId: "credits",
    productKind: "credit_pack",
    chainId: 8453,
    networkName: "Base",
    tokenAddress: `0x${"AB".repeat(20)}`,
    tokenSymbol: "USDC",
    tokenDecimals: 6,
    treasuryAddress: `0x${"CD".repeat(20)}`,
    listAmountUnits: 2_000_000n,
    paidAmountUnits: 1_999_999n,
    discountUnits: 1n,
    minimumBlock: 100n,
    status: "awaiting_payment",
    createdAt: "2026-10-18T00:00:00.000Z",
    expiresAt: "2026-10-18T01:00:00.000Z",
    updatedAt: "2026-10-18T00:00:00.000Z",
    ...changes,
  };
}
