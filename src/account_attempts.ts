import type { Kv, KvEntry, KvKey, KvPage, KvPageOptions } from "./kv.ts";
import {
  normalizeAddress,
  PaymentDomainError,
  SUPPORTED_CHAIN_IDS,
  type InvoiceRecord,
  type SupportedChainId,
} from "./payment_types.ts";

/** The stored customer reference is opaque and belongs to the authenticated merchant. */
export type AccountKey = readonly [merchantId: string, customerReference: string];
export type AttemptResolution = "unresolved" | "credited" | "invalidated";

/** An index reference, never authority to claim, credit, invalidate or release an amount. */
export type AccountAttemptReference = Readonly<{
  schemaVersion: 1;
  accountHash: string;
  invoiceId: string;
  createdAt: string;
}>;

export type AmountFingerprint = Readonly<{
  chainId: SupportedChainId;
  tokenAddress: string;
  treasuryAddress: string;
  amountUnits: bigint;
}>;

/** Historical ownership survives UI expiry and does not establish current resolution. */
export type AttemptOwnership = AccountAttemptReference &
  AmountFingerprint &
  Readonly<{
    minimumBlock: bigint;
  }>;

/** Insert immutable descriptors with an absent-entry CAS; different existing values conflict. */
export type AttemptDescriptor<T> = Readonly<{ key: KvKey; value: T }>;

const PREFIX: KvKey = ["uos_checkout", "v1", "payments"];

export function accountKeyForInvoice(
  invoice: Pick<InvoiceRecord, "merchant" | "customerReference">,
  authenticatedMerchantId: string,
): AccountKey {
  if (invoice.merchant !== authenticatedMerchantId) {
    throw new PaymentDomainError("account_merchant_mismatch");
  }
  const account = [invoice.merchant, invoice.customerReference] as const;
  validateAccount(account);
  return Object.freeze(account);
}

export async function accountHash(account: AccountKey): Promise<string> {
  validateAccount(account);
  return digest("uos_checkout.account.v1", account);
}

/** Migration classification only: expiry and confirming/reverted states remain unresolved. */
export function legacyAttemptResolution(invoice: Pick<InvoiceRecord, "status">): AttemptResolution {
  return ["provisionally_paid", "settled", "finalized"].includes(invoice.status)
    ? "credited"
    : "unresolved";
}

export async function accountAttemptDescriptor(
  invoice: InvoiceRecord,
  authenticatedMerchantId: string,
): Promise<AttemptDescriptor<AccountAttemptReference>> {
  const owner = await accountHash(accountKeyForInvoice(invoice, authenticatedMerchantId));
  const value = reference(invoice, owner);
  return descriptor([...accountPrefix(owner), chronologicalId(value)], value);
}

export async function attemptOwnershipDescriptor(
  invoice: InvoiceRecord,
  authenticatedMerchantId: string,
): Promise<AttemptDescriptor<AttemptOwnership>> {
  const owner = await accountHash(accountKeyForInvoice(invoice, authenticatedMerchantId));
  const fingerprint = normalizedFingerprint({
    chainId: invoice.chainId,
    tokenAddress: invoice.tokenAddress,
    treasuryAddress: invoice.treasuryAddress,
    amountUnits: invoice.paidAmountUnits,
  });
  if (typeof invoice.minimumBlock !== "bigint" || invoice.minimumBlock < 0n) {
    throw new PaymentDomainError("invalid_attempt_minimum_block");
  }
  const value = Object.freeze({
    ...reference(invoice, owner),
    ...fingerprint,
    minimumBlock: invoice.minimumBlock,
  });
  // Every account/invoice gets its own immutable entry, including legacy amount reuse.
  return descriptor(
    [...(await ownershipPrefix(fingerprint)), `${owner}:${encodedId(invoice.id)}`],
    value,
  );
}

/** One raw page only. Re-read and authorize each referenced invoice before domain work. */
export async function listAccountAttemptPage(
  kv: Kv,
  account: AccountKey,
  options: KvPageOptions,
): Promise<KvPage<AccountAttemptReference>> {
  const owner = await accountHash(account);
  const prefix = accountPrefix(owner);
  const page = await kv.listPage<AccountAttemptReference>(prefix, options);
  return {
    entries: page.entries.filter((entry) => {
      const value = entry.value;
      return (
        validReference(value) &&
        value.accountHash === owner &&
        exactKey(entry, [...prefix, chronologicalId(value)])
      );
    }),
    // Empty filtered pages still carry the backend's exact continuation.
    cursor: page.cursor,
  };
}

/** Global historical owners, not live reservations or permission to reuse an amount. */
export async function listAmountOwnershipPage(
  kv: Kv,
  fingerprintInput: AmountFingerprint,
  options: KvPageOptions,
): Promise<KvPage<AttemptOwnership>> {
  const fingerprint = normalizedFingerprint(fingerprintInput);
  const prefix = await ownershipPrefix(fingerprint);
  const page = await kv.listPage<AttemptOwnership>(prefix, options);
  return {
    entries: page.entries.filter((entry) => {
      const value = entry.value;
      return (
        validReference(value) &&
        value.chainId === fingerprint.chainId &&
        value.tokenAddress === fingerprint.tokenAddress &&
        value.treasuryAddress === fingerprint.treasuryAddress &&
        value.amountUnits === fingerprint.amountUnits &&
        typeof value.minimumBlock === "bigint" &&
        value.minimumBlock >= 0n &&
        exactKey(entry, [...prefix, `${value.accountHash}:${encodedId(value.invoiceId)}`])
      );
    }),
    cursor: page.cursor,
  };
}

function validateAccount(account: AccountKey): void {
  if (
    !Array.isArray(account) ||
    account.length !== 2 ||
    account.some((part) => typeof part !== "string" || !part)
  ) {
    throw new PaymentDomainError("invalid_account_key");
  }
}

function reference(invoice: InvoiceRecord, owner: string): AccountAttemptReference {
  const value = Object.freeze({
    schemaVersion: 1 as const,
    accountHash: owner,
    invoiceId: invoice.id,
    createdAt: invoice.createdAt,
  });
  if (!validReference(value)) throw new PaymentDomainError("invalid_attempt_reference");
  return value;
}

function validReference(value: AccountAttemptReference): boolean {
  return (
    !!value &&
    value.schemaVersion === 1 &&
    typeof value.accountHash === "string" &&
    /^[0-9a-f]{64}$/.test(value.accountHash) &&
    typeof value.invoiceId === "string" &&
    !!value.invoiceId &&
    typeof value.createdAt === "string" &&
    Number.isSafeInteger(Date.parse(value.createdAt)) &&
    Date.parse(value.createdAt) >= 0
  );
}

function normalizedFingerprint(value: AmountFingerprint): AmountFingerprint {
  if (
    !SUPPORTED_CHAIN_IDS.includes(value.chainId) ||
    typeof value.amountUnits !== "bigint" ||
    value.amountUnits < 0n
  )
    throw new PaymentDomainError("invalid_amount_fingerprint");
  return Object.freeze({
    chainId: value.chainId,
    tokenAddress: normalizeAddress(value.tokenAddress),
    treasuryAddress: normalizeAddress(value.treasuryAddress),
    amountUnits: value.amountUnits,
  });
}

function accountPrefix(owner: string): KvKey {
  return [...PREFIX, "account_attempts", owner];
}

async function ownershipPrefix(fingerprint: AmountFingerprint): Promise<KvKey> {
  return [...PREFIX, "amount_ownership", await amountOwnershipHash(fingerprint)];
}

export async function amountOwnershipHash(input: AmountFingerprint): Promise<string> {
  const fingerprint = normalizedFingerprint(input);
  return digest("uos_checkout.amount.v1", [
    fingerprint.chainId,
    fingerprint.tokenAddress,
    fingerprint.treasuryAddress,
    fingerprint.amountUnits.toString(),
  ]);
}

function chronologicalId(value: AccountAttemptReference): string {
  // Fixed-width time and terminated hex preserve raw JSON-key order for variable-length IDs.
  // Equal timestamps have a deterministic ID tie-break, never a causal creation order.
  return `${Date.parse(value.createdAt).toString().padStart(16, "0")}:${encodedId(value.invoiceId)}`;
}

function encodedId(value: string): string {
  let encoded = "";
  for (let index = 0; index < value.length; index++) {
    encoded += value.charCodeAt(index).toString(16).padStart(4, "0");
  }
  return `${encoded}!`;
}

function exactKey(entry: KvEntry<unknown>, expected: KvKey): boolean {
  return (
    entry.key.length === expected.length &&
    expected.every((part, index) => entry.key[index] === part)
  );
}

function descriptor<T>(key: KvKey, value: T): AttemptDescriptor<T> {
  return Object.freeze({ key: Object.freeze(key), value });
}

async function digest(domain: string, value: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(`${domain}\0${JSON.stringify(value)}`);
  const hash = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(hash), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
