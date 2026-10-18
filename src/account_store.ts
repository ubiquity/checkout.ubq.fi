import {
  accountAttemptDescriptor,
  accountHash,
  accountKeyForInvoice,
  attemptOwnershipDescriptor,
  legacyAttemptResolution,
  type AttemptResolution,
  amountOwnershipHash,
  type AccountKey,
  type AccountAttemptReference,
  type AttemptOwnership,
  type AmountFingerprint,
} from "./account_attempts.ts";
import type { Kv, KvAtomic, KvEntryMaybe, KvKey, KvPage, KvPageOptions } from "./kv.ts";
import { encodeKvValue } from "./kv_codec.ts";
import { PaymentDomainError, type InvoiceRecord } from "./payment_types.ts";
import type { WorkerRequestBudget } from "./worker_budget.ts";

const PREFIX = ["uos_checkout", "v1", "payments"];
export const ACCOUNT_INDEX_STATE_KEY: KvKey = [...PREFIX, "account_index_v1"];
export type AccountMembership = { revision: number; sequence: number; count: number };
export type HistoryMembership = { count: number };
const PAGE_SIZE = 64;
const accountPageKey = (owner: string, page: number): KvKey => [
  ...PREFIX,
  "account_pages",
  owner,
  page,
];
export const historyMembershipKey = (hash: string): KvKey => [
  ...PREFIX,
  "ownership_membership",
  hash,
];
export const historyPageKey = (hash: string, page: number): KvKey => [
  ...PREFIX,
  "ownership_pages",
  hash,
  page,
];
export type AttemptState = {
  resolution: AttemptResolution;
  sequence?: number;
  accountHash: string;
};
type IndexState = {
  complete: boolean;
  cursor: string | null;
  page?: { ids: string[]; next: string | null; offset: number };
};

export const accountMembershipKey = (hash: string): KvKey => [
  ...PREFIX,
  "account_membership",
  hash,
];
export const attemptStateKey = (id: string): KvKey => [...PREFIX, "attempt_states", id];
export const storedInvoiceKey = (id: string): KvKey => [...PREFIX, "invoices", id];
export const treasuryHistoryKey = (
  chainId: number,
  tokenAddress: string,
  treasuryAddress: string,
): KvKey => [
  ...PREFIX,
  "treasury_history",
  chainId,
  tokenAddress.toLowerCase(),
  treasuryAddress.toLowerCase(),
];
export function reservationKey(
  invoice: Pick<InvoiceRecord, "chainId" | "tokenAddress" | "treasuryAddress" | "paidAmountUnits">,
): KvKey {
  return [
    ...PREFIX,
    "amount_reservations",
    invoice.chainId,
    invoice.tokenAddress.toLowerCase(),
    invoice.treasuryAddress.toLowerCase(),
    invoice.paidAmountUnits.toString(),
  ];
}
export function locatorKey(chain: number, transaction: string, index: number): KvKey {
  return [...PREFIX, "payment_locators", chain, transaction.toLowerCase(), index];
}

/** Descriptor checks and sequence allocation belong to the same successful invoice CAS. */
export async function prepareAccountAttempt(kv: Kv, invoice: InvoiceRecord, fresh = false) {
  const [reference, history] = await Promise.all([
    accountAttemptDescriptor(invoice, invoice.merchant),
    attemptOwnershipDescriptor(invoice, invoice.merchant),
  ]);
  const owner = await accountHash(accountKeyForInvoice(invoice, invoice.merchant));
  const amountHash = await amountOwnershipHash(history.value);
  const [existingReference, existingHistory, membership, state, historical] = await kv.getMany<
    [
      typeof reference.value,
      typeof history.value,
      AccountMembership,
      AttemptState,
      HistoryMembership,
    ]
  >([
    reference.key,
    history.key,
    accountMembershipKey(owner),
    attemptStateKey(invoice.id),
    historyMembershipKey(amountHash),
  ]);
  for (const [existing, value] of [
    [existingReference, reference.value],
    [existingHistory, history.value],
  ] as const) {
    if (
      existing.value &&
      JSON.stringify(encodeKvValue(existing.value)) !== JSON.stringify(encodeKvValue(value))
    ) {
      throw new PaymentDomainError("attempt_history_conflict");
    }
  }
  const changed = !existingReference.value || !existingHistory.value || !state.value;
  const accountPage = Math.floor((membership.value?.count ?? 0) / PAGE_SIZE);
  const historyPage = Math.floor((historical.value?.count ?? 0) / PAGE_SIZE);
  const [accountEntries, historyEntries] = changed
    ? await kv.getMany<[AccountAttemptReference[], AttemptOwnership[]]>([
        accountPageKey(owner, accountPage),
        historyPageKey(amountHash, historyPage),
      ])
    : [null, null];
  return {
    changed,
    apply(atomic: KvAtomic) {
      atomic.check(existingReference, existingHistory, membership, state);
      atomic.set(
        treasuryHistoryKey(invoice.chainId, invoice.tokenAddress, invoice.treasuryAddress),
        true,
      );
      if (!existingReference.value) atomic.set(reference.key, reference.value);
      if (!existingHistory.value) atomic.set(history.key, history.value);
      if (!state.value)
        atomic.set(state.key, {
          accountHash: owner,
          resolution: legacyAttemptResolution(invoice),
          ...(fresh ? { sequence: (membership.value?.sequence ?? 0) + 1 } : {}),
        } satisfies AttemptState);
      if (changed)
        atomic
          .check(historical, accountEntries!, historyEntries!)
          .set(membership.key, {
            revision: (membership.value?.revision ?? 0) + 1,
            sequence: (membership.value?.sequence ?? 0) + (fresh ? 1 : 0),
            count: (membership.value?.count ?? 0) + 1,
          } satisfies AccountMembership)
          .set(accountEntries!.key, [...(accountEntries!.value ?? []), reference.value])
          .set(historyEntries!.key, [...(historyEntries!.value ?? []), history.value])
          .set(historical.key, {
            count: (historical.value?.count ?? 0) + 1,
          } satisfies HistoryMembership);
      return atomic;
    },
  };
}

/** Full identities and page numbers are exact keys: no LIKE/prefix fallback or foreign scan. */
export async function listAccountManifestPage(
  kv: Kv,
  account: AccountKey,
  options: KvPageOptions,
  membership?: KvEntryMaybe<AccountMembership>,
): Promise<KvPage<AccountAttemptReference>> {
  const owner = await accountHash(account);
  const control = membership ?? (await kv.get<AccountMembership>(accountMembershipKey(owner)));
  const page = options.cursor === undefined || options.cursor === null ? 0 : Number(options.cursor);
  if (!Number.isSafeInteger(page) || page < 0 || String(page) !== (options.cursor ?? "0"))
    throw new PaymentDomainError("invalid_account_manifest_cursor");
  const entry = await kv.get<AccountAttemptReference[]>(accountPageKey(owner, page));
  if ((control.value?.count ?? 0) > page * PAGE_SIZE && !entry.value)
    throw new PaymentDomainError("account_manifest_missing");
  const entries = (entry.value ?? []).map((value) => ({
    key: entry.key,
    versionstamp: entry.versionstamp!,
    value,
  }));
  if (entries.some(({ value }) => value.accountHash !== owner))
    throw new PaymentDomainError("account_manifest_owner_mismatch");
  return {
    entries,
    cursor: (page + 1) * PAGE_SIZE < (control.value?.count ?? 0) ? String(page + 1) : null,
  };
}

export async function readOwnershipManifestPage(
  kv: Kv,
  fingerprint: AmountFingerprint,
  page: number,
) {
  const hash = await amountOwnershipHash(fingerprint);
  const [control, entries] = await kv.getMany<[HistoryMembership, AttemptOwnership[]]>([
    historyMembershipKey(hash),
    historyPageKey(hash, page),
  ]);
  if ((control.value?.count ?? 0) > page * PAGE_SIZE && !entries.value)
    throw new PaymentDomainError("ownership_manifest_missing");
  return {
    control,
    entries,
    next: (page + 1) * PAGE_SIZE < (control.value?.count ?? 0) ? page + 1 : null,
  };
}

export async function adoptAccountAttempt(
  kv: Kv,
  entry: KvEntryMaybe<InvoiceRecord>,
  current: () => Promise<boolean> = async () => true,
): Promise<boolean> {
  if (
    !entry.value ||
    typeof entry.value.merchant !== "string" ||
    typeof entry.value.customerReference !== "string"
  )
    return true;
  const prepared = await prepareAccountAttempt(kv, entry.value);
  if (!(await current())) return false;
  if (!prepared.changed) return true;
  return (await prepared.apply(kv.atomic().check(entry)).commit()).ok;
}

/** Shared bounded adoption is event-driven; the complete marker never schedules a hunt. */
export async function adoptAccountIndexSlice(
  kv: Kv,
  options: { budget?: WorkerRequestBudget; isCurrent?: () => Promise<boolean> } = {},
): Promise<boolean> {
  const current = options.isCurrent ?? (async () => true);
  const room = (calls: number) => options.budget?.hasCapacity(calls) ?? true;
  if (!room(2) || !(await current())) return false;
  let entry = await kv.get<IndexState>(ACCOUNT_INDEX_STATE_KEY);
  if (entry.value?.complete) return true;
  let state: IndexState = entry.value ?? { complete: false, cursor: null };
  if (!state.page) {
    if (!room(3) || !(await current())) return false;
    const page = await kv.listPage<InvoiceRecord>([...PREFIX, "invoices"], {
      limit: 100,
      cursor: state.cursor,
    });
    if (!(await current())) return false;
    state = {
      ...state,
      page: { ids: page.entries.map((item) => item.value.id), next: page.cursor, offset: 0 },
    };
    const saved = await kv.atomic().check(entry).set(entry.key, state).commit();
    if (!saved.ok) return false;
    entry = { key: entry.key, value: state, versionstamp: saved.versionstamp };
  }
  for (let count = 0; count < 8 && state.page!.offset < state.page!.ids.length; count++) {
    if (!room(5) || !(await current())) return false;
    const invoice = await kv.get<InvoiceRecord>(
      storedInvoiceKey(state.page!.ids[state.page!.offset]),
    );
    const prepared =
      invoice.value &&
      typeof invoice.value.merchant === "string" &&
      typeof invoice.value.customerReference === "string"
        ? await prepareAccountAttempt(kv, invoice.value)
        : null;
    if (!(await current())) return false;
    const next = { ...state, page: { ...state.page!, offset: state.page!.offset + 1 } };
    const atomic = kv.atomic().check(entry, invoice).set(entry.key, next);
    const saved = await (prepared ? prepared.apply(atomic) : atomic).commit();
    if (!saved.ok) return false;
    state = next;
    entry = { key: entry.key, value: state, versionstamp: saved.versionstamp };
  }
  if (state.page!.offset === state.page!.ids.length && room(1) && (await current())) {
    const next: IndexState = { complete: state.page!.next === null, cursor: state.page!.next };
    return (await kv.atomic().check(entry).set(entry.key, next).commit()).ok && next.complete;
  }
  return false;
}

export async function markAccountIndexComplete(kv: Kv): Promise<void> {
  const entry = await kv.get<IndexState>(ACCOUNT_INDEX_STATE_KEY);
  await kv
    .atomic()
    .check(entry)
    .set(entry.key, { complete: true, cursor: null } satisfies IndexState)
    .commit();
}
