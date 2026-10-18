import type { Clock } from "./payment_catalog.ts";
import type { Kv, KvEntryMaybe, KvKey } from "./kv.ts";
import type { ClaimPaymentInput, ClaimPaymentResult } from "./payment_service.ts";
import {
  boundedText,
  invoiceJson,
  PaymentDomainError,
  type InvoiceRecord,
} from "./payment_types.ts";
import {
  chainHeads,
  discoverTransferLogs,
  RpcVerificationError,
  type DiscoveredTransferLog,
  type RpcClient,
} from "./rpc.ts";
import { WorkerBudgetError, type WorkerRequestBudget } from "./worker_budget.ts";

const PREFIX = ["uos_checkout", "v1", "payments"];
const SCAN_BLOCKS = 1000n;
const OVERLAP_BLOCKS = 128n;
const LEASE_MS = 30_000;
const COMPLETION_CALLS = 3;

export type DiscoveryOptions = {
  budget?: WorkerRequestBudget;
  deadline?: number;
  expiryToBlock?: bigint;
  isCurrent?: () => Promise<boolean>;
};
export type DiscoveryState = { searchedThrough?: bigint; leaseId?: string; leaseUntil?: number };

export function discoveryStateKey(invoiceId: string): KvKey {
  return [...PREFIX, "discovery", invoiceId];
}

export function expiryDiscoveryStateKey(invoiceId: string): KvKey {
  return [...PREFIX, "expiry_passes", invoiceId];
}

export function hasCurrentPayment(invoice: Pick<InvoiceRecord, "paymentId" | "status">): boolean {
  return Boolean(invoice.paymentId) && invoice.status !== "reverted";
}

export function invoiceMatchesTransfer(
  invoice: InvoiceRecord,
  log: DiscoveredTransferLog,
): boolean {
  return (
    (!invoice.paymentId || invoice.status === "reverted") &&
    invoice.minimumBlock <= log.blockNumber &&
    invoice.tokenAddress.toLowerCase() === log.tokenAddress.toLowerCase() &&
    invoice.treasuryAddress.toLowerCase() === log.recipientAddress.toLowerCase() &&
    invoice.paidAmountUnits === log.amountUnits
  );
}

/** Discovery owns only this invoice's progress, never either reconciliation cursor. */
export async function discoverInvoicePayment(
  dependencies: {
    kv: Kv;
    rpc: RpcClient;
    clock: Clock;
    claim: (
      invoiceId: string,
      input: ClaimPaymentInput,
      invoiceEntry?: KvEntryMaybe<InvoiceRecord>,
    ) => Promise<ClaimPaymentResult>;
    existingEvent: (invoice: InvoiceRecord) => Promise<string | undefined>;
  },
  invoiceIdInput: unknown,
  options: DiscoveryOptions = {},
): Promise<ClaimPaymentResult> {
  const { kv, rpc, clock } = dependencies;
  const invoiceId = boundedText(invoiceIdInput, "invoice_id", 128);
  const invoiceKey = [...PREFIX, "invoices", invoiceId];
  if (options.expiryToBlock !== undefined && options.expiryToBlock < 0n)
    throw new PaymentDomainError("invalid_expiry_snapshot");
  const stateKey =
    options.expiryToBlock === undefined
      ? discoveryStateKey(invoiceId)
      : expiryDiscoveryStateKey(invoiceId);
  const [invoiceEntry, state] = await kv.getMany<[InvoiceRecord, DiscoveryState]>([
    invoiceKey,
    stateKey,
  ]);
  if (!invoiceEntry.value) throw new PaymentDomainError("invoice_not_found");
  const invoice = invoiceEntry.value;
  const mayStart = (calls: number) =>
    (options.budget?.hasCapacity(calls) ?? true) && Date.now() < (options.deadline ?? Infinity);
  if (hasCurrentPayment(invoice)) return result(invoice, state.value?.searchedThrough);
  if ((state.value?.leaseUntil ?? 0) > clock.now().getTime() || !mayStart(12)) {
    return currentResult();
  }
  const leaseId = crypto.randomUUID();
  const leased: DiscoveryState = {
    ...state.value,
    leaseId,
    leaseUntil: clock.now().getTime() + LEASE_MS,
  };
  if (!(await kv.atomic().check(invoiceEntry, state).set(stateKey, leased).commit()).ok) {
    return currentResult();
  }
  let claimed: ClaimPaymentResult | undefined;
  let watchedInvoice = invoice;
  let watchedEntry = invoiceEntry;
  const releaseBudget = options.budget?.reserveForCompletion(COMPLETION_CALLS);
  try {
    const heads =
      options.expiryToBlock === undefined ? await chainHeads(rpc, invoice.chainId) : null;
    if (!(await currentForClaim())) throw new DiscoveryYield();
    const head =
      options.expiryToBlock ?? heads!.safeBlock ?? heads!.finalizedBlock ?? heads!.latestBlock;
    const overlap =
      state.value?.searchedThrough === undefined
        ? invoice.minimumBlock
        : state.value.searchedThrough +
          1n -
          (options.expiryToBlock === undefined ? OVERLAP_BLOCKS : 0n);
    const fromBlock = overlap > invoice.minimumBlock ? overlap : invoice.minimumBlock;
    const toBlock = head < fromBlock + SCAN_BLOCKS - 1n ? head : fromBlock + SCAN_BLOCKS - 1n;
    const logs =
      toBlock < fromBlock
        ? []
        : await discoverTransferLogs(
            rpc,
            invoice.chainId,
            invoice.tokenAddress,
            invoice.treasuryAddress,
            fromBlock,
            toBlock,
          );
    let incomplete = false;
    for (const log of logs) {
      if (!invoiceMatchesTransfer(watchedInvoice, log)) continue;
      if (!mayStart(8) || !(await currentForClaim())) {
        incomplete = true;
        break;
      }
      try {
        claimed = await dependencies.claim(
          invoiceId,
          {
            transactionHash: log.transactionHash,
            logIndex: log.logIndex.toString(),
            observedBlockNumber: log.blockNumber.toString(),
            observedBlockHash: log.blockHash,
          },
          watchedEntry,
        );
        watchedInvoice = {
          ...watchedInvoice,
          paymentId: claimed.invoice.paymentId,
          status: claimed.invoice.status,
        };
        // A concurrent coordinate claim can refresh into a reorg-cleared state.
        if (hasCurrentPayment(claimed.invoice)) break;
      } catch (error) {
        // An indexed log can precede its canonical receipt. Never skip its chunk.
        if (error instanceof RpcVerificationError) {
          incomplete = true;
          continue;
        }
        if (
          error instanceof PaymentDomainError &&
          ["payment_locator_already_claimed", "payment_before_minimum_block"].includes(error.code)
        )
          continue;
        throw error;
      }
    }
    const owned = claimed?.invoice.paymentId ? null : await kv.get<DiscoveryState>(stateKey);
    if (
      !incomplete &&
      !claimed?.invoice.paymentId &&
      owned?.value?.leaseId === leaseId &&
      (owned.value.leaseUntil ?? 0) > clock.now().getTime()
    ) {
      await kv
        .atomic()
        .check(invoiceEntry, owned)
        .set(stateKey, {
          ...owned.value,
          searchedThrough:
            owned.value.searchedThrough !== undefined && owned.value.searchedThrough > toBlock
              ? owned.value.searchedThrough
              : toBlock,
        } satisfies DiscoveryState)
        .commit();
    }
  } catch (error) {
    if (!(error instanceof WorkerBudgetError) && !(error instanceof DiscoveryYield)) throw error;
  } finally {
    releaseBudget?.();
    const [owned, currentInvoice] = await kv.getMany<[DiscoveryState, InvoiceRecord]>([
      stateKey,
      invoiceKey,
    ]);
    if (owned.value?.leaseId === leaseId) {
      const atomic = kv.atomic().check(owned);
      const { leaseId: _leaseId, leaseUntil: _leaseUntil, ...progress } = owned.value;
      const paidExpiry =
        options.expiryToBlock !== undefined &&
        currentInvoice.value &&
        hasCurrentPayment(currentInvoice.value);
      if (paidExpiry)
        atomic
          .check(currentInvoice)
          .set(stateKey, { ...progress, status: "paid", completedAt: clock.now().getTime() });
      await (
        paidExpiry
          ? atomic
          : Object.keys(progress).length
            ? atomic.set(stateKey, progress)
            : atomic.delete(stateKey)
      ).commit();
    }
  }
  return currentResult(claimed);

  async function currentForClaim(): Promise<boolean> {
    if (options.isCurrent && !(await options.isCurrent())) return false;
    if (clock.now().getTime() >= leased.leaseUntil!) return false;
    const [current, owned] = await kv.getMany<[InvoiceRecord, DiscoveryState]>([
      invoiceKey,
      stateKey,
    ]);
    if (!current.value || clock.now().getTime() >= leased.leaseUntil!) return false;
    watchedInvoice = current.value;
    watchedEntry = current;
    if (owned.value?.leaseId === leaseId) return true;
    // A coordinate claim followed by a reorg starts a new invoice lifecycle.
    // Reacquire from absence; never run through a successor's lease.
    if (owned.value !== null) return false;
    return (
      await kv
        .atomic()
        .check(current, owned)
        .set(stateKey, {
          leaseId,
          leaseUntil: leased.leaseUntil,
        } satisfies DiscoveryState)
        .commit()
    ).ok;
  }

  async function currentResult(previous?: ClaimPaymentResult): Promise<ClaimPaymentResult> {
    const [current, committed] = await kv.getMany<[InvoiceRecord, DiscoveryState]>([
      invoiceKey,
      stateKey,
    ]);
    if (!current.value) throw new PaymentDomainError("invoice_not_found");
    return result(current.value, committed.value?.searchedThrough, previous);
  }

  async function result(
    current: InvoiceRecord,
    searchedThrough?: bigint,
    previous?: ClaimPaymentResult,
  ): Promise<ClaimPaymentResult> {
    const samePhase =
      previous?.invoice.paymentId === current.paymentId &&
      previous?.invoice.status === current.status &&
      previous?.invoice.acceptedAt === current.acceptedAt &&
      previous?.invoice.settledAt === current.settledAt &&
      previous?.invoice.revertedAt === current.revertedAt;
    const eventJws = samePhase
      ? previous?.eventJws
      : (options.budget?.hasCapacity(4) ?? true)
        ? await dependencies.existingEvent(current)
        : undefined;
    return {
      invoice: invoiceJson(current),
      ...(eventJws ? { eventJws } : {}),
      discovery: {
        found: hasCurrentPayment(current),
        ...(searchedThrough === undefined ? {} : { searchedToBlock: searchedThrough.toString() }),
      },
    };
  }
}

class DiscoveryYield extends Error {}
