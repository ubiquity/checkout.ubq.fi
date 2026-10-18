import type { CheckoutServices } from "../main.ts";
import type { Kv, KvEntry } from "./kv.ts";
import {
  PaymentDomainError,
  RECENTLY_EXPIRED_WINDOW_MS,
  SUPPORTED_CHAIN_IDS,
  isOpenInvoiceStatus,
  type InvoiceRecord,
  type SupportedChainId,
} from "./payment_types.ts";
import { chainHeads, discoverTransferLogs, RpcVerificationError } from "./rpc.ts";
import { invoiceMatchesTransfer } from "./payment_discovery.ts";

const PREFIX = ["uos_checkout", "v1", "payments"];
const OVERLAP_BLOCKS = 128n;
const SCAN_BLOCKS = 1000n;
type Rotation = { schemaVersion: 1; after: string; updatedAt: string };
type ScanCursor = { schemaVersion: 1; blockNumber: bigint; updatedAt: string };

export function maintenanceChainAt(scheduledTime: number): SupportedChainId {
  return SUPPORTED_CHAIN_IDS[Math.floor(scheduledTime / 60_000) % SUPPORTED_CHAIN_IDS.length];
}

/** One rotating invoice and one bounded block range, with independently durable progress. */
export async function reconcileInvoiceBatch(
  services: Pick<CheckoutServices, "kv" | "payment" | "rpc">,
  chainId: SupportedChainId,
  now = new Date(),
): Promise<{ invoiceId: string; fromBlock?: bigint; toBlock?: bigint } | null> {
  const { kv, payment, rpc } = services;
  const selected = await selectInvoice(kv, chainId, now);
  if (!selected) return null;
  const invoiceId = selected.value;
  const invoiceEntry = await kv.get<InvoiceRecord>([...PREFIX, "invoices", invoiceId]);
  const invoice = invoiceEntry.value;
  const recent =
    invoice?.status === "expired" &&
    Date.parse(invoice.expiredAt ?? invoice.updatedAt) >=
      now.getTime() - RECENTLY_EXPIRED_WINDOW_MS;
  if (
    !invoice ||
    !(
      isOpenInvoiceStatus(invoice.status) ||
      invoice.status === "confirming" ||
      invoice.status === "reverted" ||
      recent
    )
  ) {
    await kv.atomic().check(invoiceEntry, selected).delete(selected.key).commit();
    return { invoiceId };
  }
  const cursorKey = [...PREFIX, "invoice_reconciliation_cursors", chainId, invoiceId];
  const cursor = await kv.get<ScanCursor>(cursorKey);
  const heads = await chainHeads(rpc, chainId);
  const head = heads.safeBlock ?? heads.finalizedBlock ?? heads.latestBlock;
  const overlapStart = cursor.value
    ? cursor.value.blockNumber + 1n - OVERLAP_BLOCKS
    : invoice.minimumBlock;
  const fromBlock = overlapStart > invoice.minimumBlock ? overlapStart : invoice.minimumBlock;
  if (head < fromBlock) return { invoiceId };
  const maximumBlock = fromBlock + SCAN_BLOCKS - 1n;
  const toBlock = head < maximumBlock ? head : maximumBlock;
  if (!invoice.paymentId || invoice.status === "reverted") {
    const logs = await discoverTransferLogs(
      rpc,
      chainId,
      invoice.tokenAddress,
      invoice.treasuryAddress,
      fromBlock,
      toBlock,
    );
    for (const log of logs) {
      if (!invoiceMatchesTransfer(invoice, log)) continue;
      try {
        await payment.claimPayment(invoice.id, {
          transactionHash: log.transactionHash,
          logIndex: log.logIndex.toString(),
          observedBlockNumber: log.blockNumber.toString(),
          observedBlockHash: log.blockHash,
        });
      } catch (error) {
        if (error instanceof RpcVerificationError) continue;
        if (
          error instanceof PaymentDomainError &&
          ["payment_locator_already_claimed", "payment_before_minimum_block"].includes(error.code)
        )
          continue;
        throw error;
      }
    }
  }
  const current = await payment.getInvoiceRecord(invoiceId);
  if (current.paymentId && current.status !== "late_payment" && current.status !== "manual_review")
    await payment.refreshInvoice(invoiceId);
  else if (!current.paymentId) await payment.expireInvoice(invoiceId);
  // No scan cursor moves if RPC, verification, or the invocation budget interrupted work.
  const result = await kv
    .atomic()
    .check(cursor)
    .set(cursorKey, {
      schemaVersion: 1,
      blockNumber: toBlock,
      updatedAt: now.toISOString(),
    } satisfies ScanCursor)
    .commit();
  if (!result.ok) throw new PaymentDomainError("reconciliation_cursor_conflict");
  return { invoiceId, fromBlock, toBlock };
}

async function selectInvoice(
  kv: Kv,
  chainId: SupportedChainId,
  now: Date,
): Promise<KvEntry<string> | null> {
  const prefix = [...PREFIX, "invoices_by_chain", chainId];
  const key = [...PREFIX, "reconciliation_rotation", chainId];
  for (let attempt = 0; attempt < 3; attempt++) {
    const rotation = await kv.get<Rotation>(key);
    const start = rotation.value ? [...prefix, rotation.value.after, ""] : undefined;
    let selected = await first(kv.list<string>({ prefix, start }, { limit: 1 }));
    if (!selected && rotation.value)
      selected = await first(kv.list<string>({ prefix }, { limit: 1 }));
    if (!selected) return null;
    const after = selected.key[selected.key.length - 1];
    if (typeof after !== "string" || typeof selected.value !== "string")
      throw new PaymentDomainError("invalid_reconciliation_index");
    // Rotation is separate from scan completion: one failed invoice cannot monopolize a chain.
    const result = await kv
      .atomic()
      .check(rotation)
      .set(key, { schemaVersion: 1, after, updatedAt: now.toISOString() } satisfies Rotation)
      .commit();
    if (result.ok) return selected;
  }
  throw new PaymentDomainError("reconciliation_rotation_conflict");
}

async function first<T>(entries: AsyncIterable<KvEntry<T>>): Promise<KvEntry<T> | null> {
  for await (const entry of entries) return entry;
  return null;
}
