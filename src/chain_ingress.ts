import {
  ALCHEMY_NETWORK_CHAIN_IDS,
  MAX_ALCHEMY_BODY_BYTES,
  verifyAlchemyWebhookDelivery,
} from "./alchemy_webhook.ts";
import {
  ChainWebhookError,
  type ChainTransferEvent,
  type VerifiedChainEventDelivery,
} from "./chain_events.ts";
import type { Kv, KvCheck, KvEntry, KvKey } from "./kv.ts";
import { type Clock, systemClock } from "./payment_catalog.ts";
import type { SupportedChainId } from "./payment_types.ts";
import { WorkerBudgetError, type WorkerRequestBudget } from "./worker_budget.ts";

const PREFIX = ["uos_checkout", "v1", "chain_events"];
const LEASE_MS = 30_000;
const COMPLETION_CALLS = 4;
export const MAX_PENDING_CHAIN_REFERENCES = 8;

export type ChainTransferMatch = {
  chainId: SupportedChainId;
  tokenAddress: string;
  recipientAddress: string;
  amountUnits: bigint;
};
export type ChainTransferContext = {
  /** The payment transaction checks this same receipt version before committing. */
  fence?: KvCheck;
  fences?: KvCheck[];
  budget?: WorkerRequestBudget;
  deadline: number;
  /** Costs one KV read; check before asynchronous canonical mutation phases. */
  isCurrent(): Promise<boolean>;
};
export type ChainIngressOptions = {
  kv: Kv;
  processTransfer(
    event: ChainTransferEvent,
    context: ChainTransferContext,
  ): Promise<{ terminal: boolean }>;
  /** Minimum calls for ONE viable callback phase, including its isCurrent reads. Larger phases must defer. */
  minimumTransferCalls: number;
  prepareTransfer?: (event: ChainTransferEvent, context: ChainTransferContext) => Promise<boolean>;
  budget?: WorkerRequestBudget;
  deadline?: number;
  clock?: Clock;
};
export type ChainIngressResult = { complete: boolean; processed: number; replay: boolean };
type Reference = Pick<
  VerifiedChainEventDelivery,
  "provider" | "subscriptionId" | "eventId" | "bodySha256"
>;
type PendingReference = Reference & { index: number };
export type ChainEventReceipt = VerifiedChainEventDelivery & {
  schemaVersion: 1;
  nextIndex: number;
  state: "pending" | "complete";
  leaseId?: string;
  leaseUntil?: number;
  accountPrepared?: number;
  requiresAccountPreparation?: boolean;
};

export function chainEventReceiptKey(identity: Reference): KvKey {
  return [...PREFIX, "receipts", identity.provider, identity.subscriptionId, identity.eventId];
}
export function pendingChainTransfersKey(match: ChainTransferMatch): KvKey {
  return [
    ...PREFIX,
    "pending",
    match.chainId,
    match.tokenAddress.toLowerCase(),
    match.recipientAddress.toLowerCase(),
    match.amountUnits.toString(),
  ];
}

/** Caller must use the same budget-wrapped KV/RPC transports and an idempotent domain callback. */
export async function processChainEventDelivery(
  delivery: VerifiedChainEventDelivery,
  options: ChainIngressOptions,
): Promise<ChainIngressResult> {
  return processReceipt(delivery, options, delivery);
}

/** Only the indexed current item is recoverable by its match tuple; no global or tail enumeration. */
export async function recoverPendingChainTransfers(
  match: ChainTransferMatch,
  options: ChainIngressOptions,
): Promise<{ complete: boolean; processed: number }> {
  if (!capacity(options, 1)) return { complete: false, processed: 0 };
  const refs =
    (await options.kv.get<PendingReference[]>(pendingChainTransfersKey(match))).value ?? [];
  let processed = 0;
  for (const ref of refs.slice(0, MAX_PENDING_CHAIN_REFERENCES)) {
    try {
      const result = await processReceipt(ref, options, undefined, match, ref.index);
      processed += result.processed;
      if (!result.complete) return { complete: false, processed };
    } catch (error) {
      if (!(error instanceof ChainWebhookError) || error.status !== 503) throw error;
      return { complete: false, processed };
    }
  }
  return { complete: true, processed };
}

async function processReceipt(
  identity: Reference,
  options: ChainIngressOptions,
  delivery?: VerifiedChainEventDelivery,
  match?: ChainTransferMatch,
  expectedIndex?: number,
): Promise<ChainIngressResult> {
  const { kv, budget } = options;
  const clock = options.clock ?? systemClock;
  const deadline = options.deadline ?? Date.now() + 20_000;
  const pending = (processed = 0): ChainIngressResult => ({
    complete: false,
    processed,
    replay: false,
  });
  if (!Number.isSafeInteger(options.minimumTransferCalls) || options.minimumTransferCalls < 0) {
    throw new ChainWebhookError("invalid_chain_processor_budget", 503);
  }
  if (!capacity(options, 3)) return pending();
  const key = chainEventReceiptKey(identity);
  let existing = await kv.get<ChainEventReceipt>(key);
  if (existing.value && existing.value.bodySha256 !== identity.bodySha256) {
    throw new ChainWebhookError("chain_webhook_replay_conflict", 409);
  }
  if (existing.value?.state === "complete") return { complete: true, processed: 0, replay: true };
  if ((existing.value?.leaseUntil ?? 0) > clock.now().getTime()) {
    throw new ChainWebhookError("chain_webhook_busy", 503);
  }
  if (!existing.value && !delivery) return pending();
  if (existing.value?.requiresAccountPreparation && !options.prepareTransfer) return pending();
  if (options.prepareTransfer) {
    if (!existing.value) {
      const initial: ChainEventReceipt = {
        ...delivery!,
        schemaVersion: 1,
        nextIndex: 0,
        state: "pending",
        accountPrepared: 0,
        requiresAccountPreparation: true,
      };
      const saved = await kv.atomic().check(existing).set(key, initial).commit();
      if (!saved.ok) return pending();
      existing = { key, value: initial, versionstamp: saved.versionstamp };
    }
    while ((existing.value!.accountPrepared ?? 0) < existing.value!.transfers.length) {
      if (!capacity(options, 4) || Date.now() >= deadline) return pending();
      const snapshot = existing;
      const release = budget?.reserveForCompletion(1);
      let prepared: boolean;
      try {
        prepared = await options.prepareTransfer(
          existing.value!.transfers[existing.value!.accountPrepared ?? 0],
          {
            budget,
            deadline,
            fence: { key, versionstamp: snapshot.versionstamp },
            isCurrent: async () =>
              Date.now() < deadline &&
              (await kv.get<ChainEventReceipt>(key)).versionstamp === snapshot.versionstamp,
          },
        );
      } catch {
        return pending();
      } finally {
        release?.();
      }
      if (!prepared) return pending();
      const next: ChainEventReceipt = {
        ...existing.value!,
        accountPrepared: (existing.value!.accountPrepared ?? 0) + 1,
      };
      const saved = await kv.atomic().check(existing).set(key, next).commit();
      if (!saved.ok) return pending();
      existing = { key, value: next, versionstamp: saved.versionstamp };
    }
  }
  if (expectedIndex !== undefined && existing.value?.nextIndex !== expectedIndex) return pending();
  if (Date.now() >= deadline) return pending();
  const leaseId = crypto.randomUUID();
  let value: ChainEventReceipt = {
    ...(existing.value ?? { ...delivery!, schemaVersion: 1, nextIndex: 0, state: "pending" }),
    leaseId,
    leaseUntil: clock.now().getTime() + LEASE_MS,
  };
  const acquired = await kv.atomic().check(existing).set(key, value).commit();
  if (!acquired.ok) throw new ChainWebhookError("chain_webhook_busy", 503);
  let owned: KvEntry<ChainEventReceipt> = { key, value, versionstamp: acquired.versionstamp };
  let processed = 0;
  let releaseReserve: (() => void) | undefined;
  const withinLifetime = () => Date.now() < deadline && clock.now().getTime() < value.leaseUntil!;
  const isCurrent = async () => {
    if (!withinLifetime()) return false;
    const current = await kv.get<ChainEventReceipt>(key);
    return (
      withinLifetime() &&
      current.versionstamp === owned.versionstamp &&
      current.value?.leaseId === leaseId
    );
  };
  try {
    while (value.nextIndex < value.transfers.length) {
      const event = value.transfers[value.nextIndex];
      if (match && !sameMatch(event, match)) return pending(processed);
      if (!withinLifetime() || !capacity(options, COMPLETION_CALLS)) return pending(processed);
      releaseReserve = budget?.reserveForCompletion(COMPLETION_CALLS);
      // Admission: get+CAS; current-owner fence: get. Completion reserve remains untouched.
      if (!capacity(options, options.minimumTransferCalls + 3)) return pending(processed);
      const indexKey = pendingChainTransfersKey(event);
      const index = await kv.get<PendingReference[]>(indexKey);
      const references = index.value ?? [];
      const currentReference = { ...reference(value), index: value.nextIndex };
      const isOwnReference = (ref: PendingReference) =>
        sameIdentity(ref, identity) && ref.index === value.nextIndex;
      const present = references.some(isOwnReference);
      if (!present && references.length >= MAX_PENDING_CHAIN_REFERENCES) return pending(processed);
      let indexVersion = index.versionstamp;
      if (!present) {
        const admitted = await kv
          .atomic()
          .check(owned, index)
          .set(indexKey, [...references, currentReference])
          .commit();
        if (!admitted.ok) return pending(processed);
        indexVersion = admitted.versionstamp;
      }
      if (!(await isCurrent())) return pending(processed);
      let terminal = false;
      try {
        terminal = (
          await options.processTransfer(event, {
            budget,
            deadline,
            isCurrent,
            fence: { key: owned.key, versionstamp: owned.versionstamp },
          })
        ).terminal;
      } catch {
        // Transient verification, budget failure, or a crash after domain commit resumes this item.
        return pending(processed);
      }
      releaseReserve?.();
      releaseReserve = undefined;
      if (!terminal || !(await isCurrent())) return pending(processed);
      const remaining = references.filter((ref) => !isOwnReference(ref));
      const nextIndex = value.nextIndex + 1;
      const complete = nextIndex === value.transfers.length;
      const next: ChainEventReceipt = {
        ...value,
        nextIndex,
        state: complete ? "complete" : "pending",
        transfers: complete ? [] : value.transfers,
        ...(complete ? { leaseId: undefined, leaseUntil: undefined } : {}),
      };
      const atomic = kv
        .atomic()
        .check(owned, { key: indexKey, versionstamp: indexVersion })
        .set(key, next);
      if (remaining.length) atomic.set(indexKey, remaining);
      else atomic.delete(indexKey);
      const checkpoint = await atomic.commit();
      if (!checkpoint.ok) return pending(processed);
      value = next;
      owned = { key, value, versionstamp: checkpoint.versionstamp };
      processed++;
      if (complete) return { complete: true, processed, replay: false };
    }
    // Empty/non-ERC20 batches still get a durable completed replay receipt.
    const next = {
      ...value,
      state: "complete" as const,
      transfers: [],
      leaseId: undefined,
      leaseUntil: undefined,
    };
    const completed = await kv.atomic().check(owned).set(key, next).commit();
    if (!completed.ok) return pending(processed);
    value = next;
    return { complete: true, processed, replay: false };
  } catch (error) {
    if (!(error instanceof WorkerBudgetError)) throw error;
    return pending(processed);
  } finally {
    releaseReserve?.();
    // Completion costs <=3: owner read + atomic cursor/index checkpoint + atomic lease release.
    // A successful terminal checkpoint removes its match reference in that same atomic commit.
    if (value.state !== "complete") {
      await kv
        .atomic()
        .check(owned)
        .set(key, { ...value, leaseId: undefined, leaseUntil: undefined })
        .commit();
    }
  }
}

export async function handleAlchemyChainWebhook(
  request: Request,
  options: ChainIngressOptions & { webhookKeys: string | undefined },
): Promise<Response> {
  if (request.method !== "POST")
    return Response.json(
      { error: "method_not_allowed" },
      { status: 405, headers: { allow: "POST" } },
    );
  try {
    const keys = configuredKeys(options.webhookKeys);
    const body = await readBody(request);
    let hint: unknown;
    try {
      hint = JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(body));
    } catch {
      throw new ChainWebhookError("invalid_chain_webhook_json");
    }
    const webhookId = record(hint)?.webhookId;
    if (typeof webhookId !== "string" || !Object.hasOwn(keys, webhookId)) {
      throw new ChainWebhookError("invalid_chain_webhook_signature", 401);
    }
    const config = keys[webhookId];
    const delivery = await verifyAlchemyWebhookDelivery(
      body,
      request.headers.get("x-alchemy-signature"),
      {
        webhookId,
        signingKey: config.signingKey,
        chainId: config.chainId,
        now: (options.clock ?? systemClock).now(),
      },
    );
    const result = await processChainEventDelivery(delivery, options);
    return Response.json(
      { complete: result.complete, processed: result.processed, replay: result.replay },
      { status: result.complete ? 200 : 503 },
    );
  } catch (error) {
    if (error instanceof ChainWebhookError)
      return Response.json({ error: error.code }, { status: error.status });
    if (error instanceof WorkerBudgetError)
      return Response.json({ error: "chain_webhook_pending" }, { status: 503 });
    return Response.json({ error: "chain_webhook_failed" }, { status: 503 });
  }
}

async function readBody(request: Request): Promise<Uint8Array<ArrayBuffer>> {
  const length = request.headers.get("content-length");
  if (length !== null && (!/^\d+$/.test(length) || Number(length) > MAX_ALCHEMY_BODY_BYTES)) {
    throw new ChainWebhookError("chain_webhook_body_too_large", 413);
  }
  const reader = request.body?.getReader();
  if (!reader) throw new ChainWebhookError("invalid_chain_webhook_json");
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const item = await reader.read();
      if (item.done) break;
      size += item.value.byteLength;
      if (size > MAX_ALCHEMY_BODY_BYTES) {
        await reader.cancel();
        throw new ChainWebhookError("chain_webhook_body_too_large", 413);
      }
      chunks.push(item.value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function configuredKeys(
  raw: string | undefined,
): Record<string, { signingKey: string; chainId: SupportedChainId }> {
  if (!raw || raw.length > 64 * 1024)
    throw new ChainWebhookError("chain_webhook_not_configured", 503);
  let parsed: Record<string, unknown> | null;
  try {
    parsed = record(JSON.parse(raw));
  } catch {
    throw new ChainWebhookError("chain_webhook_not_configured", 503);
  }
  if (!parsed || Object.keys(parsed).length > 16 || Object.keys(parsed).length === 0)
    throw new ChainWebhookError("chain_webhook_not_configured", 503);
  const result: Record<string, { signingKey: string; chainId: SupportedChainId }> =
    Object.create(null);
  for (const [id, input] of Object.entries(parsed)) {
    const value = record(input);
    if (
      !/^[A-Za-z0-9_-]{1,128}$/.test(id) ||
      !value ||
      typeof value.signingKey !== "string" ||
      !value.signingKey ||
      value.signingKey.length > 4096 ||
      typeof value.network !== "string" ||
      !Object.hasOwn(ALCHEMY_NETWORK_CHAIN_IDS, value.network)
    ) {
      throw new ChainWebhookError("chain_webhook_not_configured", 503);
    }
    result[id] = {
      signingKey: value.signingKey,
      chainId: ALCHEMY_NETWORK_CHAIN_IDS[value.network as keyof typeof ALCHEMY_NETWORK_CHAIN_IDS],
    };
  }
  return result;
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}
function capacity(options: ChainIngressOptions, calls: number): boolean {
  return options.budget?.hasCapacity(calls) ?? true;
}
function sameIdentity(left: Reference, right: Reference): boolean {
  return (
    left.provider === right.provider &&
    left.subscriptionId === right.subscriptionId &&
    left.eventId === right.eventId
  );
}
function sameMatch(left: ChainTransferMatch, right: ChainTransferMatch): boolean {
  return (
    left.chainId === right.chainId &&
    left.tokenAddress.toLowerCase() === right.tokenAddress.toLowerCase() &&
    left.recipientAddress.toLowerCase() === right.recipientAddress.toLowerCase() &&
    left.amountUnits === right.amountUnits
  );
}
function reference(delivery: Reference): Reference {
  return {
    provider: delivery.provider,
    subscriptionId: delivery.subscriptionId,
    eventId: delivery.eventId,
    bodySha256: delivery.bodySha256,
  };
}
