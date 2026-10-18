import {
  ChainWebhookError,
  type ChainTransferEvent,
  type VerifiedChainEventDelivery,
} from "./chain_events.ts";
import type { SupportedChainId } from "./payment_types.ts";
import { TRANSFER_TOPIC } from "./rpc.ts";

export const ALCHEMY_NETWORK_CHAIN_IDS = Object.freeze({
  ETH_MAINNET: 1,
  OPT_MAINNET: 10,
  BASE_MAINNET: 8453,
} as const);

// Explicit application bounds, independent of the provider's maximum batch size.
export const MAX_ALCHEMY_BODY_BYTES = 256 * 1024;
export const MAX_ALCHEMY_ACTIVITIES = 256;
export const ALCHEMY_MAX_AGE_MS = 15 * 60 * 1000;
export const ALCHEMY_FUTURE_TOLERANCE_MS = 60 * 1000;

export type AlchemyWebhookOptions = {
  signingKey: string;
  webhookId: string;
  chainId: SupportedChainId;
  now?: Date;
};

/** Verify the original bytes with this webhook's UTF-8 signing key before accepting its shape. */
export async function verifyAlchemyWebhookDelivery(
  rawBody: Uint8Array,
  signature: string | null,
  options: AlchemyWebhookOptions,
): Promise<VerifiedChainEventDelivery> {
  if (rawBody.byteLength > MAX_ALCHEMY_BODY_BYTES) {
    throw new ChainWebhookError("chain_webhook_body_too_large", 413);
  }
  if (!signature || !/^[0-9a-fA-F]{64}$/.test(signature)) {
    throw new ChainWebhookError("invalid_chain_webhook_signature", 401);
  }
  if (!options.signingKey || options.signingKey.length > 4096) {
    throw new ChainWebhookError("chain_webhook_not_configured", 503);
  }
  // A private snapshot prevents caller mutation between signature verification and parsing.
  const body = new Uint8Array(rawBody);
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(options.signingKey),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"],
  );
  if (!(await crypto.subtle.verify("HMAC", key, signatureBytes(signature), body))) {
    throw new ChainWebhookError("invalid_chain_webhook_signature", 401);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(body));
  } catch {
    throw new ChainWebhookError("invalid_chain_webhook_json");
  }
  const envelope = object(parsed);
  const subscriptionId = identifier(envelope.webhookId);
  const eventId = identifier(envelope.id);
  if (subscriptionId !== options.webhookId) {
    throw new ChainWebhookError("chain_webhook_subscription_mismatch", 401);
  }
  if (envelope.type !== "ADDRESS_ACTIVITY") {
    throw new ChainWebhookError("unsupported_chain_webhook_type");
  }
  const createdAt = timestamp(envelope.createdAt, options.now ?? new Date());
  const event = object(envelope.event);
  const network = event.network;
  if (
    typeof network !== "string" ||
    !Object.hasOwn(ALCHEMY_NETWORK_CHAIN_IDS, network) ||
    ALCHEMY_NETWORK_CHAIN_IDS[network as keyof typeof ALCHEMY_NETWORK_CHAIN_IDS] !== options.chainId
  ) {
    throw new ChainWebhookError("chain_webhook_network_mismatch");
  }
  if (!Array.isArray(event.activity)) throw new ChainWebhookError("invalid_chain_activity");
  if (event.activity.length > MAX_ALCHEMY_ACTIVITIES) {
    throw new ChainWebhookError("chain_webhook_activity_limit", 413);
  }
  const transfers: ChainTransferEvent[] = [];
  for (const activity of event.activity) {
    const transfer = parseActivity(activity, options.chainId);
    if (transfer) transfers.push(transfer);
  }
  return {
    provider: "alchemy",
    subscriptionId,
    eventId,
    deliveryId: JSON.stringify(["alchemy", subscriptionId, eventId]),
    createdAt,
    bodySha256: hex(new Uint8Array(await crypto.subtle.digest("SHA-256", body))),
    transfers,
  };
}

function parseActivity(value: unknown, chainId: SupportedChainId): ChainTransferEvent | null {
  const activity = object(value);
  if (typeof activity.category !== "string") throw new ChainWebhookError("invalid_chain_activity");
  if (activity.category !== "token" && activity.category !== "erc20") return null;
  const log = object(activity.log);
  if (!Array.isArray(log.topics)) throw new ChainWebhookError("invalid_chain_transfer_topics");
  // Address Activity also includes NFTs. ERC721 Transfer has four topics; other events differ.
  if (log.topics.length !== 3) return null;
  if (word(log.topics[0]) !== TRANSFER_TOPIC) return null;
  const topics = log.topics.map(word);
  if (typeof log.removed !== "boolean") throw new ChainWebhookError("invalid_chain_log_removed");
  const logIndex = quantity(log.logIndex);
  if (logIndex > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new ChainWebhookError("invalid_chain_log_index");
  }
  return {
    chainId,
    transactionHash: word(log.transactionHash),
    logIndex: Number(logIndex),
    blockNumber: quantity(log.blockNumber),
    blockHash: word(log.blockHash),
    tokenAddress: address(log.address),
    payerAddress: topicAddress(topics[1]),
    recipientAddress: topicAddress(topics[2]),
    amountUnits: BigInt(word(log.data)),
    removed: log.removed,
  };
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new ChainWebhookError("invalid_chain_webhook_payload");
  }
  return value as Record<string, unknown>;
}

function identifier(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(value)) {
    throw new ChainWebhookError("invalid_chain_webhook_identity");
  }
  return value;
}

function timestamp(value: unknown, now: Date): string {
  if (
    typeof value !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$/.test(value) ||
    !Number.isFinite(Date.parse(value)) ||
    !Number.isFinite(now.getTime())
  ) {
    throw new ChainWebhookError("invalid_chain_webhook_timestamp");
  }
  const createdAt = new Date(value).toISOString();
  if (createdAt.slice(0, 19) !== value.slice(0, 19)) {
    throw new ChainWebhookError("invalid_chain_webhook_timestamp");
  }
  const age = now.getTime() - Date.parse(value);
  // Application freshness policy: Alchemy has no separately signed delivery-time header.
  // Its documentation labels createdAt ambiguously; owner setup must validate these semantics.
  if (age > ALCHEMY_MAX_AGE_MS || age < -ALCHEMY_FUTURE_TOLERANCE_MS) {
    throw new ChainWebhookError("stale_chain_webhook", 401);
  }
  return createdAt;
}

function word(value: unknown): string {
  if (typeof value !== "string" || !/^0x[0-9a-fA-F]{64}$/.test(value)) {
    throw new ChainWebhookError("invalid_chain_log_word");
  }
  return value.toLowerCase();
}

function address(value: unknown): string {
  if (typeof value !== "string" || !/^0x[0-9a-fA-F]{40}$/.test(value)) {
    throw new ChainWebhookError("invalid_chain_log_address");
  }
  return value.toLowerCase();
}

function topicAddress(value: string): string {
  if (!value.startsWith(`0x${"0".repeat(24)}`)) {
    throw new ChainWebhookError("invalid_chain_log_address_topic");
  }
  return `0x${value.slice(-40)}`;
}

function quantity(value: unknown): bigint {
  if (typeof value !== "string" || !/^0x(?:0|[1-9a-fA-F][0-9a-fA-F]{0,63})$/.test(value)) {
    throw new ChainWebhookError("invalid_chain_log_quantity");
  }
  return BigInt(value);
}

function signatureBytes(signature: string): Uint8Array<ArrayBuffer> {
  return Uint8Array.from(signature.match(/../g)!, (pair) => Number.parseInt(pair, 16));
}

function hex(value: Uint8Array): string {
  return Array.from(value, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
