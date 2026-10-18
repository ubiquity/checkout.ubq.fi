import type { SupportedChainId } from "./payment_types.ts";
import type { DiscoveredTransferLog } from "./rpc.ts";

/** A signed indexer observation; canonical RPC verification is still required before payment. */
export type ChainTransferEvent = DiscoveredTransferLog & {
  chainId: SupportedChainId;
  removed: boolean;
};

export type VerifiedChainEventDelivery = {
  provider: string;
  subscriptionId: string;
  eventId: string;
  /** Application identity derived from provider, subscription, and event ID, not a transport nonce. */
  deliveryId: string;
  createdAt: string;
  /** SHA-256 of the exact signature-verified body, for durable replay conflict detection. */
  bodySha256: string;
  transfers: ChainTransferEvent[];
};

export class ChainWebhookError extends Error {
  constructor(
    public readonly code: string,
    public readonly status: number = 400,
  ) {
    super(code);
    this.name = "ChainWebhookError";
  }
}
