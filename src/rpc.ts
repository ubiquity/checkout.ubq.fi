import {
  normalizeAddress,
  normalizeTransactionHash,
  type SupportedChainId,
} from "./payment_types.ts";
import { collectRpcBatchOutcomes } from "./rpc_batch.ts";

export const RPC_ORIGIN = "https://rpc.ubq.fi";
export const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

export type JsonRpcCall = {
  id: string;
  method: string;
  params: unknown[];
};

export type JsonRpcBatchItem = {
  jsonrpc?: unknown;
  id?: unknown;
  result?: unknown;
  error?: { code?: unknown; message?: unknown; data?: unknown };
};

export type RpcBatchLimits = {
  maximumRequestBytes: number;
  maximumResponseBytes: number;
};

export interface RpcClient {
  batch(
    chainId: SupportedChainId,
    calls: readonly JsonRpcCall[],
    limits?: RpcBatchLimits,
  ): Promise<JsonRpcBatchItem[]>;
}

export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export class HttpRpcClient implements RpcClient {
  constructor(private readonly fetcher: FetchLike = fetch) {}

  async batch(
    chainId: SupportedChainId,
    calls: readonly JsonRpcCall[],
    limits?: RpcBatchLimits,
  ): Promise<JsonRpcBatchItem[]> {
    if (calls.length === 0) throw new RpcError("rpc_batch_empty");
    const body = calls.map((call) => ({
      jsonrpc: "2.0",
      id: call.id,
      method: call.method,
      params: call.params,
    }));
    const serialized = JSON.stringify(body);
    if (limits && new TextEncoder().encode(serialized).byteLength > limits.maximumRequestBytes) {
      throw new RpcError("rpc_request_too_large");
    }
    let response: Response;
    try {
      response = await this.fetcher(`${RPC_ORIGIN}/${chainId}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: serialized,
      });
    } catch (error) {
      throw new RpcError("rpc_transport_failed", error);
    }
    if (!response.ok) {
      await response.body?.cancel();
      throw new RpcError(`rpc_http_${response.status}`);
    }
    let parsed: unknown;
    try {
      parsed = limits
        ? await boundedRpcJson(response, limits.maximumResponseBytes)
        : await response.json();
    } catch (error) {
      if (error instanceof RpcError) throw error;
      throw new RpcError("rpc_invalid_json", error);
    }
    if (!Array.isArray(parsed)) throw new RpcError("rpc_batch_response_required");
    return parsed as JsonRpcBatchItem[];
  }
}

export type TransferLocator = {
  transactionHash: string;
  logIndex: number;
  observedBlockNumber: bigint;
  observedBlockHash?: string;
};

export type ExpectedTransfer = TransferLocator & {
  tokenAddress: string;
  recipientAddress: string;
  amountUnits: bigint;
  previouslyVerifiedBlockHash?: string;
};

export type VerifiedTransfer = {
  transactionHash: string;
  logIndex: number;
  payerAddress: string;
  tokenAddress: string;
  recipientAddress: string;
  amountUnits: bigint;
  blockNumber: bigint;
  blockHash: string;
  blockTimestamp: Date;
  latestBlock: bigint;
  safeBlock: bigint | null;
  finalizedBlock: bigint | null;
};

export type ChainHeads = {
  latestBlock: bigint;
  safeBlock: bigint | null;
  finalizedBlock: bigint | null;
};

export type DiscoveredTransferLog = {
  transactionHash: string;
  logIndex: number;
  blockNumber: bigint;
  blockHash: string;
  tokenAddress: string;
  payerAddress: string;
  recipientAddress: string;
  amountUnits: bigint;
};

export async function latestBlockNumber(
  client: RpcClient,
  chainId: SupportedChainId,
): Promise<bigint> {
  const calls: JsonRpcCall[] = [
    {
      id: "latest",
      method: "eth_getBlockByNumber",
      params: ["latest", false],
    },
  ];
  const results = responseMap(await client.batch(chainId, calls), calls);
  return parseBlock(results.get("latest"), "latest").number;
}

export async function chainHeads(
  client: RpcClient,
  chainId: SupportedChainId,
): Promise<ChainHeads> {
  const calls: JsonRpcCall[] = ["latest", "safe", "finalized"].map((id) => ({
    id,
    method: "eth_getBlockByNumber",
    params: [id, false],
  }));
  return canonicalHeads(await canonicalResults(client, chainId, calls));
}

export async function discoverTransferLogs(
  client: RpcClient,
  chainId: SupportedChainId,
  tokenAddressInput: string,
  recipientAddressInput: string,
  fromBlock: bigint,
  toBlock: bigint,
): Promise<DiscoveredTransferLog[]> {
  if (fromBlock < 0n || toBlock < fromBlock) throw new RpcError("invalid_log_range");
  const tokenAddress = normalizeAddress(tokenAddressInput);
  const recipientAddress = normalizeAddress(recipientAddressInput);
  const calls: JsonRpcCall[] = [
    {
      id: "transfer_logs",
      method: "eth_getLogs",
      params: [
        {
          address: tokenAddress,
          fromBlock: toQuantity(fromBlock),
          toBlock: toQuantity(toBlock),
          topics: [TRANSFER_TOPIC, null, addressTopic(recipientAddress)],
        },
      ],
    },
  ];
  const results = responseMap(await client.batch(chainId, calls), calls);
  return parseTransferLogs(results.get("transfer_logs"), tokenAddress, recipientAddress);
}

export function parseTransferLogs(
  rawLogs: unknown,
  tokenAddressInput: string,
  recipientAddressInput: string,
  range?: { fromBlock: bigint; toBlock: bigint },
): DiscoveredTransferLog[] {
  const tokenAddress = normalizeAddress(tokenAddressInput);
  const recipientAddress = normalizeAddress(recipientAddressInput);
  if (!Array.isArray(rawLogs)) throw new RpcError("rpc_transfer_logs_array_required");
  if (
    range &&
    rawLogs.some((value) => !value || typeof value !== "object" || Array.isArray(value))
  ) {
    throw new RpcError("rpc_transfer_logs_invalid_item");
  }
  return rawLogs
    .filter(
      (value) =>
        !!value &&
        typeof value === "object" &&
        !Array.isArray(value) &&
        (value as Record<string, unknown>).removed !== true,
    )
    .map((value): DiscoveredTransferLog => {
      const log = value as Record<string, unknown>;
      if (normalizeAddressForVerification(log.address) !== tokenAddress) {
        throw new RpcError("rpc_transfer_log_token_mismatch");
      }
      if (!Array.isArray(log.topics) || log.topics.length !== 3) {
        throw new RpcError("rpc_transfer_log_topics_invalid");
      }
      const topics = log.topics.map((topic) => hexWord(topic, "invalid_transfer_topic"));
      if (topics[0] !== TRANSFER_TOPIC) throw new RpcError("rpc_transfer_log_signature_invalid");
      const logIndex = parseQuantity(log.logIndex, "invalid_log_index");
      if (logIndex > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new RpcError("rpc_transfer_log_index_too_large");
      }
      const discoveredRecipient = topicAddress(topics[2], "invalid_transfer_recipient");
      if (discoveredRecipient !== recipientAddress) {
        throw new RpcError("rpc_transfer_log_recipient_mismatch");
      }
      const blockNumber = parseQuantity(log.blockNumber, "invalid_log_block_number");
      if (range && (blockNumber < range.fromBlock || blockNumber > range.toBlock)) {
        throw new RpcError("rpc_transfer_log_outside_range");
      }
      return {
        transactionHash: hexHash(log.transactionHash, "invalid_log_transaction_hash"),
        logIndex: Number(logIndex),
        blockNumber,
        blockHash: hexHash(log.blockHash, "invalid_log_block_hash"),
        tokenAddress,
        payerAddress: topicAddress(topics[1], "invalid_transfer_payer"),
        recipientAddress: discoveredRecipient,
        amountUnits: parseWord(log.data, "invalid_transfer_amount"),
      };
    });
}

export async function verifyTransferBatch(
  client: RpcClient,
  chainId: SupportedChainId,
  expectedInput: ExpectedTransfer,
): Promise<VerifiedTransfer> {
  const transactionHash = normalizeTransactionHash(expectedInput.transactionHash);
  const tokenAddress = normalizeAddress(expectedInput.tokenAddress);
  const recipientAddress = normalizeAddress(expectedInput.recipientAddress);
  const { logIndex, observedBlockNumber, amountUnits } = expectedInput;
  if (!Number.isSafeInteger(logIndex) || logIndex < 0) {
    throw new RpcVerificationError("invalid_log_index");
  }
  if (observedBlockNumber < 0n) {
    throw new RpcVerificationError("invalid_observed_block_number");
  }
  if (amountUnits <= 0n) throw new RpcVerificationError("invalid_expected_amount");
  if (
    expectedInput.observedBlockHash !== undefined &&
    !/^0x[0-9a-fA-F]{64}$/.test(expectedInput.observedBlockHash)
  ) {
    throw new RpcVerificationError("invalid_observed_block_hash");
  }
  const previouslyVerifiedBlockHash =
    expectedInput.previouslyVerifiedBlockHash === undefined
      ? undefined
      : hexHash(
          expectedInput.previouslyVerifiedBlockHash,
          "invalid_previously_verified_block_hash",
        );

  const calls: JsonRpcCall[] = [
    {
      id: "receipt",
      method: "eth_getTransactionReceipt",
      params: [transactionHash],
    },
    {
      id: "event_block",
      method: "eth_getBlockByNumber",
      params: [toQuantity(observedBlockNumber), false],
    },
    {
      id: "latest",
      method: "eth_getBlockByNumber",
      params: ["latest", false],
    },
    {
      id: "safe",
      method: "eth_getBlockByNumber",
      params: ["safe", false],
    },
    {
      id: "finalized",
      method: "eth_getBlockByNumber",
      params: ["finalized", false],
    },
  ];

  // Preserve essential successes when an optional head fails; retry only failed head IDs once.
  const results = await canonicalResults(client, chainId, calls);
  const eventBlock = parseBlock(results.get("event_block"), "event_block");
  if (
    previouslyVerifiedBlockHash !== undefined &&
    (eventBlock.number !== observedBlockNumber || eventBlock.hash !== previouslyVerifiedBlockHash)
  ) {
    throw new RpcVerificationError("non_canonical_block", true);
  }
  const receipt = objectResult(results.get("receipt"), "receipt_not_found");
  const receiptTransactionHash = hexHash(
    receipt.transactionHash,
    "invalid_receipt_transaction_hash",
  );
  if (receiptTransactionHash !== transactionHash) {
    throw new RpcVerificationError("receipt_transaction_hash_mismatch");
  }
  if (parseQuantity(receipt.status, "invalid_receipt_status") !== 1n) {
    throw new RpcVerificationError("transaction_failed");
  }
  const receiptBlockNumber = parseQuantity(receipt.blockNumber, "invalid_receipt_block_number");
  if (receiptBlockNumber !== observedBlockNumber) {
    throw new RpcVerificationError("observed_block_number_mismatch");
  }
  const receiptBlockHash = hexHash(receipt.blockHash, "invalid_receipt_block_hash");

  if (eventBlock.number !== receiptBlockNumber || eventBlock.hash !== receiptBlockHash) {
    throw new RpcVerificationError("non_canonical_block");
  }

  if (!Array.isArray(receipt.logs)) {
    throw new RpcVerificationError("receipt_logs_missing");
  }
  const rawLog = receipt.logs.find((value) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) return false;
    try {
      return (
        parseQuantity((value as Record<string, unknown>).logIndex, "invalid_log_index") ===
        BigInt(logIndex)
      );
    } catch {
      return false;
    }
  });
  if (!rawLog || typeof rawLog !== "object" || Array.isArray(rawLog)) {
    throw new RpcVerificationError("transfer_log_not_found");
  }
  const log = rawLog as Record<string, unknown>;
  if (log.removed === true) {
    throw new RpcVerificationError("transfer_log_removed");
  }
  if (normalizeAddressForVerification(log.address) !== tokenAddress) {
    throw new RpcVerificationError("wrong_token_address");
  }
  if (hexHash(log.transactionHash, "invalid_log_transaction_hash") !== transactionHash) {
    throw new RpcVerificationError("log_transaction_hash_mismatch");
  }
  if (parseQuantity(log.blockNumber, "invalid_log_block_number") !== receiptBlockNumber) {
    throw new RpcVerificationError("log_block_number_mismatch");
  }
  if (hexHash(log.blockHash, "invalid_log_block_hash") !== receiptBlockHash) {
    throw new RpcVerificationError("log_block_hash_mismatch");
  }

  if (!Array.isArray(log.topics) || log.topics.length !== 3) {
    throw new RpcVerificationError("invalid_transfer_topics");
  }
  const topics = log.topics.map((topic) => hexWord(topic, "invalid_transfer_topic"));
  if (topics[0] !== TRANSFER_TOPIC) {
    throw new RpcVerificationError("wrong_event_signature");
  }
  const payerAddress = topicAddress(topics[1], "invalid_transfer_payer");
  if (topics[2] !== addressTopic(recipientAddress)) {
    throw new RpcVerificationError("wrong_transfer_recipient");
  }
  const loggedAmount = parseWord(log.data, "invalid_transfer_amount");
  if (loggedAmount !== amountUnits) {
    throw new RpcVerificationError("wrong_transfer_amount");
  }

  const heads = canonicalHeads(results);
  if (heads.latestBlock < receiptBlockNumber) throw new RpcError("rpc_incoherent_chain_heads");
  return {
    transactionHash,
    logIndex,
    payerAddress,
    tokenAddress,
    recipientAddress,
    amountUnits,
    blockNumber: receiptBlockNumber,
    blockHash: receiptBlockHash,
    blockTimestamp: new Date(Number(eventBlock.timestamp * 1000n)),
    ...heads,
  };
}

async function canonicalResults(
  client: RpcClient,
  chainId: SupportedChainId,
  calls: readonly JsonRpcCall[],
): Promise<Map<string, unknown>> {
  const first = collectRpcBatchOutcomes(calls, await client.batch(chainId, calls));
  const results = new Map<string, unknown>();
  for (const outcome of first.outcomes) {
    if (outcome.status === "success") results.set(outcome.id, outcome.result);
    else if (!["latest", "safe", "finalized"].includes(outcome.id))
      throw new RpcError(`rpc_${outcome.reason}:${outcome.id}`);
  }
  const failedHeads = calls.filter((call) => !results.has(call.id));
  if (failedHeads.length) {
    try {
      const retry = collectRpcBatchOutcomes(failedHeads, await client.batch(chainId, failedHeads));
      for (const outcome of retry.outcomes)
        if (outcome.status === "success") results.set(outcome.id, outcome.result);
    } catch (error) {
      if (!(error instanceof RpcError)) throw error;
      // A failed optional retry cannot discard an already verified receipt/latest response.
    }
  }
  if (!results.has("latest")) throw new RpcError("rpc_missing_response:latest");
  return results;
}

function canonicalHeads(results: Map<string, unknown>): ChainHeads {
  const latest = parseBlock(results.get("latest"), "latest");
  const optional = (id: string): bigint | null => {
    try {
      const head = parseOptionalBlock(results.get(id), id);
      return head && head.number <= latest.number ? head.number : null;
    } catch (error) {
      if (!(error instanceof RpcVerificationError)) throw error;
      return null;
    }
  };
  const finalizedBlock = optional("finalized");
  let safeBlock = optional("safe");
  // A stale safe response must not invalidate independently useful finalized evidence.
  if (safeBlock !== null && finalizedBlock !== null && safeBlock < finalizedBlock) safeBlock = null;
  return { latestBlock: latest.number, safeBlock, finalizedBlock };
}

export class RpcError extends Error {
  constructor(
    public readonly code: string,
    public override readonly cause?: unknown,
  ) {
    super(code);
    this.name = "RpcError";
  }
}

export class RpcVerificationError extends Error {
  constructor(
    public readonly code: string,
    public readonly canonicalContradiction = false,
  ) {
    super(code);
    this.name = "RpcVerificationError";
  }
}

function responseMap(
  responses: JsonRpcBatchItem[],
  calls: readonly JsonRpcCall[],
): Map<string, unknown> {
  const expected = new Set(calls.map((call) => call.id));
  const results = new Map<string, unknown>();
  for (const response of responses) {
    if (!response || typeof response !== "object" || response.jsonrpc !== "2.0") {
      throw new RpcError("rpc_invalid_batch_item");
    }
    if (typeof response.id !== "string" || !expected.has(response.id)) {
      throw new RpcError("rpc_unexpected_response_id");
    }
    if (results.has(response.id)) throw new RpcError("rpc_duplicate_response_id");
    if (response.error !== undefined) {
      const message =
        typeof response.error?.message === "string"
          ? response.error.message
          : "unknown upstream error";
      throw new RpcError(`rpc_upstream_error:${response.id}:${message}`);
    }
    results.set(response.id, response.result);
  }
  for (const id of expected) {
    if (!results.has(id)) throw new RpcError(`rpc_missing_response:${id}`);
  }
  return results;
}

function objectResult(value: unknown, missingCode: string): Record<string, unknown> {
  if (value === null || value === undefined) throw new RpcVerificationError(missingCode);
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new RpcVerificationError(`invalid_${missingCode}`);
  }
  return value as Record<string, unknown>;
}

function parseBlock(
  value: unknown,
  label: string,
): {
  number: bigint;
  hash: string;
  timestamp: bigint;
} {
  const block = objectResult(value, `${label}_block_not_found`);
  return {
    number: parseQuantity(block.number, `invalid_${label}_block_number`),
    hash: hexHash(block.hash, `invalid_${label}_block_hash`),
    timestamp: parseQuantity(block.timestamp, `invalid_${label}_block_timestamp`),
  };
}

function parseOptionalBlock(
  value: unknown,
  label: string,
): { number: bigint; hash: string; timestamp: bigint } | null {
  if (value === null) return null;
  return parseBlock(value, label);
}

function normalizeAddressForVerification(value: unknown): string {
  try {
    return normalizeAddress(value);
  } catch {
    throw new RpcVerificationError("invalid_log_address");
  }
}

function parseQuantity(value: unknown, code: string): bigint {
  if (typeof value !== "string" || !/^0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)$/.test(value)) {
    throw new RpcVerificationError(code);
  }
  return BigInt(value);
}

function parseWord(value: unknown, code: string): bigint {
  return BigInt(hexWord(value, code));
}

function hexHash(value: unknown, code: string): string {
  if (typeof value !== "string" || !/^0x[0-9a-fA-F]{64}$/.test(value)) {
    throw new RpcVerificationError(code);
  }
  return value.toLowerCase();
}

function hexWord(value: unknown, code: string): string {
  if (typeof value !== "string" || !/^0x[0-9a-fA-F]{64}$/.test(value)) {
    throw new RpcVerificationError(code);
  }
  return value.toLowerCase();
}

function addressTopic(address: string): string {
  return `0x${"0".repeat(24)}${address.slice(2)}`;
}

function topicAddress(topic: string, code: string): string {
  if (!topic.startsWith(`0x${"0".repeat(24)}`)) throw new RpcVerificationError(code);
  return `0x${topic.slice(-40)}`;
}

function toQuantity(value: bigint): string {
  return `0x${value.toString(16)}`;
}

async function boundedRpcJson(response: Response, maximum: number): Promise<unknown> {
  if (!Number.isSafeInteger(maximum) || maximum <= 0) throw new RpcError("rpc_invalid_byte_limit");
  const declared = response.headers.get("content-length");
  if (declared && /^\d+$/.test(declared) && Number(declared) > maximum) {
    await response.body?.cancel();
    throw new RpcError("rpc_response_too_large");
  }
  if (!response.body) throw new RpcError("rpc_invalid_json");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > maximum) {
        await reader.cancel();
        throw new RpcError("rpc_response_too_large");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return JSON.parse(new TextDecoder().decode(bytes));
}
