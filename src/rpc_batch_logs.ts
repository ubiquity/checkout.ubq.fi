import { normalizeAddress, type SupportedChainId } from "./payment_types.ts";
import {
  type DiscoveredTransferLog,
  type JsonRpcCall,
  type RpcClient,
  parseTransferLogs,
  RpcError,
  TRANSFER_TOPIC,
} from "./rpc.ts";
import { collectRpcBatchOutcomes } from "./rpc_batch.ts";

export type TransferLogQuery = {
  tokenAddress: string;
  recipientAddress: string;
  fromBlock: bigint;
};
export type TransferLogRangeOutcome = {
  fromBlock: bigint;
  toBlock: bigint;
  status: "complete" | "failed" | "unsearched";
  logs: DiscoveredTransferLog[];
  attempts: number;
  reason?: string;
};
export type TransferLogQueryOutcome = {
  queryIndex: number;
  fromBlock: bigint;
  searchedToBlock?: bigint;
  reachedHead: boolean;
  ranges: TransferLogRangeOutcome[];
};
export type TransferLogHuntOptions = {
  toBlock: bigint;
  maximumCalls: number;
  maximumEntries?: number;
  maximumRequestBytes?: number;
  maximumResponseBytes?: number;
  observedBatchLimit?: number;
  canRequest?: () => boolean;
};
export type TransferLogHuntResult = {
  targetHead: bigint;
  callsUsed: number;
  reachedHead: boolean;
  queries: TransferLogQueryOutcome[];
  diagnostics: string[];
  observedBatchLimit?: number;
};

type PendingRange = { queryIndex: number; call: JsonRpcCall; outcome: TransferLogRangeOutcome };

/** The caller captures latest separately; JSON-RPC batches have no result references. */
export async function huntTransferLogs(
  client: RpcClient,
  chainId: SupportedChainId,
  input: readonly TransferLogQuery[],
  options: TransferLogHuntOptions,
): Promise<TransferLogHuntResult> {
  const maximumEntries = positive(options.maximumEntries ?? 256);
  const maximumRequestBytes = positive(options.maximumRequestBytes ?? 256 * 1024);
  const maximumResponseBytes = positive(options.maximumResponseBytes ?? 1024 * 1024);
  if (
    options.toBlock < 0n ||
    !Number.isSafeInteger(options.maximumCalls) ||
    options.maximumCalls < 0
  ) {
    throw new RpcError("rpc_invalid_hunt_limits");
  }
  let batchLimit = Math.min(
    maximumEntries,
    options.observedBatchLimit === undefined
      ? maximumEntries
      : positive(options.observedBatchLimit),
  );
  let observedBatchLimit = options.observedBatchLimit;
  const queries = input.map((query) => {
    if (query.fromBlock < 0n) throw new RpcError("invalid_log_range");
    return {
      ...query,
      tokenAddress: normalizeAddress(query.tokenAddress),
      recipientAddress: normalizeAddress(query.recipientAddress),
    };
  });
  const results: TransferLogQueryOutcome[] = queries.map((query, queryIndex) => ({
    queryIndex,
    fromBlock: query.fromBlock,
    reachedHead: query.fromBlock > options.toBlock,
    ranges: [],
  }));
  const cursors = queries.map((query) => query.fromBlock);
  const diagnostics = new Set<string>();
  const prefix = crypto.randomUUID();
  let serial = 0;
  let callsUsed = 0;
  let queryIndex = 0;
  let halted: string | undefined;
  const available = () => callsUsed < options.maximumCalls && (options.canRequest?.() ?? true);

  function nextPage(): PendingRange[] {
    const page: PendingRange[] = [];
    let bytes = 2;
    while (queryIndex < queries.length && page.length < batchLimit) {
      const query = queries[queryIndex];
      const fromBlock = cursors[queryIndex];
      if (fromBlock > options.toBlock) {
        queryIndex++;
        continue;
      }
      const toBlock = fromBlock + 999n < options.toBlock ? fromBlock + 999n : options.toBlock;
      const call: JsonRpcCall = {
        id: `${prefix}:${serial++}`,
        method: "eth_getLogs",
        params: [
          {
            address: query.tokenAddress,
            fromBlock: quantity(fromBlock),
            toBlock: quantity(toBlock),
            topics: [TRANSFER_TOPIC, null, `0x${"0".repeat(24)}${query.recipientAddress.slice(2)}`],
          },
        ],
      };
      const size = encodedCallBytes(call) + (page.length ? 1 : 0);
      if (bytes + size > maximumRequestBytes) {
        if (!page.length) halted = "request_byte_limit";
        break;
      }
      bytes += size;
      const outcome: TransferLogRangeOutcome = {
        fromBlock,
        toBlock,
        status: "unsearched",
        logs: [],
        attempts: 0,
      };
      results[queryIndex].ranges.push(outcome);
      page.push({ queryIndex, call, outcome });
      cursors[queryIndex] = toBlock + 1n;
    }
    return page;
  }

  async function run(entries: PendingRange[]): Promise<void> {
    const pending = [...entries];
    while (pending.length && available()) {
      const group = pending.splice(0, batchLimit);
      callsUsed++;
      let responses: unknown;
      try {
        responses = await client.batch(
          chainId,
          group.map((entry) => entry.call),
          {
            maximumRequestBytes,
            maximumResponseBytes,
          },
        );
      } catch (error) {
        // HTTP 413 and our local byte guard are size evidence, not vendor error strings.
        const oversized =
          error instanceof RpcError &&
          (error.code === "rpc_http_413" || error.code === "rpc_response_too_large");
        if (oversized && group.length > 1) {
          batchLimit = Math.max(1, Math.floor(group.length / 2));
          observedBatchLimit = batchLimit;
          diagnostics.add("batch_size_split");
          pending.unshift(...group);
          continue;
        }
        const reason = error instanceof RpcError ? error.code : "rpc_batch_failed";
        for (const entry of group) fail(entry, reason);
        continue;
      }
      const collected = collectRpcBatchOutcomes(
        group.map((entry) => entry.call),
        responses,
      );
      for (const diagnostic of collected.diagnostics) diagnostics.add(diagnostic);
      for (let index = 0; index < group.length; index++) {
        const entry = group[index];
        const outcome = collected.outcomes[index];
        if (outcome.status === "failed") {
          fail(entry, outcome.reason);
          continue;
        }
        const query = queries[entry.queryIndex];
        try {
          entry.outcome.logs = parseTransferLogs(
            outcome.result,
            query.tokenAddress,
            query.recipientAddress,
            entry.outcome,
          );
          entry.outcome.status = "complete";
          entry.outcome.attempts++;
          delete entry.outcome.reason;
        } catch {
          fail(entry, "invalid_transfer_logs");
        }
      }
    }
    if (pending.length) {
      diagnostics.add("call_budget_exhausted");
      for (const entry of pending) {
        if (entry.outcome.status === "unsearched") entry.outcome.reason = "call_budget_exhausted";
      }
    }
  }

  while (queryIndex < queries.length && available() && !halted) {
    const page = nextPage();
    if (!page.length) break;
    await run(page);
    // Size probes do not reset the one retry allowance for failed entries.
    const retry = page.filter(
      (entry) => entry.outcome.status === "failed" && entry.outcome.attempts === 1,
    );
    if (retry.length) await run(retry);
  }
  if (!available() && cursors.some((cursor) => cursor <= options.toBlock)) {
    halted ??= "call_budget_exhausted";
  }
  for (let index = 0; index < queries.length; index++) {
    const result = results[index];
    if (cursors[index] <= options.toBlock) {
      result.ranges.push({
        fromBlock: cursors[index],
        toBlock: options.toBlock,
        status: "unsearched",
        logs: [],
        attempts: 0,
        reason: halted ?? "not_searched",
      });
    }
    let next = result.fromBlock;
    for (const range of result.ranges) {
      if (range.status !== "complete" || range.fromBlock !== next) break;
      result.searchedToBlock = range.toBlock;
      next = range.toBlock + 1n;
    }
    result.reachedHead = next > options.toBlock;
  }
  if (halted) diagnostics.add(halted);
  return {
    targetHead: options.toBlock,
    callsUsed,
    reachedHead: results.every((result) => result.reachedHead),
    queries: results,
    diagnostics: [...diagnostics],
    ...(observedBatchLimit === undefined ? {} : { observedBatchLimit }),
  };
}

function fail(entry: PendingRange, reason: string): void {
  entry.outcome.status = "failed";
  entry.outcome.attempts++;
  entry.outcome.reason = reason;
}
function positive(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RpcError("rpc_invalid_hunt_limits");
  return value;
}
function quantity(value: bigint): string {
  return `0x${value.toString(16)}`;
}
function encodedCallBytes(call: JsonRpcCall): number {
  return new TextEncoder().encode(
    JSON.stringify({ jsonrpc: "2.0", id: call.id, method: call.method, params: call.params }),
  ).byteLength;
}
