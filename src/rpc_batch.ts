import { type JsonRpcCall, RpcError } from "./rpc.ts";

export type RpcBatchOutcome =
  | { id: string; status: "success"; result: unknown }
  | { id: string; status: "failed"; reason: string; errorCode?: number };

/** Preserve valid siblings; arbitrary response ordering never changes attribution. */
export function collectRpcBatchOutcomes(
  calls: readonly JsonRpcCall[],
  responses: unknown,
): { outcomes: RpcBatchOutcome[]; diagnostics: string[] } {
  if (!calls.length) throw new RpcError("rpc_batch_empty");
  const expected = new Set(calls.map((call) => call.id));
  if (expected.size !== calls.length) throw new RpcError("rpc_duplicate_request_id");
  const results = new Map<string, RpcBatchOutcome>();
  const diagnostics = new Set<string>();
  if (!Array.isArray(responses)) {
    return {
      outcomes: calls.map(({ id }) => ({
        id,
        status: "failed",
        reason: "batch_response_required",
      })),
      diagnostics: ["batch_response_required"],
    };
  }
  const seen = new Set<string>();
  for (const item of responses) {
    if (!record(item)) {
      diagnostics.add("invalid_batch_item");
      continue;
    }
    const id = item.id;
    if (typeof id !== "string" || !expected.has(id)) {
      diagnostics.add("unexpected_response_id");
      continue;
    }
    if (seen.has(id)) {
      results.set(id, { id, status: "failed", reason: "duplicate_response" });
      continue;
    }
    seen.add(id);
    const hasResult = Object.hasOwn(item, "result");
    const hasError = Object.hasOwn(item, "error");
    if (item.jsonrpc !== "2.0" || hasResult === hasError) {
      results.set(id, { id, status: "failed", reason: "malformed_response" });
    } else if (hasError) {
      const error = item.error;
      if (!record(error) || !Number.isInteger(error.code) || typeof error.message !== "string") {
        results.set(id, { id, status: "failed", reason: "malformed_response" });
      } else {
        results.set(id, {
          id,
          status: "failed",
          reason: "upstream_error",
          errorCode: error.code as number,
        });
      }
    } else {
      results.set(id, { id, status: "success", result: item.result });
    }
  }
  return {
    outcomes: calls.map(
      ({ id }) => results.get(id) ?? { id, status: "failed", reason: "missing_response" },
    ),
    diagnostics: [...diagnostics],
  };
}

function record(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
