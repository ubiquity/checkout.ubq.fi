import { expect, test } from "bun:test";
import { collectRpcBatchOutcomes } from "./rpc_batch.ts";
import { type JsonRpcCall, HttpRpcClient } from "./rpc.ts";

const calls: JsonRpcCall[] = ["a", "b"].map((id) => ({ id, method: "eth_getLogs", params: [] }));
const success = (id: unknown, result: unknown = []) => ({ jsonrpc: "2.0", id, result });

test("batch collector attributes unordered results by exact id", () => {
  const result = collectRpcBatchOutcomes(calls, [success("b", [2]), success("a", [1])]);
  expect(result.outcomes).toEqual([
    { id: "a", status: "success", result: [1] },
    { id: "b", status: "success", result: [2] },
  ]);
});

test("entry errors and missing responses preserve successful siblings", () => {
  for (const responses of [
    [success("a")],
    [
      success("a"),
      {
        jsonrpc: "2.0",
        id: "b",
        error: { code: -32000, message: "unavailable" },
      },
    ],
  ]) {
    const result = collectRpcBatchOutcomes(calls, responses);
    expect(result.outcomes[0]).toEqual({ id: "a", status: "success", result: [] });
    expect(result.outcomes[1].status).toBe("failed");
  }
});

test("duplicate expected ids are ambiguous even if another response succeeds", () => {
  const result = collectRpcBatchOutcomes(calls, [success("a"), success("a"), success("b")]);
  expect(result.outcomes[0]).toEqual({ id: "a", status: "failed", reason: "duplicate_response" });
  expect(result.outcomes[1].status).toBe("success");
});

test("unknown, numeric and null ids cannot satisfy missing string ids", () => {
  const result = collectRpcBatchOutcomes(calls, [
    success("foreign"),
    success(null),
    success(1),
    success("a"),
  ]);
  expect(result.outcomes[0].status).toBe("success");
  expect(result.outcomes[1]).toEqual({ id: "b", status: "failed", reason: "missing_response" });
  expect(result.diagnostics).toContain("unexpected_response_id");
});

test("malformed responses require version and exactly one valid result or error", () => {
  const invalid = [
    { id: "a", result: [] },
    { jsonrpc: "2.0", id: "a" },
    { ...success("a"), error: { code: 1, message: "bad" } },
    { jsonrpc: "2.0", id: "a", error: { code: 1.5, message: "bad" } },
    { jsonrpc: "2.0", id: "a", error: { code: 1 } },
  ];
  for (const item of invalid) {
    expect(collectRpcBatchOutcomes(calls, [item, success("b")]).outcomes[0]).toEqual({
      id: "a",
      status: "failed",
      reason: "malformed_response",
    });
  }
});

test("a non-array reply fails the whole batch and empty replies mean missing ids", () => {
  expect(
    collectRpcBatchOutcomes(calls, success("a")).outcomes.every((item) => item.status === "failed"),
  ).toBe(true);
  expect(collectRpcBatchOutcomes(calls, []).outcomes).toEqual(
    calls.map(({ id }) => ({ id, status: "failed", reason: "missing_response" })),
  );
  expect(() => collectRpcBatchOutcomes([], [])).toThrow("rpc_batch_empty");
  expect(() => collectRpcBatchOutcomes([calls[0], calls[0]], [])).toThrow(
    "rpc_duplicate_request_id",
  );
});

test("bounded HTTP batches reject request bytes before fetch and retain legacy two-argument use", async () => {
  let requests = 0;
  const client = new HttpRpcClient(() => {
    requests++;
    return Promise.resolve(Response.json(calls.map(({ id }) => success(id))));
  });
  await expect(
    client.batch(8453, calls, { maximumRequestBytes: 2, maximumResponseBytes: 1024 }),
  ).rejects.toThrow("rpc_request_too_large");
  expect(requests).toBe(0);
  expect(await client.batch(8453, calls)).toHaveLength(2);
  expect(requests).toBe(1);
});

test("bounded HTTP reads enforce actual streamed bytes and cancel oversized bodies", async () => {
  let cancelled = false;
  const client = new HttpRpcClient(() =>
    Promise.resolve(
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("x".repeat(200)));
          },
          cancel() {
            cancelled = true;
          },
        }),
      ),
    ),
  );
  await expect(
    client.batch(8453, calls, { maximumRequestBytes: 1024, maximumResponseBytes: 100 }),
  ).rejects.toThrow("rpc_response_too_large");
  expect(cancelled).toBe(true);
});

test("HTTP errors, malformed JSON and non-array replies fail whole HTTP batches", async () => {
  for (const response of [
    new Response("failed", { status: 503 }),
    new Response("["),
    Response.json(success("a")),
  ]) {
    const client = new HttpRpcClient(() => Promise.resolve(response));
    await expect(client.batch(8453, calls)).rejects.toThrow();
  }
  const client = new HttpRpcClient(() => {
    throw new Error("must not fetch");
  });
  await expect(client.batch(8453, [])).rejects.toThrow("rpc_batch_empty");
});
