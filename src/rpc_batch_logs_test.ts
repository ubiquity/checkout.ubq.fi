import { expect, test } from "bun:test";
import { CHAIN_CONFIGURATIONS } from "./payment_types.ts";
import { HttpRpcClient, type JsonRpcCall, RpcError, TRANSFER_TOPIC } from "./rpc.ts";
import { huntTransferLogs, type TransferLogHuntOptions } from "./rpc_batch_logs.ts";

const token = CHAIN_CONFIGURATIONS[8453].tokenAddress;
const treasury = `0x${"44".repeat(20)}`;
const query = { tokenAddress: token, recipientAddress: treasury, fromBlock: 1n };
const ok = (call: JsonRpcCall, result: unknown = []) => ({ jsonrpc: "2.0", id: call.id, result });
const error = (call: JsonRpcCall) => ({
  jsonrpc: "2.0",
  id: call.id,
  error: { code: -32000, message: "temporary failure" },
});

function fake(respond: (calls: JsonRpcCall[], count: number) => Response) {
  const requests: JsonRpcCall[][] = [];
  const client = new HttpRpcClient((_url, init) => {
    const calls: JsonRpcCall[] = JSON.parse(String(init?.body));
    requests.push(calls);
    return Promise.resolve(respond(calls, requests.length));
  });
  return { client, requests };
}
function options(
  toBlock: bigint,
  changes: Partial<TransferLogHuntOptions> = {},
): TransferLogHuntOptions {
  return { toBlock, maximumCalls: 20, ...changes };
}

for (const chunks of [2, 9, 44]) {
  test(`${chunks} chunks use one initial standard batch with opaque unique ids`, async () => {
    const { client, requests } = fake((calls) =>
      Response.json([...calls].reverse().map((call) => ok(call))),
    );
    const result = await huntTransferLogs(client, 8453, [query], options(BigInt(chunks * 1000)));
    expect(result.reachedHead).toBe(true);
    expect(result.callsUsed).toBe(1);
    expect(requests[0]).toHaveLength(chunks);
    expect(new Set(requests[0].map((call) => call.id)).size).toBe(chunks);
    for (const call of requests[0]) {
      expect(call.method).toBe("eth_getLogs");
      const range = call.params[0] as { fromBlock: string; toBlock: string };
      expect(BigInt(range.toBlock) - BigInt(range.fromBlock) + 1n).toBeLessThanOrEqual(1000n);
      expect(JSON.stringify(call)).not.toContain("customer");
    }
  });
}

test("fixed subset errors retry only failed ids once and preserve successful ranges", async () => {
  let failed: string[] = [];
  const { client, requests } = fake((calls, count) => {
    if (count === 1)
      failed = calls.filter((_call, index) => index % 9 === 0).map((call) => call.id);
    return Response.json(
      [...calls]
        .reverse()
        .map((call) => (count === 1 && failed.includes(call.id) ? error(call) : ok(call))),
    );
  });
  const result = await huntTransferLogs(client, 8453, [query], options(44_000n));
  expect(requests).toHaveLength(2);
  expect(requests[1].map((call) => call.id)).toEqual(failed);
  expect(result.reachedHead).toBe(true);
  expect(result.queries[0].ranges.filter((range) => range.attempts === 2)).toHaveLength(
    failed.length,
  );
});

test("a persistent hole stops contiguous coverage without losing newer successes", async () => {
  const { client, requests } = fake((calls) =>
    Response.json(
      calls.map((call) => {
        const from = (call.params[0] as { fromBlock: string }).fromBlock;
        return from === "0x3e9" ? error(call) : ok(call);
      }),
    ),
  );
  const result = await huntTransferLogs(client, 8453, [query], options(3000n));
  expect(requests).toHaveLength(2);
  expect(requests[1]).toHaveLength(1);
  expect(result.queries[0].searchedToBlock).toBe(1000n);
  expect(result.queries[0].ranges.map((range) => range.status)).toEqual([
    "complete",
    "failed",
    "complete",
  ]);
  expect(result.queries[0].ranges[1].attempts).toBe(2);
  expect(result.reachedHead).toBe(false);
});

test("missing ids are retried and duplicate ids remain failed when repeated", async () => {
  for (const duplicate of [false, true]) {
    const { client, requests } = fake((calls, count) => {
      if (count === 1) return Response.json([ok(calls[0]), ...(duplicate ? [ok(calls[0])] : [])]);
      return Response.json(
        calls.flatMap((call) => (duplicate ? [ok(call), ok(call)] : [ok(call)])),
      );
    });
    const result = await huntTransferLogs(client, 8453, [query], options(2000n));
    expect(requests).toHaveLength(2);
    expect(result.reachedHead).toBe(!duplicate);
  }
});

test("per-post entry caps lazily cover the entire span in the same hunt", async () => {
  const { client, requests } = fake((calls) => Response.json(calls.map((call) => ok(call))));
  const result = await huntTransferLogs(
    client,
    8453,
    [query],
    options(7000n, { maximumEntries: 3 }),
  );
  expect(requests.map((calls) => calls.length)).toEqual([3, 3, 1]);
  expect(result.queries[0].searchedToBlock).toBe(7000n);
  expect(result.reachedHead).toBe(true);
});

test("call exhaustion retains a compact unsearched tail for huge spans", async () => {
  const { client, requests } = fake((calls) => Response.json(calls.map((call) => ok(call))));
  const result = await huntTransferLogs(
    client,
    8453,
    [query],
    options(10n ** 30n, { maximumEntries: 2, maximumCalls: 1 }),
  );
  expect(requests).toHaveLength(1);
  expect(result.queries[0].ranges).toHaveLength(3);
  expect(result.queries[0].ranges[2]).toMatchObject({
    fromBlock: 2001n,
    toBlock: 10n ** 30n,
    status: "unsearched",
    attempts: 0,
  });
  expect(result.queries[0].searchedToBlock).toBe(2000n);
});

test("generic HTTP 413 reduces unknown batch size and repacks at an observed-safe cap", async () => {
  const { client, requests } = fake((calls) =>
    calls.length > 3
      ? new Response("too large", { status: 413 })
      : Response.json(calls.map((call) => ok(call))),
  );
  const result = await huntTransferLogs(client, 8453, [query], options(9000n));
  expect(requests.map((calls) => calls.length)).toEqual([9, 4, 2, 2, 2, 2, 1]);
  expect(result.reachedHead).toBe(true);
  expect(result.observedBatchLimit).toBe(2);
  expect(result.queries[0].ranges.every((range) => range.attempts === 1)).toBe(true);
});

test("an observed cap packs the fewest currently admitted batches", async () => {
  const { client, requests } = fake((calls) => Response.json(calls.map((call) => ok(call))));
  const result = await huntTransferLogs(
    client,
    8453,
    [query],
    options(9000n, { observedBatchLimit: 4 }),
  );
  expect(requests.map((calls) => calls.length)).toEqual([4, 4, 1]);
  expect(result.reachedHead).toBe(true);
});

test("size splitting preserves the single retry allowance for a persistent entry error", async () => {
  const { client } = fake((calls) =>
    calls.length > 2
      ? new Response("large", { status: 413 })
      : Response.json(
          calls.map((call) =>
            (call.params[0] as { fromBlock: string }).fromBlock === "0x1" ? error(call) : ok(call),
          ),
        ),
  );
  const result = await huntTransferLogs(client, 8453, [query], options(4000n));
  expect(result.queries[0].ranges[0]).toMatchObject({ status: "failed", attempts: 2 });
  expect(result.queries[0].ranges.slice(1).every((range) => range.status === "complete")).toBe(
    true,
  );
});

test("HTTP and non-array whole-batch failures retry once and never invent coverage", async () => {
  for (const http of [true, false]) {
    const { client, requests } = fake(() =>
      http
        ? new Response("unavailable", { status: 503 })
        : Response.json({ jsonrpc: "2.0", id: null, error: { code: -32600, message: "bad" } }),
    );
    const result = await huntTransferLogs(client, 8453, [query], options(2000n));
    expect(requests).toHaveLength(2);
    expect(result.reachedHead).toBe(false);
    expect(result.queries[0].searchedToBlock).toBeUndefined();
  }
});

test("empty inputs, future lower bounds and zero call budgets send no empty batch", async () => {
  const { client, requests } = fake(() => {
    throw new Error("must not fetch");
  });
  expect((await huntTransferLogs(client, 8453, [], options(1000n))).reachedHead).toBe(true);
  expect(
    (await huntTransferLogs(client, 8453, [{ ...query, fromBlock: 1001n }], options(1000n)))
      .reachedHead,
  ).toBe(true);
  expect(
    (await huntTransferLogs(client, 8453, [query], options(1000n, { maximumCalls: 0 })))
      .reachedHead,
  ).toBe(false);
  expect(requests).toHaveLength(0);
});

test("inclusive 1000-block boundaries and multiple historical queries share one endpoint batch", async () => {
  const { client, requests } = fake((calls) => Response.json(calls.map((call) => ok(call))));
  const result = await huntTransferLogs(
    client,
    8453,
    [query, { ...query, fromBlock: 1000n }],
    options(1001n),
  );
  expect(requests).toHaveLength(1);
  expect(
    requests[0].map((call) => {
      const range = call.params[0] as { fromBlock: string; toBlock: string };
      return [BigInt(range.fromBlock), BigInt(range.toBlock)];
    }),
  ).toEqual([
    [1n, 1000n],
    [1001n, 1001n],
    [1000n, 1001n],
  ]);
  expect(result.queries.every((item) => item.reachedHead)).toBe(true);
});

test("request-byte and caller-capacity guards report unsent ranges", async () => {
  const { client, requests } = fake(() => {
    throw new Error("must not fetch");
  });
  for (const changes of [{ maximumRequestBytes: 2 }, { canRequest: () => false }]) {
    const result = await huntTransferLogs(client, 8453, [query], options(2000n, changes));
    expect(result.reachedHead).toBe(false);
    expect(result.queries[0].ranges[0].status).toBe("unsearched");
  }
  expect(requests).toHaveLength(0);
});

test("valid canonical transfer parsing is shared and out-of-range logs fail only their chunk", async () => {
  const log = {
    address: token,
    transactionHash: `0x${"11".repeat(32)}`,
    logIndex: "0x0",
    blockNumber: "0x1",
    blockHash: `0x${"22".repeat(32)}`,
    topics: [
      TRANSFER_TOPIC,
      `0x${"0".repeat(24)}${"33".repeat(20)}`,
      `0x${"0".repeat(24)}${treasury.slice(2)}`,
    ],
    data: `0x${"0".repeat(63)}1`,
  };
  const { client } = fake((calls) => Response.json(calls.map((call) => ok(call, [log]))));
  const result = await huntTransferLogs(client, 8453, [query], options(2000n));
  expect(result.queries[0].ranges[0].logs[0].amountUnits).toBe(1n);
  expect(result.queries[0].ranges[1]).toMatchObject({
    status: "failed",
    reason: "invalid_transfer_logs",
    attempts: 2,
  });
  expect(result.queries[0].searchedToBlock).toBe(1000n);
});

test("bounded response overflow stays incomplete after the finite retry", async () => {
  const { client, requests } = fake((calls) =>
    Response.json(calls.map((call) => ok(call, "x".repeat(2000)))),
  );
  const result = await huntTransferLogs(
    client,
    8453,
    [query],
    options(1000n, { maximumResponseBytes: 200 }),
  );
  expect(requests).toHaveLength(2);
  expect(result.queries[0].ranges[0]).toMatchObject({
    status: "failed",
    reason: "rpc_response_too_large",
    attempts: 2,
  });
});

test("invalid limits are rejected without invoking the client", async () => {
  const { client, requests } = fake(() => {
    throw new RpcError("must_not_fetch");
  });
  await expect(
    huntTransferLogs(client, 8453, [query], options(1n, { maximumEntries: 0 })),
  ).rejects.toThrow("rpc_invalid_hunt_limits");
  expect(requests).toHaveLength(0);
});

test("request bytes split bounded pages without truncating the logical span", async () => {
  const { client, requests } = fake((calls) => Response.json(calls.map((call) => ok(call))));
  const result = await huntTransferLogs(
    client,
    8453,
    [query],
    options(3000n, { maximumRequestBytes: 500 }),
  );
  expect(result.reachedHead).toBe(true);
  expect(requests).toHaveLength(3);
  expect(
    requests.every((calls) => new TextEncoder().encode(JSON.stringify(calls)).byteLength <= 500),
  ).toBe(true);
});

test("malformed log values cannot masquerade as completed empty coverage", async () => {
  const { client, requests } = fake((calls) =>
    Response.json(calls.map((call) => ok(call, [null]))),
  );
  const result = await huntTransferLogs(client, 8453, [query], options(1000n));
  expect(requests).toHaveLength(2);
  expect(result.reachedHead).toBe(false);
  expect(result.queries[0].ranges[0]).toMatchObject({
    status: "failed",
    reason: "invalid_transfer_logs",
  });
});
