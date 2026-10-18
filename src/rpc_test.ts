import { inspect } from "node:util";
import { test, describe } from "bun:test";
import {
  chainHeads,
  discoverTransferLogs,
  HttpRpcClient,
  type JsonRpcBatchItem,
  type JsonRpcCall,
  type RpcClient,
  RpcError,
  RpcVerificationError,
  TRANSFER_TOPIC,
  verifyTransferBatch,
} from "./rpc.ts";
import { CHAIN_CONFIGURATIONS } from "./payment_types.ts";

const TRANSACTION_HASH = `0x${"11".repeat(32)}`;
const BLOCK_HASH = `0x${"22".repeat(32)}`;
const PAYER = `0x${"33".repeat(20)}`;
const RECIPIENT = `0x${"44".repeat(20)}`;
const TOKEN = CHAIN_CONFIGURATIONS[8453].tokenAddress;
const AMOUNT = 999_999n;

test("payment evidence is verified from one HTTP JSON batch", async () => {
  let requestCount = 0;
  let requestUrl = "";
  let requestBody: Array<{ id: string; method: string; params: unknown[] }> = [];
  const client = new HttpRpcClient((input, init) => {
    requestCount++;
    requestUrl = String(input);
    requestBody = JSON.parse(String(init?.body));
    return Promise.resolve(Response.json(batchResponse(requestBody)));
  });

  const verified = await verifyTransferBatch(client, 8453, expected());
  assertEquals(requestCount, 1);
  assertEquals(requestUrl, "https://rpc.ubq.fi/8453");
  assertEquals(requestBody.length, 5);
  assertEquals(
    requestBody.map((call) => call.method),
    [
      "eth_getTransactionReceipt",
      "eth_getBlockByNumber",
      "eth_getBlockByNumber",
      "eth_getBlockByNumber",
      "eth_getBlockByNumber",
    ],
  );
  assertEquals(requestBody[1].params[0], "0x64");
  assertEquals(verified.blockNumber, 100n);
  assertEquals(verified.latestBlock, 103n);
  assertEquals(verified.safeBlock, 101n);
  assertEquals(verified.finalizedBlock, 99n);
  assertEquals(verified.payerAddress, PAYER);
  assertEquals(verified.blockTimestamp.toISOString(), "2023-11-14T22:13:20.000Z");
});

describe("wrong token, recipient, amount, failed receipts, and noncanonical blocks are rejected", () => {
  test("wrong recipient", async () => {
    const client = mockClient((calls) =>
      batchResponse(calls, { recipient: `0x${"55".repeat(20)}` }),
    );
    await assertRejectsCode(
      () => verifyTransferBatch(client, 8453, expected()),
      "wrong_transfer_recipient",
    );
  });
  test("wrong amount", async () => {
    const client = mockClient((calls) => batchResponse(calls, { amount: AMOUNT + 1n }));
    await assertRejectsCode(
      () => verifyTransferBatch(client, 8453, expected()),
      "wrong_transfer_amount",
    );
  });
  test("failed receipt", async () => {
    const client = mockClient((calls) => batchResponse(calls, { status: "0x0" }));
    await assertRejectsCode(
      () => verifyTransferBatch(client, 8453, expected()),
      "transaction_failed",
    );
  });
  test("orphaned receipt block", async () => {
    const client = mockClient((calls) =>
      batchResponse(calls, { canonicalHash: `0x${"66".repeat(32)}` }),
    );
    await assertRejectsCode(
      () => verifyTransferBatch(client, 8453, expected()),
      "non_canonical_block",
    );
  });
  test("missing receipt", async () => {
    const client = mockClient((calls) => batchResponse(calls, { receipt: null }));
    await assertRejectsCode(
      () => verifyTransferBatch(client, 8453, expected()),
      "receipt_not_found",
    );
  });
});

test("browser block hash is not trusted as canonical evidence", async () => {
  const client = mockClient((calls) => batchResponse(calls));
  const verified = await verifyTransferBatch(client, 8453, {
    ...expected(),
    observedBlockHash: `0x${"ff".repeat(32)}`,
  });
  assertEquals(verified.blockHash, BLOCK_HASH);
});

for (const [safe, finalized, expectedSafe, expectedFinalized] of [
  [101n, 99n, null, 99n],
  [90n, 99n, null, 99n],
  [99n, 101n, 99n, null],
] as const) {
  test(`invalid optional heads preserve useful essential evidence (${safe}/${finalized})`, async () => {
    const client = mockClient((calls) => batchResponse(calls, { latest: 100n, safe, finalized }));
    for (const heads of [
      await chainHeads(client, 8453),
      await verifyTransferBatch(client, 8453, expected()),
    ]) {
      assertEquals(heads.latestBlock, 100n);
      assertEquals(heads.safeBlock, expectedSafe);
      assertEquals(heads.finalizedBlock, expectedFinalized);
    }
  });
}

test("canonical batches retry only failed heads once and retain unordered successes", async () => {
  const requests: string[][] = [];
  const client = mockClient((calls) => {
    requests.push(calls.map(({ id }) => id));
    if (requests.length === 1)
      return batchResponse(calls)
        .reverse()
        .map((item) =>
          item.id === "safe"
            ? { jsonrpc: "2.0", id: "safe", error: { code: -32000, message: "unavailable" } }
            : item,
        );
    return batchResponse(calls, { safe: 102n });
  });
  const transfer = await verifyTransferBatch(client, 8453, expected());
  assertEquals(requests, [["receipt", "event_block", "latest", "safe", "finalized"], ["safe"]]);
  assertEquals(transfer.blockHash, BLOCK_HASH);
  assertEquals(transfer.safeBlock, 102n);
  assertEquals(transfer.finalizedBlock, 99n);
});

test("missing and failed optional heads get one retry while receipt/latest survive", async () => {
  let requests = 0;
  const client = mockClient((calls) => {
    requests++;
    return batchResponse(calls).filter((item) => item.id !== "safe" && item.id !== "finalized");
  });
  const transfer = await verifyTransferBatch(client, 8453, expected());
  assertEquals(requests, 2);
  assertEquals(transfer.blockNumber, 100n);
  assertEquals(transfer.safeBlock, null);
  assertEquals(transfer.finalizedBlock, null);
});

test("malformed and null optional blocks remain unknown without retrying successful entries", async () => {
  let requests = 0;
  const client = mockClient((calls) => {
    requests++;
    return batchResponse(calls).map((item) =>
      item.id === "safe"
        ? { ...item, result: {} }
        : item.id === "finalized"
          ? { ...item, result: null }
          : item,
    );
  });
  const transfer = await verifyTransferBatch(client, 8453, expected());
  assertEquals(requests, 1);
  assertEquals(transfer.safeBlock, null);
  assertEquals(transfer.finalizedBlock, null);
});

test("essential failures never become successful payment evidence", async () => {
  let requests = 0;
  const client = mockClient((calls) => {
    requests++;
    return batchResponse(calls).filter((item) => item.id !== "latest");
  });
  await assertRejectsRpcCode(
    () => verifyTransferBatch(client, 8453, expected()),
    "rpc_missing_response:latest",
  );
  assertEquals(requests, 2);
});

test("reconciliation log discovery is constrained by token and indexed treasury", async () => {
  let captured: readonly JsonRpcCall[] = [];
  const client: RpcClient = {
    batch: (_chainId, calls) => {
      captured = calls;
      return Promise.resolve([{ jsonrpc: "2.0", id: "transfer_logs", result: [] }]);
    },
  };
  assertEquals(await discoverTransferLogs(client, 8453, TOKEN, RECIPIENT, 100n, 200n), []);
  assertEquals(captured.length, 1);
  assertEquals(captured[0].method, "eth_getLogs");
  const filter = captured[0].params[0] as Record<string, unknown>;
  assertEquals(filter.address, TOKEN.toLowerCase());
  assertEquals(filter.fromBlock, "0x64");
  assertEquals(filter.toBlock, "0xc8");
  assertEquals(filter.topics, [TRANSFER_TOPIC, null, addressTopic(RECIPIENT)]);
});

function expected() {
  return {
    transactionHash: TRANSACTION_HASH,
    logIndex: 7,
    observedBlockNumber: 100n,
    observedBlockHash: BLOCK_HASH,
    tokenAddress: TOKEN,
    recipientAddress: RECIPIENT,
    amountUnits: AMOUNT,
  };
}

function mockClient(responder: (calls: readonly JsonRpcCall[]) => JsonRpcBatchItem[]): RpcClient {
  return { batch: (_chainId, calls) => Promise.resolve(responder(calls)) };
}

function batchResponse(
  calls: readonly { id: string; method: string; params: unknown[] }[],
  override: {
    recipient?: string;
    amount?: bigint;
    status?: string;
    canonicalHash?: string;
    receipt?: null;
    latest?: bigint;
    safe?: bigint;
    finalized?: bigint;
  } = {},
): JsonRpcBatchItem[] {
  const recipient = override.recipient ?? RECIPIENT;
  const amount = override.amount ?? AMOUNT;
  const values: Record<string, unknown> = {
    receipt:
      override.receipt === null
        ? null
        : {
            transactionHash: TRANSACTION_HASH,
            blockNumber: "0x64",
            blockHash: BLOCK_HASH,
            status: override.status ?? "0x1",
            logs: [
              {
                address: TOKEN,
                transactionHash: TRANSACTION_HASH,
                blockNumber: "0x64",
                blockHash: BLOCK_HASH,
                logIndex: "0x7",
                removed: false,
                topics: [TRANSFER_TOPIC, addressTopic(PAYER), addressTopic(recipient)],
                data: word(amount),
              },
            ],
          },
    event_block: block("0x64", override.canonicalHash ?? BLOCK_HASH, "0x6553f100"),
    latest: block(`0x${(override.latest ?? 103n).toString(16)}`, `0x${"77".repeat(32)}`),
    safe: block(`0x${(override.safe ?? 101n).toString(16)}`, `0x${"88".repeat(32)}`),
    finalized: block(`0x${(override.finalized ?? 99n).toString(16)}`, `0x${"99".repeat(32)}`),
  };
  return calls.map((call) => ({ jsonrpc: "2.0", id: call.id, result: values[call.id] }));
}

function block(number: string, hash: string, timestamp = "0x6553f101") {
  return { number, hash, timestamp };
}

function addressTopic(address: string): string {
  return `0x${"0".repeat(24)}${address.slice(2).toLowerCase()}`;
}

function word(value: bigint): string {
  return `0x${value.toString(16).padStart(64, "0")}`;
}

async function assertRejectsCode(fn: () => Promise<unknown>, code: string): Promise<void> {
  try {
    await fn();
  } catch (error) {
    if (error instanceof RpcVerificationError && error.code === code) return;
    throw new Error(`Expected ${code}, got ${String(error)}`, { cause: error });
  }
  throw new Error(`Expected rejection ${code}`);
}

async function assertRejectsRpcCode(fn: () => Promise<unknown>, code: string): Promise<void> {
  try {
    await fn();
  } catch (error) {
    if (error instanceof RpcError && error.code === code) return;
    throw new Error(`Expected ${code}, got ${String(error)}`, { cause: error });
  }
  throw new Error(`Expected rejection ${code}`);
}

function assertEquals(actual: unknown, expected: unknown): void {
  if (!deepEqual(actual, expected)) {
    throw new Error(`Expected ${inspect(expected)}, got ${inspect(actual)}`);
  }
}

function deepEqual(actual: unknown, expected: unknown): boolean {
  if (typeof actual === "bigint" || typeof expected === "bigint") return actual === expected;
  return JSON.stringify(actual) === JSON.stringify(expected);
}
