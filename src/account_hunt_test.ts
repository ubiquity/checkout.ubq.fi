import { test } from "bun:test";
import { deepStrictEqual as equal, ok, rejects } from "node:assert/strict";
import { accountAttemptDescriptor } from "./account_attempts.ts";
import {
  huntAccountAttempts,
  type AccountHuntOptions,
  type AccountHuntResult,
  type LoadedAccountAttempt,
} from "./account_hunt.ts";
import type { InvoiceRecord, SupportedChainId } from "./payment_types.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { UbqKv } from "./ubq_kv.ts";
import { encodeKvValue } from "./kv_codec.ts";
import {
  RpcError,
  TRANSFER_TOPIC,
  type JsonRpcCall,
  type RpcBatchLimits,
  type RpcClient,
} from "./rpc.ts";

const treasury = `0x${"44".repeat(20)}`;
const token = `0x${"ab".repeat(20)}`;
const quantity = (value: bigint) => `0x${value.toString(16)}`;
const hash = (value: bigint) => `0x${value.toString(16).padStart(64, "0")}`;
const topic = (value: string) => `0x${value.slice(2).toLowerCase().padStart(64, "0")}`;

test("one account search includes expired attempts through latest and preserves existing resolved outcomes", async () => {
  const expired = invoice({ id: "a", status: "expired" });
  const peer = invoice({
    id: "aa",
    minimumBlock: 1001n,
    paidAmountUnits: 900n,
    productId: "another-product",
  });
  const credited = invoice({ id: "credited", status: "finalized" });
  const invalidated = invoice({ id: "invalidated" });
  const app = await setup([expired, peer, credited, invalidated]);
  try {
    app.rpc.logs.set(8453, [transfer(expired, 2999n)]);
    const result = await huntAccountAttempts(
      {
        ...app.dependencies,
        loadAttempt: async (id) => {
          const value = await app.dependencies.loadAttempt(id);
          return value && id === "invalidated" ? { ...value, resolution: "invalidated" } : value;
        },
      },
      options({ pageSize: 1 }),
    );
    equal(result.enumerationComplete, true);
    equal(result.reachedHead, true);
    equal(
      result.unresolvedAttempts.map((entry) => entry.invoice.id),
      ["a", "aa"],
    );
    equal(
      result.resolvedAttempts.map((entry) => [entry.invoice.id, entry.resolution]),
      [
        ["credited", "credited"],
        ["invalidated", "invalidated"],
      ],
    );
    equal(
      result.observations.map((entry) => entry.candidateInvoiceIds),
      [["a"]],
    );
    equal(result.observations[0].log.blockNumber, 2999n);
    equal(result.groups[0].targetHead, 3000n);
    equal(result.groups[0].searchedToBlock, 3000n);
    equal(result.groups[0].attemptIds, ["a", "aa"]);
    equal(result.callsUsed, 2);
    equal(
      app.rpc.calls.map((entry) => entry.calls.length),
      [1, 3],
    );
    equal(app.rpc.calls[0].calls[0].params, ["latest", false]);
    for (const entry of app.rpc.calls) ok(entry.limits && entry.limits.maximumResponseBytes > 0);
    equal((await app.kv.get<InvoiceRecord>(key("a"))).value?.status, "expired");
  } finally {
    app.kv.close();
  }
});

test("historical chain/token/treasury groups share one latest capture per chain and immutable snapshots", async () => {
  const first = invoice({ id: "old", tokenAddress: token.toUpperCase().replace("0X", "0x") });
  const moved = invoice({
    id: "moved",
    treasuryAddress: `0x${"55".repeat(20)}`,
    minimumBlock: 100n,
  });
  const ethereum = invoice({ id: "eth", chainId: 1, minimumBlock: 10n });
  const app = await setup([first, moved, ethereum]);
  try {
    const shared: LoadedAccountAttempt = { invoice: first, versionstamp: "shared-version" };
    app.rpc.logs.set(8453, [transfer(first, 500n), transfer(moved, 700n)]);
    app.rpc.onBatch = () => {
      first.minimumBlock = 999_999n;
      first.tokenAddress = `0x${"99".repeat(20)}`;
      first.paidAmountUnits = 123n;
    };
    const result = await huntAccountAttempts(
      {
        ...app.dependencies,
        loadAttempt: async (id) => (id === "old" ? shared : app.dependencies.loadAttempt(id)),
      },
      options(),
    );
    equal(result.reachedHead, true);
    equal(result.groups.length, 3);
    equal(result.groups.find((group) => group.attemptIds.includes("old"))?.tokenAddress, token);
    equal(
      result.unresolvedAttempts.find((entry) => entry.invoice.id === "old")?.invoice.minimumBlock,
      1n,
    );
    equal(result.observations.map((entry) => entry.candidateInvoiceIds).sort(), [
      ["moved"],
      ["old"],
    ]);
    for (const attempt of result.unresolvedAttempts) {
      ok(Object.isFrozen(attempt));
      ok(Object.isFrozen(attempt.invoice));
    }
    equal(
      app.rpc.calls
        .filter((entry) => entry.calls[0].method === "eth_getBlockByNumber")
        .map((entry) => entry.chainId),
      [1, 8453],
    );
    equal(app.rpc.calls.filter((entry) => entry.calls[0].method === "eth_getLogs").length, 2);
    equal(result.callsUsed, 4);
  } finally {
    app.kv.close();
  }
});

test("incomplete index selection and exhausted RPC capacity never report whole-account coverage", async () => {
  const first = invoice({ id: "a" });
  const app = await setup([first, invoice({ id: "b" })]);
  try {
    const partial = await huntAccountAttempts(
      app.dependencies,
      options({ pageSize: 1, maximumPages: 1 }),
    );
    equal(partial.pagesRead, 1);
    equal(partial.attemptReads, 1);
    equal(partial.groups[0].reachedHead, true);
    equal(partial.enumerationComplete, false);
    equal(partial.reachedHead, false);
    const headOnly = await huntAccountAttempts(app.dependencies, options({ maximumCalls: 1 }));
    equal(headOnly.callsUsed, 1);
    equal(
      headOnly.groups[0].ranges.map((range) => range.status),
      ["unsearched"],
    );
    equal(headOnly.reachedHead, false);
    const noCalls = await huntAccountAttempts(app.dependencies, options({ maximumCalls: 0 }));
    equal(noCalls.callsUsed, 0);
    equal(noCalls.groups[0].reason, "rpc_budget_exhausted");
    equal(noCalls.reachedHead, false);
  } finally {
    app.kv.close();
  }
});

test("raw service-page continuation survives an empty account page without an indexed backend", async () => {
  const attempt = invoice();
  const app = await setup([attempt]);
  try {
    const descriptor = await accountAttemptDescriptor(attempt, "merchant");
    const cursors: unknown[] = [];
    const kv = new UbqKv({
      url: "https://offline.invalid",
      token: "fake",
      fetchImpl: async (_url, init) => {
        const body = JSON.parse(String(init.body));
        cursors.push(body.cursor);
        return Response.json({
          ok: true,
          entries: [
            {
              key: body.cursor ? descriptor.key : ["uos_checkout", "v1", "unrelated"],
              value: encodeKvValue(descriptor.value),
              versionstamp: "index",
            },
          ],
          cursor: body.cursor ? null : "raw-next",
        });
      },
    });
    const result = await huntAccountAttempts({ ...app.dependencies, kv }, options({ pageSize: 1 }));
    equal(cursors, [undefined, "raw-next"]);
    equal(result.pagesRead, 2);
    equal(result.attemptReads, 1);
    equal(result.enumerationComplete, true);
    equal(result.reachedHead, true);
  } finally {
    app.kv.close();
  }
});

test("canonical merchant, customer, ID and creation mismatches cannot escape account isolation", async () => {
  const attempt = invoice();
  const app = await setup([attempt]);
  try {
    for (const changes of [
      { merchant: "foreign" },
      { customerReference: "foreign" },
      { id: "foreign" },
      { createdAt: "2026-10-17T00:00:00.000Z" },
    ]) {
      const result = await huntAccountAttempts(
        {
          ...app.dependencies,
          loadAttempt: async () => ({ invoice: { ...attempt, ...changes }, versionstamp: "wrong" }),
        },
        options(),
      );
      equal(result.enumerationComplete, false);
      equal(result.reachedHead, false);
      equal(result.unresolvedAttempts, []);
      equal(result.resolvedAttempts, []);
      equal(result.callsUsed, 0);
    }
    const missing = await huntAccountAttempts(
      { ...app.dependencies, loadAttempt: async () => null },
      options(),
    );
    equal(missing.reachedHead, false);
    equal(missing.attemptReads, 1);
  } finally {
    app.kv.close();
  }
});

test("partial retries preserve later matches while persistent middle holes prevent complete coverage", async () => {
  const attempt = invoice();
  const app = await setup([attempt]);
  try {
    app.rpc.heads.set(8453, 9000n);
    app.rpc.logs.set(8453, [transfer(attempt, 8000n)]);
    for (const persistent of [false, true]) {
      app.rpc.calls.length = 0;
      app.rpc.failedIds.clear();
      app.rpc.failure = { from: 3001n, persistent };
      const result = await huntAccountAttempts(app.dependencies, options());
      equal(result.callsUsed, 3);
      equal(
        app.rpc.calls.map((entry) => entry.calls.length),
        [1, 9, 1],
      );
      equal(result.observations[0].candidateInvoiceIds, ["invoice"]);
      equal(result.reachedHead, !persistent);
      equal(result.groups[0].searchedToBlock, persistent ? 3000n : 9000n);
      equal(result.groups[0].ranges[3].attempts, 2);
      equal(result.groups[0].ranges[3].status, persistent ? "failed" : "complete");
    }
  } finally {
    app.kv.close();
  }
});

test("unsupported chain log queries retry failed entries once without losing healthy-chain coverage", async () => {
  const unsupported = invoice({ id: "unsupported", chainId: 1 });
  const healthy = invoice({ id: "healthy", chainId: 8453 });
  const app = await setup([unsupported, healthy]);
  try {
    app.rpc.unsupportedLogChains.add(1);
    app.rpc.logs.set(8453, [transfer(healthy, 2500n)]);
    const result = await huntAccountAttempts(app.dependencies, options());
    equal(result.enumerationComplete, true);
    equal(result.reachedHead, false);
    equal(result.callsUsed, 5);
    const failed = result.groups.find((group) => group.chainId === 1)!;
    equal(failed.targetHead, 3000n);
    equal(failed.reachedHead, false);
    equal(failed.searchedToBlock, undefined);
    equal(
      failed.ranges.map(({ fromBlock, toBlock, status, attempts, reason, logs }) => ({
        fromBlock,
        toBlock,
        status,
        attempts,
        reason,
        logs,
      })),
      [
        {
          fromBlock: 1n,
          toBlock: 1000n,
          status: "failed",
          attempts: 2,
          reason: "upstream_error",
          logs: [],
        },
        {
          fromBlock: 1001n,
          toBlock: 2000n,
          status: "failed",
          attempts: 2,
          reason: "upstream_error",
          logs: [],
        },
        {
          fromBlock: 2001n,
          toBlock: 3000n,
          status: "failed",
          attempts: 2,
          reason: "upstream_error",
          logs: [],
        },
      ],
    );
    const covered = result.groups.find((group) => group.chainId === 8453)!;
    equal(covered.reachedHead, true);
    equal(covered.searchedToBlock, 3000n);
    equal(
      covered.ranges.map(({ status, attempts }) => [status, attempts]),
      [
        ["complete", 1],
        ["complete", 1],
        ["complete", 1],
      ],
    );
    equal(
      result.observations.map(({ chainId, candidateInvoiceIds, log }) => [
        chainId,
        candidateInvoiceIds,
        log.blockNumber,
      ]),
      [[8453, ["healthy"], 2500n]],
    );
    const requests = (chain: SupportedChainId) =>
      app.rpc.calls.filter((entry) => entry.chainId === chain);
    equal(
      requests(1).map(({ calls }) => calls.length),
      [1, 3, 3],
    );
    equal(requests(1)[2].calls, requests(1)[1].calls);
    equal(
      requests(8453).map(({ calls }) => calls.length),
      [1, 3],
    );
    for (const request of app.rpc.calls) {
      ok(request.calls.length > 0);
      equal(new Set(request.calls.map(({ id }) => id)).size, request.calls.length);
      ok(
        request.calls.every(({ method }) =>
          ["eth_getBlockByNumber", "eth_getLogs"].includes(method),
        ),
      );
    }
  } finally {
    app.kv.close();
  }
});

test("shared matching returns raw ambiguous candidates and unclassified transfers without inferring payment phases", async () => {
  const first = invoice({ id: "a", status: "expired" });
  const records = [
    first,
    invoice({ id: "b", status: "reverted", paymentId: "old-inclusion" }),
    invoice({ id: "known", status: "confirming", paymentId: "known" }),
    invoice({ id: "later", minimumBlock: 2500n }),
    invoice({ id: "amount", paidAmountUnits: 999n }),
  ];
  const app = await setup(records);
  try {
    const duplicate = transfer(first, 2000n);
    app.rpc.logs.set(8453, [
      duplicate,
      { ...duplicate },
      transfer(first, 2100n, { data: hash(700n) }),
      { ...duplicate, blockHash: hash(77n) },
    ]);
    const result = await huntAccountAttempts(app.dependencies, options());
    equal(result.observations.length, 3);
    equal(
      result.observations
        .filter((entry) => entry.log.amountUnits === 1000n)
        .map((entry) => entry.candidateInvoiceIds),
      [
        ["a", "b"],
        ["a", "b"],
      ],
    );
    equal(
      result.observations.find((entry) => entry.log.amountUnits === 700n)?.candidateInvoiceIds,
      [],
    );
    equal(
      result.unresolvedAttempts.find((entry) => entry.invoice.id === "known")?.invoice.status,
      "confirming",
    );
    ok(!("confirming" in result) && !("credited" in result) && !("unmatchedMoney" in result));
    equal((await app.kv.get<InvoiceRecord>(key("a"))).value?.status, "expired");
  } finally {
    app.kv.close();
  }
});

test("failed or corrupt latest heads and known creation-floor contradictions never invent empty success", async () => {
  const app = await setup([invoice({ minimumBlock: 101n })]);
  try {
    for (const mode of ["throw", "malformed", "stale"] as const) {
      app.rpc.headMode = mode;
      app.rpc.heads.set(8453, 99n);
      const result = await huntAccountAttempts(app.dependencies, options());
      equal(result.callsUsed, 1);
      equal(result.reachedHead, false);
      equal(
        result.groups[0].reason,
        mode === "stale" ? "latest_before_attempt_snapshot" : "latest_lookup_failed",
      );
      equal(result.groups[0].ranges, []);
    }
    app.rpc.headMode = "normal";
    app.rpc.heads.set(8453, 100n);
    const newlyCreated = await huntAccountAttempts(app.dependencies, options());
    equal(newlyCreated.callsUsed, 1);
    equal(newlyCreated.reachedHead, true);
    equal(newlyCreated.groups[0].ranges, []);
  } finally {
    app.kv.close();
  }
});

for (const mixed of [false, true]) {
  test(`a rejected latest capture remains retryable after retention (mixed attempts=${mixed})`, async () => {
    const newer = invoice({ id: "newer", minimumBlock: 101n, paidAmountUnits: 900n });
    const older = invoice({ id: "older" });
    const ethereum = invoice({ id: "ethereum", chainId: 1 });
    const app = await setup(mixed ? [older, newer, ethereum] : [newer]);
    try {
      app.rpc.heads.set(8453, 99n);
      app.rpc.heads.set(1, 20n);
      app.rpc.logs.set(1, [transfer(ethereum, 5n)]);
      const rejected = await huntAccountAttempts(app.dependencies, options());
      equal(rejected.enumerationComplete, true);
      equal(rejected.reachedHead, false);
      const base = rejected.groups.find((group) => group.chainId === 8453)!;
      equal(base.reason, "latest_before_attempt_snapshot");
      equal(base.ranges, []);
      equal(base.reachedHead, false);
      equal(app.rpc.calls.filter((call) => call.chainId === 8453).length, 1);
      if (mixed) {
        equal(rejected.groups.find((group) => group.chainId === 1)?.targetHead, 20n);
        equal(rejected.groups.find((group) => group.chainId === 1)?.reachedHead, true);
        equal(
          rejected.observations.map((observation) => observation.candidateInvoiceIds),
          [["ethereum"]],
        );
      }

      const retainedKey = ["m14", "retained-hunt"];
      await app.kv.set(retainedKey, rejected);
      const retained = (await app.kv.get<AccountHuntResult>(retainedKey)).value!;
      equal(retained.groups.find((group) => group.chainId === 8453)?.targetHead, undefined);
      equal(retained.observations, rejected.observations);
      app.rpc.calls.length = 0;
      app.rpc.heads.set(8453, 103n);
      app.rpc.logs.set(
        8453,
        mixed ? [transfer(older, 10n), transfer(newer, 102n)] : [transfer(newer, 102n)],
      );
      const resumed = await huntAccountAttempts(
        {
          ...app.dependencies,
          loadAttempt: async () => {
            throw new Error("retained attempts must not reload");
          },
        },
        options({ retained: retained.selection }),
      );
      equal(resumed.pagesRead, 0);
      equal(resumed.attemptReads, 0);
      equal(resumed.reachedHead, true);
      const recovered = resumed.groups.find((group) => group.chainId === 8453)!;
      equal(recovered.targetHead, 103n);
      equal(recovered.searchedToBlock, 103n);
      equal(recovered.reachedHead, true);
      equal(
        recovered.ranges.map((range) => [range.fromBlock, range.toBlock, range.status]),
        [[mixed ? 1n : 101n, 103n, "complete"]],
      );
      equal(
        resumed.observations.flatMap((observation) => observation.candidateInvoiceIds).sort(),
        mixed ? ["ethereum", "newer", "older"] : ["newer"],
      );
      equal(
        app.rpc.calls.filter((call) => call.chainId === 8453).map((call) => call.calls[0].method),
        ["eth_getBlockByNumber", "eth_getLogs"],
      );
      equal(resumed.diagnostics, []);
    } finally {
      app.kv.close();
    }
  });
}

test("caller guards preserve protected capacity across reads and RPC continuations", async () => {
  const app = await setup([invoice({ id: "a" }), invoice({ id: "b" })]);
  try {
    let reads = 0;
    const selected = await huntAccountAttempts(
      {
        ...app.dependencies,
        loadAttempt: async (id) => {
          reads++;
          return app.dependencies.loadAttempt(id);
        },
      },
      options({ canRead: (kind) => kind === "page" || reads < 1 }),
    );
    equal(selected.attemptReads, 1);
    equal(selected.enumerationComplete, false);
    equal(selected.reachedHead, false);
    let allowed = true;
    app.rpc.onBatch = () => {
      allowed = false;
    };
    const budgeted = await huntAccountAttempts(
      app.dependencies,
      options({ canRequest: () => allowed }),
    );
    equal(budgeted.callsUsed, 1);
    equal(budgeted.reachedHead, false);
    await rejects(
      () => huntAccountAttempts(app.dependencies, options({ maximumPages: 0 })),
      /invalid_account_hunt_limits/,
    );
  } finally {
    app.kv.close();
  }
});

function options(changes: Partial<AccountHuntOptions> = {}): AccountHuntOptions {
  return {
    account: ["merchant", "customer"],
    pageSize: 10,
    maximumPages: 10,
    maximumAttempts: 20,
    maximumCalls: 10,
    ...changes,
  };
}

async function setup(invoices: InvoiceRecord[]) {
  const kv = new SqliteKv();
  for (const invoice of invoices) {
    await kv.set(key(invoice.id), invoice);
    const reference = await accountAttemptDescriptor(invoice, invoice.merchant);
    await kv.set(reference.key, reference.value);
  }
  const rpc = new FakeRpc();
  const dependencies = {
    kv,
    rpc,
    loadAttempt: async (id: string): Promise<LoadedAccountAttempt | null> => {
      const entry = await kv.get<InvoiceRecord>(key(id));
      return entry.value ? { invoice: entry.value, versionstamp: entry.versionstamp } : null;
    },
  };
  return { kv, rpc, dependencies };
}

function key(id: string) {
  return ["uos_checkout", "v1", "payments", "invoices", id];
}

function invoice(changes: Partial<InvoiceRecord> = {}): InvoiceRecord {
  return {
    schemaVersion: 1,
    id: "invoice",
    merchant: "merchant",
    customerReference: "customer",
    idempotencyKey: "request",
    pricingVersionId: "price",
    catalogDigest: "digest",
    productId: "credits",
    productKind: "credit_pack",
    chainId: 8453,
    networkName: "Base",
    tokenAddress: token,
    tokenSymbol: "USDC",
    tokenDecimals: 6,
    treasuryAddress: treasury,
    listAmountUnits: 1100n,
    paidAmountUnits: 1000n,
    discountUnits: 100n,
    minimumBlock: 1n,
    status: "awaiting_payment",
    createdAt: "2026-10-18T00:00:00.000Z",
    expiresAt: "2026-10-18T01:00:00.000Z",
    updatedAt: "2026-10-18T00:00:00.000Z",
    ...changes,
  };
}

function transfer(attempt: InvoiceRecord, block: bigint, changes: Record<string, unknown> = {}) {
  return {
    address: attempt.tokenAddress.toLowerCase(),
    topics: [TRANSFER_TOPIC, topic(treasury), topic(attempt.treasuryAddress)],
    data: hash(attempt.paidAmountUnits),
    transactionHash: hash(block),
    logIndex: "0x0",
    blockNumber: quantity(block),
    blockHash: hash(block + 1n),
    ...changes,
  };
}

class FakeRpc implements RpcClient {
  calls: { chainId: SupportedChainId; calls: readonly JsonRpcCall[]; limits?: RpcBatchLimits }[] =
    [];
  heads = new Map<SupportedChainId, bigint>([
    [1, 3000n],
    [10, 3000n],
    [8453, 3000n],
  ]);
  logs = new Map<SupportedChainId, ReturnType<typeof transfer>[]>();
  unsupportedLogChains = new Set<SupportedChainId>();
  failure?: { from: bigint; persistent: boolean };
  failedIds = new Set<string>();
  headMode: "normal" | "throw" | "malformed" | "stale" = "normal";
  onBatch?: () => void;

  async batch(chainId: SupportedChainId, calls: readonly JsonRpcCall[], limits?: RpcBatchLimits) {
    this.calls.push({ chainId, calls, limits });
    this.onBatch?.();
    return [...calls].reverse().map((call) => {
      if (call.method === "eth_getBlockByNumber") {
        equal(call.params, ["latest", false]);
        if (this.headMode === "throw") throw new RpcError("rpc_transport_failed");
        return {
          jsonrpc: "2.0",
          id: call.id,
          result:
            this.headMode === "malformed"
              ? {}
              : {
                  number: quantity(this.heads.get(chainId)!),
                  hash: hash(1n),
                  timestamp: "0x1",
                },
        };
      }
      equal(call.method, "eth_getLogs");
      if (this.unsupportedLogChains.has(chainId)) {
        return {
          jsonrpc: "2.0",
          id: call.id,
          error: { code: -32601, message: "eth_getLogs is unsupported on this chain" },
        };
      }
      const query = call.params[0] as {
        address: string;
        fromBlock: string;
        toBlock: string;
        topics: string[];
      };
      if (
        this.failure?.from === BigInt(query.fromBlock) &&
        (this.failure.persistent || !this.failedIds.has(call.id))
      ) {
        this.failedIds.add(call.id);
        return {
          jsonrpc: "2.0",
          id: call.id,
          error: { code: -32000, message: "offline subset failure" },
        };
      }
      return {
        jsonrpc: "2.0",
        id: call.id,
        result: (this.logs.get(chainId) ?? []).filter(
          (log) =>
            log.address === query.address &&
            log.topics[2] === query.topics[2] &&
            BigInt(log.blockNumber) >= BigInt(query.fromBlock) &&
            BigInt(log.blockNumber) <= BigInt(query.toBlock),
        ),
      };
    });
  }
}

test("retained selection resumes an interrupted page without losing earlier attempts or repeating their loads", async () => {
  const app = await setup([invoice({ id: "a" }), invoice({ id: "b" }), invoice({ id: "c" })]);
  try {
    const loaded: string[] = [];
    const dependencies = {
      ...app.dependencies,
      loadAttempt: async (id: string) => {
        loaded.push(id);
        return app.dependencies.loadAttempt(id);
      },
    };
    const first = await huntAccountAttempts(
      dependencies,
      options({ maximumAttempts: 1, deferSearchUntilEnumerated: true }),
    );
    equal(first.enumerationComplete, false);
    equal(first.selection.pendingReferences.length, 2);
    equal(first.callsUsed, 0);
    const second = await huntAccountAttempts(
      dependencies,
      options({ maximumAttempts: 1, deferSearchUntilEnumerated: true, retained: first.selection }),
    );
    equal(second.enumerationComplete, false);
    equal(second.selection.pendingReferences.length, 1);
    equal(second.callsUsed, 0);
    const third = await huntAccountAttempts(
      dependencies,
      options({ maximumAttempts: 1, deferSearchUntilEnumerated: true, retained: second.selection }),
    );
    equal(third.enumerationComplete, true);
    equal(third.reachedHead, true);
    equal(third.selection.pendingReferences, []);
    equal(
      third.unresolvedAttempts.map((snapshot) => snapshot.invoice.id),
      ["a", "b", "c"],
    );
    equal(loaded, ["a", "b", "c"]);
    equal(third.callsUsed, 2);
  } finally {
    app.kv.close();
  }
});
