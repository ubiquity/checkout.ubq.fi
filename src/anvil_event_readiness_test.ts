import { expect, test } from "bun:test";
import { ANVIL_CHAIN_ID, ANVIL_FORK_BLOCK, AnvilBaseFork } from "../demo/anvil.ts";
import { observeAnvilReadiness } from "../demo/anvil_readiness.ts";
import { CHAIN_CONFIGURATIONS } from "./payment_types.ts";
import { TRANSFER_TOPIC } from "./rpc.ts";

const TX_HASH = `0x${"ab".repeat(32)}`;
const BLOCK_HASH = `0x${"cd".repeat(32)}`;
const RECIPIENT = `0x${"12".repeat(20)}`;
const AMOUNT = 123456n;

test("readiness uses the exact owned loopback listen event across stream chunks", async () => {
  const child = fakeProcess();
  const observed = observeAnvilReadiness(child.process.stdout, child.process.exited);
  let ready = false;
  const waiting = observed.ready.then(() => {
    ready = true;
  });
  child.emit("unrelated startup output\nListening on 0.0.0.0:18545\n");
  await Promise.resolve();
  expect(ready).toBe(false);
  child.emit("Listening on 127.0.");
  child.emit("0.1:18545\r\n");
  await waiting;
  expect(ready).toBe(true);
  child.emit("later output is drained privately\n");
  child.exit();
  await observed.drained;
});

test("process exit, timeout signal and oversized output fail readiness without repeating work", async () => {
  for (const cause of ["exit", "abort", "size"]) {
    const child = fakeProcess();
    const controller = new AbortController();
    const observed = observeAnvilReadiness(
      child.process.stdout,
      child.process.exited,
      controller.signal,
    );
    if (cause === "exit") child.resolveExit();
    if (cause === "abort") controller.abort();
    if (cause === "size") child.emit("x".repeat(64 * 1024 + 1));
    await expect(observed.ready).rejects.toThrow(
      cause === "exit"
        ? "anvil_exited_before_ready"
        : cause === "abort"
          ? "anvil_readiness_timeout"
          : "anvil_startup_output_limit",
    );
    child.exit();
    await observed.drained;
  }
});

test("Anvil startup makes no readiness RPC before stdout and verifies the pinned fork exactly once", async () => {
  const child = fakeProcess();
  const calls: string[] = [];
  const fork = new AnvilBaseFork({
    spawn: () => child.process,
    request: async <T>(method: string): Promise<T> => {
      calls.push(method);
      return (
        method === "eth_chainId"
          ? `0x${ANVIL_CHAIN_ID.toString(16)}`
          : `0x${ANVIL_FORK_BLOCK.toString(16)}`
      ) as T;
    },
  });
  const starting = fork.start();
  await Promise.resolve();
  expect(calls).toEqual([]);
  child.emit("Listening on 127.0.0.1:18545\n");
  await starting;
  expect(calls).toEqual(["eth_chainId", "eth_blockNumber"]);
  await fork.stop();
  expect(child.kills()).toBe(1);
});

test("a mismatched pinned fork stops its owned child after one verification pair", async () => {
  const child = fakeProcess();
  const calls: string[] = [];
  const fork = new AnvilBaseFork({
    spawn: () => child.process,
    request: async <T>(method: string): Promise<T> => {
      calls.push(method);
      return "0x1" as T;
    },
  });
  const starting = fork.start();
  child.emit("Listening on 127.0.0.1:18545\n");
  await expect(starting).rejects.toThrow("anvil_pinned_fork_mismatch");
  expect(calls).toEqual(["eth_chainId", "eth_blockNumber"]);
  expect(child.kills()).toBe(1);
});

test("pending receipt is read once and a later user transfer step checks the same transaction without resending", async () => {
  const calls: Array<{ method: string; params: unknown[] }> = [];
  let reads = 0;
  const fork = new AnvilBaseFork({
    request: async <T>(method: string, params: unknown[]): Promise<T> => {
      calls.push({ method, params });
      if (method === "eth_call") return "0xffffffffff" as T;
      if (method === "eth_sendTransaction") return TX_HASH as T;
      if (method === "eth_getTransactionReceipt") return (++reads === 1 ? null : receipt()) as T;
      return null as T;
    },
  });
  await expect(fork.sendExactUsdc(RECIPIENT, AMOUNT)).rejects.toThrow(
    `demo_usdc_receipt_pending:${TX_HASH}`,
  );
  expect(reads).toBe(1);
  expect(calls.filter((call) => call.method === "eth_sendTransaction")).toHaveLength(1);
  const beforeRetry = calls.length;
  const transfer = await fork.sendExactUsdc(RECIPIENT, AMOUNT);
  expect(calls.slice(beforeRetry)).toEqual([
    { method: "eth_getTransactionReceipt", params: [TX_HASH] },
  ]);
  expect(transfer).toMatchObject({
    transactionHash: TX_HASH,
    blockNumber: 123n,
    blockHash: BLOCK_HASH,
    logIndex: 2,
  });
});

test("receipt transport failure also retains the known transaction for an explicit retry", async () => {
  let sent = 0;
  let reads = 0;
  const fork = new AnvilBaseFork({
    request: async <T>(method: string): Promise<T> => {
      if (method === "eth_call") return "0xffffffffff" as T;
      if (method === "eth_sendTransaction") {
        sent++;
        return TX_HASH as T;
      }
      if (method === "eth_getTransactionReceipt") {
        reads++;
        if (reads === 1) throw new Error("local RPC unavailable");
        return receipt() as T;
      }
      return null as T;
    },
  });
  await expect(fork.sendExactUsdc(RECIPIENT, AMOUNT)).rejects.toThrow("local RPC unavailable");
  await expect(fork.sendExactUsdc(RECIPIENT, AMOUNT)).resolves.toMatchObject({
    transactionHash: TX_HASH,
  });
  expect(sent).toBe(1);
  expect(reads).toBe(2);
});

function fakeProcess() {
  let output!: ReadableStreamDefaultController<Uint8Array>;
  let resolve!: (code: number) => void;
  let closed = false;
  let kills = 0;
  const stdout = new ReadableStream<Uint8Array>({
    start(controller) {
      output = controller;
    },
  });
  const exited = new Promise<number>((done) => {
    resolve = done;
  });
  const exit = () => {
    if (!closed) {
      closed = true;
      output.close();
    }
    resolve(0);
  };
  return {
    process: {
      stdout,
      exited,
      kill(_signal: "SIGTERM") {
        kills++;
        exit();
      },
    },
    emit: (value: string) => output.enqueue(new TextEncoder().encode(value)),
    resolveExit: () => resolve(1),
    exit,
    kills: () => kills,
  };
}

function receipt() {
  return {
    status: "0x1",
    blockNumber: "0x7b",
    blockHash: BLOCK_HASH,
    logs: [
      {
        address: CHAIN_CONFIGURATIONS[8453].tokenAddress,
        transactionHash: TX_HASH,
        topics: [TRANSFER_TOPIC, `0x${"0".repeat(64)}`, `0x${"0".repeat(24)}${RECIPIENT.slice(2)}`],
        data: `0x${AMOUNT.toString(16).padStart(64, "0")}`,
        logIndex: "0x2",
      },
    ],
  };
}
