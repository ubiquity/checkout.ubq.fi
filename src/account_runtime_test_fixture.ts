import {
  TRANSFER_TOPIC,
  type RpcClient,
  type JsonRpcCall,
  type JsonRpcBatchItem,
  type DiscoveredTransferLog,
} from "./rpc.ts";
import type { InvoiceRecord } from "./payment_types.ts";
const hash = (number: bigint) => `0x${number.toString(16).padStart(64, "0")}`;
const quantity = (number: bigint) => `0x${number.toString(16)}`;
const topic = (address: string) => `0x${address.slice(2).padStart(64, "0")}`;
export class RuntimeRpc implements RpcClient {
  latest = 99n;
  safe: bigint | null = null;
  logs: (DiscoveredTransferLog & { timestamp: number })[] = [];
  add(invoice: InvoiceRecord, number: bigint) {
    const log = {
      transactionHash: hash(number),
      logIndex: 0,
      blockNumber: number,
      blockHash: hash(number + 1000n),
      tokenAddress: invoice.tokenAddress,
      recipientAddress: invoice.treasuryAddress,
      amountUnits: invoice.paidAmountUnits,
      payerAddress: `0x${"33".repeat(20)}`,
      timestamp: Date.parse("2026-10-18T02:00:00Z") / 1000,
    };
    this.logs.push(log);
    return log;
  }
  async batch(_chain: number, calls: readonly JsonRpcCall[]): Promise<JsonRpcBatchItem[]> {
    const receiptCall = calls.find((call) => call.id === "receipt");
    const observed = this.logs.find((log) => log.transactionHash === receiptCall?.params[0]);
    const block = (number: bigint) => ({
      number: quantity(number),
      hash: hash(number + 1000n),
      timestamp: quantity(BigInt(observed?.timestamp ?? Date.parse("2026-10-18T02:00:00Z") / 1000)),
    });
    const wire = (log: DiscoveredTransferLog) => ({
      address: log.tokenAddress,
      transactionHash: log.transactionHash,
      logIndex: quantity(BigInt(log.logIndex)),
      blockNumber: quantity(log.blockNumber),
      blockHash: log.blockHash,
      topics: [TRANSFER_TOPIC, topic(log.payerAddress), topic(log.recipientAddress)],
      data: hash(log.amountUnits),
      removed: false,
    });
    return calls.map((call) => ({
      jsonrpc: "2.0",
      id: call.id,
      result:
        call.method === "eth_getLogs"
          ? this.logs
              .filter((log) => {
                const query = call.params[0] as {
                  address: string;
                  fromBlock: string;
                  toBlock: string;
                  topics: string[];
                };
                return (
                  log.tokenAddress === query.address &&
                  topic(log.recipientAddress) === query.topics[2] &&
                  log.blockNumber >= BigInt(query.fromBlock) &&
                  log.blockNumber <= BigInt(query.toBlock)
                );
              })
              .map(wire)
          : call.id === "receipt"
            ? observed
              ? {
                  transactionHash: observed.transactionHash,
                  blockNumber: quantity(observed.blockNumber),
                  blockHash: observed.blockHash,
                  status: "0x1",
                  logs: [wire(observed)],
                }
              : null
            : call.id === "event_block"
              ? observed
                ? block(observed.blockNumber)
                : null
              : call.id === "safe" || call.id === "finalized"
                ? this.safe === null
                  ? null
                  : block(this.safe)
                : block(this.latest),
    }));
  }
}
