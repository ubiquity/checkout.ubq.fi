import {
  accountHash,
  accountKeyForInvoice,
  legacyAttemptResolution,
  listAccountAttemptPage,
  type AccountKey,
  type AttemptResolution,
  type AccountAttemptReference,
} from "./account_attempts.ts";
import type { Kv, KvPage, KvPageOptions } from "./kv.ts";
import { invoiceMatchesTransfer } from "./payment_discovery.ts";
import {
  normalizeAddress,
  PaymentDomainError,
  SUPPORTED_CHAIN_IDS,
  type InvoiceRecord,
  type SupportedChainId,
} from "./payment_types.ts";
import { latestBlockNumber, RpcError, type DiscoveredTransferLog, type RpcClient } from "./rpc.ts";
import { huntTransferLogs, type TransferLogRangeOutcome } from "./rpc_batch_logs.ts";

export type LoadedAccountAttempt = {
  invoice: InvoiceRecord;
  versionstamp: string;
  resolution?: AttemptResolution;
};
export type AccountHuntDependencies = {
  kv: Kv;
  rpc: RpcClient;
  loadAttempt(invoiceId: string): Promise<LoadedAccountAttempt | null>;
  listAttempts?: (
    account: AccountKey,
    options: KvPageOptions,
  ) => Promise<KvPage<AccountAttemptReference>>;
};
export type AccountHuntOptions = {
  account: AccountKey;
  pageSize: number;
  maximumPages: number;
  maximumAttempts: number;
  /** Includes every latest-head and log POST across all chains. */
  maximumCalls: number;
  /** These guards run after the caller protects persistence and verification capacity. */
  canRead?: (kind: "page" | "attempt") => boolean;
  canRequest?: () => boolean;
  maximumEntries?: number;
  maximumRequestBytes?: number;
  maximumResponseBytes?: number;
  /** A durable selection fenced to this account's membership revision by the caller. */
  retained?: AccountHuntSelection;
  deferSearchUntilEnumerated?: boolean;
};
export type AccountHuntSelection = {
  accountHash: string;
  snapshots: AccountAttemptSnapshot[];
  cursor: string | null;
  pendingReferences: AccountAttemptReference[];
  complete: boolean;
};
export type AccountAttemptSnapshot = Readonly<{
  invoice: Readonly<InvoiceRecord>;
  versionstamp: string;
  resolution: AttemptResolution;
}>;
export type AccountHuntObservation = {
  chainId: SupportedChainId;
  log: Readonly<DiscoveredTransferLog>;
  /** Unverified candidates; an empty list is unclassified, never this account's money. */
  candidateInvoiceIds: string[];
};
export type AccountHuntGroup = {
  chainId: SupportedChainId;
  tokenAddress: string;
  treasuryAddress: string;
  attemptIds: readonly string[];
  fromBlock: bigint;
  targetHead?: bigint;
  searchedToBlock?: bigint;
  reachedHead: boolean;
  ranges: readonly TransferLogRangeOutcome[];
  reason?: string;
};
export type AccountHuntResult = {
  accountHash: string;
  unresolvedAttempts: AccountAttemptSnapshot[];
  resolvedAttempts: AccountAttemptSnapshot[];
  pagesRead: number;
  attemptReads: number;
  callsUsed: number;
  enumerationComplete: boolean;
  groups: AccountHuntGroup[];
  observations: AccountHuntObservation[];
  diagnostics: string[];
  /** Coverage of the selected index snapshot, never canonical settlement or an account lock. */
  reachedHead: boolean;
  selection: AccountHuntSelection;
};

/** Read-only: no claims, domain writes, reservation changes or scheduled work. */
export async function huntAccountAttempts(
  dependencies: AccountHuntDependencies,
  options: AccountHuntOptions,
): Promise<AccountHuntResult> {
  validateOptions(options);
  const account: AccountKey = Object.freeze([...options.account] as [string, string]);
  const owner = await accountHash(account);
  const diagnostics = new Set<string>();
  if (options.retained && options.retained.accountHash !== owner)
    throw new PaymentDomainError("account_selection_mismatch");
  const selected: AccountAttemptSnapshot[] = (options.retained?.snapshots ?? []).map((snapshot) =>
    capture({ ...snapshot, invoice: { ...snapshot.invoice } }, account),
  );
  const seen = new Set(selected.map((snapshot) => snapshot.invoice.id));
  let pagesRead = 0;
  let attemptReads = 0;
  let finished = options.retained?.complete ?? false;
  let inconsistent = false;
  let cursor: string | null = options.retained?.cursor ?? null;
  let pendingReferences = options.retained?.pendingReferences ?? [];
  const failedReferences: AccountAttemptReference[] = [];
  enumeration: while (!finished && pagesRead < options.maximumPages) {
    if (!(options.canRead?.("page") ?? true)) break;
    let page;
    try {
      if (pendingReferences.length) {
        page = { entries: pendingReferences.map((value) => ({ value })), cursor };
        pendingReferences = [];
      } else {
        pagesRead++;
        page = await (
          dependencies.listAttempts ??
          ((account, options) => listAccountAttemptPage(dependencies.kv, account, options))
        )(account, {
          limit: options.pageSize,
          cursor,
        });
      }
    } catch {
      diagnostics.add("attempt_page_failed");
      break;
    }
    for (let offset = 0; offset < page.entries.length; offset++) {
      const reference = page.entries[offset].value;
      if (seen.has(reference.invoiceId)) continue;
      if (attemptReads >= options.maximumAttempts || !(options.canRead?.("attempt") ?? true)) {
        pendingReferences = page.entries.slice(offset).map((entry) => entry.value);
        cursor = page.cursor;
        break enumeration;
      }
      seen.add(reference.invoiceId);
      try {
        attemptReads++;
        const loaded = await dependencies.loadAttempt(reference.invoiceId);
        if (!loaded) throw new PaymentDomainError("attempt_not_found");
        const snapshot = capture(loaded, account);
        if (
          snapshot.invoice.id !== reference.invoiceId ||
          snapshot.invoice.createdAt !== reference.createdAt
        ) {
          throw new PaymentDomainError("attempt_reference_mismatch");
        }
        selected.push(snapshot);
      } catch {
        inconsistent = true;
        failedReferences.push(reference);
        diagnostics.add("attempt_load_or_identity_failed");
      }
    }
    cursor = page.cursor;
    if (cursor === null) {
      finished = true;
      break;
    }
  }
  pendingReferences.push(...failedReferences);
  const enumerationComplete = finished && !inconsistent && pendingReferences.length === 0;
  if (!enumerationComplete) diagnostics.add("attempt_selection_incomplete");
  const unresolvedAttempts = selected.filter((attempt) => attempt.resolution === "unresolved");
  const resolvedAttempts = selected.filter((attempt) => attempt.resolution !== "unresolved");
  const groups = groupAttempts(unresolvedAttempts);
  let callsUsed = 0;
  const canRequest = () =>
    !(options.deferSearchUntilEnumerated && !enumerationComplete) &&
    callsUsed < options.maximumCalls &&
    (options.canRequest?.() ?? true);
  const limits = {
    maximumRequestBytes: options.maximumRequestBytes ?? 256 * 1024,
    maximumResponseBytes: options.maximumResponseBytes ?? 1024 * 1024,
  };
  const rpc: RpcClient = {
    batch: async (chainId, calls, batchLimits) => {
      if (!canRequest()) throw new RpcError("account_hunt_rpc_budget_exhausted");
      callsUsed++;
      return dependencies.rpc.batch(chainId, calls, batchLimits ?? limits);
    },
  };
  const observations = new Map<string, AccountHuntObservation>();
  const inclusions = new Set<string>();
  for (const chainId of [...new Set(groups.map((group) => group.chainId))].sort((a, b) => a - b)) {
    const chainGroups = groups.filter((group) => group.chainId === chainId);
    if (!canRequest()) {
      for (const group of chainGroups) group.reason = "rpc_budget_exhausted";
      diagnostics.add("rpc_budget_exhausted");
      continue;
    }
    let latest: bigint;
    try {
      latest = await latestBlockNumber(rpc, chainId);
    } catch {
      for (const group of chainGroups) group.reason = "latest_lookup_failed";
      diagnostics.add(`latest_lookup_failed:${chainId}`);
      continue;
    }
    const creationFloor = unresolvedAttempts
      .filter((attempt) => attempt.invoice.chainId === chainId)
      .reduce(
        (floor, attempt) =>
          attempt.invoice.minimumBlock - 1n > floor ? attempt.invoice.minimumBlock - 1n : floor,
        0n,
      );
    if (latest < creationFloor) {
      for (const group of chainGroups) group.reason = "latest_before_attempt_snapshot";
      diagnostics.add(`latest_before_attempt_snapshot:${chainId}`);
      continue;
    }
    for (const group of chainGroups) group.targetHead = latest;
    try {
      const result = await huntTransferLogs(
        rpc,
        chainId,
        chainGroups.map((group) => ({
          tokenAddress: group.tokenAddress,
          recipientAddress: group.treasuryAddress,
          fromBlock: group.fromBlock,
        })),
        {
          toBlock: latest,
          maximumCalls: options.maximumCalls - callsUsed,
          maximumEntries: options.maximumEntries,
          ...limits,
          canRequest,
        },
      );
      for (const diagnostic of result.diagnostics) diagnostics.add(`${diagnostic}:${chainId}`);
      for (const outcome of result.queries) {
        const group = chainGroups[outcome.queryIndex];
        group.ranges = outcome.ranges;
        group.reachedHead = outcome.reachedHead;
        group.searchedToBlock = outcome.searchedToBlock;
        const attempts = unresolvedAttempts.filter((attempt) =>
          group.attemptIds.includes(attempt.invoice.id),
        );
        for (const range of outcome.ranges) {
          if (range.status !== "complete") continue;
          for (const log of range.logs) {
            const identity = JSON.stringify([
              chainId,
              log.transactionHash,
              log.logIndex,
              log.blockHash,
            ]);
            const key = JSON.stringify([
              identity,
              log.blockNumber.toString(),
              log.tokenAddress,
              log.payerAddress,
              log.recipientAddress,
              log.amountUnits.toString(),
            ]);
            const candidateIds = attempts
              .filter((attempt) => invoiceMatchesTransfer(attempt.invoice, log))
              .map((attempt) => attempt.invoice.id);
            const existing = observations.get(key);
            if (existing) {
              existing.candidateInvoiceIds = [
                ...new Set([...existing.candidateInvoiceIds, ...candidateIds]),
              ];
              continue;
            }
            if (inclusions.has(identity)) diagnostics.add("conflicting_log_observation");
            inclusions.add(identity);
            observations.set(key, {
              chainId,
              log: Object.freeze({ ...log }),
              candidateInvoiceIds: candidateIds,
            });
          }
        }
      }
    } catch {
      for (const group of chainGroups) group.reason = "log_hunt_failed";
      diagnostics.add(`log_hunt_failed:${chainId}`);
    }
  }
  return {
    accountHash: owner,
    unresolvedAttempts,
    resolvedAttempts,
    pagesRead,
    attemptReads,
    callsUsed,
    enumerationComplete,
    groups,
    observations: [...observations.values()],
    diagnostics: [...diagnostics],
    reachedHead: enumerationComplete && groups.every((group) => group.reachedHead),
    selection: {
      accountHash: owner,
      snapshots: selected,
      cursor,
      pendingReferences,
      complete: enumerationComplete,
    },
  };
}

function capture(loaded: LoadedAccountAttempt, account: AccountKey): AccountAttemptSnapshot {
  const invoice = Object.freeze({ ...loaded.invoice });
  const identity = accountKeyForInvoice(invoice, account[0]);
  const resolution = loaded.resolution ?? legacyAttemptResolution(invoice);
  if (
    identity[1] !== account[1] ||
    typeof loaded.versionstamp !== "string" ||
    !loaded.versionstamp ||
    invoice.schemaVersion !== 1 ||
    !["unresolved", "credited", "invalidated"].includes(resolution) ||
    !SUPPORTED_CHAIN_IDS.includes(invoice.chainId) ||
    typeof invoice.minimumBlock !== "bigint" ||
    invoice.minimumBlock < 0n ||
    typeof invoice.paidAmountUnits !== "bigint" ||
    invoice.paidAmountUnits < 0n
  ) {
    throw new PaymentDomainError("invalid_account_attempt_snapshot");
  }
  normalizeAddress(invoice.tokenAddress);
  normalizeAddress(invoice.treasuryAddress);
  return Object.freeze({ invoice, versionstamp: loaded.versionstamp, resolution });
}

function groupAttempts(attempts: readonly AccountAttemptSnapshot[]): AccountHuntGroup[] {
  const groups = new Map<string, AccountHuntGroup>();
  for (const { invoice } of attempts) {
    const tokenAddress = normalizeAddress(invoice.tokenAddress);
    const treasuryAddress = normalizeAddress(invoice.treasuryAddress);
    const key = JSON.stringify([invoice.chainId, tokenAddress, treasuryAddress]);
    const existing = groups.get(key);
    if (existing) {
      existing.attemptIds = Object.freeze([...existing.attemptIds, invoice.id]);
      if (invoice.minimumBlock < existing.fromBlock) existing.fromBlock = invoice.minimumBlock;
    } else
      groups.set(key, {
        chainId: invoice.chainId,
        tokenAddress,
        treasuryAddress,
        attemptIds: Object.freeze([invoice.id]),
        fromBlock: invoice.minimumBlock,
        reachedHead: false,
        ranges: [],
      });
  }
  return [...groups.values()];
}

function validateOptions(options: AccountHuntOptions): void {
  const positive = [
    options.pageSize,
    options.maximumPages,
    options.maximumAttempts,
    options.maximumEntries ?? 256,
    options.maximumRequestBytes ?? 256 * 1024,
    options.maximumResponseBytes ?? 1024 * 1024,
  ];
  if (
    positive.some((value) => !Number.isSafeInteger(value) || value < 1) ||
    options.pageSize > 500 ||
    !Number.isSafeInteger(options.maximumCalls) ||
    options.maximumCalls < 0
  ) {
    throw new PaymentDomainError("invalid_account_hunt_limits");
  }
}
