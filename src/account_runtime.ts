import { accountHash, accountKeyForInvoice, legacyAttemptResolution } from "./account_attempts.ts";
import { huntAccountAttempts, type AccountHuntResult } from "./account_hunt.ts";
import {
  accountMembershipKey,
  adoptAccountIndexSlice,
  attemptStateKey,
  locatorKey,
  reservationKey,
  storedInvoiceKey,
  listAccountManifestPage,
  readOwnershipManifestPage,
  type AccountMembership,
  type AttemptState,
} from "./account_store.ts";
import type { ChainTransferContext } from "./chain_ingress.ts";
import type { Kv, KvAtomic, KvCheck, KvEntryMaybe, KvKey } from "./kv.ts";
import { invoiceJson, type InvoiceRecord, type InvoiceJson } from "./payment_types.ts";
import {
  latestBlockNumber,
  RpcVerificationError,
  type DiscoveredTransferLog,
  type RpcClient,
} from "./rpc.ts";
import { huntTransferLogs } from "./rpc_batch_logs.ts";
import { StaleWakeupError } from "./wakeup.ts";
import { invoiceMatchesTransfer } from "./payment_discovery.ts";
import type { WorkerRequestBudget } from "./worker_budget.ts";

const PREFIX = ["uos_checkout", "v1", "payments"];
const runKey = (owner: string): KvKey => [...PREFIX, "account_runs", owner];
export const observationKey = (
  chain: number,
  log: Pick<DiscoveredTransferLog, "transactionHash" | "logIndex" | "blockHash">,
): KvKey => [...PREFIX, "transfer_review", chain, log.transactionHash, log.logIndex, log.blockHash];
export type TransferReview = {
  chainId: number;
  log: DiscoveredTransferLog;
  classification: "unclassified" | "unowned" | "ambiguous" | "invalidated";
  provenance: string[];
  removed: boolean;
  owners: string[];
  cursor: string | null;
  complete: boolean;
  historyVersion?: string | null;
  canonicalContradiction?: string;
};
type Run = {
  membership: string | null;
  lease?: { id: string; until: number };
  result?: AccountHuntResult;
  offset: number;
  refreshed: string[];
  invalidated: string[];
  complete: boolean;
  versions?: Record<string, string | null>;
  trigger?: string;
  response?: AccountHuntJson;
  restart?: boolean;
  quarantinedInclusions?: string[];
};
export type AccountHuntJson = {
  complete: boolean;
  reachedHead: boolean;
  pending: boolean;
  diagnostics: string[];
  attempts: {
    invoiceId: string;
    outcome: "credited" | "confirming" | "nothing_yet" | "invalidated";
    searchComplete: boolean;
  }[];
  groups: {
    chainId: number;
    tokenAddress: string;
    treasuryAddress: string;
    targetHead?: string;
    searchedToBlock?: string;
    reachedHead: boolean;
  }[];
};
export type Attribution = {
  classification: "owned" | TransferReview["classification"];
  locator?: KvEntryMaybe<{ invoiceId: string; paymentId: string; duplicate: boolean }>;
  reservation?: KvEntryMaybe<{ invoiceId: string }>;
  complete: boolean;
  checks?: KvCheck[];
};

/** Retains the full observation before an incomplete global ownership lookup can return. */
export async function resolveTransferAttribution(options: {
  kv: Kv;
  chainId: number;
  log: DiscoveredTransferLog;
  invoice?: InvoiceRecord;
  budget?: WorkerRequestBudget;
  context?: ChainTransferContext;
  provenance: string;
  removed?: boolean;
}): Promise<Attribution> {
  const { kv, log, invoice, budget, context } = options;
  const room = (calls: number) => budget?.hasCapacity(calls) ?? true;
  if (!room(3) || (context && !(await context.isCurrent())))
    return { classification: "unclassified", complete: false };
  const [locator, reservation, prior, state] = await kv.getMany<
    [
      { invoiceId: string; paymentId: string; duplicate: boolean },
      { invoiceId: string },
      TransferReview,
      AttemptState,
    ]
  >([
    locatorKey(options.chainId, log.transactionHash, log.logIndex),
    reservationKey({
      chainId: options.chainId as InvoiceRecord["chainId"],
      tokenAddress: log.tokenAddress,
      treasuryAddress: log.recipientAddress,
      paidAmountUnits: log.amountUnits,
    }),
    observationKey(options.chainId, log),
    attemptStateKey(invoice?.id ?? ""),
  ]);
  if (locator.value)
    return {
      classification: !invoice || locator.value.invoiceId === invoice.id ? "owned" : "ambiguous",
      complete: true,
      locator,
      reservation,
    };
  const ownerInvoice =
    invoice ??
    (reservation.value
      ? ((await kv.get<InvoiceRecord>(storedInvoiceKey(reservation.value.invoiceId))).value ??
        undefined)
      : undefined);
  const owner = ownerInvoice
    ? await accountHash(accountKeyForInvoice(ownerInvoice, ownerInvoice.merchant))
    : undefined;
  const record: TransferReview = prior.value
    ? {
        ...prior.value,
        owners: [...prior.value.owners],
        provenance: [...new Set([...prior.value.provenance, options.provenance])],
        removed: prior.value.removed || !!options.removed,
      }
    : {
        chainId: options.chainId,
        log: { ...log },
        classification: "unclassified",
        provenance: [options.provenance],
        removed: !!options.removed,
        owners: [],
        cursor: null,
        complete: false,
      };
  let ownershipChecks: KvCheck[] = [];
  if (room(2)) {
    const page = await readOwnershipManifestPage(
      kv,
      {
        chainId: options.chainId as InvoiceRecord["chainId"],
        tokenAddress: log.tokenAddress,
        treasuryAddress: log.recipientAddress,
        amountUnits: log.amountUnits,
      },
      record.cursor === null ? 0 : Number(record.cursor),
    );
    ownershipChecks = [page.control, page.entries, state, reservation];
    const restart = record.cursor !== null && record.historyVersion !== page.control.versionstamp;
    if (record.historyVersion !== page.control.versionstamp) {
      record.owners = [];
      record.complete = false;
    }
    if (restart) {
      record.cursor = "0";
      record.complete = false;
    } else {
      record.owners = [
        ...new Set([
          ...record.owners,
          ...(page.entries.value ?? []).map((entry) => entry.accountHash),
        ]),
      ];
      record.cursor = page.next === null ? null : String(page.next);
      record.complete = page.next === null;
    }
    record.historyVersion = page.control.versionstamp;
  }
  const unambiguous =
    record.complete &&
    owner !== undefined &&
    record.owners.length === 1 &&
    record.owners[0] === owner &&
    reservation.value?.invoiceId === ownerInvoice?.id &&
    state.value?.resolution !== "invalidated" &&
    !options.removed;
  if (unambiguous)
    return {
      classification: "owned",
      complete: true,
      locator,
      reservation,
      checks: ownershipChecks,
    };
  record.classification = !record.complete
    ? "unclassified"
    : state.value?.resolution === "invalidated"
      ? "invalidated"
      : record.owners.length
        ? "ambiguous"
        : "unowned";
  if (!room(1) || (context && !(await context.isCurrent())))
    return { classification: "unclassified", complete: false, locator, reservation };
  let atomic = kv.atomic().check(prior, locator, reservation).set(prior.key, record);
  if (context?.fence) atomic = atomic.check(context.fence);
  if (context?.fences) atomic = atomic.check(...context.fences);
  const saved = await atomic.commit();
  return {
    classification: record.classification,
    complete: saved.ok && record.complete,
    locator,
    reservation,
  };
}

export async function runAccountHunt(options: {
  kv: Kv;
  rpc: RpcClient;
  anchor: KvEntryMaybe<InvoiceRecord>;
  budget?: WorkerRequestBudget;
  deadline?: number;
  context?: ChainTransferContext;
  reuseComplete?: boolean;
  indexReady?: boolean;
  verifyCredit(
    entry: KvEntryMaybe<InvoiceRecord>,
    context: ChainTransferContext,
  ): Promise<KvCheck[] | null>;
  claim(
    entry: KvEntryMaybe<InvoiceRecord>,
    log: DiscoveredTransferLog,
    context: ChainTransferContext,
    attribution: Attribution,
  ): Promise<void>;
  refresh(
    entry: KvEntryMaybe<InvoiceRecord>,
    context: ChainTransferContext,
  ): Promise<KvCheck[] | null>;
  invalidate(
    entry: KvEntryMaybe<InvoiceRecord>,
    state: KvEntryMaybe<AttemptState>,
    reservation: KvEntryMaybe<{ invoiceId: string }>,
    context: ChainTransferContext,
    checks: KvCheck[],
  ): Promise<boolean>;
}): Promise<{
  accountHunt: AccountHuntJson;
  anchor: InvoiceJson;
  anchorVersionstamp?: string | null;
}> {
  const { kv, budget } = options;
  const anchor = options.anchor.value!;
  const account = accountKeyForInvoice(anchor, anchor.merchant);
  const owner = await accountHash(account);
  const deadline = options.deadline ?? Date.now() + 20_000;
  const room = (calls: number) => Date.now() < deadline && (budget?.hasCapacity(calls) ?? true);
  const pending = (
    diagnostic: string,
  ): { accountHunt: AccountHuntJson; anchor: InvoiceJson; anchorVersionstamp?: string | null } => ({
    anchor: invoiceJson(anchor),
    anchorVersionstamp: options.anchor.versionstamp,
    accountHunt: {
      complete: false,
      reachedHead: false,
      pending: true,
      diagnostics: [diagnostic],
      attempts: [],
      groups: [],
    },
  });
  if (!room(5)) return pending("account_budget_pending");
  let ready =
    options.indexReady ||
    (await adoptAccountIndexSlice(kv, { budget, isCurrent: options.context?.isCurrent }));
  if (!budget)
    for (let slice = 0; !ready && slice < 1000; slice++)
      ready = await adoptAccountIndexSlice(kv, { isCurrent: options.context?.isCurrent });
  if (!ready || !room(4)) return pending("account_index_pending");
  const [membership, stored, anchorState] = await kv.getMany<
    [AccountMembership, Run, AttemptState]
  >([accountMembershipKey(owner), runKey(owner), attemptStateKey(anchor.id)]);
  const trigger =
    options.reuseComplete && options.context?.fence
      ? JSON.stringify(options.context.fence.key)
      : undefined;
  if (
    trigger &&
    stored.value?.trigger === trigger &&
    stored.value.complete &&
    stored.value.membership === membership.versionstamp &&
    stored.value.response
  )
    return {
      accountHunt: stored.value.response,
      anchor: invoiceJson(anchor),
      anchorVersionstamp: options.anchor.versionstamp,
    };
  if (stored.value?.lease && stored.value.lease.until > Date.now())
    return pending("account_hunt_busy");
  const run: Run =
    stored.value &&
    !stored.value.complete &&
    !stored.value.restart &&
    stored.value.membership === membership.versionstamp
      ? { ...stored.value }
      : {
          membership: membership.versionstamp,
          offset: 0,
          refreshed: [],
          invalidated: [],
          complete: false,
          trigger,
          quarantinedInclusions:
            stored.value && !stored.value.complete ? stored.value.quarantinedInclusions : undefined,
        };
  if (trigger) run.trigger = trigger;
  // A membership change cannot erase an unprocessed observation suffix.
  const carried =
    stored.value && (stored.value.restart || stored.value.membership !== membership.versionstamp)
      ? (stored.value.result?.observations.slice(stored.value.offset) ?? [])
      : [];
  const id = crypto.randomUUID();
  run.lease = { id, until: Math.min(deadline, Date.now() + 30_000) };
  let entry: KvEntryMaybe<Run> = stored;
  const current = async () =>
    Date.now() < run.lease!.until && (options.context ? await options.context.isCurrent() : true);
  async function save(extend?: (atomic: KvAtomic) => KvAtomic, next: Run = run) {
    if (!(await current())) throw new StaleWakeupError();
    let atomic = kv.atomic().check(entry, membership).set(entry.key, next);
    if (extend) atomic = extend(atomic);
    if (options.context?.fence) atomic = atomic.check(options.context.fence);
    const committed = await atomic.commit();
    if (!committed.ok) throw new StaleWakeupError();
    entry = { key: entry.key, value: next, versionstamp: committed.versionstamp };
  }
  const release = budget?.reserveForCompletion(5);
  const verifiedThisEvent = new Map<string, KvCheck[]>();
  const quarantinedThisEvent = new Set<string>();
  let resolutionComplete = true;
  const scoped = (): ChainTransferContext => ({
    budget,
    deadline,
    isCurrent: current,
    fence: options.context?.fence,
    fences: [entry, membership, ...(options.context?.fences ?? [])],
  });
  try {
    await save();
    if (!run.result?.selection.complete) {
      if (!room(4)) return pending("account_search_pending");
      run.result = await huntAccountAttempts(
        {
          kv,
          rpc: options.rpc,
          listAttempts: (account, page) => listAccountManifestPage(kv, account, page, membership),
          loadAttempt: async (invoiceId) => {
            if (invoiceId === anchor.id)
              return {
                invoice: anchor,
                versionstamp: options.anchor.versionstamp!,
                resolution:
                  anchorState.value?.resolution === "invalidated"
                    ? "invalidated"
                    : legacyAttemptResolution(anchor),
              };
            const [invoice, state] = await kv.getMany<[InvoiceRecord, AttemptState]>([
              storedInvoiceKey(invoiceId),
              attemptStateKey(invoiceId),
            ]);
            return invoice.value
              ? {
                  invoice: invoice.value,
                  versionstamp: invoice.versionstamp!,
                  resolution:
                    state.value?.resolution === "invalidated"
                      ? "invalidated"
                      : legacyAttemptResolution(invoice.value),
                }
              : null;
          },
        },
        {
          account,
          pageSize: 500,
          maximumPages: 32,
          maximumAttempts: 500,
          maximumCalls: 8,
          maximumEntries: 256,
          retained: run.result?.selection,
          deferSearchUntilEnumerated: true,
          canRead: () => room(4),
          canRequest: () => room(4),
        },
      );
      run.result.observations.unshift(
        ...carried.map((observed) => ({
          ...observed,
          candidateInvoiceIds: run
            .result!.unresolvedAttempts.filter(
              ({ invoice }) =>
                invoice.chainId === observed.chainId &&
                invoiceMatchesTransfer(invoice, { ...observed.log }),
            )
            .map(({ invoice }) => invoice.id),
        })),
      );
      run.versions = Object.fromEntries(
        run.result.selection.snapshots.map((snapshot) => [
          snapshot.invoice.id,
          snapshot.versionstamp,
        ]),
      );
      await save();
    } else if (!run.result.reachedHead && room(3)) {
      // Only uncovered ranges resume; successful ranges and captured heads stay fixed.
      for (const group of run.result.groups) {
        if (group.targetHead === undefined && room(3)) {
          const latest = await latestBlockNumber(options.rpc, group.chainId);
          const floor = run.result.selection.snapshots
            .filter((snapshot) => snapshot.invoice.chainId === group.chainId)
            .reduce(
              (maximum, snapshot) =>
                snapshot.invoice.minimumBlock - 1n > maximum
                  ? snapshot.invoice.minimumBlock - 1n
                  : maximum,
              0n,
            );
          if (latest < floor) continue;
          group.targetHead = latest;
          group.ranges =
            latest < group.fromBlock
              ? []
              : [
                  {
                    fromBlock: group.fromBlock,
                    toBlock: latest,
                    status: "unsearched",
                    logs: [],
                    attempts: 0,
                  },
                ];
        }
        for (let index = 0; index < group.ranges.length && room(3); index++) {
          const range = group.ranges[index];
          if (range.status === "complete") continue;
          const retried = await huntTransferLogs(
            options.rpc,
            group.chainId,
            [
              {
                tokenAddress: group.tokenAddress,
                recipientAddress: group.treasuryAddress,
                fromBlock: range.fromBlock,
              },
            ],
            { toBlock: range.toBlock, maximumCalls: 2, canRequest: () => room(3) },
          );
          group.ranges = [
            ...group.ranges.slice(0, index),
            ...retried.queries[0].ranges,
            ...group.ranges.slice(index + 1),
          ];
          for (const result of retried.queries[0].ranges)
            if (result.status === "complete")
              for (const log of result.logs) {
                const ids = run.result.unresolvedAttempts
                  .filter(
                    (attempt) =>
                      group.attemptIds.includes(attempt.invoice.id) &&
                      invoiceMatchesTransfer(attempt.invoice, log),
                  )
                  .map((attempt) => attempt.invoice.id);
                run.result.observations.push({
                  chainId: group.chainId,
                  log,
                  candidateInvoiceIds: ids,
                });
              }
        }
        let next = group.fromBlock;
        group.searchedToBlock = undefined;
        for (const range of group.ranges) {
          if (range.status !== "complete" || range.fromBlock !== next) break;
          group.searchedToBlock = range.toBlock;
          next = range.toBlock + 1n;
        }
        group.reachedHead = group.targetHead !== undefined && next > group.targetHead;
      }
      run.result.reachedHead =
        run.result.enumerationComplete && run.result.groups.every((group) => group.reachedHead);
      await save();
    }
    const result = run.result;
    if (!result) return pending("account_search_pending");
    const invalidateCoverage = (observed: AccountHuntResult["observations"][number]) => {
      result.reachedHead = false;
      run.restart = true;
      resolutionComplete = false;
      result.diagnostics = [...new Set([...result.diagnostics, "canonical_range_pending"])];
      for (const group of result.groups) {
        if (
          group.chainId !== observed.chainId ||
          group.tokenAddress !== observed.log.tokenAddress.toLowerCase() ||
          group.treasuryAddress !== observed.log.recipientAddress.toLowerCase()
        )
          continue;
        group.reachedHead = false;
        group.searchedToBlock = undefined;
        group.ranges = group.ranges.map((range) =>
          range.fromBlock <= observed.log.blockNumber && observed.log.blockNumber <= range.toBlock
            ? { ...range, status: "failed" as const, reason: "canonical_contradiction" }
            : range,
        );
      }
    };
    while (run.offset < result.observations.length && room(options.context ? 6 : 5)) {
      if (!(await current())) throw new StaleWakeupError();
      const observed = result.observations[run.offset];
      const inclusion = JSON.stringify(observationKey(observed.chainId, observed.log));
      if (quarantinedThisEvent.has(inclusion)) {
        invalidateCoverage(observed);
        run.offset++;
        await save();
        continue;
      }
      const candidateId =
        observed.candidateInvoiceIds.length === 1 ? observed.candidateInvoiceIds[0] : undefined;
      const [invoice, state] = candidateId
        ? await kv.getMany<[InvoiceRecord, AttemptState]>([
            storedInvoiceKey(candidateId),
            attemptStateKey(candidateId),
          ])
        : [null, null];
      const eligible =
        invoice?.value &&
        invoice.value.merchant === anchor.merchant &&
        invoice.value.customerReference === anchor.customerReference &&
        state?.value?.resolution !== "invalidated";
      const attribution = await resolveTransferAttribution({
        kv,
        chainId: observed.chainId,
        log: { ...observed.log },
        invoice: eligible ? invoice!.value! : undefined,
        budget,
        context: scoped(),
        provenance: "account_hunt",
      });
      if (!attribution.complete) break;
      if (
        run.quarantinedInclusions?.includes(inclusion) &&
        (!eligible || attribution.classification !== "owned")
      ) {
        invalidateCoverage(observed);
        run.offset++;
        await save();
        continue;
      }
      if (eligible && attribution.classification === "owned") {
        if (!room(options.context ? 13 : 8)) break;
        const scope = scoped();
        scope.fences = [...(scope.fences ?? []), ...(attribution.checks ?? [])];
        try {
          await options.claim(invoice!, { ...observed.log }, scope, attribution);
          if (run.quarantinedInclusions?.includes(inclusion)) {
            run.quarantinedInclusions = run.quarantinedInclusions.filter(
              (item) => item !== inclusion,
            );
          }
        } catch (error) {
          if (
            !(error instanceof RpcVerificationError) ||
            (!error.canonicalContradiction && error.code !== "non_canonical_block")
          )
            throw error;
          quarantinedThisEvent.add(inclusion);
          invalidateCoverage(observed);
          if (!room(2)) break;
          const prior = await kv.get<TransferReview>(
            observationKey(observed.chainId, observed.log),
          );
          const review: TransferReview = {
            ...(prior.value ?? {
              chainId: observed.chainId,
              log: { ...observed.log },
              classification: "unclassified" as const,
              provenance: [],
              removed: false,
              owners: [],
              cursor: null,
              complete: false,
            }),
            canonicalContradiction: error.code,
            provenance: [
              ...new Set([
                ...(prior.value?.provenance ?? []),
                "account_hunt_canonical_contradiction",
              ]),
            ],
          };
          const next = {
            ...run,
            offset: run.offset + 1,
            quarantinedInclusions: [...(run.quarantinedInclusions ?? []), inclusion],
          };
          // History and replay progress commit together; a failed save leaves the raw observation pending.
          await save(
            (atomic) => atomic.check(prior, invoice!, state!).set(prior.key, review),
            next,
          );
          Object.assign(run, next);
          continue;
        }
        const changed = await kv.get<InvoiceRecord>(invoice!.key);
        (run.versions ??= {})[invoice!.value!.id] = changed.versionstamp;
      }
      run.offset++;
      await save();
    }
    const snapshots = [...result.unresolvedAttempts, ...result.resolvedAttempts];
    if (run.offset === result.observations.length)
      for (const selected of snapshots) {
        if (run.refreshed.includes(selected.invoice.id) || !room(options.context ? 12 : 9))
          continue;
        const invoice = await kv.get<InvoiceRecord>(storedInvoiceKey(selected.invoice.id));
        if (invoice.value?.paymentId && invoice.value.status !== "reverted") {
          const verified = await options.refresh(invoice, scoped());
          if (verified) verifiedThisEvent.set(selected.invoice.id, verified);
          const changed = await kv.get<InvoiceRecord>(invoice.key);
          (run.versions ??= {})[selected.invoice.id] = changed.versionstamp;
          if (!changed.value?.paymentId || changed.value.status === "reverted") {
            // Matching excluded this payment-bearing snapshot. Preserve every observation
            // and replay against the refreshed state before any reservation can be released.
            run.restart = true;
            run.offset = 0;
            resolutionComplete = false;
            result.diagnostics = [...new Set([...result.diagnostics, "account_matching_changed"])];
            await save();
            break;
          }
        }
        run.refreshed.push(selected.invoice.id);
        await save();
      }
    if (
      result.reachedHead &&
      run.offset === result.observations.length &&
      run.refreshed.length === snapshots.length &&
      room(4)
    ) {
      const records = await kv.getMany<InvoiceRecord[]>(
        snapshots.map((snapshot) => storedInvoiceKey(snapshot.invoice.id)),
      );
      if (
        records.some(
          (record) => !record.value || record.versionstamp !== run.versions?.[record.value.id],
        )
      ) {
        resolutionComplete = false;
        run.restart = true;
      }
      const credited = records.filter(
        (record) => record.value && legacyAttemptResolution(record.value) === "credited",
      );
      const allStates = await kv.getMany<AttemptState[]>(
        snapshots.map((snapshot) => attemptStateKey(snapshot.invoice.id)),
      );
      for (const invoice of resolutionComplete && credited.length ? records : []) {
        if (
          !invoice.value ||
          (invoice.value.paymentId && invoice.value.status !== "reverted") ||
          run.invalidated.includes(invoice.value.id)
        )
          continue;
        if (!room(7)) {
          resolutionComplete = false;
          break;
        }
        const [state, reservation] = await kv.getMany<[AttemptState, { invoiceId: string }]>([
          attemptStateKey(invoice.value.id),
          reservationKey(invoice.value),
        ]);
        if (state.value?.resolution === "invalidated") continue;
        const later = credited.find((record) => {
          const laterState = allStates[records.indexOf(record)].value;
          return state.value?.sequence !== undefined && laterState?.sequence !== undefined
            ? laterState.sequence > state.value.sequence
            : Date.parse(record.value!.createdAt) > Date.parse(invoice.value!.createdAt);
        });
        if (!later) continue;
        let creditChecks = verifiedThisEvent.get(later.value!.id);
        if (!creditChecks) {
          if (!room(options.context ? 12 : 9)) {
            resolutionComplete = false;
            break;
          }
          const verified = await options.verifyCredit(later, scoped());
          if (!verified) {
            resolutionComplete = false;
            break;
          }
          creditChecks = verified;
          verifiedThisEvent.set(later.value!.id, verified);
        }
        // All observed matches were canonically processed; current known payments never reach here.
        if (
          await options.invalidate(invoice, state, reservation, scoped(), [
            ...creditChecks,
            ...records,
            ...allStates,
            later,
          ])
        ) {
          run.invalidated.push(invoice.value.id);
          const [freshInvoice, freshState] = await kv.getMany<[InvoiceRecord, AttemptState]>([
            invoice.key,
            state.key,
          ]);
          records[records.indexOf(invoice)] = freshInvoice;
          (run.versions ??= {})[invoice.value.id] = freshInvoice.versionstamp;
          allStates[snapshots.findIndex((snapshot) => snapshot.invoice.id === invoice.value!.id)] =
            freshState;
          await save();
        } else resolutionComplete = false;
      }
    }
    release?.();
    const records = room(2)
      ? await kv.getMany<InvoiceRecord[]>(
          snapshots.map((snapshot) => storedInvoiceKey(snapshot.invoice.id)),
        )
      : [];
    const states = room(2)
      ? await kv.getMany<AttemptState[]>(
          snapshots.map((snapshot) => attemptStateKey(snapshot.invoice.id)),
        )
      : [];
    run.complete =
      result.reachedHead &&
      run.offset === result.observations.length &&
      run.refreshed.length === snapshots.length;
    run.complete = run.complete && resolutionComplete;

    const returnedEntry = records.find((record) => record.value?.id === anchor.id);
    const returned = returnedEntry?.value ?? anchor;
    const response: {
      anchor: InvoiceJson;
      accountHunt: AccountHuntJson;
      anchorVersionstamp?: string | null;
    } = {
      anchor: invoiceJson(returned),
      anchorVersionstamp: returnedEntry?.versionstamp ?? options.anchor.versionstamp,
      accountHunt: {
        complete: run.complete,
        pending: !run.complete,
        reachedHead: result.reachedHead,
        diagnostics: result.diagnostics,
        attempts: snapshots.map((snapshot, index) => {
          const invoice = records[index]?.value ?? snapshot.invoice;
          return {
            invoiceId: invoice.id,
            outcome:
              states[index]?.value?.resolution === "invalidated"
                ? "invalidated"
                : legacyAttemptResolution(invoice) === "credited"
                  ? "credited"
                  : invoice.paymentId && invoice.status !== "reverted"
                    ? "confirming"
                    : "nothing_yet",
            searchComplete: result.reachedHead,
          };
        }),
        groups: result.groups.map((group) => ({
          chainId: group.chainId,
          tokenAddress: group.tokenAddress,
          treasuryAddress: group.treasuryAddress,
          targetHead: group.targetHead?.toString(),
          searchedToBlock: (
            group.searchedToBlock ?? (group.reachedHead ? group.targetHead : undefined)
          )?.toString(),
          reachedHead: group.reachedHead,
        })),
      },
    };
    run.response = response.accountHunt;
    run.lease = undefined;
    // Lease release and retained suffix checkpoint are one fenced write.
    const committed = await kv.atomic().check(entry, membership).set(entry.key, run).commit();
    if (!committed.ok) throw new StaleWakeupError();
    return response;
  } finally {
    release?.();
    if (run.lease && room(1)) {
      run.lease = undefined;
      await kv.atomic().check(entry).set(entry.key, run).commit();
    }
  }
}
