import type { Kv, KvAtomic, KvCommitResult, KvKey } from "./kv.ts";
import { WorkerBudgetError, type WorkerRequestBudget } from "./worker_budget.ts";
import type { SupportedChainId } from "./payment_types.ts";

export type WakeTarget = {
  kind: "invoice" | "delivery";
  id: string;
  dueAt: number | null;
  reason: "expiry" | "confirmation" | "delivery" | "cancel";
};
export type EntityJob = {
  schemaVersion: 1;
  target: WakeTarget;
  generation: string;
  sourceVersion: string;
  dueAt: number;
  /** Expiry is one finite scan; this snapshot never advances with the chain. */
  expiry?: { toBlock: bigint; searchedThrough?: bigint; failures: number };
};
export type EntityJobRunner = {
  plan(target: WakeTarget, previous?: EntityJob | null, now?: number): Promise<EntityJob | null>;
  run(job: EntityJob, isCurrent: () => Promise<boolean>, now?: number): Promise<EntityJob | null>;
};

/** Drivers append their durable enqueue or prepared-intent fence to this same CAS. */
export type WakeupPort = {
  ensure?(target: WakeTarget): Promise<void>;
  commit(
    atomic: KvAtomic,
    targets: readonly WakeTarget[],
    isCurrent?: () => Promise<boolean>,
  ): Promise<KvCommitResult>;
};
export type WakeIntent = {
  schemaVersion: 1;
  operationId: string;
  state: "prepared" | "committed";
  targets: WakeTarget[];
  adopted: string[];
  preparedAt: number;
};
export type PreparedWakeIntent = WakeIntent & { key: KvKey; versionstamp: string };
export type WakeTransport = { prepare(intent: PreparedWakeIntent): Promise<void> };

const INTENTS = ["uos_checkout", "v1", "payments", "wake_intents"];

/** Expected times only; canonical receipt and RPC heads still decide every phase.
 * ETH estimates use 12s slots/32-slot epochs; OP safe is a local 5m estimate.
 * https://ethereum.org/developers/docs/consensus-mechanisms/pos/
 * https://docs.optimism.io/op-stack/transactions/transaction-finality
 * https://docs.base.org/specifications/transactions/transaction-finality
 */
export function confirmationMilestones(chainId: SupportedChainId, includedAt: number) {
  const [block, safe, finalized] =
    chainId === 1
      ? [12_000, 768_000, 1_152_000]
      : chainId === 10
        ? [2_000, 300_000, 1_800_000]
        : [2_000, 120_000, 1_200_000];
  return {
    sealedAt: includedAt + 3 * block,
    safeAt: includedAt + safe,
    finalizedAt: includedAt + finalized,
  };
}

export function wakeTargetKey(target: WakeTarget): string {
  if (!["invoice", "delivery"].includes(target.kind) || !target.id || target.id.length > 128) {
    throw new TypeError("invalid_wake_target");
  }
  if (
    (target.dueAt === null) !== (target.reason === "cancel") ||
    (target.dueAt !== null && (!Number.isSafeInteger(target.dueAt) || target.dueAt < 0))
  )
    throw new TypeError("invalid_wake_deadline");
  return `${target.kind}:${target.id}`;
}

export function wakeIntentKey(operationId: string): KvKey {
  if (!operationId || operationId.length > 128) throw new TypeError("invalid_wake_operation");
  return [...INTENTS, operationId];
}

export function createIntentWakeupPort(options: {
  kv: Kv;
  transport: WakeTransport;
  budget?: WorkerRequestBudget;
  now?: () => number;
  isCurrent?: () => Promise<boolean>;
}): WakeupPort {
  return {
    async commit(atomic, requested, current) {
      const isCurrent = async () =>
        (!options.isCurrent || (await options.isCurrent())) && (!current || (await current()));
      const targets = [
        ...new Map(requested.map((target) => [wakeTargetKey(target), target])).values(),
      ];
      if (!targets.length) return atomic.commit();
      // One preparation KV call, one DO RPC per target, and the original domain CAS.
      // No synchronous confirmation/ACK is necessary: each target is already armed.
      if (options.budget && !options.budget.hasCapacity(targets.length + 2))
        throw new WorkerBudgetError();
      await requireCurrent(isCurrent);
      const operationId = crypto.randomUUID();
      const key = wakeIntentKey(operationId);
      const intent: WakeIntent = {
        schemaVersion: 1,
        operationId,
        state: "prepared",
        targets,
        adopted: [],
        preparedAt: (options.now ?? Date.now)(),
      };
      const prepared = await options.kv
        .atomic()
        .check({ key, versionstamp: null })
        .set(key, intent)
        .commit();
      if (!prepared.ok) return prepared;
      await requireCurrent(isCurrent);
      await options.transport.prepare({ ...intent, key, versionstamp: prepared.versionstamp });
      await requireCurrent(isCurrent);
      return atomic
        .check({ key, versionstamp: prepared.versionstamp })
        .set(key, { ...intent, state: "committed" } satisfies WakeIntent)
        .commit();
    },
  };
}

/** Called by an originally armed recovery alarm after its preparation grace. */
export async function resolveWakeIntent(
  kv: Kv,
  operationId: string,
  isCurrent?: () => Promise<boolean>,
): Promise<WakeIntent | null> {
  const key = wakeIntentKey(operationId);
  await requireCurrent(isCurrent);
  const entry = await kv.get<WakeIntent>(key);
  if (!entry.value || entry.value.state === "committed") return entry.value;
  await requireCurrent(isCurrent);
  if ((await kv.atomic().check(entry).delete(key).commit()).ok) return null;
  // The only successful competing transition is commit or cancellation.
  await requireCurrent(isCurrent);
  return (await kv.get<WakeIntent>(key)).value;
}

/** The caller must durably adopt its target before acknowledging it here. */
export async function acknowledgeWakeIntent(
  kv: Kv,
  operationId: string,
  target: WakeTarget,
  isCurrent?: () => Promise<boolean>,
): Promise<boolean> {
  const key = wakeIntentKey(operationId);
  await requireCurrent(isCurrent);
  const entry = await kv.get<WakeIntent>(key);
  if (!entry.value) return true;
  const intent = entry.value;
  const name = wakeTargetKey(target);
  if (
    intent.state !== "committed" ||
    !intent.targets.some((candidate) => wakeTargetKey(candidate) === name)
  )
    return false;
  if (intent.adopted.includes(name)) return true;
  const adopted = [...intent.adopted, name];
  await requireCurrent(isCurrent);
  const atomic = kv.atomic().check(entry);
  return (
    await (
      adopted.length === intent.targets.length
        ? atomic.delete(key)
        : atomic.set(key, { ...intent, adopted } satisfies WakeIntent)
    ).commit()
  ).ok;
}

export class StaleWakeupError extends Error {
  constructor() {
    super("stale_wakeup");
  }
}

async function requireCurrent(isCurrent?: () => Promise<boolean>): Promise<void> {
  if (isCurrent && !(await isCurrent())) throw new StaleWakeupError();
}
