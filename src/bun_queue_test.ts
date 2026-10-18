import { test } from "bun:test";
import { deepStrictEqual as equal, ok, rejects } from "node:assert/strict";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import {
  BunDelayedQueue,
  BUN_DUE_PREFIX,
  bunJobKey,
  MAX_BUN_TIMER_DELAY,
  BUN_ATTEMPT_LEASE_MS,
  type BunQueuedJob,
} from "./bun_queue.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import {
  StaleWakeupError,
  type EntityJob,
  type EntityJobRunner,
  type WakeTarget,
} from "./wakeup.ts";

type Timer = ReturnType<typeof setTimeout>;
class Clock {
  now = 1_000;
  next = 0;
  active = new Map<number, { callback: () => void; at: number }>();
  delays: number[] = [];
  callbacks: (() => void)[] = [];
  timers = {
    set: (callback: () => void, delay: number) => {
      const id = ++this.next;
      this.delays.push(delay);
      this.callbacks.push(callback);
      this.active.set(id, { callback, at: this.now + delay });
      return id as unknown as Timer;
    },
    clear: (id: Timer) => {
      this.active.delete(id as unknown as number);
    },
  };
  async flush() {
    for (let turn = 0; turn < 80; turn++) await Promise.resolve();
  }
  async advance(milliseconds: number) {
    const end = this.now + milliseconds;
    await this.flush();
    for (;;) {
      const next = [...this.active].sort((a, b) => a[1].at - b[1].at)[0];
      if (!next || next[1].at > end) break;
      this.now = next[1].at;
      this.active.delete(next[0]);
      next[1].callback();
      await this.flush();
    }
    this.now = end;
    await this.flush();
  }
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function target(id: string, dueAt: number | null): WakeTarget {
  return { kind: "invoice", id, dueAt, reason: dueAt === null ? "cancel" : "expiry" };
}

function job(target: WakeTarget): EntityJob {
  return {
    schemaVersion: 1,
    target,
    generation: crypto.randomUUID(),
    sourceVersion: "canonical",
    dueAt: target.dueAt!,
  };
}

function harness(clock = new Clock(), kv = new SqliteKv()) {
  const delivered: string[] = [];
  const errors: unknown[] = [];
  const runner: EntityJobRunner = {
    plan: async (target) => ({
      ...job(target),
      sourceVersion:
        (
          await kv.get([
            "uos_checkout",
            "v1",
            "payments",
            target.kind === "invoice" ? "invoices" : "events",
            target.id,
          ])
        ).versionstamp ?? "canonical",
    }),
    run: async (job, current) => {
      if (await current()) delivered.push(job.target.id);
      return null;
    },
  };
  const queue = new BunDelayedQueue({
    kv,
    runner: () => runner,
    now: () => clock.now,
    timers: clock.timers,
    onError: (error) => errors.push(error),
  });
  return { queue, kv, clock, runner, delivered, errors };
}

test("Bun enqueues the domain write, durable job, and due index in the same CAS", async () => {
  const h = harness();
  try {
    await h.queue.start();
    const domain = ["domain"];
    const absent = await h.kv.get(domain);
    const invoice = target("atomic", h.clock.now + 50);
    const result = await h.queue.commit(h.kv.atomic().check(absent).set(domain, "created"), [
      invoice,
    ]);
    ok(result.ok);
    const stored = await h.kv.get<BunQueuedJob>(bunJobKey(invoice));
    const due = (await h.kv.listPage<BunQueuedJob>(BUN_DUE_PREFIX, { limit: 1 })).entries[0];
    equal((await h.kv.get(domain)).versionstamp, result.versionstamp);
    equal(stored.versionstamp, result.versionstamp);
    equal(due.versionstamp, result.versionstamp);
    equal(due.value.generation, stored.value?.generation);
    const conflict = await h.queue.commit(h.kv.atomic().check(absent).set(domain, "lost"), [
      target("atomic", h.clock.now),
    ]);
    equal(conflict.ok, false);
    equal((await h.kv.get(domain)).value, "created");
    equal(
      (await h.kv.get<BunQueuedJob>(bunJobKey(invoice))).value?.generation,
      stored.value?.generation,
    );
    await h.clock.advance(49);
    equal(h.delivered, []);
    await h.clock.advance(1);
    equal(h.delivered, ["atomic"]);
    equal(h.clock.active.size, 0);
  } finally {
    await h.queue.close();
    h.kv.close();
  }
});

test("Bun keeps one earliest timer and fences replaced or cancelled callbacks", async () => {
  const h = harness();
  try {
    await h.queue.start();
    await h.queue.commit(h.kv.atomic(), [target("later", 5_000)]);
    await h.queue.refresh();
    const old = h.clock.callbacks.at(-1)!;
    await h.queue.commit(h.kv.atomic(), [target("earlier", 2_000)]);
    await h.queue.refresh();
    equal(h.clock.active.size, 1);
    await h.queue.commit(h.kv.atomic(), [target("earlier", null)]);
    await h.queue.refresh();
    old();
    await h.clock.flush();
    equal(h.clock.active.size, 1);
    await h.clock.advance(3_999);
    equal(h.delivered, []);
    await h.clock.advance(1);
    equal(h.delivered, ["later"]);
    equal(h.clock.active.size, 0);
  } finally {
    await h.queue.close();
    h.kv.close();
  }
});

for (const [label, duration] of [
  ["90 days", 90 * 86_400_000],
  ["13 months", Date.UTC(2027, 10, 18) - Date.UTC(2026, 9, 18)],
] as const) {
  test(`Bun chains ${label} toward the absolute deadline without early delivery`, async () => {
    const h = harness();
    try {
      await h.queue.start();
      await h.queue.commit(h.kv.atomic(), [target("long", h.clock.now + duration)]);
      await h.clock.advance(duration - 1);
      equal(h.delivered, []);
      equal(h.clock.active.size, 1);
      ok(h.clock.delays.length > 1);
      ok(h.clock.delays.every((delay) => delay >= 0 && delay <= MAX_BUN_TIMER_DELAY));
      await h.clock.advance(1);
      equal(h.delivered, ["long"]);
      equal(h.clock.active.size, 0);
    } finally {
      await h.queue.close();
      h.kv.close();
    }
  });
}

test("Bun resumes a committed delayed job from the same SQLite file after restart", async () => {
  const root = join(process.cwd(), ".codex-worktrees", "task-evidence");
  await mkdir(root, { recursive: true });
  const directory = await mkdtemp(join(root, "m06-queue-"));
  const clock = new Clock();
  const first = harness(clock, new SqliteKv(join(directory, "queue.sqlite")));
  let second: ReturnType<typeof harness> | undefined;
  try {
    await first.queue.start();
    const invoice = target("restart", clock.now + 500);
    await first.queue.commit(first.kv.atomic().set(["domain"], "saved"), [invoice]);
    const before = await first.kv.get<BunQueuedJob>(bunJobKey(invoice));
    await first.queue.close();
    first.kv.close();
    equal(clock.active.size, 0);
    second = harness(clock, new SqliteKv(join(directory, "queue.sqlite")));
    await second.queue.start();
    equal(
      (await second.kv.get<BunQueuedJob>(bunJobKey(invoice))).value?.generation,
      before.value?.generation,
    );
    equal((await second.kv.get(["domain"])).value, "saved");
    await clock.advance(499);
    equal(second.delivered, []);
    await clock.advance(1);
    equal(second.delivered, ["restart"]);
  } finally {
    if (second) {
      await second.queue.close();
      second.kv.close();
    } else {
      await first.queue.close();
      first.kv.close();
    }
    await rm(directory, { recursive: true });
  }
});

test("a long-delay continuation observes cancellation committed by another instance", async () => {
  const first = harness();
  const second = harness(new Clock(), first.kv);
  try {
    await first.queue.start();
    await second.queue.start();
    await first.queue.commit(first.kv.atomic(), [
      target("cancel-long", first.clock.now + 90 * 86_400_000),
    ]);
    await first.queue.refresh();
    await second.queue.commit(second.kv.atomic(), [target("cancel-long", null)]);
    await first.clock.advance(MAX_BUN_TIMER_DELAY);
    equal(first.delivered, []);
    equal(first.clock.active.size, 0);
    equal(first.clock.delays.length, 1);
  } finally {
    await first.queue.close();
    await second.queue.close();
    first.kv.close();
  }
});

test("shutdown waits for a paused plan and fences its late continuation", async () => {
  const h = harness();
  const entered = deferred<void>();
  const release = deferred<EntityJob>();
  h.runner.plan = async () => {
    entered.resolve();
    return release.promise;
  };
  try {
    await h.queue.start();
    const invoice = target("shutdown", h.clock.now);
    await h.queue.commit(h.kv.atomic(), [invoice]);
    await h.clock.advance(0);
    await entered.promise;
    let closed = false;
    const closing = h.queue.close().then(() => {
      closed = true;
    });
    await h.clock.flush();
    equal(closed, false);
    equal(h.clock.active.size, 0);
    release.resolve(job(invoice));
    await closing;
    equal(h.delivered, []);
    ok((await h.kv.get(bunJobKey(invoice))).value);
    await h.clock.advance(90 * 86_400_000);
    equal(h.clock.active.size, 0);
  } finally {
    await h.queue.close();
    h.kv.close();
  }
});

test("shutdown fences a pending enqueue before its domain atomic can commit", async () => {
  const h = harness();
  try {
    await h.queue.start();
    const release = deferred<void>();
    const original = h.kv.getMany.bind(h.kv);
    h.kv.getMany = async (...args) => {
      await release.promise;
      return original(...args);
    };
    const committing = h.queue.commit(h.kv.atomic().set(["domain"], "late"), [
      target("late", h.clock.now),
    ]);
    const rejection = rejects(committing, StaleWakeupError);
    const closing = h.queue.close();
    release.resolve();
    await rejection;
    await closing;
    equal((await h.kv.get(["domain"])).value, null);
    equal(h.clock.active.size, 0);
  } finally {
    await h.queue.close();
    h.kv.close();
  }
});

test("a per-call ownership guard fences both domain and queue writes", async () => {
  const h = harness();
  try {
    await h.queue.start();
    const invoice = target("guarded", h.clock.now);
    await rejects(
      h.queue.commit(h.kv.atomic().set(["domain"], "stale"), [invoice], async () => false),
      StaleWakeupError,
    );
    equal((await h.kv.get(["domain"])).value, null);
    equal((await h.kv.get(bunJobKey(invoice))).value, null);
    equal(h.clock.active.size, 0);
  } finally {
    await h.queue.close();
    h.kv.close();
  }
});

test("an interrupted delivery resumes at the deadline persisted before its original attempt", async () => {
  const h = harness();
  let attempts = 0;
  h.runner.run = async (job, current) => {
    attempts++;
    if (attempts === 1) throw new Error("delivery_interrupted");
    if (await current()) h.delivered.push(job.target.id);
    return null;
  };
  try {
    await h.queue.start();
    const invoice = target("retry", h.clock.now);
    await h.queue.commit(h.kv.atomic(), [invoice]);
    await h.clock.advance(0);
    equal(attempts, 1);
    equal(h.errors.length, 1);
    equal(h.clock.active.size, 1);
    const retry = await h.kv.get<BunQueuedJob>(bunJobKey(invoice));
    equal(retry.value?.dueAt, h.clock.now + BUN_ATTEMPT_LEASE_MS);
    equal(retry.value?.lease?.expiresAt, retry.value?.dueAt);
    await h.clock.advance(BUN_ATTEMPT_LEASE_MS - 1);
    equal(attempts, 1);
    await h.clock.advance(1);
    equal(h.delivered, ["retry"]);
    equal(h.clock.active.size, 0);
  } finally {
    await h.queue.close();
    h.kv.close();
  }
});

test("two queue instances cannot concurrently deliver the same durable job", async () => {
  const first = harness();
  const second = harness(first.clock, first.kv);
  const entered = deferred<void>();
  const release = deferred<void>();
  let attempts = 0;
  const run: EntityJobRunner["run"] = async (_job, current) => {
    attempts++;
    entered.resolve();
    await release.promise;
    if (await current()) first.delivered.push("shared");
    return null;
  };
  first.runner.run = run;
  second.runner.run = run;
  try {
    await first.queue.start();
    await second.queue.start();
    await first.queue.commit(first.kv.atomic(), [target("shared", first.clock.now)]);
    await second.queue.refresh();
    await first.clock.advance(0);
    await entered.promise;
    await first.queue.refresh();
    await second.queue.refresh();
    await first.clock.advance(100);
    equal(attempts, 1);
    release.resolve();
    await first.clock.flush();
    equal(first.delivered, ["shared"]);
    await first.clock.advance(BUN_ATTEMPT_LEASE_MS);
    equal(attempts, 1);
  } finally {
    release.resolve();
    await first.queue.close();
    await second.queue.close();
    first.kv.close();
  }
});

test("SIGKILL and reopen before lease expiry resumes the recorded job without HTTP traffic", async () => {
  const root = join(process.cwd(), ".codex-worktrees", "task-evidence");
  await mkdir(root, { recursive: true });
  const directory = await mkdtemp(join(root, "m06-killed-queue-"));
  const path = join(directory, "queue.sqlite");
  const source = `
    import { BunDelayedQueue } from ${JSON.stringify(new URL("./bun_queue.ts", import.meta.url).href)};
    import { SqliteKv } from ${JSON.stringify(new URL("./sqlite_kv.ts", import.meta.url).href)};
    const kv = new SqliteKv(${JSON.stringify(path)});
    const canonical = ["uos_checkout", "v1", "payments", "invoices", "killed"];
    const target = { kind: "invoice", id: "killed", reason: "expiry", dueAt: 1000 };
    const runner = {
      plan: async (target) => ({ schemaVersion: 1, target, generation: "stable-job", sourceVersion: (await kv.get(canonical)).versionstamp, dueAt: 1000, expiry: { toBlock: 100n, searchedThrough: 50n, failures: 1 } }),
      run: async () => { console.log("attempt_claimed"); await Bun.sleep(60000); return null; },
    };
    const queue = new BunDelayedQueue({ kv, runner: () => runner, now: () => 1000 });
    await queue.start();
    await queue.commit(kv.atomic().set(canonical, { id: "killed" }).set(["domain"], "retained"), [target]);
    await Bun.sleep(60000);
  `;
  const child = Bun.spawn(["bun", "--no-env-file", "--eval", source], {
    cwd: process.cwd(),
    stdout: "pipe",
    stderr: "pipe",
  });
  let reopened: ReturnType<typeof harness> | undefined;
  try {
    const reader = child.stdout.getReader();
    const line = await reader.read();
    equal(new TextDecoder().decode(line.value).trim(), "attempt_claimed");
    child.kill("SIGKILL");
    await child.exited;
    await reader.cancel();
    reopened = harness(new Clock(), new SqliteKv(path));
    await reopened.queue.start();
    const queued = await reopened.kv.get<BunQueuedJob>(bunJobKey(target("killed", 1000)));
    equal(queued.value?.generation, "stable-job");
    equal(queued.value?.dueAt, 1000 + BUN_ATTEMPT_LEASE_MS);
    equal(queued.value?.job?.expiry, { toBlock: 100n, searchedThrough: 50n, failures: 1 });
    equal((await reopened.kv.get(["domain"])).value, "retained");
    await reopened.clock.advance(BUN_ATTEMPT_LEASE_MS - 1);
    equal(reopened.delivered, []);
    await reopened.clock.advance(1);
    equal(reopened.delivered, ["killed"]);
  } finally {
    child.kill("SIGKILL");
    await child.exited;
    if (reopened) {
      await reopened.queue.close();
      reopened.kv.close();
    }
    await rm(directory, { recursive: true });
  }
});

test("a newer domain event invalidates a paused delivery and preserves its replacement", async () => {
  const h = harness();
  const entered = deferred<void>();
  const release = deferred<void>();
  h.runner.run = async (old, current) => {
    entered.resolve();
    await release.promise;
    if (await current()) h.delivered.push(old.target.id);
    return null;
  };
  try {
    await h.queue.start();
    await h.queue.commit(h.kv.atomic(), [target("newer", h.clock.now)]);
    await h.clock.advance(0);
    await entered.promise;
    const replacement = target("newer", h.clock.now + 50_000);
    await h.queue.commit(h.kv.atomic().set(["domain"], "new"), [replacement]);
    const next = await h.kv.get<BunQueuedJob>(bunJobKey(replacement));
    release.resolve();
    await h.clock.flush();
    equal(h.delivered, []);
    equal(
      (await h.kv.get<BunQueuedJob>(bunJobKey(replacement))).value?.generation,
      next.value?.generation,
    );
    equal(h.clock.active.size, 1);
  } finally {
    release.resolve();
    await h.queue.close();
    h.kv.close();
  }
});

test("each startup adopts legacy domains through bounded pages and preserves queued generations", async () => {
  const h = harness();
  const calls: { prefix: string; limit: number }[] = [];
  const page = h.kv.listPage.bind(h.kv);
  h.kv.listPage = async (prefix, options) => {
    calls.push({ prefix: String(prefix.at(-1)), limit: options.limit });
    return page(prefix, options);
  };
  h.kv.list = () => {
    throw new Error("unbounded_list_forbidden");
  };
  const plan = h.runner.plan;
  h.runner.plan = async (hint) => ({ ...(await plan(hint))!, dueAt: h.clock.now + 100_000 });
  try {
    for (let index = 0; index < 205; index++)
      await h.kv.set(["uos_checkout", "v1", "payments", "invoices", `i${index}`], {
        id: `i${index}`,
      });
    await h.kv.set(["uos_checkout", "v1", "payments", "events", "e1"], { id: "e1" });
    await h.queue.start();
    equal(calls.filter((call) => call.prefix === "invoices").length, 3);
    equal(calls.filter((call) => call.prefix === "events").length, 1);
    ok(calls.every((call) => call.limit === (call.prefix === "bun_due" ? 1 : 100)));
    equal(h.clock.active.size, 1);
    const before = await h.kv.get<BunQueuedJob>(bunJobKey(target("i1", 0)));
    await h.queue.close();
    await h.kv.set(["uos_checkout", "v1", "payments", "invoices", "worker-created"], {
      id: "worker-created",
    });
    calls.length = 0;
    const restarted = new BunDelayedQueue({
      kv: h.kv,
      runner: () => h.runner,
      now: () => h.clock.now,
      timers: h.clock.timers,
    });
    try {
      await restarted.start();
      equal(
        calls.map((call) => call.prefix),
        ["invoices", "invoices", "invoices", "events", "bun_due"],
      );
      equal(
        (await h.kv.get<BunQueuedJob>(bunJobKey(target("i1", 0)))).value?.generation,
        before.value?.generation,
      );
      ok((await h.kv.get(bunJobKey(target("worker-created", 0)))).value);
    } finally {
      await restarted.close();
    }
  } finally {
    await h.queue.close();
    h.kv.close();
  }
});

test("a lost post-commit head refresh cannot lose the domain job or create a recovery timer", async () => {
  const h = harness();
  try {
    await h.queue.start();
    const original = h.kv.listPage.bind(h.kv);
    let fail = true;
    h.kv.listPage = async (prefix, options) => {
      if (fail && String(prefix.at(-1)) === "bun_due") {
        fail = false;
        throw new Error("head_unavailable");
      }
      return original(prefix, options);
    };
    const invoice = target("head", h.clock.now + 50);
    equal((await h.queue.commit(h.kv.atomic().set(["domain"], "committed"), [invoice])).ok, true);
    await h.clock.flush();
    equal((await h.kv.get(["domain"])).value, "committed");
    ok((await h.kv.get(bunJobKey(invoice))).value);
    equal(h.errors.length, 1);
    equal(h.clock.active.size, 0);
    await h.queue.refresh();
    await h.clock.advance(50);
    equal(h.delivered, ["head"]);
  } finally {
    await h.queue.close();
    h.kv.close();
  }
});

test("a related invoice event adopts missing work and preserves the current generation", async () => {
  const h = harness();
  const canonical = ["uos_checkout", "v1", "payments", "invoices", "ensure"];
  const hint = target("ensure", h.clock.now + 50_000);
  try {
    await h.queue.start();
    await h.kv.set(canonical, { id: "ensure" });
    await h.queue.ensure(hint);
    const first = await h.kv.get<BunQueuedJob>(bunJobKey(hint));
    equal(first.value?.job?.sourceVersion, (await h.kv.get(canonical)).versionstamp);
    await h.queue.ensure(hint);
    equal(
      (await h.kv.get<BunQueuedJob>(bunJobKey(hint))).value?.generation,
      first.value?.generation,
    );
    await h.kv.set(canonical, { id: "ensure", revised: true });
    await h.queue.ensure({ ...hint, dueAt: h.clock.now + 100 });
    const revised = await h.kv.get<BunQueuedJob>(bunJobKey(hint));
    equal(revised.value?.dueAt, h.clock.now + 100);
    ok(revised.value?.generation !== first.value?.generation);
  } finally {
    await h.queue.close();
    h.kv.close();
  }
});
