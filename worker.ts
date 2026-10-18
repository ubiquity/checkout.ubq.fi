import { DurableObject } from "cloudflare:workers";
import { createCheckoutServices, createHandler, withPeerAddress } from "./main.ts";
import { createEntityJobRunner } from "./src/entity_jobs.ts";
import { UbqKv } from "./src/ubq_kv.ts";
import { createWorkerTransport } from "./src/worker_transport.ts";
import { WorkerRequestBudget } from "./src/worker_budget.ts";
import { createWorkerScheduler } from "./src/worker_scheduler.ts";
import { ensureWorkerMigration, MIGRATION_OBJECT_NAME } from "./src/worker_migration.ts";
import {
  createIntentWakeupPort,
  wakeTargetKey,
  type PreparedWakeIntent,
  type WakeTarget,
} from "./src/wakeup.ts";

function services(env: Env, budget: WorkerRequestBudget, isCurrent?: () => Promise<boolean>) {
  if (!env.CHECKOUT_KV_SERVICE_TOKEN) throw new Error("checkout_kv_not_configured");
  const network = budget.wrap(fetch);
  const kv = new UbqKv({
    url: "https://kv.ubq.fi",
    token: env.CHECKOUT_KV_SERVICE_TOKEN,
    fetchImpl: budget.wrap((input, init) => env.UOS_KV.fetch(input, init)),
  });
  const wakeups = {
    ...createIntentWakeupPort({
      kv,
      budget,
      isCurrent,
      transport: {
        async prepare(intent) {
          for (const target of intent.targets) {
            budget.reserve();
            await env.CHECKOUT_SCHEDULER.getByName(wakeTargetKey(target)).prepare(intent, target);
          }
        },
      },
    }),
    async ensure(target: WakeTarget) {
      budget.reserveNestedInvocations(7);
      budget.reserve();
      await env.CHECKOUT_SCHEDULER.getByName(wakeTargetKey(target)).adopt(target);
    },
  };
  return createCheckoutServices(kv, {
    ...createWorkerTransport(network),
    assetFetcher: (request) => {
      budget.reserve();
      return env.ASSETS.fetch(request);
    },
    fetch: network,
    discovery: { budget, deadline: Date.now() + 20_000 },
    wakeups,
    budget,
    chainWebhookKeys: env.CHECKOUT_ALCHEMY_WEBHOOK_KEYS,
  });
}

/** Platform wrapper; the coordinator remains independently testable in Bun. */
export class CheckoutScheduler extends DurableObject<Env> {
  private readonly scheduler;
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.scheduler = createWorkerScheduler(ctx.storage, (isCurrent, existingBudget) => {
      const budget = existingBudget ?? new WorkerRequestBudget(28, AbortSignal.timeout(20_000));
      const app = services(env, budget, isCurrent);
      return { kv: app.kv, runner: createEntityJobRunner(app, { budget }), budget };
    });
  }
  async prepare(intent: PreparedWakeIntent, target: WakeTarget): Promise<void> {
    this.requireEntity(target);
    await this.scheduler.prepare(intent, target);
  }
  async adopt(target: WakeTarget): Promise<void> {
    this.requireEntity(target);
    // Every caller charges this full nested allowance before issuing adopt().
    const budget = new WorkerRequestBudget(7, AbortSignal.timeout(20_000));
    await this.scheduler.ensure(target, budget);
  }
  async ensure(): Promise<"pending" | "complete"> {
    if (!this.ctx.id.equals(this.env.CHECKOUT_SCHEDULER.idFromName(MIGRATION_OBJECT_NAME)))
      throw new TypeError("invalid_migration_object");
    const budget = new WorkerRequestBudget(24, AbortSignal.timeout(20_000));
    const app = services(this.env, budget);
    return ensureWorkerMigration({
      storage: this.ctx.storage,
      kv: app.kv,
      budget,
      adopt: async (target) => {
        budget.reserveNestedInvocations(7);
        budget.reserve();
        await this.env.CHECKOUT_SCHEDULER.getByName(wakeTargetKey(target)).adopt(target);
      },
    });
  }
  async alarm(): Promise<void> {
    await this.scheduler.alarm();
  }
  private requireEntity(target: WakeTarget): void {
    if (!this.ctx.id.equals(this.env.CHECKOUT_SCHEDULER.idFromName(wakeTargetKey(target))))
      throw new TypeError("invalid_scheduler_object");
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      const budget = new WorkerRequestBudget(
        30,
        AbortSignal.any([request.signal, AbortSignal.timeout(20_000)]),
      );
      // Chain authentication precedes every KV operation and ordinary-request bootstrap.
      const chainEvent = new URL(request.url).pathname === "/api/v1/chain-events/alchemy";
      let migration: "pending" | "complete" | undefined;
      if (!chainEvent) {
        budget.reserve();
        migration = await env.CHECKOUT_SCHEDULER.getByName(MIGRATION_OBJECT_NAME).ensure();
        if (
          migration === "pending" &&
          !(request.method === "GET" && new URL(request.url).pathname === "/api/health")
        ) {
          return identifyRelease(
            Response.json(
              { error: "migration_pending" },
              {
                status: 503,
                headers: { "x-checkout-events-migration": "pending" },
              },
            ),
            env,
          );
        }
      }
      const response = await createHandler(services(env, budget))(withPeerAddress(request, ""));
      if (migration) response.headers.set("x-checkout-events-migration", migration);
      return identifyRelease(response, env);
    } catch {
      console.error(JSON.stringify({ message: "checkout_worker_unavailable" }));
      return identifyRelease(Response.json({ error: "service_unavailable" }, { status: 503 }), env);
    }
  },
} satisfies ExportedHandler<Env>;

function identifyRelease(response: Response, env: Env): Response {
  response.headers.set("x-uos-runtime", "cloudflare-workers");
  response.headers.set("x-uos-version", env.UOS_VERSION.id);
  return response;
}
