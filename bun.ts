import { createCheckoutServices, createHandler } from "./main.ts";
import { bunServiceOptions, openCheckoutKv, withBunIngress } from "./src/bun_runtime.ts";
import { BunDelayedQueue } from "./src/bun_queue.ts";
import { createEntityJobRunner } from "./src/entity_jobs.ts";
import type { Kv } from "./src/kv.ts";

export function startCheckoutServer(options: { kv?: Kv; port?: number; hostname?: string } = {}) {
  const kv = options.kv ?? openCheckoutKv();
  const queue = new BunDelayedQueue({ kv, runner: () => jobs });
  const services = createCheckoutServices(kv, {
    ...bunServiceOptions,
    wakeups: queue,
    chainWebhookKeys: process.env.CHECKOUT_ALCHEMY_WEBHOOK_KEYS,
  });
  const jobs = createEntityJobRunner(services);
  const handler = createHandler(services);
  const requests = new Set<Promise<Response>>();
  try {
    const ready = queue.start();
    void ready.catch((error) => console.error("checkout_queue_start_failed", error));
    const server = Bun.serve({
      port: options.port ?? parsePort(process.env.PORT),
      hostname: options.hostname,
      fetch(request, server) {
        const response = (async () => {
          await queue.start();
          return handler(withBunIngress(request, server.requestIP(request)?.address ?? ""));
        })();
        requests.add(response);
        void response.then(
          () => requests.delete(response),
          () => requests.delete(response),
        );
        return response;
      },
    });
    return {
      server,
      services,
      queue,
      ready,
      async close() {
        const settled = queue.close();
        await server.stop(true);
        await Promise.allSettled([...requests]);
        await settled;
        kv.close();
      },
    };
  } catch (error) {
    void queue.close().then(() => kv.close());
    throw error;
  }
}

function parsePort(raw: string | undefined): number {
  const parsed = Number.parseInt(raw ?? "", 10);
  return Number.isSafeInteger(parsed) && parsed > 0 && parsed <= 65535 ? parsed : 8000;
}

if (import.meta.main) {
  const app = startCheckoutServer();
  console.log(`Ubiquity Checkout: ${app.server.url}`);
  let closing = false;
  const close = async () => {
    if (closing) return;
    closing = true;
    await app.close();
  };
  process.once("SIGINT", () => {
    void close();
  });
  process.once("SIGTERM", () => {
    void close();
  });
}
