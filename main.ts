import { WorkerBudgetError, type WorkerRequestBudget } from "./src/worker_budget.ts";
import { handleAlchemyChainWebhook, recoverPendingChainTransfers } from "./src/chain_ingress.ts";
import type { DiscoveryOptions } from "./src/payment_discovery.ts";
import type { WakeupPort } from "./src/wakeup.ts";
import type { FetchLike } from "./src/rpc.ts";
import type { Kv } from "./src/kv.ts";
import {
  AdminAuthError,
  type AdminAuthService,
  createAdminAuthService,
  LOCAL_PEER_HEADER,
} from "./src/admin_auth.ts";
import {
  createMerchantAuthService,
  MerchantAuthError,
  type MerchantScope,
} from "./src/merchant_auth.ts";
import { type CompactJwsSigner, createPersistentJwsSigner, JwsError } from "./src/jws.ts";
import {
  createPaymentService,
  type PaymentService,
  type SignedOutboxEvent,
} from "./src/payment_service.ts";
import { type InvoiceJson, PaymentDomainError } from "./src/payment_types.ts";
import { HttpRpcClient, type RpcClient, RpcError, RpcVerificationError } from "./src/rpc.ts";
import { type ResolvedWebhookTarget, type WebhookDeliveryResponse } from "./src/webhook.ts";

const LINKEDIN_MERCHANT_ID = "linkedin-scraper-2025";
const ADMIN_MUTATION_HEADER = "x-uos-requested-with";
const ADMIN_MUTATION_VALUE = "checkout-admin";
const JSON_BODY_LIMIT_BYTES = 128 * 1024;
const WEBHOOK_TIMEOUT_MS = 10_000;
const OUTBOX_PAGE_SIZE = 100;
const OUTBOX_MAX_EVENTS_PER_RUN = 1_000;

type MerchantAuthService = ReturnType<typeof createMerchantAuthService>;

export type CheckoutServices = {
  chainWebhookKeys?: string;
  budget?: WorkerRequestBudget;
  wakeups?: WakeupPort;
  kv: Kv;
  rpc: RpcClient;
  adminAuth: AdminAuthService;
  merchantAuth: MerchantAuthService;
  payment: PaymentService;
  fetcher: FetchLike;
  assetFetcher: (request: Request) => Promise<Response>;
  webhookSender: (
    target: ResolvedWebhookTarget,
    body: string,
    signal: AbortSignal,
  ) => Promise<WebhookDeliveryResponse>;
};

type CheckoutServiceOptions = {
  chainWebhookKeys?: string;
  budget?: WorkerRequestBudget;
  fetch?: FetchLike;
  assetFetcher?: CheckoutServices["assetFetcher"];
  rpc?: RpcClient;
  signer?: CompactJwsSigner;
  resolveDns?: (hostname: string, recordType: "A" | "AAAA") => Promise<string[]>;
  webhookSender?: CheckoutServices["webhookSender"];
  discovery?: DiscoveryOptions;
  wakeups?: WakeupPort;
};

export function createCheckoutServices(
  kv: Kv,
  options: CheckoutServiceOptions = {},
): CheckoutServices {
  const fetcher = options.fetch ?? fetch;
  const signer = options.signer ?? createPersistentJwsSigner(kv);
  const rpc = options.rpc ?? new HttpRpcClient(fetcher);
  return {
    chainWebhookKeys: options.chainWebhookKeys,
    budget: options.budget,
    wakeups: options.wakeups,
    kv,
    rpc,
    adminAuth: createAdminAuthService(kv, { fetch: fetcher }),
    merchantAuth: createMerchantAuthService(kv, { resolveDns: options.resolveDns }),
    payment: createPaymentService({
      kv,
      rpc,
      signer,
      discovery: options.discovery,
      wakeups: options.wakeups,
    }),
    fetcher,
    assetFetcher: options.assetFetcher ?? (async () => new Response(null, { status: 404 })),
    webhookSender:
      options.webhookSender ??
      (async () => {
        throw new Error("webhook_transport_required");
      }),
  };
}

export function createHandler(services: CheckoutServices): (request: Request) => Promise<Response> {
  return async (request: Request) => {
    try {
      return await route(request, services);
    } catch (error) {
      const resolved = httpError(error);
      if (resolved.status >= 500) console.error(error);
      return json({ error: resolved.code }, { status: resolved.status });
    }
  };
}

async function route(request: Request, services: CheckoutServices): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname === "/api/v1/chain-events/alchemy" && request.method === "POST") {
    return handleAlchemyChainWebhook(request, {
      webhookKeys: services.chainWebhookKeys,
      kv: services.kv,
      budget: services.budget,
      minimumTransferCalls: 12,
      prepareTransfer: services.payment.prepareChainTransfer,
      processTransfer: services.payment.processChainTransfer,
    });
  }

  if (url.pathname === "/api/health" && request.method === "GET") {
    return json({ status: "ready" });
  }

  if (url.pathname === "/.well-known/jwks.json" && request.method === "GET") {
    return json(await services.payment.jwks(), {
      headers: { "cache-control": "public, max-age=300" },
    });
  }

  if (url.pathname === "/api/auth/login" && request.method === "GET") {
    return services.adminAuth.loginRedirect(request);
  }
  if (url.pathname === "/auth/callback" && request.method === "GET") {
    return await services.adminAuth.callback(request);
  }
  if (url.pathname === "/api/auth/logout" && request.method === "POST") {
    requireAdminMutation(request);
    return services.adminAuth.logout(request);
  }

  if (url.pathname === "/api/v1/admin/session" && request.method === "GET") {
    const session = await services.adminAuth.sessionFromRequest(request);
    return session
      ? json({ authenticated: true, member: session.member, expiresAt: session.claims.exp * 1000 })
      : json({ authenticated: false });
  }

  if (url.pathname === "/api/v1/admin/overview" && request.method === "GET") {
    await services.adminAuth.requireRole(request, ["owner", "admin", "viewer"]);
    return json(await adminOverview(services));
  }

  if (url.pathname === "/api/v1/admin/members" && request.method === "GET") {
    return json({
      members: await services.adminAuth.listMembers(request),
      invitations: await services.adminAuth.listInvitations(request),
    });
  }

  const adminMemberMatch = url.pathname.match(/^\/api\/v1\/admin\/members\/([^/]+)$/);
  if (adminMemberMatch && request.method === "PUT") {
    requireAdminMutation(request);
    const body = await requestJson(request);
    return json(
      await services.adminAuth.upsertMember(
        request,
        decodeURIComponent(adminMemberMatch[1]),
        memberRole(body.role),
      ),
    );
  }

  const adminChainMatch = url.pathname.match(/^\/api\/v1\/admin\/chains\/(\d+)$/);
  if (adminChainMatch && request.method === "PUT") {
    requireAdminMutation(request);
    await services.adminAuth.requireRole(request, ["owner", "admin"]);
    const body = await requestJson(request);
    const setting = await services.payment.configureChain(Number(adminChainMatch[1]), {
      enabled: body.enabled,
      treasuryAddress: body.treasuryAddress,
    });
    return json(setting);
  }

  const adminMerchantMatch = url.pathname.match(/^\/api\/v1\/admin\/merchants\/([^/]+)$/);
  if (adminMerchantMatch && request.method === "PUT") {
    requireAdminMutation(request);
    await services.adminAuth.requireRole(request, ["owner", "admin"]);
    const body = await requestJson(request);
    return json({
      merchant: await services.merchantAuth.upsertMerchant({
        id: decodeURIComponent(adminMerchantMatch[1]),
        name: requiredString(body.name, "merchant_name_required"),
        settlementWebhookUrl: requiredString(
          body.settlementWebhookUrl,
          "settlement_webhook_url_required",
        ),
      }),
    });
  }

  if (url.pathname === "/api/v1/admin/merchant-keys" && request.method === "POST") {
    requireAdminMutation(request);
    await services.adminAuth.requireRole(request, ["owner"]);
    const body = await requestJson(request);
    return json(
      await services.merchantAuth.createKey({
        merchantId: requiredString(body.merchantId, "merchant_id_required"),
        label: requiredString(body.label, "key_label_required"),
        scopes: optionalScopes(body.scopes),
      }),
      { status: 201 },
    );
  }

  const adminKeyMatch = url.pathname.match(/^\/api\/v1\/admin\/merchant-keys\/([^/]+)$/);
  if (adminKeyMatch && request.method === "DELETE") {
    requireAdminMutation(request);
    await services.adminAuth.requireRole(request, ["owner"]);
    await services.merchantAuth.revokeKey(decodeURIComponent(adminKeyMatch[1]));
    return new Response(null, { status: 204 });
  }

  const catalogMatch = url.pathname.match(/^\/api\/v1\/catalog\/versions\/([^/]+)$/);
  if (catalogMatch && request.method === "PUT") {
    const principal = await services.merchantAuth.authenticate(request, ["catalog:write"]);
    const version = await services.payment.publishCatalogVersion(
      { merchantId: principal.merchant.id },
      decodeURIComponent(catalogMatch[1]),
      await requestJson(request),
    );
    return json({
      pricingVersionId: version.pricingVersionId,
      digest: version.digest,
      projection: version.projection,
      createdAt: version.createdAt,
    });
  }

  if (url.pathname === "/api/v1/invoices" && request.method === "POST") {
    const principal = await services.merchantAuth.authenticate(request, ["invoice:create"]);
    const idempotencyKey = request.headers.get("idempotency-key")?.trim();
    if (!idempotencyKey) throw new HttpError(400, "idempotency_key_required");
    const body = await requestJson(request);
    const invoice = await services.payment.createInvoice(
      {
        merchantId: principal.merchant.id,
        idempotencyKey,
      },
      {
        customerReference: body.customerReference,
        pricingVersionId: body.pricingVersionId,
        productId: body.productId,
        chainId: body.chainId,
      },
    );
    await relatedInvoiceEvent(services, invoice);
    return json(invoice, { status: 201 });
  }

  const invoiceMatch = url.pathname.match(/^\/api\/v1\/invoices\/([^/]+)$/);
  if (invoiceMatch && request.method === "GET") {
    const principal = await services.merchantAuth.authenticate(request, ["invoice:read"]);
    const invoiceId = decodeURIComponent(invoiceMatch[1]);
    await requireMerchantInvoice(services, principal.merchant.id, invoiceId);
    const invoice = await services.payment.getInvoice(invoiceId);
    await relatedInvoiceEvent(services, invoice);
    return json(invoice);
  }

  const claimMatch = url.pathname.match(/^\/api\/v1\/invoices\/([^/]+)\/claims$/);
  if (claimMatch && request.method === "POST") {
    const principal = await services.merchantAuth.authenticate(request, ["claim:create"]);
    const invoiceId = decodeURIComponent(claimMatch[1]);
    await requireMerchantInvoice(services, principal.merchant.id, invoiceId);
    const body = await requestJson(request);
    const keys = Object.keys(body);
    if (keys.length === 0) {
      const result = await services.payment.discoverPayment(invoiceId);
      await relatedInvoiceEvent(services, result.invoice);
      return json(result);
    }
    if (
      keys.length !== 4 ||
      keys.some(
        (key) =>
          !["transactionHash", "logIndex", "observedBlockNumber", "observedBlockHash"].includes(
            key,
          ),
      )
    )
      throw new HttpError(400, "invalid_claim_input");
    const result = await services.payment.reportPayment(invoiceId, {
      transactionHash: body.transactionHash,
      logIndex: body.logIndex,
      observedBlockNumber: body.observedBlockNumber,
      observedBlockHash: body.observedBlockHash,
    });
    await relatedInvoiceEvent(services, result.invoice);
    return json(result);
  }

  if (request.method === "GET" || request.method === "HEAD") {
    const staticResponse = maybeStatic(request, url, services.assetFetcher);
    if (staticResponse) return staticResponse;
  }

  throw new HttpError(404, "not_found");
}

async function requireMerchantInvoice(
  services: CheckoutServices,
  merchantId: string,
  invoiceId: string,
): Promise<void> {
  const invoice = await services.payment.getInvoiceRecord(invoiceId);
  if (invoice.merchant !== merchantId) throw new HttpError(404, "invoice_not_found");
}

async function adminOverview(services: CheckoutServices) {
  const [storedChains, merchant, invoices] = await Promise.all([
    services.payment.listChainConfigurations(),
    services.merchantAuth.getMerchant(LINKEDIN_MERCHANT_ID),
    services.payment.listInvoices(50),
  ]);
  const count = (statuses: string[]) =>
    invoices.filter((invoice: InvoiceJson) => statuses.includes(invoice.status)).length;
  return {
    health: "Ready",
    metrics: {
      awaiting: count(["awaiting_payment", "confirming"]),
      provisional: count(["provisionally_paid"]),
      settled: count(["settled", "finalized"]),
      review: invoices.filter(
        (invoice: InvoiceJson) =>
          invoice.requiresManualReview ||
          ["late_payment", "manual_review", "reverted"].includes(invoice.status),
      ).length,
    },
    chains: storedChains,
    merchant,
    keys: merchant ? await services.merchantAuth.listKeys(merchant.id) : [],
    invoices,
  };
}

async function relatedInvoiceEvent(
  services: CheckoutServices,
  invoice: InvoiceJson,
): Promise<void> {
  try {
    // Receipt/reference reads, lookup/admission, and four protected completion calls need thirteen.
    if (services.chainWebhookKeys && (services.budget?.hasCapacity(13) ?? true)) {
      await recoverPendingChainTransfers(
        {
          chainId: invoice.chainId,
          tokenAddress: invoice.tokenAddress,
          recipientAddress: invoice.treasuryAddress,
          amountUnits: BigInt(invoice.paidAmountUnits),
        },
        {
          kv: services.kv,
          budget: services.budget,
          minimumTransferCalls: 3,
          prepareTransfer: services.payment.prepareChainTransfer,
          processTransfer: services.payment.processChainTransfer,
        },
      );
    }
    if (services.wakeups?.ensure && (services.budget?.hasCapacity(9) ?? true)) {
      await services.wakeups.ensure({
        kind: "invoice",
        id: invoice.id,
        dueAt: Date.parse(invoice.expiresAt),
        reason: "expiry",
      });
    }
  } catch {
    // The successful domain response remains valid; the next related event may retry repair.
    console.error(JSON.stringify({ message: "checkout_related_event_pending" }));
  }
}

export async function runHourlyMaintenance(
  services: CheckoutServices,
  deadline = Number.POSITIVE_INFINITY,
  options: { chainId?: number; maxOutboxEvents?: number } = {},
): Promise<void> {
  const chains = await services.payment.listChainConfigurations();
  for (const chain of chains) {
    if (Date.now() >= deadline) return;
    if (
      chain.revision === 0 ||
      (options.chainId !== undefined && chain.chainId !== options.chainId)
    )
      continue;
    try {
      await services.payment.reconcileHourly(chain.chainId);
    } catch (error) {
      console.error("checkout_reconciliation_chain_failed", chain.chainId, error);
    }
  }
  if (options.maxOutboxEvents !== 0)
    await deliverOutbox(services, deadline, options.maxOutboxEvents);
}

export async function deliverOutbox(
  services: CheckoutServices,
  deadline = Number.POSITIVE_INFINITY,
  maxEvents = OUTBOX_MAX_EVENTS_PER_RUN,
): Promise<void> {
  let remaining = maxEvents;
  while (remaining > 0 && Date.now() < deadline) {
    const pageSize = Math.min(OUTBOX_PAGE_SIZE, remaining);
    const events = await services.payment.listOutbox(pageSize);
    for (const event of events) {
      if (Date.now() >= deadline) return;
      await deliverSignedOutboxEvent(services, event, deadline);
    }
    remaining -= events.length;
    if (events.length < pageSize) return;
  }
}

export async function deliverOutboxEvent(
  services: CheckoutServices,
  eventId: string,
  options: { deadline?: number; isCurrent?: () => Promise<boolean> } = {},
): Promise<void> {
  const event = await services.payment.getOutboxEvent(eventId);
  if (!event || (options.isCurrent && !(await options.isCurrent()))) return;
  await deliverSignedOutboxEvent(services, event, options.deadline ?? Infinity, options.isCurrent);
}

async function deliverSignedOutboxEvent(
  services: CheckoutServices,
  event: SignedOutboxEvent,
  deadline: number,
  isCurrent: () => Promise<boolean> = async () => true,
): Promise<void> {
  const current = async () => Date.now() < deadline && (await isCurrent());
  if (!(await current())) return;
  const merchant = await services.merchantAuth.getMerchant(event.payload.merchantId);
  if (!(await current())) return;
  if (!merchant) {
    await services.payment.markOutboxFailed(event.id, "merchant_not_found");
    return;
  }
  let failure: string | undefined;
  try {
    const target = await services.merchantAuth.resolveWebhookTarget(merchant.settlementWebhookUrl);
    if (!(await current())) return;
    const response = await services.webhookSender(
      target,
      JSON.stringify({ eventJws: event.compactJws }),
      AbortSignal.timeout(Math.max(1, Math.min(WEBHOOK_TIMEOUT_MS, deadline - Date.now()))),
    );
    if (!response.ok) failure = `webhook_http_${response.status}`;
  } catch (error) {
    failure = error instanceof Error ? error.message : String(error);
    console.error("checkout_settlement_webhook_failed", event.id, error);
  }
  if (!(await current())) return;
  if (failure) await services.payment.markOutboxFailed(event.id, failure);
  else await services.payment.markOutboxDelivered(event.id);
}

export function withPeerAddress(
  request: Request,
  peerAddress: string,
  requestUrl = request.url,
): Request {
  const headers = new Headers(request.headers);
  headers.delete(LOCAL_PEER_HEADER);
  if (peerAddress === "127.0.0.1" || peerAddress === "::1") {
    headers.set(LOCAL_PEER_HEADER, peerAddress);
  }
  const init: RequestInit & { duplex: "half" } = {
    method: request.method,
    headers,
    signal: request.signal,
    redirect: request.redirect,
    body: request.method === "GET" || request.method === "HEAD" ? undefined : request.body,
    duplex: "half",
  };
  // Bun 1.4 merges original Request headers even when init.headers is empty.
  // Construct from the URL so removing the trust marker cannot retain it.
  return new Request(requestUrl, init);
}

function requireAdminMutation(request: Request): void {
  if (request.headers.get(ADMIN_MUTATION_HEADER) !== ADMIN_MUTATION_VALUE) {
    throw new HttpError(403, "admin_mutation_header_required");
  }
  const origin = request.headers.get("origin");
  if (origin && origin !== new URL(request.url).origin) {
    throw new HttpError(403, "cross_origin_admin_mutation");
  }
}

async function requestJson(request: Request): Promise<Record<string, unknown>> {
  const contentLength = Number(request.headers.get("content-length"));
  if (Number.isFinite(contentLength) && contentLength > JSON_BODY_LIMIT_BYTES) {
    throw new HttpError(413, "request_body_too_large");
  }
  const reader = request.body?.getReader();
  const decoder = new TextDecoder();
  let text = "";
  let bytes = 0;
  if (reader) {
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        bytes += value.byteLength;
        if (bytes > JSON_BODY_LIMIT_BYTES) {
          await reader.cancel();
          throw new HttpError(413, "request_body_too_large");
        }
        text += decoder.decode(value, { stream: true });
      }
      text += decoder.decode();
    } finally {
      reader.releaseLock();
    }
  }
  if (!text.trim()) return {};
  try {
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new HttpError(400, "json_object_required");
    }
    return parsed as Record<string, unknown>;
  } catch (error) {
    if (error instanceof HttpError) throw error;
    throw new HttpError(400, "invalid_json");
  }
}

function requiredString(value: unknown, code: string): string {
  if (typeof value !== "string" || !value.trim()) throw new HttpError(400, code);
  return value.trim();
}

function optionalScopes(value: unknown): MerchantScope[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.some((scope) => typeof scope !== "string")) {
    throw new HttpError(400, "invalid_scopes");
  }
  return value as MerchantScope[];
}

function memberRole(value: unknown): "admin" | "viewer" {
  if (value === "admin" || value === "viewer") return value;
  throw new HttpError(400, "invalid_member_role");
}

function maybeStatic(
  request: Request,
  url: URL,
  assetFetcher: CheckoutServices["assetFetcher"],
): Response | Promise<Response> | null {
  if (url.pathname === "/" || url.pathname === "/index.html") {
    return staticFile("index.html", "text/html; charset=utf-8", request, assetFetcher);
  }
  if (url.pathname === "/styles.css") {
    return staticFile("styles.css", "text/css; charset=utf-8", request, assetFetcher);
  }
  if (url.pathname === "/app.js") {
    return staticFile("app.js", "text/javascript; charset=utf-8", request, assetFetcher);
  }
  return null;
}

async function staticFile(
  name: string,
  contentType: string,
  request: Request,
  assetFetcher: CheckoutServices["assetFetcher"],
): Promise<Response> {
  const headers = new Headers({
    "cache-control": name === "index.html" ? "no-cache" : "public, max-age=300",
    "content-type": contentType,
    "content-security-policy":
      "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'; img-src 'self' data:; script-src 'self'; style-src 'self'; connect-src 'self'",
    "referrer-policy": "no-referrer",
    "x-content-type-options": "nosniff",
  });
  if (request.method === "HEAD") return new Response(null, { headers });
  const asset = await assetFetcher(
    new Request(new URL(name === "index.html" ? "/" : `/${name}`, request.url)),
  );
  if (!asset.ok) throw new Error("static_asset_unavailable");
  return new Response(asset.body, { headers });
}

function json(body: unknown, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  headers.set("content-type", "application/json; charset=utf-8");
  if (!headers.has("cache-control")) headers.set("cache-control", "no-store");
  return new Response(init.status === 204 ? null : JSON.stringify(body), { ...init, headers });
}

function httpError(error: unknown): { status: number; code: string } {
  if (
    error instanceof HttpError ||
    error instanceof AdminAuthError ||
    error instanceof MerchantAuthError ||
    error instanceof WorkerBudgetError
  ) {
    return { status: error.status, code: error.message };
  }
  if (error instanceof PaymentDomainError) {
    if (error.code.endsWith("_not_found")) return { status: 404, code: error.code };
    if (
      error.code.includes("conflict") ||
      error.code.includes("already_claimed") ||
      error.code === "amount_space_exhausted" ||
      error.code === "chain_disabled" ||
      error.code === "chain_not_configured"
    )
      return { status: 409, code: error.code };
    return { status: 400, code: error.code };
  }
  if (error instanceof RpcVerificationError) return { status: 422, code: error.code };
  if (error instanceof RpcError) return { status: 503, code: "rpc_unavailable" };
  if (error instanceof JwsError) return { status: 500, code: "signing_unavailable" };
  return { status: 500, code: "internal_error" };
}

class HttpError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "HttpError";
  }
}
