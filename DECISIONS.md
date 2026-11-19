# Checkout decisions

## 2026-11-19: Checkout uses canonical auth.ubq.fi

Checkout's external Auth Hub origin and expected JWT issuer become `https://auth.ubq.fi`. Client `uos-checkout`, audience `uos-checkout`, origin `https://checkout.ubq.fi`, callback `/auth/callback`, PKCE, local membership storage and the existing administrator role contract remain unchanged. The existing owner user ID already matches a Passport administrator account, so no membership remap is required.

The retired `deno-universal-auth.0x4007.deno.net` origin is no longer accepted. `passport.ubq.fi` tokens are not accepted. Existing Checkout admin cookies contain the old issuer and require a fresh sign-in.

Reason: consolidate live Checkout authentication onto the canonical Ubiquity Passport identity.

Reversal risk: rollback requires restoring the previous Checkout Worker and the old auth service together; a partial rollback leaves administrators signed out.

## 2026-10-18: Bun development and Workers Free production

The application uses pinned Bun 1.4.0 for local development, scripts, demo, tests, and dependency installation. Production uses Cloudflare Workers and its `workerd` runtime. This explicitly replaces the former Deno-only project rule; no Deno runtime, CLI, configuration, or deployment requirement remains. Node 24.20.0 is a narrow exception for Cloudflare's supported Wrangler/Miniflare control plane. The Bun test runner builds and launches that control plane when testing real workerd; application execution stays Bun or workerd.

The portable application remains in `main.ts`; `bun.ts` owns the actual Bun listener and optional hosted maintenance interval, and `worker.ts` owns Worker HTTP, assets, and scheduled events. Worker services are request-local. The Worker import graph must contain no Bun-only storage or TLS modules. No Container, Durable Object, paid relay, or Docker resource belongs to this deployment.

## Persistence and compatibility

Production continues to use the existing `ubq-kv` Worker service, its D1 database, the `uos_checkout/v1` keyspace, and `CHECKOUT_KV_SERVICE_TOKEN`. No data copy, reset, database replacement, or signing-key rotation is part of this migration. Legacy raw JSON values remain readable. New writes use the versioned `uos_checkout_kv` envelope; every object and array is encoded structurally so arbitrary merchant JSON cannot collide with a nested bigint tag. Public token amounts remain decimal strings.

The shared service implements JSON values, CAS atomics, TTL, and prefix/cursor listing. The adapter treats HTTP 200 atomic conflicts as conflicts instead of transport failures. It fetches all prefix pages, decodes values, then applies typed tuple ordering, start-inclusive/end-exclusive ranges, reverse, and limit. Shared D1 LIKE patterns have a 50-byte bound: oversized derived prefixes fall back only to the wholly Checkout-owned `["uos_checkout", "v1"]` prefix before client filtering. This preserves current service behavior without changing the shared auth backend, at the cost of additional Checkout-only reads as that keyspace grows.

Local state uses Bun SQLite with transaction-backed CAS/versionstamps, snapshot values, expiry-as-absent reads/checks, and the same list semantics. `CHECKOUT_KV_URL` is a local SQLite path; service URL/token settings continue to select the hosted HTTP adapter for Bun.

## Webhook and ingress boundaries

Workers sends the original normalized HTTPS URL using native fetch, fresh application headers, `redirect: "manual"`, a timeout, and no incoming Host, credentials, or Cloudflare destination override. Public DNS prevalidation remains in place. The explicit `global_fetch_strictly_public` and `no_cots_on_external_fetch` compatibility flags enforces the actual connection's public destination after DNS resolution. Worker transport relies on platform TLS verification and does not claim to inspect or pin a peer address. A redirect is a failed delivery and never receives a replay of signed bytes. Response bodies are cancelled without unbounded buffering.

Bun retains hostname-verified TLS plus connected-peer comparison with the approved DNS result set before writing the HTTP request. Real local TLS tests cover trusted certificates, hostname mismatch, and peer mismatch with zero decrypted application bytes on rejection. Tests never disable certificate verification globally.

The Bun listener removes and rebuilds the internal local-peer marker from `server.requestIP()`. Both loopback peer and local HTTP origin are required for the development identity; a production hostname proxied over loopback remains authenticated normally. A Bun process behind the established local reverse proxy reconstructs HTTPS only when the actual peer is loopback, Host is exactly `checkout.ubq.fi`, and `x-forwarded-proto` is exactly `https`; this grants no development identity. The Worker strips the marker unconditionally, including on forged localhost URLs. Checkout retains its existing external Auth Hub origin/issuer pair; this runtime migration does not migrate that external authentication service.

## Maintenance and deployment contract

Workers Free currently allows 10 ms CPU per HTTP/cron invocation, 50 external subrequests per invocation, and 100,000 requests per day. One cron every minute uses 1,440 ticks/day and rotates three chains, giving each chain a turn every three minutes. A tick prioritizes at most one outbox delivery and one invoice from that chain, scans at most 1,000 blocks with a 128-block overlap, and yields under a 24-call application budget and a 20-second wall deadline. Normal HTTP invocations use a 40-call budget. No paid CPU override is configured. Incomplete reconciliation does not advance its durable cursor; unattempted deliveries remain retryable. A CAS-protected per-chain invoice rotation pointer prevents a failing invoice from starving later invoices. Each invoice has its own scan cursor, advanced only after completed work. Broad prefix listings can still consume the free budget as storage grows; this is not a claim that every invoice is checked hourly or of unlimited throughput under the free CPU ceiling.

`bun run check` covers Prettier, ESLint, both Bun and Worker TypeScript entry points, the isolated offline suite, real Bun ingress/TLS checks, and the real workerd harness with mocked storage/RPC/DNS/webhook upstreams. The harness does not write production state. `bun run test:anvil` is a separate networked demo acceptance requiring Foundry and pinned Base fork access. `bun run deploy:dry-run` verifies Worker packaging. Production readiness additionally requires the deployed public path, retained JWKS identity, authenticated API behavior, and scheduled acceptance; build and mock evidence do not establish them.

The `candidate` environment has no routes or cron. Production targets only `checkout.ubq.fi/*` and the established `ubq-kv` service. CI deploys the `development` branch using the existing protected `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` CI credentials; the existing Checkout KV token stays a Worker secret. Cutover must stop the previous hosted scheduler before enabling Worker production cron, and must retain a migrated Bun release with the same codec as a reversible target until live acceptance passes. The old adapter cannot read newly enveloped values and must not be restored after new writes.

Current platform contracts were checked against [Workers limits](https://developers.cloudflare.com/workers/platform/limits/), [fetch](https://developers.cloudflare.com/workers/runtime-apis/fetch/), [request redirects](https://developers.cloudflare.com/workers/runtime-apis/request/), [Cron Triggers](https://developers.cloudflare.com/workers/configuration/cron-triggers/), and [Wrangler installation](https://developers.cloudflare.com/workers/wrangler/install-and-update/). Deployment receipts belong to the release handback rather than this decision record.
