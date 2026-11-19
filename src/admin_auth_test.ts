import type { FetchLike } from "./rpc.ts";
import { SqliteKv } from "./sqlite_kv.ts";
import { test } from "bun:test";
import type { Kv } from "./kv.ts";
import {
  ADMIN_SESSION_COOKIE,
  AdminAuthError,
  AUTH_HUB_AUDIENCE,
  AUTH_HUB_CLIENT_ID,
  AUTH_HUB_ORIGIN,
  type AuthHubClaims,
  claimOrLoadMembership,
  createAdminAuthService,
  LOCAL_PEER_HEADER,
  verifyAuthHubToken,
} from "./admin_auth.ts";

const NOW = Date.parse("2026-07-21T12:00:00.000Z");

test("Auth Hub verifier accepts SSO and rejects agent tokens", async () => {
  const keys = await generateSigningKeys();
  const sso = await signToken(keys.privateKey, keys.kid, claims({ auth: "sso" }));
  const accepted = await verifyAuthHubToken(sso, { keys: [keys.publicJwk] }, NOW);
  assertEquals(accepted?.sub, "hub-owner");

  const agent = await signToken(keys.privateKey, keys.kid, {
    ...claims(),
    auth: "agent-token",
    agentTokenId: "agent-1",
  });
  assertEquals(await verifyAuthHubToken(agent, { keys: [keys.publicJwk] }, NOW), null);
});

test("first Auth Hub admin atomically claims the Checkout owner", async () => {
  await withKv(async (kv) => {
    const owner = await claimOrLoadMembership(
      kv,
      claims({ sub: "owner", isAdmin: true }),
      () => NOW,
    );
    assertEquals(owner.role, "owner");

    await assertRejects(
      () => claimOrLoadMembership(kv, claims({ sub: "second", isAdmin: true }), () => NOW),
      "not_a_checkout_member",
    );
    const sameOwner = await claimOrLoadMembership(
      kv,
      claims({ sub: "owner", handle: "renamed", isAdmin: false }),
      () => NOW + 1_000,
    );
    assertEquals(sameOwner.handle, "renamed");
  });
});

test("the owner can invite an Auth Hub handle that atomically becomes a member", async () => {
  await withKv(async (kv) => {
    await claimOrLoadMembership(
      kv,
      claims({ sub: "owner", handle: "owner", isAdmin: true }),
      () => NOW,
    );
    const service = createAdminAuthService(kv, { now: () => NOW + 1_000 });
    const ownerRequest = new Request("http://localhost:8000/api/v1/admin/members/invitee", {
      headers: { [LOCAL_PEER_HEADER]: "127.0.0.1" },
    });
    const granted = await service.upsertMember(ownerRequest, "Invitee", "admin");
    assertEquals("invitation" in granted, true);
    assertEquals((await service.listInvitations(ownerRequest)).length, 1);

    const member = await claimOrLoadMembership(
      kv,
      claims({ sub: "invited-user", handle: "invitee", isAdmin: false }),
      () => NOW + 2_000,
    );
    assertEquals(member.role, "admin");
    assertEquals(member.handle, "invitee");
    assertEquals((await service.listInvitations(ownerRequest)).length, 0);
    assertEquals((await service.listMembers(ownerRequest)).length, 2);
  });
});

test("login redirects to the registered Checkout Auth Hub client", async () => {
  await withKv(async (kv) => {
    const service = createAdminAuthService(kv, {
      now: () => NOW,
      randomUuid: () => "state-123",
    });
    const response = await service.loginRedirect(
      new Request("https://checkout.ubq.fi/api/auth/login?returnTo=%2Fsettings"),
    );
    assertEquals(response.status, 302);
    const location = new URL(response.headers.get("location") ?? "");
    assertEquals(location.origin, AUTH_HUB_ORIGIN);
    assertEquals(location.searchParams.get("client_id"), AUTH_HUB_CLIENT_ID);
    assertEquals(location.searchParams.get("audience"), AUTH_HUB_AUDIENCE);
    assertEquals(location.searchParams.get("state"), "state-123");
    assertEquals(location.searchParams.get("code_challenge_method"), "S256");
    const challenge = location.searchParams.get("code_challenge") ?? "";
    assertEquals(/^[A-Za-z0-9_-]{43}$/u.test(challenge), true);
    const state = JSON.parse(authStateCookie(response));
    assertEquals(state.state, "state-123");
    assertEquals(state.returnTo, "/settings");
    assertEquals(/^[A-Za-z0-9_-]{43}$/u.test(state.codeVerifier), true);
    assertEquals(
      base64Url(
        new Uint8Array(
          await crypto.subtle.digest("SHA-256", new TextEncoder().encode(state.codeVerifier)),
        ),
      ),
      challenge,
    );
    assert((response.headers.get("set-cookie") ?? "").includes("HttpOnly"));
  });
});

test("callback exchanges the PKCE verifier bound to the login state", async () => {
  await withKv(async (kv) => {
    let exchange: Record<string, unknown> = {};
    const service = createAdminAuthService(kv, {
      now: () => NOW,
      fetch: ((_input, init) => {
        exchange = JSON.parse(String(init?.body));
        return Promise.resolve(Response.json({ error: "invalid_code" }, { status: 400 }));
      }) as FetchLike,
    });
    const login = await service.loginRedirect(
      new Request("https://checkout.ubq.fi/api/auth/login"),
    );
    const state = JSON.parse(authStateCookie(login));
    const callback = new Request(
      `https://checkout.ubq.fi/auth/callback?state=${encodeURIComponent(state.state)}&code=test-code`,
      {
        headers: { cookie: `uos_checkout_auth_state=${encodeURIComponent(JSON.stringify(state))}` },
      },
    );
    await assertRejects(() => service.callback(callback), "auth_exchange_failed");
    assertEquals(exchange.code, "test-code");
    assertEquals(exchange.codeVerifier, state.codeVerifier);
  });
});

test("only an internally marked loopback peer receives local owner access", async () => {
  await withKv(async (kv) => {
    const service = createAdminAuthService(kv);
    const untrusted = await service.sessionFromRequest(new Request("http://localhost:8000/"));
    assertEquals(untrusted, null);

    const headers = new Headers({ [LOCAL_PEER_HEADER]: "127.0.0.1" });
    const local = await service.requireSession(new Request("http://localhost:8000/", { headers }));
    assertEquals(local.member.role, "owner");
    assertEquals(local.claims.sub, "local-owner");
    const remote = await service.sessionFromRequest(
      new Request("https://checkout.ubq.fi/", { headers }),
    );
    assertEquals(remote, null);
  });
});

test("JWKS refreshes only once for an unknown kid and never for known-key failures", async () => {
  const trusted = await generateSigningKeys();
  const attacker = await generateSigningKeys();
  let currentNow = NOW;
  let fetchCount = 0;
  await withKv(async (kv) => {
    const service = createAdminAuthService(kv, {
      now: () => currentNow,
      fetch: (() => {
        fetchCount++;
        return Promise.resolve(Response.json({ keys: [trusted.publicJwk] }));
      }) as FetchLike,
    });
    const valid = await signToken(trusted.privateKey, trusted.kid, claims());
    await service.sessionFromRequest(sessionRequest(valid));
    assertEquals(fetchCount, 1);

    const badKnownSignature = await signToken(attacker.privateKey, trusted.kid, claims());
    await service.sessionFromRequest(sessionRequest(badKnownSignature));
    assertEquals(fetchCount, 1);

    currentNow += 61_000;
    const unknown = await signToken(attacker.privateKey, "unknown-key", claims());
    await service.sessionFromRequest(sessionRequest(unknown));
    assertEquals(fetchCount, 2);
    await service.sessionFromRequest(sessionRequest(unknown));
    const anotherUnknown = await signToken(attacker.privateKey, "another-unknown", claims());
    await service.sessionFromRequest(sessionRequest(anotherUnknown));
    assertEquals(fetchCount, 2);

    const malformedKid = await signToken(attacker.privateKey, "../../jwks", claims());
    await service.sessionFromRequest(sessionRequest(malformedKid));
    assertEquals(fetchCount, 2);
  });
});

function claims(overrides: Partial<AuthHubClaims> = {}): AuthHubClaims {
  return {
    iss: AUTH_HUB_ORIGIN,
    sub: "hub-owner",
    handle: "owner",
    isAdmin: true,
    scopes: ["admin"],
    aud: AUTH_HUB_AUDIENCE,
    clientId: AUTH_HUB_CLIENT_ID,
    auth: "sso",
    iat: Math.floor(NOW / 1000) - 5,
    exp: Math.floor(NOW / 1000) + 15 * 60,
    jti: "token-1",
    ...overrides,
  };
}

function sessionRequest(token: string): Request {
  return new Request("https://checkout.ubq.fi/", {
    headers: { cookie: `${ADMIN_SESSION_COOKIE}=${encodeURIComponent(token)}` },
  });
}

function authStateCookie(response: Response): string {
  const match = (response.headers.get("set-cookie") ?? "").match(
    /(?:^|, )uos_checkout_auth_state=([^;]*)/u,
  );
  if (!match) throw new Error("Expected auth state cookie");
  return decodeURIComponent(match[1]);
}

async function generateSigningKeys() {
  const pair = await crypto.subtle.generateKey(
    {
      name: "RSASSA-PKCS1-v1_5",
      modulusLength: 2048,
      publicExponent: new Uint8Array([1, 0, 1]),
      hash: "SHA-256",
    },
    true,
    ["sign", "verify"],
  );
  const kid = "test-key";
  return {
    privateKey: pair.privateKey,
    kid,
    publicJwk: { ...(await crypto.subtle.exportKey("jwk", pair.publicKey)), kid },
  };
}

async function signToken(
  key: CryptoKey,
  kid: string,
  payload: Record<string, unknown>,
): Promise<string> {
  const header = base64Url(JSON.stringify({ alg: "RS256", typ: "JWT", kid }));
  const body = base64Url(JSON.stringify(payload));
  const input = `${header}.${body}`;
  const signature = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5",
    key,
    new TextEncoder().encode(input),
  );
  return `${input}.${base64Url(new Uint8Array(signature))}`;
}

function base64Url(value: string | Uint8Array): string {
  const bytes = typeof value === "string" ? new TextEncoder().encode(value) : value;
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/g, "");
}

async function withKv(run: (kv: Kv) => void | Promise<void>): Promise<void> {
  const kv = new SqliteKv(":memory:");
  try {
    await run(kv);
  } finally {
    kv.close();
  }
}

async function assertRejects(run: () => Promise<unknown>, message: string): Promise<void> {
  try {
    await run();
  } catch (error) {
    if (error instanceof AdminAuthError && error.message === message) return;
    throw error;
  }
  throw new Error(`Expected rejection with ${message}`);
}

function assert(value: unknown): asserts value {
  if (!value) throw new Error("Expected value to be truthy");
}

function assertEquals(actual: unknown, expected: unknown): void {
  if (actual !== expected) {
    throw new Error(`Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}
