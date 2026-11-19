import type { FetchLike } from "./rpc.ts";
import type { Kv } from "./kv.ts";
export type AdminRole = "owner" | "admin" | "viewer";

export type AuthHubClaims = {
  iss: string;
  sub: string;
  handle: string;
  isAdmin: boolean;
  scopes: string[];
  aud: string;
  clientId: string;
  auth: "sso";
  iat: number;
  exp: number;
  jti: string;
  agentTokenId?: never;
};

export type AdminMember = {
  userId: string;
  handle: string;
  role: AdminRole;
  claimedAt: string;
  updatedAt: string;
};

export type AdminInvitation = {
  handle: string;
  role: Exclude<AdminRole, "owner">;
  invitedAt: string;
  updatedAt: string;
};

export type AdminSession = {
  claims: AuthHubClaims;
  member: AdminMember;
  token: string;
};

export const AUTH_HUB_ORIGIN = "https://auth.ubq.fi";
export const AUTH_HUB_CLIENT_ID = "uos-checkout";
export const AUTH_HUB_AUDIENCE = "uos-checkout";
export const ADMIN_SESSION_COOKIE = "uos_checkout_session";
export const AUTH_STATE_COOKIE = "uos_checkout_auth_state";
export const LOCAL_PEER_HEADER = "x-uos-checkout-peer-address";

const PREFIX = ["uos_checkout", "v1", "admin"];
const OWNER_KEY = [...PREFIX, "owner"];
const memberKey = (userId: string) => [...PREFIX, "members", userId];
const invitationKey = (handle: string) => [...PREFIX, "invitations", handle];
const JWKS_KEY = [...PREFIX, "auth_hub_jwks"];
const JWKS_REFRESH_COOLDOWN_KEY = [...PREFIX, "auth_hub_jwks_refresh_cooldown"];
const unknownKidKey = (kid: string) => [...PREFIX, "auth_hub_unknown_kids", kid];
const AUTH_CALLBACK_PATH = "/auth/callback";
const AUTH_STATE_TTL_MS = 10 * 60 * 1000;
const JWKS_TTL_MS = 60 * 60 * 1000;
const JWKS_REFRESH_COOLDOWN_MS = 60 * 1000;

type AuthJwk = JsonWebKey & { kid?: string };
type JsonWebKeySet = { keys: AuthJwk[] };

type StoredJwks = JsonWebKeySet & { fetchedAt: string };

type AdminAuthOptions = {
  fetch?: FetchLike;
  now?: () => number;
  randomUuid?: () => string;
};

export type AdminAuthService = ReturnType<typeof createAdminAuthService>;

export function createAdminAuthService(kv: Kv, options: AdminAuthOptions = {}) {
  const fetcher = options.fetch ?? fetch;
  const now = options.now ?? Date.now;
  const randomUuid = options.randomUuid ?? (() => crypto.randomUUID());

  const loadJwks = async (force = false): Promise<JsonWebKeySet> => {
    if (!force) {
      const cached = (await kv.get<StoredJwks>(JWKS_KEY)).value;
      if (cached && Date.parse(cached.fetchedAt) + JWKS_TTL_MS > now()) {
        return { keys: cached.keys };
      }
    }

    const response = await fetcher(`${AUTH_HUB_ORIGIN}/api/auth/jwks`);
    if (!response.ok) throw new AdminAuthError(503, "auth_hub_unavailable");
    const value = (await responseJson(response)) as Partial<JsonWebKeySet>;
    if (!Array.isArray(value.keys) || value.keys.some((key) => !key || typeof key !== "object")) {
      throw new AdminAuthError(503, "auth_hub_invalid_jwks");
    }
    const stored: StoredJwks = { keys: value.keys as AuthJwk[], fetchedAt: nowIso(now()) };
    await kv
      .atomic()
      .set(JWKS_KEY, stored)
      .set(JWKS_REFRESH_COOLDOWN_KEY, stored.fetchedAt, {
        expireIn: JWKS_REFRESH_COOLDOWN_MS,
      })
      .commit();
    return { keys: stored.keys };
  };

  const verify = async (token: string): Promise<AuthHubClaims | null> => {
    const kid = jwtKid(token);
    if (!isWellFormedKid(kid)) return null;
    const first = await loadJwks();
    if (first.keys.some((key) => key.kid === kid)) {
      return await verifyAuthHubToken(token, first, now());
    }
    if ((await kv.get(unknownKidKey(kid))).value !== null) return null;

    const cooldown = await kv.get<string>(JWKS_REFRESH_COOLDOWN_KEY);
    if (cooldown.value !== null && Date.parse(cooldown.value) + JWKS_REFRESH_COOLDOWN_MS > now()) {
      await rememberUnknownKid(kv, kid);
      return null;
    }
    const locked = await kv
      .atomic()
      .check(cooldown)
      .set(JWKS_REFRESH_COOLDOWN_KEY, nowIso(now()), { expireIn: JWKS_REFRESH_COOLDOWN_MS })
      .commit();
    if (!locked.ok) {
      await rememberUnknownKid(kv, kid);
      return null;
    }
    const refreshed = await loadJwks(true);
    if (!refreshed.keys.some((key) => key.kid === kid)) {
      await rememberUnknownKid(kv, kid);
      return null;
    }
    return await verifyAuthHubToken(token, refreshed, now());
  };

  const sessionFromRequest = async (request: Request): Promise<AdminSession | null> => {
    if (isTrustedLocalRequest(request)) return localSession(now);
    const token = cookieValue(request, ADMIN_SESSION_COOKIE);
    if (!token) return null;
    const claims = await verify(token);
    if (!claims) return null;
    const member = (await kv.get<AdminMember>(memberKey(claims.sub))).value;
    if (!member) return null;
    return { claims, member, token };
  };

  return {
    sessionFromRequest,
    async requireSession(request: Request): Promise<AdminSession> {
      const session = await sessionFromRequest(request);
      if (!session) throw new AdminAuthError(401, "unauthorized");
      return session;
    },
    async requireRole(request: Request, roles: AdminRole[]): Promise<AdminSession> {
      const session = await sessionFromRequest(request);
      if (!session) throw new AdminAuthError(401, "unauthorized");
      if (!roles.includes(session.member.role)) throw new AdminAuthError(403, "forbidden");
      return session;
    },
    async loginRedirect(request: Request): Promise<Response> {
      if (isTrustedLocalRequest(request)) return redirect(new URL("/", request.url).toString());
      const requestUrl = new URL(request.url);
      assertAllowedOrigin(requestUrl.origin);
      const state = randomUuid();
      const codeVerifier = base64UrlEncode(crypto.getRandomValues(new Uint8Array(32)));
      const codeChallenge = base64UrlEncode(
        new Uint8Array(
          await crypto.subtle.digest("SHA-256", new TextEncoder().encode(codeVerifier)),
        ),
      );
      const returnTo = safeReturnTo(requestUrl.searchParams.get("returnTo"), requestUrl.origin);
      const redirectUri = `${requestUrl.origin}${AUTH_CALLBACK_PATH}`;
      const authUrl = new URL("/authorize", AUTH_HUB_ORIGIN);
      authUrl.searchParams.set("client_id", AUTH_HUB_CLIENT_ID);
      authUrl.searchParams.set("audience", AUTH_HUB_AUDIENCE);
      authUrl.searchParams.set("origin", requestUrl.origin);
      authUrl.searchParams.set("redirect_uri", redirectUri);
      authUrl.searchParams.set("state", state);
      authUrl.searchParams.set("code_challenge", codeChallenge);
      authUrl.searchParams.set("code_challenge_method", "S256");
      return new Response(null, {
        status: 302,
        headers: {
          location: authUrl.toString(),
          "set-cookie": serializeCookie(
            request,
            AUTH_STATE_COOKIE,
            JSON.stringify({ state, returnTo, codeVerifier }),
            now() + AUTH_STATE_TTL_MS,
          ),
        },
      });
    },
    async callback(request: Request): Promise<Response> {
      if (isTrustedLocalRequest(request)) return redirect(new URL("/", request.url).toString());
      const requestUrl = new URL(request.url);
      assertAllowedOrigin(requestUrl.origin);
      const expected = readAuthState(request);
      const state = requestUrl.searchParams.get("state") ?? "";
      const code = requestUrl.searchParams.get("code") ?? "";
      if (!expected || !state || !code || !constantTimeEqual(state, expected.state)) {
        throw new AdminAuthError(400, "invalid_auth_callback");
      }

      const redirectUri = `${requestUrl.origin}${AUTH_CALLBACK_PATH}`;
      const exchange = await fetcher(`${AUTH_HUB_ORIGIN}/api/auth/sso/exchange`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          code,
          clientId: AUTH_HUB_CLIENT_ID,
          audience: AUTH_HUB_AUDIENCE,
          origin: requestUrl.origin,
          redirectUri,
          codeVerifier: expected.codeVerifier,
        }),
      });
      const body = await responseJson(exchange);
      const token = typeof body.accessToken === "string" ? body.accessToken : "";
      if (!exchange.ok || !token) {
        throw new AdminAuthError(exchange.status === 401 ? 401 : 502, "auth_exchange_failed");
      }
      const claims = await verify(token);
      if (!claims) throw new AdminAuthError(401, "auth_hub_invalid_token");
      await claimOrLoadMembership(kv, claims, now);

      const headers = new Headers({ location: expected.returnTo });
      headers.append(
        "set-cookie",
        serializeCookie(request, ADMIN_SESSION_COOKIE, token, claims.exp * 1000),
      );
      headers.append("set-cookie", clearCookie(request, AUTH_STATE_COOKIE));
      return new Response(null, { status: 302, headers });
    },
    logout(request: Request): Response {
      return new Response(null, {
        status: 204,
        headers: { "set-cookie": clearCookie(request, ADMIN_SESSION_COOKIE) },
      });
    },
    async listMembers(request: Request): Promise<AdminMember[]> {
      await this.requireRole(request, ["owner"]);
      const members: AdminMember[] = [];
      for await (const entry of kv.list<AdminMember>({ prefix: [...PREFIX, "members"] })) {
        members.push(entry.value);
      }
      return members.sort((a, b) => a.claimedAt.localeCompare(b.claimedAt));
    },
    async listInvitations(request: Request): Promise<AdminInvitation[]> {
      await this.requireRole(request, ["owner"]);
      const invitations: AdminInvitation[] = [];
      for await (const entry of kv.list<AdminInvitation>({ prefix: [...PREFIX, "invitations"] })) {
        invitations.push(entry.value);
      }
      return invitations.sort((a, b) => a.invitedAt.localeCompare(b.invitedAt));
    },
    async upsertMember(
      request: Request,
      handleInput: string,
      role: Exclude<AdminRole, "owner">,
    ): Promise<{ member: AdminMember } | { invitation: AdminInvitation }> {
      await this.requireRole(request, ["owner"]);
      if (role !== "admin" && role !== "viewer") {
        throw new AdminAuthError(400, "invalid_member_role");
      }
      const handle = normalizeHandle(handleInput);
      for await (const entry of kv.list<AdminMember>({ prefix: [...PREFIX, "members"] })) {
        if (entry.value.handle !== handle) continue;
        if (entry.value.role === "owner") throw new AdminAuthError(409, "owner_role_immutable");
        const member = { ...entry.value, role, updatedAt: nowIso(now()) };
        const result = await kv.atomic().check(entry).set(entry.key, member).commit();
        if (!result.ok) throw new AdminAuthError(409, "member_changed");
        return { member };
      }

      const entry = await kv.get<AdminInvitation>(invitationKey(handle));
      const timestamp = nowIso(now());
      const invitation: AdminInvitation = {
        handle,
        role,
        invitedAt: entry.value?.invitedAt ?? timestamp,
        updatedAt: timestamp,
      };
      const result = await kv.atomic().check(entry).set(entry.key, invitation).commit();
      if (!result.ok) throw new AdminAuthError(409, "invitation_changed");
      return { invitation };
    },
  };
}

export async function claimOrLoadMembership(
  kv: Kv,
  claims: AuthHubClaims,
  now: () => number = Date.now,
): Promise<AdminMember> {
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const handle = normalizeHandle(claims.handle);
    const [owner, member, invitation] = await kv.getMany<[string, AdminMember, AdminInvitation]>([
      OWNER_KEY,
      memberKey(claims.sub),
      invitationKey(handle),
    ]);
    if (owner.value) {
      if (!member.value) {
        if (!invitation.value) throw new AdminAuthError(403, "not_a_checkout_member");
        const timestamp = nowIso(now());
        const created: AdminMember = {
          userId: claims.sub,
          handle,
          role: invitation.value.role,
          claimedAt: timestamp,
          updatedAt: timestamp,
        };
        const result = await kv
          .atomic()
          .check(owner)
          .check(member)
          .check(invitation)
          .set(memberKey(claims.sub), created)
          .delete(invitationKey(handle))
          .commit();
        if (result.ok) return created;
        continue;
      }
      if (member.value.handle !== handle) {
        const updated = { ...member.value, handle, updatedAt: nowIso(now()) };
        const result = await kv.atomic().check(member).set(memberKey(claims.sub), updated).commit();
        if (result.ok) return updated;
        continue;
      }
      return member.value;
    }
    if (!claims.isAdmin) throw new AdminAuthError(403, "checkout_owner_not_claimed");

    const timestamp = nowIso(now());
    const created: AdminMember = {
      userId: claims.sub,
      handle,
      role: "owner",
      claimedAt: timestamp,
      updatedAt: timestamp,
    };
    const result = await kv
      .atomic()
      .check(owner)
      .check(member)
      .set(OWNER_KEY, claims.sub)
      .set(memberKey(claims.sub), created)
      .commit();
    if (result.ok) return created;
  }
  throw new AdminAuthError(409, "checkout_owner_claim_race");
}

export async function verifyAuthHubToken(
  token: string,
  jwks: JsonWebKeySet,
  now = Date.now(),
): Promise<AuthHubClaims | null> {
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  const [encodedHeader, encodedClaims, encodedSignature] = parts;
  try {
    const header = decodeJson<{ alg?: unknown; kid?: unknown }>(encodedHeader);
    const claims = decodeJson<AuthHubClaims & { auth?: unknown; agentTokenId?: unknown }>(
      encodedClaims,
    );
    if (header.alg !== "RS256" || typeof header.kid !== "string") return null;
    if (!validClaims(claims, now)) return null;
    const jwk = jwks.keys.find((candidate) => candidate.kid === header.kid);
    if (!jwk) return null;
    const key = await crypto.subtle.importKey(
      "jwk",
      jwk,
      { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
      false,
      ["verify"],
    );
    const verified = await crypto.subtle.verify(
      "RSASSA-PKCS1-v1_5",
      key,
      decodeBase64Url(encodedSignature),
      new TextEncoder().encode(`${encodedHeader}.${encodedClaims}`),
    );
    return verified ? claims : null;
  } catch {
    return null;
  }
}

function validClaims(
  claims: AuthHubClaims & { auth?: unknown; agentTokenId?: unknown },
  now: number,
): claims is AuthHubClaims {
  const nowSeconds = Math.floor(now / 1000);
  return Boolean(
    claims &&
    claims.iss === AUTH_HUB_ORIGIN &&
    claims.aud === AUTH_HUB_AUDIENCE &&
    claims.clientId === AUTH_HUB_CLIENT_ID &&
    claims.auth === "sso" &&
    claims.agentTokenId === undefined &&
    typeof claims.sub === "string" &&
    claims.sub.length > 0 &&
    typeof claims.handle === "string" &&
    claims.handle.length > 0 &&
    typeof claims.isAdmin === "boolean" &&
    Array.isArray(claims.scopes) &&
    claims.scopes.every((scope) => typeof scope === "string") &&
    Number.isInteger(claims.iat) &&
    claims.iat <= nowSeconds + 60 &&
    Number.isInteger(claims.exp) &&
    claims.exp > nowSeconds &&
    typeof claims.jti === "string" &&
    claims.jti.length > 0,
  );
}

function localSession(now: () => number): AdminSession {
  const nowSeconds = Math.floor(now() / 1000);
  return {
    token: "",
    claims: {
      iss: "local-development",
      sub: "local-owner",
      handle: "local-owner",
      isAdmin: true,
      scopes: ["*"],
      aud: AUTH_HUB_AUDIENCE,
      clientId: AUTH_HUB_CLIENT_ID,
      auth: "sso",
      iat: nowSeconds,
      exp: nowSeconds + 24 * 60 * 60,
      jti: "local-owner",
    },
    member: {
      userId: "local-owner",
      handle: "local-owner",
      role: "owner",
      claimedAt: nowIso(now()),
      updatedAt: nowIso(now()),
    },
  };
}

function isTrustedLocalRequest(request: Request): boolean {
  const peer = request.headers.get(LOCAL_PEER_HEADER);
  const url = new URL(request.url);
  const localOrigin =
    url.protocol === "http:" &&
    (url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "[::1]");
  return localOrigin && (peer === "127.0.0.1" || peer === "::1");
}

function assertAllowedOrigin(origin: string): void {
  const allowed = new Set([
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "https://checkout.ubq.fi",
  ]);
  if (!allowed.has(origin)) throw new AdminAuthError(400, "invalid_checkout_origin");
}

function safeReturnTo(value: string | null, origin: string): string {
  if (!value) return "/";
  try {
    const url = new URL(value, origin);
    return url.origin === origin ? `${url.pathname}${url.search}${url.hash}` : "/";
  } catch {
    return "/";
  }
}

function normalizeHandle(value: unknown): string {
  if (typeof value !== "string") throw new AdminAuthError(400, "invalid_member_handle");
  const handle = value.trim().toLowerCase();
  if (!/^[a-z0-9._@-]{1,80}$/u.test(handle)) {
    throw new AdminAuthError(400, "invalid_member_handle");
  }
  return handle;
}

function readAuthState(
  request: Request,
): { state: string; returnTo: string; codeVerifier: string } | null {
  try {
    const value = JSON.parse(cookieValue(request, AUTH_STATE_COOKIE));
    return typeof value?.state === "string" &&
      typeof value?.returnTo === "string" &&
      typeof value?.codeVerifier === "string" &&
      /^[A-Za-z0-9_-]{43}$/u.test(value.codeVerifier)
      ? value
      : null;
  } catch {
    return null;
  }
}

function jwtKid(token: string): string {
  try {
    return decodeJson<{ kid?: string }>(token.split(".")[0]).kid ?? "";
  } catch {
    return "";
  }
}

function isWellFormedKid(kid: string): boolean {
  return /^[A-Za-z0-9._:-]{1,128}$/u.test(kid);
}

async function rememberUnknownKid(kv: Kv, kid: string): Promise<void> {
  await kv.set(unknownKidKey(kid), true, { expireIn: JWKS_REFRESH_COOLDOWN_MS });
}

function serializeCookie(request: Request, name: string, value: string, expiresAt: number): string {
  const parts = [
    `${name}=${encodeURIComponent(value)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax",
    `Expires=${new Date(expiresAt).toUTCString()}`,
    `Max-Age=${Math.max(0, Math.floor((expiresAt - Date.now()) / 1000))}`,
  ];
  if (new URL(request.url).protocol === "https:") parts.push("Secure");
  return parts.join("; ");
}

function clearCookie(request: Request, name: string): string {
  const parts = [
    `${name}=`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax",
    "Expires=Thu, 01 Jan 1970 00:00:00 GMT",
    "Max-Age=0",
  ];
  if (new URL(request.url).protocol === "https:") parts.push("Secure");
  return parts.join("; ");
}

function cookieValue(request: Request, name: string): string {
  const prefix = `${name}=`;
  for (const part of (request.headers.get("cookie") ?? "").split(";")) {
    const value = part.trim();
    if (!value.startsWith(prefix)) continue;
    try {
      return decodeURIComponent(value.slice(prefix.length));
    } catch {
      return value.slice(prefix.length);
    }
  }
  return "";
}

function constantTimeEqual(left: string, right: string): boolean {
  const leftBytes = new TextEncoder().encode(left);
  const rightBytes = new TextEncoder().encode(right);
  if (leftBytes.length !== rightBytes.length) return false;
  let difference = 0;
  for (let index = 0; index < leftBytes.length; index += 1) {
    difference |= leftBytes[index] ^ rightBytes[index];
  }
  return difference === 0;
}

function decodeJson<T>(value: string): T {
  return JSON.parse(new TextDecoder().decode(decodeBase64Url(value))) as T;
}

function decodeBase64Url(value: string): ArrayBuffer {
  const base64 =
    value.replaceAll("-", "+").replaceAll("_", "/") + "=".repeat((4 - (value.length % 4)) % 4);
  const binary = atob(base64);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0)).buffer;
}

function base64UrlEncode(value: Uint8Array): string {
  let binary = "";
  for (const byte of value) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "");
}

async function responseJson(response: Response): Promise<Record<string, unknown>> {
  try {
    const value = await response.json();
    return value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
}

function redirect(location: string): Response {
  return new Response(null, { status: 302, headers: { location } });
}

function nowIso(now: number): string {
  return new Date(now).toISOString();
}

export class AdminAuthError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "AdminAuthError";
  }
}
