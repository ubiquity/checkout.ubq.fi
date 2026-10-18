import { type KvKey, type KvPageOptions, validateKey } from "./kv.ts";

export function validatePage(prefix: KvKey, options: KvPageOptions): void {
  validateKey(prefix);
  if (!Number.isSafeInteger(options.limit) || options.limit < 1 || options.limit > 500) {
    throw new TypeError("invalid_kv_page_limit");
  }
}

export function pageCursor(
  backend: string,
  prefix: KvKey,
  serverPrefix: KvKey,
  after: string,
): string {
  return JSON.stringify({ version: 1, backend, prefix, serverPrefix, after });
}

export function pageAfter(
  cursor: string | null | undefined,
  backend: string,
  prefix: KvKey,
  serverPrefix: KvKey,
): string | undefined {
  if (cursor === undefined || cursor === null) return undefined;
  try {
    if (typeof cursor !== "string" || cursor.length > 16_384) throw new Error();
    const value: unknown = JSON.parse(cursor);
    if (!value || typeof value !== "object") throw new Error();
    const decoded = value as Record<string, unknown>;
    if (
      decoded.version !== 1 ||
      decoded.backend !== backend ||
      JSON.stringify(decoded.prefix) !== JSON.stringify(prefix) ||
      JSON.stringify(decoded.serverPrefix) !== JSON.stringify(serverPrefix) ||
      typeof decoded.after !== "string" ||
      !decoded.after
    )
      throw new Error();
    return decoded.after;
  } catch {
    throw new TypeError("invalid_kv_page_cursor");
  }
}
