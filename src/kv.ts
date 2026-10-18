/** The shared store contract; production and local adapters have identical semantics. */
export type KvKey = readonly (string | number | boolean)[];
export type KvEntry<T> = { key: KvKey; value: T; versionstamp: string };
export type KvEntryMaybe<T> = KvEntry<T> | { key: KvKey; value: null; versionstamp: null };
export type KvCheck = { key: KvKey; versionstamp: string | null };
export type KvCommitResult = { ok: true; versionstamp: string } | { ok: false; versionstamp: null };
export type KvListSelector = { prefix?: KvKey; start?: KvKey; end?: KvKey };
export type KvListOptions = { limit?: number; reverse?: boolean };
export type KvPageOptions = { limit: number; cursor?: string | null };
export type KvPage<T> = { entries: KvEntry<T>[]; cursor: string | null };
export type KvEntries<T extends readonly unknown[]> = { [K in keyof T]: KvEntryMaybe<T[K]> };

export interface KvAtomic {
  check(...entries: KvCheck[]): this;
  set(key: KvKey, value: unknown, options?: { expireIn?: number }): this;
  delete(key: KvKey): this;
  commit(): Promise<KvCommitResult>;
}

export interface Kv {
  get<T>(key: KvKey): Promise<KvEntryMaybe<T>>;
  getMany<T extends readonly unknown[]>(keys: readonly KvKey[]): Promise<KvEntries<T>>;
  set(
    key: KvKey,
    value: unknown,
    options?: { expireIn?: number },
  ): Promise<{ ok: true; versionstamp: string }>;
  delete(key: KvKey): Promise<void>;
  list<T>(selector: KvListSelector, options?: KvListOptions): AsyncIterableIterator<KvEntry<T>>;
  /** One bounded backend page, with a cursor scoped to this exact prefix. */
  listPage<T>(prefix: KvKey, options: KvPageOptions): Promise<KvPage<T>>;
  atomic(): KvAtomic;
  close(): void;
}

export function validateKey(key: KvKey): void {
  if (
    !Array.isArray(key) ||
    key.some(
      (part) =>
        !["string", "number", "boolean"].includes(typeof part) ||
        (typeof part === "number" && !Number.isFinite(part)),
    )
  )
    throw new TypeError("invalid_kv_key");
}

/** Supported tuple order matches the original store: strings, numbers, booleans. */
export function compareKeys(left: KvKey, right: KvKey): number {
  for (let index = 0; index < Math.min(left.length, right.length); index++) {
    const a = left[index];
    const b = right[index];
    if (a === b) continue;
    const rank = (part: KvKey[number]) =>
      typeof part === "string" ? 0 : typeof part === "number" ? 1 : 2;
    if (typeof a !== typeof b) return rank(a) - rank(b);
    if (typeof a === "string" && typeof b === "string") {
      const aBytes = new TextEncoder().encode(a);
      const bBytes = new TextEncoder().encode(b);
      for (let n = 0; n < Math.min(aBytes.length, bBytes.length); n++) {
        if (aBytes[n] !== bBytes[n]) return aBytes[n] - bBytes[n];
      }
      return aBytes.length - bBytes.length;
    }
    return a < b ? -1 : 1;
  }
  return left.length - right.length;
}

export function listPrefix(selector: KvListSelector): KvKey {
  if (selector.prefix) return selector.prefix;
  const start = selector.start ?? [];
  const end = selector.end ?? [];
  let length = 0;
  while (length < Math.min(start.length, end.length) && start[length] === end[length]) length++;
  return start.slice(0, length);
}

export function selectEntries<T>(
  entries: KvEntry<T>[],
  selector: KvListSelector,
  options: KvListOptions = {},
): KvEntry<T>[] {
  for (const key of [selector.prefix, selector.start, selector.end]) if (key) validateKey(key);
  if (options.limit !== undefined && (!Number.isSafeInteger(options.limit) || options.limit <= 0)) {
    throw new TypeError("invalid_kv_list_limit");
  }
  const filtered = entries.filter(({ key }) => {
    if (
      selector.prefix &&
      (key.length <= selector.prefix.length ||
        selector.prefix.some((part, index) => part !== key[index]))
    )
      return false;
    return (
      (!selector.start || compareKeys(key, selector.start) >= 0) &&
      (!selector.end || compareKeys(key, selector.end) < 0)
    );
  });
  filtered.sort((a, b) => compareKeys(a.key, b.key) * (options.reverse ? -1 : 1));
  return filtered.slice(0, options.limit);
}
