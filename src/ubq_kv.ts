import { decodeKvValue, encodeKvValue } from "./kv_codec.ts";
import {
  type Kv,
  type KvAtomic,
  type KvCheck,
  type KvCommitResult,
  type KvEntries,
  type KvEntry,
  type KvEntryMaybe,
  type KvKey,
  type KvListOptions,
  type KvListSelector,
  type KvPage,
  type KvPageOptions,
  listPrefix,
  selectEntries,
  validateKey,
} from "./kv.ts";
import { pageAfter, pageCursor, validatePage } from "./kv_paging.ts";

type WireEntry = { key: KvKey; value: unknown; versionstamp: string };
type Fetcher = (input: string, init: RequestInit) => Promise<Response>;
const REQUEST_TIMEOUT_MS = 15_000;

/** HTTP protocol for the existing shared D1 service; no new database or keyspace. */
export class UbqKv implements Kv {
  constructor(private readonly options: { url: string; token: string; fetchImpl?: Fetcher }) {}

  async request<T>(path: string, body: unknown): Promise<T> {
    const response = await (this.options.fetchImpl ?? fetch)(
      `${this.options.url.replace(/\/$/, "")}${path}`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${this.options.token}`,
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      },
    );
    const payload = (await response.json()) as { ok?: boolean; error?: string };
    const conflict = path === "/kv/atomic" && response.ok && payload.ok === false && !payload.error;
    if (!response.ok || (payload.ok !== true && !conflict)) {
      throw new Error(
        `uos_kv_request_failed:${path}:${response.status}:${payload.error ?? "unknown"}`,
      );
    }
    return payload as T;
  }

  async get<T>(key: KvKey): Promise<KvEntryMaybe<T>> {
    validateKey(key);
    const result = await this.request<{ entry: WireEntry | null }>("/kv/get", { key });
    return result.entry ? fromWire<T>(result.entry) : { key, value: null, versionstamp: null };
  }

  async getMany<T extends readonly unknown[]>(keys: readonly KvKey[]): Promise<KvEntries<T>> {
    keys.forEach(validateKey);
    const entries: KvEntryMaybe<unknown>[] = [];
    for (let offset = 0; offset < keys.length; offset += 100) {
      const batch = keys.slice(offset, offset + 100);
      const result = await this.request<{ entries: (WireEntry | null)[] }>("/kv/getMany", {
        keys: batch,
      });
      if (result.entries.length !== batch.length) throw new Error("uos_kv_invalid_get_many");
      entries.push(
        ...result.entries.map((entry, index) =>
          entry ? fromWire(entry) : { key: batch[index], value: null, versionstamp: null },
        ),
      );
    }
    return entries as KvEntries<T>;
  }

  async set(
    key: KvKey,
    value: unknown,
    options?: { expireIn?: number },
  ): Promise<{ ok: true; versionstamp: string }> {
    validateKey(key);
    const result = await this.request<{ versionstamp: string }>("/kv/set", {
      key,
      value: encodeKvValue(value),
      expireInMs: options?.expireIn,
    });
    return { ok: true, versionstamp: result.versionstamp };
  }

  async delete(key: KvKey): Promise<void> {
    validateKey(key);
    await this.request("/kv/delete", { key });
  }

  async *list<T>(
    selector: KvListSelector = {},
    options?: KvListOptions,
  ): AsyncIterableIterator<KvEntry<T>> {
    const prefix = serverListPrefix(listPrefix(selector));
    validateKey(prefix);
    const entries: KvEntry<T>[] = [];
    const cursors = new Set<string>();
    let cursor: string | undefined;
    do {
      const page = await this.request<{ entries: WireEntry[]; cursor: string | null }>("/kv/list", {
        prefix,
        limit: 500,
        cursor,
      });
      entries.push(...page.entries.map(fromWire<T>));
      cursor = page.cursor ?? undefined;
      if (cursor && cursors.has(cursor)) throw new Error("uos_kv_repeated_cursor");
      if (cursor) cursors.add(cursor);
    } while (cursor);
    // The service orders encoded keys lexically and supports prefix/cursor only.
    // Fetch the derived prefix completely before applying tuple ranges and limit.
    yield* selectEntries(entries, selector, options);
  }

  async listPage<T>(logicalPrefix: KvKey, options: KvPageOptions): Promise<KvPage<T>> {
    validatePage(logicalPrefix, options);
    if (logicalPrefix[0] !== "uos_checkout" || logicalPrefix[1] !== "v1") {
      throw new TypeError("uos_kv_page_namespace_required");
    }
    const prefix = serverListPrefix(logicalPrefix);
    const after = pageAfter(options.cursor, "service", logicalPrefix, prefix);
    const page = await this.request<{ entries: WireEntry[]; cursor: string | null }>("/kv/list", {
      prefix,
      limit: options.limit,
      cursor: after,
    });
    if (
      !Array.isArray(page.entries) ||
      page.entries.length > options.limit ||
      (page.cursor !== null &&
        (typeof page.cursor !== "string" || !page.cursor || page.cursor === after))
    ) {
      throw new Error("uos_kv_invalid_page");
    }
    return {
      entries: page.entries
        .filter(
          (entry) =>
            entry.key.length > logicalPrefix.length &&
            logicalPrefix.every((part, index) => part === entry.key[index]),
        )
        .map(fromWire<T>),
      // The cursor belongs to the raw page even when every row was filtered out.
      cursor:
        page.cursor === null ? null : pageCursor("service", logicalPrefix, prefix, page.cursor),
    };
  }

  atomic(): KvAtomic {
    return new UbqKvAtomic(this);
  }
  close(): void {}
}

class UbqKvAtomic implements KvAtomic {
  private readonly checks: KvCheck[] = [];
  private readonly mutations: {
    type: "set" | "delete";
    key: KvKey;
    value?: unknown;
    expireInMs?: number;
  }[] = [];
  private committed = false;
  constructor(private readonly client: UbqKv) {}
  check(...entries: KvCheck[]): this {
    entries.forEach((entry) => validateKey(entry.key));
    this.checks.push(
      ...entries.map((entry) => ({
        key: structuredClone(entry.key),
        versionstamp: entry.versionstamp,
      })),
    );
    return this;
  }
  set(key: KvKey, value: unknown, options?: { expireIn?: number }): this {
    validateKey(key);
    this.mutations.push({
      type: "set",
      key: [...key],
      value: encodeKvValue(value),
      expireInMs: options?.expireIn,
    });
    return this;
  }
  delete(key: KvKey): this {
    validateKey(key);
    this.mutations.push({ type: "delete", key: [...key] });
    return this;
  }
  async commit(): Promise<KvCommitResult> {
    if (this.committed) throw new Error("atomic_already_committed");
    this.committed = true;
    const result = await this.client.request<{ ok: boolean; versionstamp?: string }>("/kv/atomic", {
      checks: this.checks,
      mutations: this.mutations,
    });
    if (!result.ok) return { ok: false, versionstamp: null };
    if (!result.versionstamp) throw new Error("uos_kv_missing_versionstamp");
    return { ok: true, versionstamp: result.versionstamp };
  }
}

function fromWire<T>(entry: WireEntry): KvEntry<T> {
  return {
    key: entry.key,
    value: decodeKvValue(entry.value) as T,
    versionstamp: entry.versionstamp,
  };
}

function serverListPrefix(prefix: KvKey): KvKey {
  const root = ["uos_checkout", "v1"];
  if (!prefix.length) return root;
  const encoded = `u1|${prefix.map((part) => (typeof part === "string" ? `s${part.length}:${part}` : typeof part === "number" ? `n:${part}` : `b:${part ? 1 : 0}`)).join("|")}|`;
  const slash = String.fromCharCode(92);
  const pattern =
    Array.from(encoded, (part) => ([slash, "%", "_"].includes(part) ? slash + part : part)).join(
      "",
    ) + "%";
  if (new TextEncoder().encode(pattern).byteLength <= 50) return prefix;
  if (prefix[0] !== root[0] || prefix[1] !== root[1]) throw new Error("uos_kv_prefix_too_long");
  return root;
}
