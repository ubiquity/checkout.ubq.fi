import { Database } from "bun:sqlite";
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
  selectEntries,
  validateKey,
} from "./kv.ts";
import { pageAfter, pageCursor, validatePage } from "./kv_paging.ts";

type Row = { k: string; v: string; version: string; expires: number | null };
type Mutation =
  { type: "set"; key: KvKey; value: unknown; expireIn?: number } | { type: "delete"; key: KvKey };

/** SQLite is exclusively for Bun development/tests; it never enters the Worker graph. */
export class SqliteKv implements Kv {
  private readonly db: Database;
  constructor(
    path = ":memory:",
    private readonly now: () => number = Date.now,
  ) {
    if (/^[a-z][a-z0-9+.-]*:\/\//i.test(path)) throw new Error("local_kv_path_required");
    this.db = new Database(path, { create: true });
    this.db.exec(
      "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS uos_kv (k TEXT PRIMARY KEY, v TEXT NOT NULL, version TEXT NOT NULL, expires INTEGER); CREATE TABLE IF NOT EXISTS uos_kv_meta (id INTEGER PRIMARY KEY CHECK (id=1), version INTEGER NOT NULL); INSERT OR IGNORE INTO uos_kv_meta VALUES (1,0)",
    );
  }

  async get<T>(key: KvKey): Promise<KvEntryMaybe<T>> {
    validateKey(key);
    return this.entry<T>(key, this.row(key));
  }

  async getMany<T extends readonly unknown[]>(keys: readonly KvKey[]): Promise<KvEntries<T>> {
    return this.db.transaction(() =>
      keys.map((key) => {
        validateKey(key);
        return this.entry(key, this.row(key));
      }),
    )() as KvEntries<T>;
  }

  async set(
    key: KvKey,
    value: unknown,
    options?: { expireIn?: number },
  ): Promise<{ ok: true; versionstamp: string }> {
    const result = await this.atomic().set(key, value, options).commit();
    if (!result.ok) throw new Error("unchecked_kv_write_failed");
    return result;
  }

  async delete(key: KvKey): Promise<void> {
    await this.atomic().delete(key).commit();
  }

  async *list<T>(
    selector: KvListSelector,
    options?: KvListOptions,
  ): AsyncIterableIterator<KvEntry<T>> {
    const rows = this.db
      .query<Row, [number]>(
        "SELECT k,v,version,expires FROM uos_kv WHERE expires IS NULL OR expires > ?",
      )
      .all(this.now());
    const entries = rows.map((row) => ({
      key: JSON.parse(row.k) as KvKey,
      value: decodeKvValue(JSON.parse(row.v)) as T,
      versionstamp: row.version,
    }));
    yield* selectEntries(entries, selector, options);
  }

  async listPage<T>(prefix: KvKey, options: KvPageOptions): Promise<KvPage<T>> {
    validatePage(prefix, options);
    const after = pageAfter(options.cursor, "sqlite", prefix, prefix) ?? "";
    const rawPrefix = prefix.length ? JSON.stringify(prefix).slice(0, -1) + "," : "[";
    const rows = this.db
      .query<Row, [number, number, string, string, number]>(
        "SELECT k,v,version,expires FROM uos_kv WHERE (expires IS NULL OR expires > ?) AND substr(k,1,?)=? AND k>? ORDER BY k LIMIT ?",
      )
      .all(this.now(), rawPrefix.length, rawPrefix, after, options.limit + 1);
    const page = rows.slice(0, options.limit);
    return {
      entries: page.map((row) => ({
        key: JSON.parse(row.k) as KvKey,
        value: decodeKvValue(JSON.parse(row.v)) as T,
        versionstamp: row.version,
      })),
      cursor:
        rows.length > options.limit ? pageCursor("sqlite", prefix, prefix, page.at(-1)!.k) : null,
    };
  }

  atomic(): KvAtomic {
    const checks: KvCheck[] = [];
    const mutations: Mutation[] = [];
    let committed = false;
    const operation: KvAtomic = {
      check(...entries) {
        checks.push(...entries.map((entry) => structuredClone(entry)));
        return this;
      },
      set(key, value, options) {
        validateKey(key);
        mutations.push({
          type: "set",
          key: [...key],
          value: encodeKvValue(value),
          expireIn: options?.expireIn,
        });
        return this;
      },
      delete(key) {
        validateKey(key);
        mutations.push({ type: "delete", key: [...key] });
        return this;
      },
      commit: async () => {
        if (committed) throw new Error("atomic_already_committed");
        committed = true;
        return this.commit(checks, mutations);
      },
    };
    return operation;
  }

  close(): void {
    this.db.close();
  }

  private row(key: KvKey): Row | null {
    const row = this.db
      .query<Row, [string]>("SELECT k,v,version,expires FROM uos_kv WHERE k=?")
      .get(JSON.stringify(key));
    return row && (row.expires === null || row.expires > this.now()) ? row : null;
  }

  private entry<T>(key: KvKey, row: Row | null): KvEntryMaybe<T> {
    return row
      ? { key: [...key], value: decodeKvValue(JSON.parse(row.v)) as T, versionstamp: row.version }
      : { key: [...key], value: null, versionstamp: null };
  }

  private commit(checks: KvCheck[], mutations: Mutation[]): KvCommitResult {
    return this.db
      .transaction((): KvCommitResult => {
        for (const check of checks) {
          validateKey(check.key);
          if ((this.row(check.key)?.version ?? null) !== check.versionstamp)
            return { ok: false, versionstamp: null };
        }
        for (const mutation of mutations) {
          if (
            mutation.type === "set" &&
            mutation.expireIn !== undefined &&
            (!Number.isFinite(mutation.expireIn) || mutation.expireIn <= 0)
          )
            throw new TypeError("invalid_kv_expiration");
        }
        this.db.exec("UPDATE uos_kv_meta SET version=version+1 WHERE id=1");
        const versionstamp = String(
          this.db
            .query<{ version: number }, []>("SELECT version FROM uos_kv_meta WHERE id=1")
            .get()!.version,
        );
        for (const mutation of mutations) {
          const key = JSON.stringify(mutation.key);
          if (mutation.type === "delete") this.db.query("DELETE FROM uos_kv WHERE k=?").run(key);
          else
            this.db
              .query(
                "INSERT INTO uos_kv (k,v,version,expires) VALUES (?,?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v,version=excluded.version,expires=excluded.expires",
              )
              .run(
                key,
                JSON.stringify(mutation.value),
                versionstamp,
                mutation.expireIn === undefined ? null : this.now() + mutation.expireIn,
              );
        }
        return { ok: true, versionstamp };
      })
      .immediate();
  }
}
