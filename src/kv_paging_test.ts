import { test } from "bun:test";
import { deepStrictEqual as equal, rejects } from "node:assert/strict";
import { SqliteKv } from "./sqlite_kv.ts";
import { UbqKv } from "./ubq_kv.ts";
import { encodeKvValue } from "./kv_codec.ts";

const invoices = ["uos_checkout", "v1", "payments", "invoices"];

test("SQLite pages retain independent cursor scope and resume around deletions", async () => {
  const kv = new SqliteKv();
  try {
    for (const id of ["a", "b", "c", "d", "e"]) await kv.set([...invoices, id], { amount: 7n });
    await kv.set(["foreign", "unrelated"], "private");
    const first = await kv.listPage<{ amount: bigint }>(invoices, { limit: 2 });
    equal(
      first.entries.map((entry) => entry.key.at(-1)),
      ["a", "b"],
    );
    equal(first.entries[0].value.amount, 7n);
    await kv.delete([...invoices, "a"]);
    const second = await kv.listPage(invoices, { limit: 2, cursor: first.cursor });
    equal(
      second.entries.map((entry) => entry.key.at(-1)),
      ["c", "d"],
    );
    const third = await kv.listPage(invoices, { limit: 2, cursor: second.cursor });
    equal(
      third.entries.map((entry) => entry.key.at(-1)),
      ["e"],
    );
    equal(third.cursor, null);
    await rejects(
      () =>
        kv.listPage(["uos_checkout", "v1", "payments", "outbox"], {
          limit: 2,
          cursor: first.cursor,
        }),
      /invalid_kv_page_cursor/,
    );
  } finally {
    kv.close();
  }
});

test("service pagination reads exactly one bounded raw page even when the fallback page filters to empty", async () => {
  const calls: { prefix: unknown; limit: number; cursor?: string }[] = [];
  const kv = new UbqKv({
    url: "https://kv.invalid",
    token: "offline",
    fetchImpl: async (_url, init) => {
      const body = JSON.parse(String(init.body));
      calls.push(body);
      return Response.json({
        ok: true,
        entries: [
          {
            key: body.cursor
              ? [...invoices, "invoice"]
              : ["uos_checkout", "v1", "auth", "unrelated"],
            value: encodeKvValue({ amount: 5n }),
            versionstamp: "1",
          },
        ],
        cursor: body.cursor ? null : "raw-after-unrelated",
      });
    },
  });
  const first = await kv.listPage(invoices, { limit: 1 });
  equal(calls.length, 1);
  equal(calls[0].prefix, ["uos_checkout", "v1"]);
  equal(calls[0].limit, 1);
  equal(first.entries, []);
  const second = await kv.listPage<{ amount: bigint }>(invoices, {
    limit: 1,
    cursor: first.cursor,
  });
  equal(calls.length, 2);
  equal(calls[1].cursor, "raw-after-unrelated");
  equal(second.entries[0].value.amount, 5n);
  equal(second.cursor, null);
  await rejects(() => kv.listPage(["foreign"], { limit: 1 }), /namespace_required/);
  await rejects(() => kv.listPage(invoices, { limit: 501 }), /invalid_kv_page_limit/);
  await rejects(
    () => kv.listPage(invoices, { limit: 1, cursor: "malformed" }),
    /invalid_kv_page_cursor/,
  );
});
