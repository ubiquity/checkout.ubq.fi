import { test } from "bun:test";
import { equal, ok } from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("real workerd exercises the production Worker with isolated services", async () => {
  const directory = await mkdtemp(join(tmpdir(), "uos-checkout-workerd-"));
  try {
    const build = await Bun.build({
      entrypoints: ["worker.ts"],
      outdir: directory,
      target: "browser",
      format: "esm",
      external: ["cloudflare:workers"],
      naming: "worker.mjs",
    });
    ok(build.success, build.logs.map(String).join("\n"));
    const script = await Bun.file(join(directory, "worker.mjs")).text();
    ok(
      !script.includes('from "bun:') && !script.includes('from "node:tls"'),
      "Bun-only modules must not enter the Worker bundle",
    );
    ok(
      !script.includes("HarnessCheckoutScheduler"),
      "harness-only RPC helpers entered production bundle",
    );
    // Miniflare's CLI/control plane requires supported Node; app/runtime tests stay Bun.
    const harness = await Bun.build({
      entrypoints: ["tests/worker_harness.ts"],
      outdir: directory,
      target: "node",
      format: "esm",
      external: ["miniflare"],
      naming: "harness.mjs",
    });
    ok(harness.success, harness.logs.map(String).join("\n"));
    const child = Bun.spawn(
      ["node", join(directory, "harness.mjs"), join(directory, "worker.mjs")],
      { cwd: process.cwd(), stdout: "pipe", stderr: "pipe" },
    );
    const [stdout, stderr, code] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    if (stdout) console.log(stdout.trim());
    if (stderr) console.error(stderr.trim());
    equal(code, 0, stderr);
  } finally {
    await rm(directory, { recursive: true });
  }
}, 60_000);
