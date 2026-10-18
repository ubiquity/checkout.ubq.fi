const READY_LINE = "Listening on 127.0.0.1:18545";
const MAX_STARTUP_BYTES = 64 * 1024;

/** Foundry 6c21ffe229e9963bf7e97c4879cd95c8a0463be3, crates/anvil/src/lib.rs:328. */
export function observeAnvilReadiness(
  stdout: ReadableStream<Uint8Array>,
  exited: Promise<number>,
  signal: AbortSignal = AbortSignal.timeout(20_000),
): { ready: Promise<void>; drained: Promise<void> } {
  let settled = false;
  let resolveReady!: () => void;
  let rejectReady!: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });
  const fail = (code: string) => {
    if (settled) return;
    settled = true;
    signal.removeEventListener("abort", abort);
    rejectReady(new Error(code));
  };
  const abort = () => fail("anvil_readiness_timeout");
  signal.addEventListener("abort", abort, { once: true });
  if (signal.aborted) abort();
  const reader = stdout.getReader();
  const pump = (async () => {
    const decoder = new TextDecoder();
    let buffered = "";
    let bytes = 0;
    try {
      while (true) {
        const item = await reader.read();
        if (item.done) break;
        // Startup output can include local account credentials: never return or log any line.
        if (settled) continue;
        bytes += item.value.byteLength;
        if (bytes > MAX_STARTUP_BYTES) {
          fail("anvil_startup_output_limit");
          continue;
        }
        buffered += decoder.decode(item.value, { stream: true });
        let newline: number;
        while ((newline = buffered.indexOf("\n")) !== -1) {
          const line = buffered.slice(0, newline).trim();
          buffered = buffered.slice(newline + 1);
          if (line !== READY_LINE) continue;
          settled = true;
          buffered = "";
          signal.removeEventListener("abort", abort);
          resolveReady();
          break;
        }
      }
      fail("anvil_stdout_closed_before_ready");
    } catch {
      fail("anvil_stdout_failed");
    } finally {
      reader.releaseLock();
    }
  })();
  const ended = exited.then(
    () => fail("anvil_exited_before_ready"),
    () => fail("anvil_process_failed"),
  );
  return { ready, drained: Promise.all([pump, ended]).then(() => {}) };
}
