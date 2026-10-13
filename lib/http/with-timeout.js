/**
 * Bounded async for customer-facing reads.
 * Prevents Railway "Application failed to respond" when Airtable hangs.
 */

export function withTimeout(promise, ms, label = "operation") {
  const timeoutMs = Math.max(1000, Number(ms) || 15000);
  let timer = null;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const err = new Error(`${label}_timeout_after_${timeoutMs}ms`);
      err.code = "UPSTREAM_TIMEOUT";
      err.timeoutMs = timeoutMs;
      err.label = label;
      reject(err);
    }, timeoutMs);
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

export function gdiReadTimeoutMs() {
  const n = parseInt(process.env.GDI_READ_TIMEOUT_MS || "", 10);
  return Number.isFinite(n) && n > 0 ? n : 15000;
}

export function airtableRequestTimeoutMs() {
  const n = parseInt(process.env.AIRTABLE_REQUEST_TIMEOUT_MS || "", 10);
  // Keep well under typical Railway edge limits (~30–60s).
  return Number.isFinite(n) && n > 0 ? n : 20000;
}
