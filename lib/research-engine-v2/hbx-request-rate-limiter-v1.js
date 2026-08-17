/**
 * Conservative HBX Content API request rate limiter (single-flight).
 * Used by geography discovery — concurrency 1, budgeted, Retry-After aware.
 */
export const HBX_REQUEST_RATE_LIMITER_VERSION = "hbx-request-rate-limiter-v1";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function jitter(ms) {
  const n = Math.max(0, Number(ms) || 0);
  return Math.floor(n * (0.85 + Math.random() * 0.3));
}

/**
 * @param {object} [opts]
 * @param {number} [opts.minIntervalMs] default 1200 — floor between successful requests
 * @param {number} [opts.maxRequestsPerRun] hard stop budget (default 800)
 * @param {number} [opts.maxRetriesOn429] default 4
 */
export function createHbxRequestRateLimiter(opts = {}) {
  const minIntervalMs = Math.max(
    250,
    Number(opts.minIntervalMs ?? process.env.HBX_MIN_REQUEST_INTERVAL_MS ?? 1200)
  );
  const maxRequestsPerRun = Math.max(
    1,
    Number(opts.maxRequestsPerRun ?? process.env.HBX_MAX_REQUESTS_PER_RUN ?? 800)
  );
  const maxRetriesOn429 = Math.max(
    0,
    Number(opts.maxRetriesOn429 ?? process.env.HBX_MAX_RETRIES_ON_429 ?? 4)
  );

  let lastRequestAt = 0;
  let requestCount = 0;
  let chain = Promise.resolve();

  async function waitTurn() {
    const now = Date.now();
    const wait = Math.max(0, lastRequestAt + minIntervalMs - now);
    if (wait > 0) await sleep(jitter(wait));
  }

  /**
   * Serialize all HBX GETs through one limiter (concurrency = 1).
   * @param {() => Promise<{ok:boolean,status:number,response_headers?:object,error_message?:string}>} fn
   */
  function schedule(fn) {
    const run = chain.then(async () => {
      if (requestCount >= maxRequestsPerRun) {
        return {
          ok: false,
          status: 0,
          error_code: "REQUEST_BUDGET_EXCEEDED",
          error_message: `HBX request budget exceeded (${maxRequestsPerRun})`,
          response_headers: {},
          budget_exceeded: true,
        };
      }

      let attempt = 0;
      let last = null;
      while (attempt <= maxRetriesOn429) {
        await waitTurn();
        requestCount += 1;
        lastRequestAt = Date.now();
        last = await fn();
        if (last?.budget_exceeded) return last;
        if (last?.ok) return last;

        const status = Number(last?.status || 0);
        const msg = String(last?.error_message || last?.error_code || "");
        const isQuota = status === 403 && /quota/i.test(msg);
        if (isQuota) {
          return {
            ...last,
            quota_exceeded: true,
            error_code: last.error_code || "QUOTA_EXCEEDED",
          };
        }
        if (status !== 429 || attempt >= maxRetriesOn429) {
          return last;
        }
        const retryAfterHdr = last?.response_headers?.["retry-after"];
        const retryAfterSec = retryAfterHdr ? Number(retryAfterHdr) : NaN;
        const backoffMs = Number.isFinite(retryAfterSec)
          ? Math.max(1000, retryAfterSec * 1000)
          : Math.min(60_000, 1500 * 2 ** attempt);
        await sleep(jitter(backoffMs));
        attempt += 1;
      }
      return last;
    });
    // Keep chain alive even if a call throws
    chain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  return {
    version: HBX_REQUEST_RATE_LIMITER_VERSION,
    minIntervalMs,
    maxRequestsPerRun,
    maxRetriesOn429,
    get requestCount() {
      return requestCount;
    },
    schedule,
  };
}
