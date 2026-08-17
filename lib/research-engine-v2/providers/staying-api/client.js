/**
 * StayingAPI HTTP client — server-side only.
 * NEVER logs, returns, or serializes the API key.
 */

import "dotenv/config";

export const STAYING_API_BASE = "https://api.stayingapi.com/v1";
export const CLIENT_VERSION = "stayingapi-client-v1";

function parseRetryAfter(res, json, text) {
  const header = res.headers?.get?.("retry-after");
  if (header && Number(header) > 0) return Number(header);
  const msg = String(json?.error?.message || text || "");
  const m = msg.match(/retry after\s+(\d+)\s+seconds?/i);
  if (m) return Number(m[1]);
  return null;
}

function getApiKey() {
  const key = String(process.env.STAYINGAPI_KEY || "").trim();
  if (!key) {
    throw new Error("STAYINGAPI_KEY missing from environment (server-side .env)");
  }
  return key;
}

/**
 * Redact secrets from any object before logging/artifacts.
 */
export function redactSecrets(value) {
  if (value == null) return value;
  if (typeof value === "string") {
    // Never echo bearer-looking strings
    if (/^stay_(live|test)_/i.test(value) || value === process.env.STAYINGAPI_KEY) {
      return "[REDACTED]";
    }
    return value;
  }
  if (Array.isArray(value)) return value.map(redactSecrets);
  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (/authorization|api[_-]?key|bearer|secret|token/i.test(k)) {
        out[k] = "[REDACTED]";
      } else {
        out[k] = redactSecrets(v);
      }
    }
    return out;
  }
  return value;
}

/**
 * Safe error message — never include key or Authorization header.
 * @param {unknown} err
 */
export function safeErrorMessage(err) {
  const msg = err?.message || String(err || "unknown_error");
  return msg
    .replace(/stay_(live|test)_[A-Za-z0-9_-]+/gi, "[REDACTED]")
    .replace(/Bearer\s+\S+/gi, "Bearer [REDACTED]");
}

/**
 * @param {string} path - path under /v1 (e.g. /account or /search?…)
 * @param {{ method?: string, query?: Record<string,string|number|undefined>, body?: object, timeoutMs?: number }} [opts]
 */
export async function stayingRequest(path, opts = {}) {
  const key = getApiKey();
  const method = (opts.method || "GET").toUpperCase();
  const url = new URL(path.startsWith("http") ? path : `${STAYING_API_BASE}${path.startsWith("/") ? path : `/${path}`}`);
  if (opts.query) {
    for (const [k, v] of Object.entries(opts.query)) {
      if (v == null || v === "") continue;
      url.searchParams.set(k, String(v));
    }
  }

  const controller = new AbortController();
  const timeoutMs = opts.timeoutMs ?? 60000;
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url.toString(), {
      method,
      headers: {
        Authorization: `Bearer ${key}`,
        Accept: "application/json",
        ...(opts.body ? { "Content-Type": "application/json" } : {}),
      },
      body: opts.body ? JSON.stringify(opts.body) : undefined,
      signal: controller.signal,
    });

    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }

    const creditsCharged =
      Number(json?.meta?.creditsCharged ?? json?.meta?.credits_charged ?? 0) || 0;
    const requestId = json?.meta?.requestId || json?.meta?.request_id || null;

    // Async job pattern
    if (res.status === 202 && (json?.data?.jobId || json?.jobId)) {
      const jobId = json?.data?.jobId || json?.jobId;
      return {
        ok: true,
        async: true,
        status: 202,
        jobId,
        data: null,
        meta: json?.meta || null,
        creditsCharged: 0, // settled on job completion typically
        requestId,
        raw: redactSecrets(json),
      };
    }

    if (!res.ok) {
      return {
        ok: false,
        async: false,
        status: res.status,
        data: null,
        meta: json?.meta || null,
        error: redactSecrets(json?.error || { message: `http_${res.status}` }),
        creditsCharged: 0, // failed calls free
        requestId,
        raw: redactSecrets(json),
        retryAfterSec: parseRetryAfter(res, json, text),
      };
    }

    return {
      ok: true,
      async: false,
      status: res.status,
      data: json?.data ?? json,
      meta: json?.meta || null,
      creditsCharged,
      requestId,
      raw: redactSecrets(json),
    };
  } catch (err) {
    return {
      ok: false,
      async: false,
      status: 0,
      data: null,
      meta: null,
      error: { message: safeErrorMessage(err) },
      creditsCharged: 0,
      requestId: null,
      raw: null,
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Poll async job until complete (free).
 * @param {string} jobId
 * @param {{ timeoutMs?: number, pollMs?: number }} [opts]
 */
export async function pollJob(jobId, opts = {}) {
  const timeoutMs = opts.timeoutMs ?? 90000;
  const pollMs = opts.pollMs ?? 1500;
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const res = await stayingRequest(`/jobs/${encodeURIComponent(jobId)}`, {
      timeoutMs: 30000,
    });
    if (!res.ok) return res;
    const status = res.data?.status || res.raw?.data?.status;
    if (status === "completed" || status === "succeeded" || res.data?.result) {
      const result = res.data?.result ?? res.data;
      const creditsCharged =
        Number(res.meta?.creditsCharged ?? result?.meta?.creditsCharged ?? 0) || 0;
      return {
        ok: true,
        async: false,
        status: 200,
        data: result?.data ?? result,
        meta: res.meta || result?.meta || null,
        creditsCharged,
        requestId: res.requestId,
        raw: redactSecrets(res.raw),
        jobId,
      };
    }
    if (status === "failed" || status === "error") {
      return {
        ok: false,
        async: false,
        status: 200,
        data: null,
        error: redactSecrets(res.data?.error || { message: "job_failed" }),
        creditsCharged: 0,
        requestId: res.requestId,
        raw: redactSecrets(res.raw),
        jobId,
      };
    }
    await new Promise((r) => setTimeout(r, pollMs));
  }
  return {
    ok: false,
    status: 0,
    data: null,
    error: { message: "job_poll_timeout" },
    creditsCharged: 0,
    requestId: null,
    raw: null,
    jobId,
  };
}

/**
 * GET /account — free, no credits.
 */
export async function getAccount() {
  const res = await stayingRequest("/account");
  if (!res.ok) return res;
  const credits = res.data?.credits || {};
  return {
    ok: true,
    plan: res.data?.plan || null,
    key_env: res.data?.key?.env || null,
    credits: {
      available: credits.available ?? null,
      balance: credits.balance ?? null,
      held: credits.held ?? null,
      expiringSoon: credits.expiringSoon ?? null,
    },
    rateLimit: res.data?.rateLimit || null,
    // never include key material
  };
}
