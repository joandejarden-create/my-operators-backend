/**
 * SerpApi Google Hotels HTTP client — server-side only.
 * NEVER logs, returns, or serializes SERPAPI_KEY.
 */

import "dotenv/config";

export const SERPAPI_BASE = "https://serpapi.com";
export const CLIENT_VERSION = "serpapi-google-hotels-client-v1";

function getApiKey() {
  // Canonical: SERPAPI_KEY. Temporary fallback for misnamed SERPAPI_API_KEY.
  const key = String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim();
  if (!key) {
    throw new Error("SERPAPI_KEY missing from environment (server-side .env)");
  }
  return key;
}

/**
 * Redact secrets from any object before logging/artifacts.
 * Never persist api_key / email / account_id from Account API.
 */
export function redactSecrets(value) {
  if (value == null) return value;
  if (typeof value === "string") {
    const key = process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY;
    if (key && value.includes(key)) return "[REDACTED]";
    return value;
  }
  if (Array.isArray(value)) return value.map(redactSecrets);
  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (/authorization|api[_-]?key|bearer|secret|password|account_email|account_id/i.test(k)) {
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
 * Safe error message — never include key.
 * @param {unknown} err
 */
export function safeErrorMessage(err) {
  const msg = err?.message || String(err || "unknown_error");
  const key = process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY;
  let out = msg;
  if (key) out = out.split(key).join("[REDACTED]");
  return out.replace(/api_key=[^&\s]+/gi, "api_key=[REDACTED]");
}

/**
 * @param {Record<string, string|number|boolean|undefined|null>} params
 * @param {{ timeoutMs?: number }} [opts]
 */
export async function serpapiSearch(params, opts = {}) {
  const key = getApiKey();
  const url = new URL(`${SERPAPI_BASE}/search.json`);
  for (const [k, v] of Object.entries(params || {})) {
    if (v == null || v === "") continue;
    url.searchParams.set(k, String(v));
  }
  url.searchParams.set("api_key", key);

  const controller = new AbortController();
  const timeoutMs = opts.timeoutMs ?? 90000;
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url.toString(), {
      method: "GET",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });

    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }

    // SerpApi: successful search typically costs 1 search (cached searches free per docs).
    // Account API delta is authoritative for ledger; per-request assume 1 on Success.
    const searchMetadata = json?.search_metadata || null;
    const charged = res.ok && json && !json.error ? 1 : 0;

    if (!res.ok || json?.error) {
      return {
        ok: false,
        status: res.status,
        data: null,
        error: redactSecrets(json?.error ? { message: String(json.error) } : { message: `http_${res.status}` }),
        creditsCharged: 0,
        search_id: searchMetadata?.id || null,
        raw: redactSecrets(json),
      };
    }

    return {
      ok: true,
      status: res.status,
      data: json,
      error: null,
      creditsCharged: charged,
      search_id: searchMetadata?.id || null,
      search_metadata: redactSecrets(searchMetadata),
      raw: redactSecrets(json),
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      data: null,
      error: { message: safeErrorMessage(err) },
      creditsCharged: 0,
      search_id: null,
      raw: null,
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * GET /account.json — free, not counted toward quota.
 * Never returns api_key / email / account_id.
 */
export async function getAccount() {
  const key = getApiKey();
  const url = new URL(`${SERPAPI_BASE}/account.json`);
  url.searchParams.set("api_key", key);

  try {
    const res = await fetch(url.toString(), { headers: { Accept: "application/json" } });
    const json = await res.json().catch(() => null);
    if (!res.ok || !json) {
      return {
        ok: false,
        error: { message: safeErrorMessage(json?.error || `http_${res.status}`) },
      };
    }
    return {
      ok: true,
      account_status: json.account_status || null,
      plan_name: json.plan_name || null,
      plan_id: json.plan_id || null,
      searches_per_month: json.searches_per_month ?? null,
      plan_searches_left: json.plan_searches_left ?? null,
      total_searches_left: json.total_searches_left ?? null,
      this_month_usage: json.this_month_usage ?? null,
      extra_credits: json.extra_credits ?? null,
      account_rate_limit_per_hour: json.account_rate_limit_per_hour ?? null,
      plan_renewal_date: json.plan_renewal_date ?? null,
      // intentionally omit api_key, account_email, account_id
    };
  } catch (err) {
    return { ok: false, error: { message: safeErrorMessage(err) } };
  }
}
