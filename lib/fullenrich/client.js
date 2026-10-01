/**
 * FullEnrich REST adapter — server-side only.
 * Never import from browser/public code. Never log or serialize the API key.
 *
 * Env: FULL_ENRICH_API_KEY
 * Docs index: https://docs.fullenrich.com/llms.txt
 * Auth: Authorization Bearer
 * Base: https://app.fullenrich.com/api/v2
 */

import "dotenv/config";

export const FULLENRICH_CLIENT_VERSION = "fullenrich-client-v1";
export const FULLENRICH_BASE_URL = "https://app.fullenrich.com/api/v2";

/** Documented credit costs — https://docs.fullenrich.com/api/v2/general/credit.md */
export const FULLENRICH_CREDIT_COSTS = Object.freeze({
  work_email_found_deliverable_high_probability_or_catchall: 1,
  mobile_phone_found: 10,
  personal_email_found: 3,
  no_result: 0,
  duplicate_within_3_months: 0,
  docs: {
    index: "https://docs.fullenrich.com/llms.txt",
    credits: "https://docs.fullenrich.com/api/v2/general/credit.md",
    enrich_post: "https://docs.fullenrich.com/api/v2/contact/enrich/bulk/post.md",
    enrich_get: "https://docs.fullenrich.com/api/v2/contact/enrich/bulk/get.md",
    credits_get: "https://docs.fullenrich.com/api/v2/account/credits/get.md",
    verify_key: "https://docs.fullenrich.com/api/v2/account/keys/verify/get.md",
    email_status: "https://docs.fullenrich.com/api/v2/general/email-status.md",
    rate_limit: "https://docs.fullenrich.com/api/v2/general/ratelimit.md",
  },
});

export const FULLENRICH_ENRICH_FIELDS = Object.freeze({
  WORK_EMAILS: "contact.work_emails",
  PHONES: "contact.phones",
  PERSONAL_EMAILS: "contact.personal_emails",
});

export const FULLENRICH_JOB_STATUS = Object.freeze({
  CREATED: "CREATED",
  IN_PROGRESS: "IN_PROGRESS",
  CANCELED: "CANCELED",
  CREDITS_INSUFFICIENT: "CREDITS_INSUFFICIENT",
  FINISHED: "FINISHED",
  RATE_LIMIT: "RATE_LIMIT",
  UNKNOWN: "UNKNOWN",
});

export function getFullEnrichApiKeyFromEnv() {
  const key = process.env.FULL_ENRICH_API_KEY;
  if (!key || !String(key).trim()) return null;
  return String(key).trim();
}

export function describeFullEnrichKeyPresence() {
  const key = getFullEnrichApiKeyFromEnv();
  if (!key) return { present: false, env_var: "FULL_ENRICH_API_KEY" };
  return {
    present: true,
    env_var: "FULL_ENRICH_API_KEY",
    prefix: key.slice(0, Math.min(4, key.length)),
    len: key.length,
  };
}

async function fullEnrichFetch(path, { method = "GET", body, apiKey } = {}) {
  const key = apiKey || getFullEnrichApiKeyFromEnv();
  if (!key) {
    const err = new Error("FULL_ENRICH_API_KEY missing");
    err.code = "MISSING_FULL_ENRICH_API_KEY";
    throw err;
  }
  const res = await fetch(`${FULLENRICH_BASE_URL}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${key}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: body != null ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch (parseErr) {
    payload = {
      parse_error: String(parseErr?.message || parseErr),
      raw_preview: text.slice(0, 400),
    };
  }
  return { http_status: res.status, payload };
}

/** GET /account/keys/verify — free */
export async function verifyFullEnrichApiKey(apiKey) {
  return fullEnrichFetch("/account/keys/verify", { method: "GET", apiKey });
}

/** GET /account/credits — free */
export async function getFullEnrichCreditBalance(apiKey) {
  return fullEnrichFetch("/account/credits", { method: "GET", apiKey });
}

/**
 * POST /contact/enrich/bulk
 * Default: work emails only (no phones, no personal emails).
 */
export async function startFullEnrichBulkWorkEmailsOnly(
  {
    name,
    data,
    webhook_url,
    silentFail = true,
  } = {},
  apiKey
) {
  if (!name || !Array.isArray(data) || !data.length) {
    const err = new Error("name and non-empty data required");
    err.code = "INVALID_BULK_INPUT";
    throw err;
  }
  const sanitized = data.map((row) => {
    const out = {
      first_name: row.first_name,
      last_name: row.last_name,
      domain: row.domain,
      company_name: row.company_name,
      enrich_fields: [FULLENRICH_ENRICH_FIELDS.WORK_EMAILS],
    };
    if (row.linkedin_url) out.linkedin_url = row.linkedin_url;
    if (row.custom && typeof row.custom === "object") {
      out.custom = {};
      for (const [k, v] of Object.entries(row.custom)) {
        out.custom[k] = String(v);
      }
    }
    // Never forward email as enrichment input
    return out;
  });

  const qs = silentFail ? "?silentFail=true" : "";
  const body = { name, data: sanitized };
  if (webhook_url) body.webhook_url = webhook_url;
  return fullEnrichFetch(`/contact/enrich/bulk${qs}`, {
    method: "POST",
    body,
    apiKey,
  });
}

/** GET /contact/enrich/bulk/{enrichment_id} */
export async function getFullEnrichBulkResult(enrichmentId, { forceResults = false } = {}, apiKey) {
  if (!enrichmentId) {
    const err = new Error("enrichment_id required");
    err.code = "MISSING_ENRICHMENT_ID";
    throw err;
  }
  const qs = forceResults ? "?forceResults=true" : "";
  return fullEnrichFetch(`/contact/enrich/bulk/${encodeURIComponent(enrichmentId)}${qs}`, {
    method: "GET",
    apiKey,
  });
}

/**
 * Poll until terminal status. Does not resubmit.
 * Rate limit: 60 calls/min — default interval 5s keeps us well under.
 */
export async function pollFullEnrichBulkUntilDone(
  enrichmentId,
  {
    intervalMs = 5000,
    maxWaitMs = 10 * 60 * 1000,
    onTick,
  } = {},
  apiKey
) {
  const started = Date.now();
  const terminal = new Set([
    FULLENRICH_JOB_STATUS.FINISHED,
    FULLENRICH_JOB_STATUS.CANCELED,
    FULLENRICH_JOB_STATUS.CREDITS_INSUFFICIENT,
    FULLENRICH_JOB_STATUS.RATE_LIMIT,
    FULLENRICH_JOB_STATUS.UNKNOWN,
  ]);
  let last = null;
  while (Date.now() - started < maxWaitMs) {
    last = await getFullEnrichBulkResult(enrichmentId, { forceResults: false }, apiKey);
    const status = last.payload?.status || null;
    if (typeof onTick === "function") {
      await onTick({ status, http_status: last.http_status, elapsed_ms: Date.now() - started });
    }
    if (last.http_status === 401 || last.http_status === 404) return last;
    if (status && terminal.has(status)) return last;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return {
    http_status: last?.http_status ?? null,
    payload: last?.payload ?? null,
    timed_out: true,
    note: "POLL_TIMEOUT — do not resubmit; treat as PENDING_OR_TIMEOUT not NO_MATCH",
  };
}

/** Worst-case credits if every contact yields a billable work email. */
export function worstCaseWorkEmailCredits(contactCount) {
  return Number(contactCount) * FULLENRICH_CREDIT_COSTS.work_email_found_deliverable_high_probability_or_catchall;
}
