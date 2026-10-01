/**
 * Surfe REST adapter — server-side only.
 * Never import from browser/public code. Never log or serialize the API key.
 *
 * Env: SURFE_API_KEY
 * Docs: https://developers.surfe.com/
 * Auth: Authorization Bearer
 * People API base: https://api.surfe.com/v2
 * Credits: GET https://api.surfe.com/v1/credits
 */
import "dotenv/config";

export const SURFE_CLIENT_VERSION = "surfe-client-v1";
export const SURFE_PEOPLE_BASE_URL = "https://api.surfe.com/v2";
export const SURFE_CREDITS_URL = "https://api.surfe.com/v1/credits";

/**
 * Documented billing model (developers.surfe.com/credits-and-quotas):
 * - Search credits: deducted per result returned (people search).
 * - Email credits: people enrichment level 1 (email / landline / profile fields).
 * - Mobile credits: people enrichment level 2 (mobile phone).
 * Cascade/consume-per-found details: see enrich include flags; worst-case for
 * budgeting assumes 1 email credit and 1 mobile credit per requested person
 * when the corresponding include.* flag is true.
 */
export const SURFE_CREDIT_COSTS = Object.freeze({
  search_per_result_returned: 1,
  email_enrichment_per_person_worst_case: 1,
  mobile_enrichment_per_person_worst_case: 1,
  docs: {
    index: "https://developers.surfe.com/",
    credits_quotas: "https://developers.surfe.com/credits-and-quotas",
    get_credits: "https://developers.surfe.com/public-017-get-credits",
    enrich_get: "https://developers.surfe.com/public-016-get-bulk-enrichment",
    responses: "https://developers.surfe.com/api-responses",
    openapi_mirror:
      "https://raw.githubusercontent.com/api-evangelist/surfe/refs/heads/main/openapi/surfe-people-api-openapi.yml",
  },
});

export const SURFE_JOB_STATUS = Object.freeze({
  PENDING: "PENDING",
  IN_PROGRESS: "IN_PROGRESS",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
});

/** Provider-reported email validationStatus values observed in docs. */
export const SURFE_EMAIL_VALIDATION = Object.freeze({
  VALID: "VALID",
});

export function getSurfeApiKeyFromEnv() {
  const key = process.env.SURFE_API_KEY;
  if (!key || !String(key).trim()) return null;
  return String(key).trim();
}

export function describeSurfeKeyPresence() {
  const key = getSurfeApiKeyFromEnv();
  if (!key) return { present: false, env_var: "SURFE_API_KEY" };
  return {
    present: true,
    env_var: "SURFE_API_KEY",
    len: key.length,
  };
}

async function surfeFetch(url, { method = "GET", body, apiKey, timeoutMs = 90000, retries = 2 } = {}) {
  const key = apiKey || getSurfeApiKeyFromEnv();
  if (!key) {
    const err = new Error("SURFE_API_KEY missing");
    err.code = "MISSING_SURFE_API_KEY";
    throw err;
  }

  let lastErr = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        method,
        headers: {
          Authorization: `Bearer ${key}`,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: body != null ? JSON.stringify(body) : undefined,
        signal: controller.signal,
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
      const insufficient =
        res.status === 402 ||
        res.status === 403 ||
        /insufficient|out of .*credits|not enough credits/i.test(
          String(payload?.message || payload?.code || "")
        );
      const rateLimited = res.status === 429;
      return {
        http_status: res.status,
        ok: res.status >= 200 && res.status < 300,
        insufficient_credits: insufficient,
        rate_limited: rateLimited,
        payload,
      };
    } catch (err) {
      lastErr = err;
      const transient =
        /timeout|UND_ERR_CONNECT|ECONNRESET|ENOTFOUND|fetch failed|AbortError/i.test(
          String(err?.cause?.code || err?.code || err?.message || err)
        );
      if (!transient || attempt === retries) break;
      await new Promise((r) => setTimeout(r, 1500 * (attempt + 1)));
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastErr;
}

/** GET /v1/credits — remaining email / mobile / search balances. */
export async function getSurfeCredits(apiKey) {
  return surfeFetch(SURFE_CREDITS_URL, { method: "GET", apiKey });
}

/**
 * POST /v2/people/search
 * @param {{ limit: number, people?: object, companies?: object, peoplePerCompany?: number, pageToken?: string }} body
 */
export async function searchSurfePeople(body, apiKey) {
  return surfeFetch(`${SURFE_PEOPLE_BASE_URL}/people/search`, {
    method: "POST",
    body,
    apiKey,
  });
}

/**
 * POST /v2/people/enrich — start async job. Persist enrichmentID; do not resubmit blindly.
 * @param {{ people: object[], include: { email?: boolean, mobile?: boolean, linkedInUrl?: boolean, jobHistory?: boolean }, enrichmentOptions?: object }} body
 */
export async function startSurfePeopleEnrichment(body, apiKey) {
  // Enrich start can be slower than credits/search; use longer per-attempt timeout.
  return surfeFetch(`${SURFE_PEOPLE_BASE_URL}/people/enrich`, {
    method: "POST",
    body,
    apiKey,
    timeoutMs: Number(process.env.SURFE_ENRICH_START_TIMEOUT_MS || 120000),
    retries: 1, // avoid double-charge: one retry only on clear connect abort before body sent
  });
}

/** GET /v2/people/enrich/{id} */
export async function getSurfePeopleEnrichment(enrichmentId, apiKey) {
  const id = encodeURIComponent(String(enrichmentId || "").trim());
  return surfeFetch(`${SURFE_PEOPLE_BASE_URL}/people/enrich/${id}`, {
    method: "GET",
    apiKey,
  });
}

/**
 * Poll enrichment until COMPLETED/FAILED or timeout.
 * Persists job id via caller; never starts a second job for the same subjects.
 *
 * @param {string} enrichmentId
 * @param {{ maxWaitMs?: number, timeoutMs?: number, intervalMs?: number, apiKey?: string }} [opts]
 *   `timeoutMs` accepted as alias of `maxWaitMs` (V8 scripts historically passed the wrong name).
 */
export async function pollSurfePeopleEnrichment(
  enrichmentId,
  { maxWaitMs, timeoutMs, intervalMs = 2000, apiKey } = {}
) {
  const waitMs = Number(maxWaitMs ?? timeoutMs ?? 240000);
  const started = Date.now();
  let last = null;
  if (!enrichmentId) {
    return {
      http_status: 0,
      ok: false,
      timed_out: false,
      error: "missing_enrichment_id",
      enrichment_id: null,
      payload: null,
    };
  }
  while (Date.now() - started < waitMs) {
    last = await getSurfePeopleEnrichment(enrichmentId, apiKey);
    if (!last.ok) return last;
    const status = String(last.payload?.status || "").toUpperCase();
    if (status === SURFE_JOB_STATUS.COMPLETED || status === SURFE_JOB_STATUS.FAILED) {
      return last;
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return {
    http_status: last?.http_status ?? 0,
    ok: false,
    timed_out: true,
    enrichment_id: enrichmentId,
    payload: last?.payload || null,
  };
}

export function normalizeSurfePerson(p = {}) {
  const emails = Array.isArray(p.emails) ? p.emails : [];
  const mobiles = Array.isArray(p.mobilePhones) ? p.mobilePhones : [];
  return {
    first_name: p.firstName || null,
    last_name: p.lastName || null,
    full_name: [p.firstName, p.lastName].filter(Boolean).join(" ") || null,
    job_title: p.jobTitle || null,
    company_name: p.companyName || null,
    company_domain: p.companyDomain || null,
    linkedin_url: p.linkedInUrl || p.linkedinUrl || null,
    seniorities: p.seniorities || [],
    departments: p.departments || [],
    country: p.country || null,
    external_id: p.externalID || null,
    emails: emails.map((e) => ({
      email: e.email || null,
      type: e.emailType || e.type || "unknown",
      validation_status: e.validationStatus || null,
    })),
    mobile_phones: mobiles.map((m) => ({
      number: m.mobilePhone || m.phone || null,
      // Surfe schema labels these mobilePhones; do not invent landline/business type.
      provider_phone_field: "mobilePhones",
      phone_type: "mobile_provider_reported",
      business_use: "UNKNOWN",
      confidence_score: m.confidenceScore ?? null,
    })),
    job_history: p.jobHistory || [],
    status: p.status || null,
    raw_keys: Object.keys(p),
  };
}

export function estimateWorstCaseCredits({
  searchResults = 0,
  emailPeople = 0,
  mobilePeople = 0,
} = {}) {
  return {
    search: searchResults * SURFE_CREDIT_COSTS.search_per_result_returned,
    email: emailPeople * SURFE_CREDIT_COSTS.email_enrichment_per_person_worst_case,
    mobile: mobilePeople * SURFE_CREDIT_COSTS.mobile_enrichment_per_person_worst_case,
  };
}
