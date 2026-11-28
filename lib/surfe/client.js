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

/**
 * Canonical Surfe people-input LinkedIn field is `linkedinUrl` (docs curl / OpenAPI).
 * Older Dealality callers used `linkedInUrl`; always emit the documented key.
 */
export function normalizeLinkedInUrlForSurfe(url) {
  const raw = String(url || "").trim();
  if (!raw || !/linkedin\.com\/in\//i.test(raw)) return null;
  try {
    const u = new URL(raw.startsWith("http") ? raw : `https://${raw}`);
    u.hash = "";
    u.search = "";
    let path = u.pathname.replace(/\/+$/, "");
    return `${u.origin}${path}`;
  } catch {
    return raw.replace(/\/+$/, "");
  }
}

/** Strip generational suffixes so lastName is usable when LinkedIn is absent. */
export function splitSurfePersonName(fullName = "") {
  const parts = String(fullName || "")
    .trim()
    .replace(/[(),]/g, " ")
    .split(/\s+/)
    .filter(Boolean);
  if (!parts.length) return { firstName: null, lastName: null };
  if (parts.length === 1) return { firstName: parts[0], lastName: null };
  const suffixRe = /^(jr\.?|sr\.?|ii|iii|iv|v|vi|2nd|3rd|4th)$/i;
  while (parts.length > 2 && suffixRe.test(parts[parts.length - 1])) {
    parts.pop();
  }
  return { firstName: parts[0], lastName: parts.slice(1).join(" ") };
}

/**
 * Build a Surfe POST /v2/people/enrich person object.
 * Prefer verified LinkedIn (`linkedinUrl`) over name-only matching.
 */
export function buildSurfeEnrichPersonInput(p = {}) {
  const linkedinUrl = normalizeLinkedInUrlForSurfe(
    p.linkedinUrl || p.linkedInUrl || p.linkedin_url || ""
  );
  const parts =
    p.firstName || p.lastName
      ? {
          firstName: p.firstName || null,
          lastName: p.lastName || null,
        }
      : splitSurfePersonName(p.personName || p.fullName || "");
  const out = {};
  if (linkedinUrl) out.linkedinUrl = linkedinUrl;
  if (parts.firstName) out.firstName = parts.firstName;
  if (parts.lastName) out.lastName = parts.lastName;
  if (p.companyName) out.companyName = p.companyName;
  if (p.companyDomain) out.companyDomain = p.companyDomain;
  if (p.jobTitle) out.jobTitle = p.jobTitle;
  if (p.providerPersonId || p.externalID) {
    out.externalID = p.providerPersonId || p.externalID;
  }
  return out;
}

export function normalizeSurfePerson(p = {}) {
  const emailRows = Array.isArray(p.emails) ? p.emails.slice() : [];
  // Surfe also returns a singular top-level `email` on enriched people.
  if (p.email && !emailRows.some((e) => e && e.email === p.email)) {
    emailRows.unshift({
      email: p.email,
      emailType: p.emailType || "professional",
      validationStatus: p.emailValidationStatus || null,
    });
  }
  const mobiles = Array.isArray(p.mobilePhones) ? p.mobilePhones : [];
  // Landlines are free and returned whether or not include.mobile is set (Surfe changelog 2026-11-16).
  const landlines = Array.isArray(p.landlines) ? p.landlines : [];
  const mobile_phones = mobiles.map((m) => ({
    number: m.mobilePhone || m.phone || null,
    provider_phone_field: "mobilePhones",
    phone_type: "mobile_provider_reported",
    line_type: m.lineType || null,
    business_use: "UNKNOWN",
    confidence_score: m.confidenceScore ?? null,
  }));
  const landline_phones = landlines.map((l) => ({
    number: l.landline || l.phone || null,
    provider_phone_field: "landlines",
    phone_type: "landline_provider_reported",
    line_type: l.lineType || null,
    validation_status: l.validationStatus || null,
    business_use: "UNKNOWN",
    confidence_score: null,
  }));
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
    found: p.found === true ? true : p.found === false ? false : null,
    emails: emailRows.map((e) => ({
      email: e.email || null,
      type: e.emailType || e.type || "unknown",
      validation_status: e.validationStatus || null,
    })),
    mobile_phones,
    landlines: landline_phones,
    // Prefer mobile when present; otherwise surface free landlines for session display.
    phones: [...mobile_phones, ...landline_phones].filter((x) => x.number),
    job_history: p.jobHistory || [],
    status: p.status || null,
    raw_keys: Object.keys(p),
  };
}

/** Prefer mobile, then landline — never invent values. */
export function pickSurfePhoneNumber(norm = {}) {
  const mobile = Array.isArray(norm.mobile_phones)
    ? norm.mobile_phones.find((m) => m && m.number)
    : null;
  if (mobile?.number) {
    return {
      phone: mobile.number,
      phoneKind: "mobile",
      providerPhoneField: "mobilePhones",
    };
  }
  const landline = Array.isArray(norm.landlines)
    ? norm.landlines.find((l) => l && l.number)
    : null;
  if (landline?.number) {
    return {
      phone: landline.number,
      phoneKind: "landline",
      providerPhoneField: "landlines",
    };
  }
  const any = Array.isArray(norm.phones) ? norm.phones.find((x) => x && x.number) : null;
  if (any?.number) {
    return {
      phone: any.number,
      phoneKind: any.phone_type?.includes("landline") ? "landline" : "phone",
      providerPhoneField: any.provider_phone_field || null,
    };
  }
  return { phone: null, phoneKind: null, providerPhoneField: null };
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
