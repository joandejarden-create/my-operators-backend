/**
 * Prospeo REST adapter — server-side only.
 * Never import from browser/public. Never log or serialize the API key.
 *
 * Env: PROSPEO_API_KEY (alias PROSPEO_KEY)
 * Docs: https://prospeo.io/api-docs
 * Account (free): GET https://api.prospeo.io/account-information
 * Enrich: POST https://api.prospeo.io/enrich-person
 *
 * Email-only default: enrich_mobile=false.
 * Credits: 1 per verified email found; 10 per mobile; 0 on no match / 90-day re-enrich.
 */
import "dotenv/config";

export const PROSPEO_CLIENT_VERSION = "prospeo-client-v1";
export const PROSPEO_API_BASE = "https://api.prospeo.io";
export const PROSPEO_ACCOUNT_URL = `${PROSPEO_API_BASE}/account-information`;
export const PROSPEO_ENRICH_PERSON_URL = `${PROSPEO_API_BASE}/enrich-person`;

export const PROSPEO_CREDIT_COSTS = Object.freeze({
  email_found: 1,
  mobile_found: 10,
  no_match: 0,
  reenrich_within_90_days: 0,
  docs: {
    account: "https://prospeo.io/api-docs/account-information",
    enrich_person: "https://prospeo.io/api-docs/enrich-person",
    credits: "https://help.prospeo.io/en/article/how-prospeo-credits-work-yso5gk/",
  },
});

export function getProspeoApiKeyFromEnv(env = process.env) {
  const key = env.PROSPEO_API_KEY || env.PROSPEO_KEY;
  if (!key || !String(key).trim()) return null;
  return String(key).trim();
}

export function describeProspeoKeyPresence(env = process.env) {
  const key = getProspeoApiKeyFromEnv(env);
  if (!key) {
    return { present: false, env_vars: ["PROSPEO_API_KEY", "PROSPEO_KEY"] };
  }
  return {
    present: true,
    env_var: env.PROSPEO_API_KEY ? "PROSPEO_API_KEY" : "PROSPEO_KEY",
    len: key.length,
  };
}

async function prospeoFetch(url, { method = "GET", body, apiKey, timeoutMs = 45000 } = {}) {
  const key = apiKey || getProspeoApiKeyFromEnv();
  if (!key) {
    const err = new Error("PROSPEO_API_KEY missing");
    err.code = "MISSING_PROSPEO_API_KEY";
    throw err;
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method,
      headers: {
        "X-KEY": key,
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
        raw_preview: String(text).slice(0, 400),
      };
    }
    return {
      http_status: res.status,
      ok: res.status >= 200 && res.status < 300 && payload?.error !== true,
      rate_limited: res.status === 429,
      insufficient_credits:
        res.status === 402 ||
        /credit|quota/i.test(String(payload?.error_code || payload?.message || "")),
      payload,
    };
  } finally {
    clearTimeout(timer);
  }
}

/** Free account information endpoint. */
export async function getProspeoAccountInformation(apiKey) {
  return prospeoFetch(PROSPEO_ACCOUNT_URL, { method: "GET", apiKey });
}

/**
 * Build enrich-person body. Mobile enrichment is OFF unless explicitly authorized.
 */
export function buildProspeoEnrichPersonBody(person = {}, opts = {}) {
  const first = String(person.firstName || person.first_name || "").trim();
  const last = String(person.lastName || person.last_name || "").trim();
  const full =
    String(person.fullName || person.full_name || "").trim() ||
    [first, last].filter(Boolean).join(" ");
  const companyName = String(
    person.companyName || person.company_name || person.organization || "",
  ).trim();
  const companyWebsite = String(
    person.companyDomain ||
      person.company_website ||
      person.domain ||
      person.employerDomain ||
      "",
  )
    .trim()
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .split("/")[0];
  const linkedinUrl = String(
    person.linkedinUrl || person.linkedin_url || person.profile || "",
  )
    .trim()
    .replace(/\/$/, "");

  if (opts.authorizeMobileEnrichment === true) {
    const err = new Error("PROSPEO_MOBILE_NOT_AUTHORIZED_IN_CLIENT_DEFAULT");
    err.code = "PROSPEO_MOBILE_BLOCKED";
    // Caller must use authorizeMobileEnrichment AND enrich_mobile — still default false below.
    throw err;
  }

  const data = {};
  if (first) data.first_name = first;
  if (last) data.last_name = last;
  if (full) data.full_name = full;
  if (companyName) data.company_name = companyName;
  if (companyWebsite) data.company_website = companyWebsite;
  if (linkedinUrl && /linkedin\.com\/in\//i.test(linkedinUrl)) {
    data.linkedin_url = linkedinUrl;
  }

  const hasIdentity =
    data.linkedin_url ||
    ((data.first_name && data.last_name) || data.full_name) &&
      (data.company_name || data.company_website);
  if (!hasIdentity) {
    const err = new Error("PROSPEO_INPUT_INSUFFICIENT");
    err.code = "PROSPEO_INPUT_INSUFFICIENT";
    throw err;
  }

  return {
    only_verified_email: opts.onlyVerifiedEmail !== false,
    enrich_mobile: false,
    only_verified_mobile: false,
    data,
  };
}

/**
 * Email-only person enrichment. Never requests mobile in this Dealality path.
 */
export async function enrichProspeoPersonEmailOnly(person = {}, opts = {}) {
  if (opts.authorizeLiveProspeo !== true) {
    return {
      http_status: 0,
      ok: false,
      skipped: true,
      reason: "PROSPEO_NOT_AUTHORIZED",
      normalized: null,
      credits_consumed_estimate: 0,
    };
  }
  const body = buildProspeoEnrichPersonBody(person, {
    onlyVerifiedEmail: opts.onlyVerifiedEmail !== false,
  });
  // Hard ban mobile regardless of opts
  body.enrich_mobile = false;
  body.only_verified_mobile = false;

  const started = Date.now();
  const res = await prospeoFetch(PROSPEO_ENRICH_PERSON_URL, {
    method: "POST",
    body,
    apiKey: opts.apiKey,
    timeoutMs: opts.timeoutMs,
  });
  const normalized = normalizeProspeoEnrichPersonResponse(res);
  return {
    ...res,
    latency_ms: Date.now() - started,
    request_sanitized: {
      only_verified_email: body.only_verified_email,
      enrich_mobile: false,
      data_keys: Object.keys(body.data || {}),
      company_website: body.data?.company_website || null,
      has_linkedin: Boolean(body.data?.linkedin_url),
      full_name: body.data?.full_name || null,
    },
    normalized,
    credits_consumed_estimate: normalized.credits_consumed_estimate,
  };
}

export function normalizeProspeoEnrichPersonResponse(res = {}) {
  const payload = res.payload || {};
  const err = payload.error === true;
  const person =
    payload.response?.person ||
    payload.person ||
    payload.response ||
    null;
  const company = payload.response?.company || payload.company || null;

  const extractEmail = (v) => {
    if (!v) return null;
    if (typeof v === "string" && v.includes("@")) return v.trim().toLowerCase();
    if (typeof v === "object") {
      const cand =
        v.email || v.address || v.value || v.work_email || v.verified_email;
      if (typeof cand === "string" && cand.includes("@")) {
        return cand.trim().toLowerCase();
      }
    }
    return null;
  };
  const email =
    extractEmail(person?.email) ||
    extractEmail(person?.work_email) ||
    extractEmail(person?.verified_email) ||
    (typeof person?.emails === "string" ? extractEmail(person.emails) : null) ||
    (Array.isArray(person?.emails)
      ? extractEmail(person.emails[0])
      : null) ||
    null;

  const freeEnrichment = payload.free_enrichment === true;
  const matched = Boolean(!err && res.ok && (email || person?.full_name || person?.first_name));

  let credits = 0;
  if (matched && email && !freeEnrichment) credits = PROSPEO_CREDIT_COSTS.email_found;
  if (matched && email && freeEnrichment) credits = 0;
  if (!matched) credits = 0;

  return {
    matched,
    error: err ? payload.error_code || payload.message || "PROSPEO_ERROR" : null,
    free_enrichment: freeEnrichment,
    full_name:
      person?.full_name ||
      [person?.first_name, person?.last_name].filter(Boolean).join(" ") ||
      null,
    first_name: person?.first_name || null,
    last_name: person?.last_name || null,
    job_title: person?.job_title || person?.title || null,
    linkedin_url: person?.linkedin_url || person?.linkedin || null,
    company_name: company?.name || person?.company_name || null,
    company_website: company?.website || company?.domain || null,
    email: email ? String(email).trim().toLowerCase() : null,
    email_status: person?.email_status || person?.email_verification || null,
    mobile: null, // never requested
    credits_consumed_estimate: credits,
    raw_person_keys: person && typeof person === "object" ? Object.keys(person) : [],
  };
}

export default {
  PROSPEO_CLIENT_VERSION,
  PROSPEO_CREDIT_COSTS,
  getProspeoApiKeyFromEnv,
  describeProspeoKeyPresence,
  getProspeoAccountInformation,
  buildProspeoEnrichPersonBody,
  enrichProspeoPersonEmailOnly,
  normalizeProspeoEnrichPersonResponse,
};
