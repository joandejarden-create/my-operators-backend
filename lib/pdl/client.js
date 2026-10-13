/**
 * People Data Labs (PDL) REST adapter — server-side only.
 * Evaluation / post-gate contact enrichment. Never import from browser/public.
 * Never log or serialize the API key. Never treat PDL as ownership evidence.
 *
 * Env: PDL_API_KEY (also accepts PEOPLE_DATA_LABS_API_KEY)
 * Docs: https://docs.peopledatalabs.com/docs/person-enrichment-api
 * Bulk: POST https://api.peopledatalabs.com/v5/person/bulk
 * Single: GET https://api.peopledatalabs.com/v5/person/enrich
 *
 * Written permission: Dealality embedded product use approved (2026-09).
 * Persistence: evaluation metadata + minimum contact fields only — no bulk cache.
 */
import "dotenv/config";

export const PDL_CLIENT_VERSION = "pdl-client-v1";
export const PDL_API_BASE = "https://api.peopledatalabs.com/v5";
export const PDL_PERSON_ENRICH_URL = `${PDL_API_BASE}/person/enrich`;
export const PDL_PERSON_BULK_URL = `${PDL_API_BASE}/person/bulk`;

/** Documented: one Enrichment credit per successful (HTTP 200) person match. */
export const PDL_CREDIT_COSTS = Object.freeze({
  enrichment_per_200_match: 1,
  docs: {
    person_enrichment: "https://docs.peopledatalabs.com/docs/person-enrichment-api",
    input_parameters: "https://docs.peopledatalabs.com/docs/input-parameters-person-enrichment-api",
    bulk: "https://docs.peopledatalabs.com/docs/bulk-enrichment-api",
  },
});

/** Fields requested for Dealality contact-enrichment evaluation (Person Base).
 * Do not depend on premium `phones` objects — use mobile_phone + phone_numbers.
 */
export const PDL_EVAL_DATA_INCLUDE = [
  "full_name",
  "first_name",
  "last_name",
  "linkedin_url",
  "job_title",
  "job_company_name",
  "job_company_website",
  "job_company_id",
  "job_company_size",
  "work_email",
  "emails",
  "personal_emails",
  "recommended_personal_email",
  "mobile_phone",
  "phone_numbers",
  "experience",
  "profiles",
  "likelihood",
  "id",
  "job_last_verified",
  "job_last_changed",
  "dataset_version",
].join(",");

export function getPdlApiKeyFromEnv(env = process.env) {
  const key = env.PDL_API_KEY || env.PEOPLE_DATA_LABS_API_KEY;
  if (!key || !String(key).trim()) return null;
  return String(key).trim();
}

export function describePdlKeyPresence(env = process.env) {
  const key = getPdlApiKeyFromEnv(env);
  if (!key) {
    return { present: false, env_vars: ["PDL_API_KEY", "PEOPLE_DATA_LABS_API_KEY"] };
  }
  return {
    present: true,
    env_var: env.PDL_API_KEY ? "PDL_API_KEY" : "PEOPLE_DATA_LABS_API_KEY",
    len: key.length,
  };
}

export function isPdlConfigured(env = process.env) {
  return Boolean(getPdlApiKeyFromEnv(env));
}

/**
 * Build Person Enrichment params. Never includes email/phone as input.
 */
export function buildPdlPersonEnrichParams(person = {}, opts = {}) {
  const first = String(person.first_name || "").trim();
  const last = String(person.last_name || "").trim();
  const company = String(person.company_name || person.organization || "").trim();
  const website = String(person.company_domain || person.domain || "")
    .trim()
    .replace(/^https?:\/\//i, "")
    .replace(/\/$/, "");
  const profile = String(person.linkedin_url || person.profile || "").trim();

  if (person.email || person.work_email || person.phone) {
    const err = new Error("PDL_INPUT_MUST_NOT_INCLUDE_EMAIL_OR_PHONE");
    err.code = "PDL_INPUT_FORBIDDEN_FIELD";
    throw err;
  }

  if ((!first || !last) && !profile) {
    const err = new Error("PDL_INPUT_REQUIRES_NAME_OR_PROFILE");
    err.code = "PDL_INPUT_INSUFFICIENT";
    throw err;
  }
  if (!website && !company && !profile) {
    const err = new Error("PDL_INPUT_REQUIRES_COMPANY_OR_DOMAIN_OR_PROFILE");
    err.code = "PDL_INPUT_INSUFFICIENT";
    throw err;
  }

  const params = {
    first_name: first || undefined,
    last_name: last || undefined,
    company: website || company || undefined,
    profile: profile || undefined,
    min_likelihood: Number(opts.min_likelihood ?? 6),
    include_if_matched: true,
    titlecase: true,
    data_include: opts.data_include || PDL_EVAL_DATA_INCLUDE,
  };
  // Strip undefined
  return Object.fromEntries(Object.entries(params).filter(([, v]) => v != null && v !== ""));
}

async function pdlFetch(url, { method = "GET", body, apiKey, timeoutMs = 45000, retries = 0 } = {}) {
  const key = apiKey || getPdlApiKeyFromEnv();
  if (!key) {
    const err = new Error("PDL_API_KEY missing");
    err.code = "MISSING_PDL_API_KEY";
    throw err;
  }

  let lastErr = null;
  const maxAttempts = Math.max(0, Number(retries)) + 1;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        method,
        headers: {
          "X-Api-Key": key,
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
      const rateLimited = res.status === 429;
      const insufficient =
        res.status === 402 ||
        /credit|quota|insufficient/i.test(String(payload?.error?.message || payload?.message || ""));
      return {
        http_status: res.status,
        ok: res.status >= 200 && res.status < 300,
        rate_limited: rateLimited,
        insufficient_credits: insufficient,
        payload,
        credit_headers: extractPdlCreditHeaders(res.headers),
      };
    } catch (err) {
      lastErr = err;
      const transient =
        /timeout|UND_ERR_CONNECT|ECONNRESET|ENOTFOUND|fetch failed|AbortError/i.test(
          String(err?.cause?.code || err?.code || err?.message || err)
        );
      if (!transient || attempt + 1 >= maxAttempts) break;
    } finally {
      clearTimeout(timer);
    }
  }
  const err = lastErr || new Error("PDL_FETCH_FAILED");
  err.code = err.code || "PDL_TRANSPORT_ERROR";
  throw err;
}

/** Documented response headers only — not a separate balance API. */
export function extractPdlCreditHeaders(headers) {
  if (!headers || typeof headers.get !== "function") return null;
  const get = (name) => {
    const v = headers.get(name) ?? headers.get(name.toLowerCase());
    return v == null || v === "" ? null : String(v);
  };
  const num = (name) => {
    const v = get(name);
    if (v == null) return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };
  return {
    call_credits_spent: num("x-call-credits-spent"),
    call_credits_type: get("x-call-credits-type"),
    totallimit_remaining: num("x-totallimit-remaining"),
    totallimit_purchased_remaining: num("x-totallimit-purchased-remaining"),
    totallimit_overages_remaining: num("x-totallimit-overages-remaining"),
    lifetime_used: num("x-lifetime-used"),
    ratelimit_remaining: get("x-ratelimit-remaining"),
  };
}

/**
 * Single person enrich. Default retries=0 (eval policy: no retries unless transport failure
 * and caller passes retries>0).
 */
export async function enrichPdlPerson(person, opts = {}) {
  const params = buildPdlPersonEnrichParams(person, opts);
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === true || v === false) qs.set(k, String(v));
    else qs.set(k, String(v));
  }
  const started = Date.now();
  const res = await pdlFetch(`${PDL_PERSON_ENRICH_URL}?${qs.toString()}`, {
    method: "GET",
    apiKey: opts.apiKey,
    timeoutMs: opts.timeoutMs,
    retries: opts.retries ?? 0,
  });
  return {
    ...res,
    latency_ms: Date.now() - started,
    request_params_sanitized: sanitizePdlParamsForLog(params),
    normalized: normalizePdlPersonResponse(res),
  };
}

/**
 * Bulk enrich (≤100). Credits charged per status=200 row.
 */
export async function enrichPdlPersonBulk(people = [], opts = {}) {
  if (!Array.isArray(people) || !people.length) {
    return { http_status: 0, ok: false, rows: [], latency_ms: 0 };
  }
  if (people.length > 100) {
    const err = new Error("PDL_BULK_MAX_100");
    err.code = "PDL_BULK_TOO_LARGE";
    throw err;
  }
  const requests = people.map((p, i) => ({
    params: buildPdlPersonEnrichParams(p, opts),
    metadata: { subject_id: p.id || p.subject_id || String(i) },
  }));
  const started = Date.now();
  const res = await pdlFetch(PDL_PERSON_BULK_URL, {
    method: "POST",
    body: {
      requests,
      include_if_matched: true,
      min_likelihood: Number(opts.min_likelihood ?? 6),
      titlecase: true,
      data_include: opts.data_include || PDL_EVAL_DATA_INCLUDE,
    },
    apiKey: opts.apiKey,
    timeoutMs: opts.timeoutMs ?? 120000,
    retries: opts.retries ?? 0,
  });
  const arr = Array.isArray(res.payload) ? res.payload : [];
  return {
    ...res,
    latency_ms: Date.now() - started,
    rows: arr.map((row, i) => ({
      http_status: row.status,
      ok: row.status === 200,
      likelihood: row.likelihood ?? null,
      metadata: row.metadata || requests[i]?.metadata || null,
      request_params_sanitized: sanitizePdlParamsForLog(requests[i].params),
      normalized: normalizePdlPersonResponse({
        http_status: row.status,
        ok: row.status === 200,
        payload: row,
      }),
    })),
  };
}

export function sanitizePdlParamsForLog(params = {}) {
  const out = { ...params };
  delete out.api_key;
  delete out.email;
  delete out.phone;
  delete out.email_hash;
  return out;
}

/**
 * Classify raw contact-field presence BEFORE normalization.
 * Free / under-entitled plans may return boolean true/false instead of values
 * (PDL Person Schema: contact fields appear as existence flags until Pro unlock).
 */
export function classifyPdlFieldPresence(value) {
  if (value === undefined) {
    return { presence: "ABSENT", typeof: "undefined", usable_contact_value: false };
  }
  if (value === null) {
    return { presence: "NULL", typeof: "object", usable_contact_value: false };
  }
  if (typeof value === "boolean") {
    return {
      presence: "MASKED_BOOLEAN",
      typeof: "boolean",
      masked_exists: value,
      usable_contact_value: false,
      note: "Plan returned existence flag instead of contact value (pre-Pro / field exclusion pattern)",
    };
  }
  if (Array.isArray(value)) {
    if (!value.length) {
      return { presence: "EMPTY_ARRAY", typeof: "object", length: 0, usable_contact_value: false };
    }
    return {
      presence: "POPULATED",
      typeof: "object",
      length: value.length,
      usable_contact_value: true,
    };
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return { presence: "EMPTY_STRING", typeof: "string", usable_contact_value: false };
    }
    return {
      presence: "POPULATED",
      typeof: "string",
      usable_contact_value: true,
      looks_like_email: trimmed.includes("@"),
      looks_like_phone: /^\+?\d[\d\s().-]{6,}$/.test(trimmed),
    };
  }
  return {
    presence: "OTHER",
    typeof: typeof value,
    usable_contact_value: false,
  };
}

/**
 * Restricted pre-normalization contact inspection (eval retention — no credentials).
 */
export function inspectPdlContactFieldsRaw(payload = {}) {
  const status = payload?.status ?? null;
  const data = payload?.data || (status === 200 && payload && !payload.error ? payload : null);
  const keys = data && typeof data === "object" ? Object.keys(data) : [];
  const pick = (name) => {
    if (!data || typeof data !== "object") {
      return { field: name, in_data_object: false, ...classifyPdlFieldPresence(undefined) };
    }
    const inObj = Object.prototype.hasOwnProperty.call(data, name);
    return {
      field: name,
      in_data_object: inObj,
      ...(inObj ? classifyPdlFieldPresence(data[name]) : classifyPdlFieldPresence(undefined)),
    };
  };
  return {
    response_nesting: {
      top_level_keys: payload && typeof payload === "object" ? Object.keys(payload) : [],
      status: payload?.status ?? null,
      likelihood: payload?.likelihood ?? null,
      has_data_object: Boolean(data && typeof data === "object"),
      matched_inputs: payload?.matched ?? null,
      data_field_count: keys.length,
    },
    contact_fields: {
      work_email: pick("work_email"),
      emails: pick("emails"),
      mobile_phone: pick("mobile_phone"),
      phone_numbers: pick("phone_numbers"),
      phones: pick("phones"),
    },
    schema_notes: {
      emails_official: "Array[{ address, type, ... }] — use address, not email",
      phone_numbers_official: "Array[String] E.164",
      phones_official: "Array[{ number, first_seen, last_seen, num_sources }] — no explicit type",
      mobile_phone_official: "String — PDL-defined personal mobile",
      free_plan_masking:
        "Contact fields may be boolean true/false until Pro contact entitlement unlocks values",
    },
  };
}

function asUsableEmailString(value) {
  if (typeof value !== "string") return null;
  const s = value.trim().toLowerCase();
  if (!s || !s.includes("@") || s === "true" || s === "false") return null;
  return s;
}

function asUsablePhoneString(value) {
  if (typeof value === "boolean") return null;
  if (typeof value !== "string" && typeof value !== "number") return null;
  const s = String(value).trim();
  if (!s || s === "true" || s === "false") return null;
  return s;
}

/**
 * Strip to evaluation-safe fields — no full raw persistence.
 * Does not treat boolean plan-masks as contact values.
 */
export function normalizePdlPersonResponse(res = {}) {
  const status = res.http_status ?? res.payload?.status ?? null;
  const data = res.payload?.data || (status === 200 ? res.payload : null);
  const field_presence = inspectPdlContactFieldsRaw(res.payload || {});

  if (status === 404 || status === 400) {
    return {
      matched: false,
      status,
      error: res.payload?.error || res.payload?.message || "NO_MATCH",
      work_email: null,
      emails: [],
      phones: [],
      identity: null,
      likelihood: res.payload?.likelihood ?? null,
      field_presence,
    };
  }
  if (!data || typeof data !== "object") {
    return {
      matched: false,
      status,
      error: res.payload?.error || "EMPTY_OR_ERROR",
      work_email: null,
      emails: [],
      phones: [],
      identity: null,
      likelihood: null,
      field_presence,
    };
  }

  const emails = [];
  const workEmail = asUsableEmailString(data.work_email);
  if (workEmail) {
    emails.push({
      email: workEmail,
      type: "work",
      source: "work_email",
    });
  } else if (typeof data.work_email === "boolean") {
    // Masked — do not invent an address
  }

  for (const e of Array.isArray(data.emails) ? data.emails : []) {
    if (typeof e === "boolean") continue;
    const addr = asUsableEmailString(e?.address || e?.email || (typeof e === "string" ? e : null));
    if (!addr || emails.some((x) => x.email === addr)) continue;
    emails.push({
      email: addr,
      type: e?.type || e?.email_type || "unknown",
      status: e?.status || null,
    });
  }

  const phones = [];
  const mobile = asUsablePhoneString(data.mobile_phone);
  if (mobile) {
    phones.push({ number: mobile, type: "mobile", source: "mobile_phone" });
  }
  for (const p of Array.isArray(data.phone_numbers) ? data.phone_numbers : []) {
    if (typeof p === "boolean") continue;
    const num = asUsablePhoneString(typeof p === "object" && p ? p.number : p);
    if (!num || phones.some((x) => x.number === num)) continue;
    phones.push({ number: num, type: "unknown", source: "phone_numbers" });
  }
  // Premium `phones` objects intentionally not required — ignore if present for Person Base eval

  const personal_emails = [];
  for (const pe of Array.isArray(data.personal_emails) ? data.personal_emails : []) {
    const addr = asUsableEmailString(typeof pe === "string" ? pe : pe?.address || pe?.email);
    if (!addr || personal_emails.includes(addr)) continue;
    personal_emails.push(addr);
  }
  const recommended_personal_email = asUsableEmailString(data.recommended_personal_email);

  return {
    matched: status === 200,
    status,
    likelihood: res.payload?.likelihood ?? data.likelihood ?? null,
    matched_inputs: res.payload?.matched || null,
    work_email: workEmail,
    emails,
    personal_emails,
    recommended_personal_email,
    phones,
    identity: {
      full_name: data.full_name || null,
      first_name: data.first_name || null,
      last_name: data.last_name || null,
      linkedin_url: data.linkedin_url || null,
      job_title: data.job_title || null,
      job_company_name: data.job_company_name || null,
      job_company_website: data.job_company_website || null,
      job_company_id: data.job_company_id || null,
      job_company_size: data.job_company_size || null,
      job_last_verified: data.job_last_verified || null,
      job_last_changed: data.job_last_changed || null,
      pdl_id: data.id || null,
      dataset_version: data.dataset_version || null,
      experience_count: Array.isArray(data.experience) ? data.experience.length : null,
      profiles_count: Array.isArray(data.profiles) ? data.profiles.length : null,
    },
    field_presence,
    field_inventory: {
      work_email: classifyPdlFieldPresence(data.work_email),
      emails: classifyPdlFieldPresence(data.emails),
      personal_emails: classifyPdlFieldPresence(data.personal_emails),
      recommended_personal_email: classifyPdlFieldPresence(data.recommended_personal_email),
      mobile_phone: classifyPdlFieldPresence(data.mobile_phone),
      phone_numbers: classifyPdlFieldPresence(data.phone_numbers),
      phones_premium_ignored: classifyPdlFieldPresence(data.phones),
      job_title: classifyPdlFieldPresence(data.job_title),
      job_company_name: classifyPdlFieldPresence(data.job_company_name),
      job_company_website: classifyPdlFieldPresence(data.job_company_website),
      experience: classifyPdlFieldPresence(data.experience),
      profiles: classifyPdlFieldPresence(data.profiles),
      job_last_verified: classifyPdlFieldPresence(data.job_last_verified),
      job_last_changed: classifyPdlFieldPresence(data.job_last_changed),
    },
    error: null,
  };
}

export function estimatePdlWorstCaseCredits(personCount) {
  return Number(personCount || 0) * PDL_CREDIT_COSTS.enrichment_per_200_match;
}
