/**
 * Phone pipeline — normalize + staging claim (no Cvent/legacy).
 */

/**
 * @param {string} raw
 * @param {{ default_country_code?: string }} [opts]
 */
export function normalizePhone(raw, opts = {}) {
  const original = String(raw || "").trim();
  if (!original) {
    return { ok: false, raw_phone: null, normalized_phone: null, country_code: null, national_number: null, extension: null };
  }
  let ext = null;
  let body = original;
  const extMatch = body.match(/(?:ext\.?|x|extension)\s*[:.]?\s*(\d{1,8})$/i);
  if (extMatch) {
    ext = extMatch[1];
    body = body.slice(0, extMatch.index).trim();
  }
  const digits = body.replace(/[^\d+]/g, "");
  let country_code = null;
  let national = digits;
  if (digits.startsWith("+")) {
    const m = digits.match(/^\+(\d{1,3})(\d{6,14})$/);
    if (m) {
      country_code = m[1];
      national = m[2];
    }
  } else if (opts.default_country_code && /^\d{6,14}$/.test(digits)) {
    country_code = String(opts.default_country_code).replace(/\D/g, "");
    national = digits;
  }
  const normalized =
    country_code && national
      ? `+${country_code}${national}${ext ? ` ext ${ext}` : ""}`
      : original;
  return {
    ok: true,
    raw_phone: original,
    normalized_phone: normalized,
    country_code,
    national_number: national,
    extension: ext,
  };
}

export function buildPhoneStaging(opts = {}) {
  const raw = opts.raw_phone || opts.phone || null;
  if (!raw) return { ok: false, claim: null };
  const n = normalizePhone(raw, { default_country_code: opts.default_country_code });
  if (!n.ok) return { ok: false, claim: null };
  const sourceType = opts.source_type || "official_property_page";
  const serpapi = /serpapi/i.test(sourceType) || opts.serpapi_used === true;
  return {
    ok: true,
    ...n,
    claim: {
      value: n.normalized_phone,
      source: opts.source || sourceType,
      source_type: sourceType,
      source_url: opts.source_url || null,
      retrieved_at: opts.retrieved_at || new Date().toISOString(),
      confidence: opts.confidence || "High",
      match_confidence: opts.match_confidence || "High",
      research_run: opts.research_run || null,
      serpapi_used: serpapi,
      cvent_used_as_production_evidence: false,
      legacy_used_as_production_evidence: false,
      status: "active",
      raw_phone: n.raw_phone,
    },
  };
}
