/**
 * Census Autopilot V3.0.1 — Canonical property claim store + best-eligible selection.
 * A blocked lower-authority claim must never suppress a permitted higher-authority claim.
 */

import { WRITE_CLASS } from "./constants.js";

export const CLAIM_STORE_VERSION = "census-autopilot-v3.0.1-claim-store";

/** Higher = preferred authority for production persist. */
export const SOURCE_AUTHORITY_RANK = Object.freeze({
  official_property_page: 100,
  official_brand_directory: 90,
  official_brand_structured: 88,
  official_owner_operator: 80,
  dealality_geography: 75,
  approved_geocode: 70,
  approved_structured_public: 60,
  serpapi_google_hotels: 40,
  cvent: 10,
  legacy_census: 5,
  unknown: 0,
});

export const RIGHTS_STATUS = Object.freeze({
  ELIGIBLE: "ELIGIBLE",
  BLOCKED_RIGHTS: "BLOCKED_RIGHTS",
  PROHIBITED: "PROHIBITED",
  STEWARD_REVIEW: "STEWARD_REVIEW",
  FIRST_PARTY_PENDING: "FIRST_PARTY_PENDING",
});

/**
 * @typedef {object} FieldClaim
 * @property {*} value
 * @property {string} source
 * @property {string} source_type
 * @property {string|null} [source_url]
 * @property {string|null} [retrieved_at]
 * @property {string} [confidence]
 * @property {string} [match_confidence]
 * @property {string} [rights_status]
 * @property {string} [research_run]
 * @property {string|null} [temporal_validity]
 * @property {string} [status]
 * @property {boolean} [serpapi_used]
 * @property {boolean} [cvent_used_as_production_evidence]
 * @property {boolean} [legacy_used_as_production_evidence]
 */

/**
 * Create empty store keyed by property_identity_id → field → claim[].
 */
export function createClaimStore() {
  return {
    version: CLAIM_STORE_VERSION,
    properties: /** @type {Record<string, Record<string, FieldClaim[]>>} */ ({}),
  };
}

/**
 * Merge claims without erasing prior verified claims.
 * Same source+value upserts; otherwise appends. Never deletes prior claims.
 */
export function upsertClaim(store, propertyIdentityId, field, claim) {
  const pid = String(propertyIdentityId || "").trim();
  const f = String(field || "").trim();
  if (!pid || !f || claim?.value == null || claim.value === "") return store;

  if (!store.properties[pid]) store.properties[pid] = {};
  if (!store.properties[pid][f]) store.properties[pid][f] = [];

  const list = store.properties[pid][f];
  const key = `${claim.source_type}|${claim.source}|${String(claim.value)}`;
  const idx = list.findIndex(
    (c) => `${c.source_type}|${c.source}|${String(c.value)}` === key
  );
  const normalized = normalizeClaim(claim);
  if (idx >= 0) list[idx] = { ...list[idx], ...normalized };
  else list.push(normalized);
  return store;
}

/**
 * Carry-forward: copy all claims from prior store into next without dropping.
 */
export function mergeClaimStores(prior, next) {
  const out = createClaimStore();
  for (const store of [prior, next]) {
    if (!store?.properties) continue;
    for (const [pid, fields] of Object.entries(store.properties)) {
      for (const [field, claims] of Object.entries(fields || {})) {
        for (const c of claims || []) upsertClaim(out, pid, field, c);
      }
    }
  }
  return out;
}

export function normalizeClaim(claim) {
  const sourceType = String(claim.source_type || claim.source || "unknown");
  const serpapi =
    claim.serpapi_used === true || /serpapi/i.test(sourceType) || /serpapi/i.test(claim.source || "");
  const cvent =
    claim.cvent_used_as_production_evidence === true || /cvent/i.test(sourceType);
  const legacy =
    claim.legacy_used_as_production_evidence === true || /legacy/i.test(sourceType);

  let rights = claim.rights_status || null;
  if (!rights) {
    if (cvent || legacy) rights = RIGHTS_STATUS.PROHIBITED;
    else if (serpapi) rights = RIGHTS_STATUS.BLOCKED_RIGHTS; // pending persistence clarification
    else rights = RIGHTS_STATUS.ELIGIBLE;
  }

  return {
    value: claim.value,
    source: claim.source || sourceType,
    source_type: sourceType,
    source_url: claim.source_url || null,
    retrieved_at: claim.retrieved_at || null,
    confidence: claim.confidence || "Unknown",
    match_confidence: claim.match_confidence || "Unknown",
    rights_status: rights,
    research_run: claim.research_run || null,
    temporal_validity: claim.temporal_validity || null,
    status: claim.status || "active",
    serpapi_used: serpapi,
    cvent_used_as_production_evidence: cvent,
    legacy_used_as_production_evidence: legacy,
  };
}

function authorityRank(claim) {
  const t = String(claim.source_type || "");
  if (SOURCE_AUTHORITY_RANK[t] != null) return SOURCE_AUTHORITY_RANK[t];
  if (/official_brand/i.test(t)) return SOURCE_AUTHORITY_RANK.official_brand_directory;
  if (/official_property/i.test(t)) return SOURCE_AUTHORITY_RANK.official_property_page;
  if (/dealality/i.test(t)) return SOURCE_AUTHORITY_RANK.dealality_geography;
  if (/serpapi/i.test(t)) return SOURCE_AUTHORITY_RANK.serpapi_google_hotels;
  if (/cvent/i.test(t)) return SOURCE_AUTHORITY_RANK.cvent;
  if (/legacy/i.test(t)) return SOURCE_AUTHORITY_RANK.legacy_census;
  return SOURCE_AUTHORITY_RANK.unknown;
}

function confRank(c) {
  const m = { Exact: 5, High: 4, Medium: 3, Low: 2, Insufficient: 1, Unknown: 0 };
  return m[c] ?? 0;
}

/**
 * Select best production-eligible claim for a field.
 * Blocked lower-authority claims remain rejected_claims — they do NOT block eligible official claims.
 *
 * @param {FieldClaim[]} claims
 * @param {{ field?: string, requireEligible?: boolean }} [opts]
 */
export function resolveBestEligibleClaim(claims = [], opts = {}) {
  const requireEligible = opts.requireEligible !== false;
  const rejected = [];
  const candidates = [];

  for (const raw of claims) {
    const c = normalizeClaim(raw);
    if (c.status === "superseded" || c.status === "deleted") {
      rejected.push({ claim: c, reason: `status_${c.status}` });
      continue;
    }
    if (c.cvent_used_as_production_evidence || c.rights_status === RIGHTS_STATUS.PROHIBITED) {
      rejected.push({ claim: c, reason: "prohibited_cvent_or_legacy_or_policy" });
      continue;
    }
    if (c.legacy_used_as_production_evidence) {
      rejected.push({ claim: c, reason: "legacy_production_evidence_prohibited" });
      continue;
    }
    if (requireEligible && c.rights_status === RIGHTS_STATUS.BLOCKED_RIGHTS) {
      rejected.push({ claim: c, reason: "blocked_rights_serpapi_or_policy" });
      continue;
    }
    if (c.value == null || c.value === "") {
      rejected.push({ claim: c, reason: "blank_value" });
      continue;
    }
    candidates.push(c);
  }

  candidates.sort((a, b) => {
    const auth = authorityRank(b) - authorityRank(a);
    if (auth) return auth;
    const mc = confRank(b.match_confidence) - confRank(a.match_confidence);
    if (mc) return mc;
    const cc = confRank(b.confidence) - confRank(a.confidence);
    if (cc) return cc;
    const ta = Date.parse(a.retrieved_at || "") || 0;
    const tb = Date.parse(b.retrieved_at || "") || 0;
    return tb - ta;
  });

  const selected = candidates[0] || null;
  for (const c of candidates.slice(1)) {
    rejected.push({ claim: c, reason: "lower_authority_or_confidence_than_selected" });
  }

  return {
    selected_claim: selected,
    selected_source: selected?.source || null,
    selected_source_type: selected?.source_type || null,
    selected_rights_status: selected?.rights_status || null,
    rejected_claims_with_reason: rejected,
    eligible_count: candidates.length,
    blocked_but_not_suppressing: rejected.filter((r) => r.reason === "blocked_rights_serpapi_or_policy")
      .length,
  };
}

/**
 * Map selected claim → V3 write class for production candidate.
 */
export function writeClassForSelectedClaim(field, selected) {
  if (!selected) return null;
  if (selected.rights_status === RIGHTS_STATUS.BLOCKED_RIGHTS) return WRITE_CLASS.BLOCKED_RIGHTS;
  if (selected.rights_status === RIGHTS_STATUS.PROHIBITED) return WRITE_CLASS.PROHIBITED;
  if (field === "Rooms / Keys") return WRITE_CLASS.FIRST_PARTY_VALIDATION;
  if (["Latitude", "Longitude", "Address", "Phone"].includes(field)) {
    return WRITE_CLASS.CORROBORATED_WRITE;
  }
  if (
    [
      "State / Region",
      "Submarket",
      "Market",
      "City",
      "Country",
      "Continent",
      "Sub-Continent",
    ].includes(field)
  ) {
    return WRITE_CLASS.AUTO_WRITE_SAFE;
  }
  return WRITE_CLASS.CORROBORATED_WRITE;
}

/**
 * Regression helper: official + SerpApi claims → official must win.
 */
export function assertOfficialBeatsBlockedSerpApi(field, officialValue, serpapiValue) {
  const result = resolveBestEligibleClaim([
    {
      value: serpapiValue,
      source: "serpapi",
      source_type: "serpapi_google_hotels",
      serpapi_used: true,
      confidence: "High",
      match_confidence: "High",
    },
    {
      value: officialValue,
      source: "hilton_official",
      source_type: "official_brand_directory",
      serpapi_used: false,
      confidence: "High",
      match_confidence: "High",
    },
  ], { field });
  return {
    field,
    pass:
      result.selected_claim?.value === officialValue &&
      result.selected_rights_status === RIGHTS_STATUS.ELIGIBLE &&
      !/serpapi/i.test(result.selected_source_type || ""),
    result,
  };
}
