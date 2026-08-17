/**
 * Golden field semantic validation + V4 future quality gate helpers.
 * Completeness ≠ quality. Prefer blank over wrong.
 */

import {
  isDescriptorCity,
  classifyAndNormalizeCityState,
} from "../census-city-state-normalizer.js";
import {
  AFFILIATION_STATUS,
  evaluateCurrentAffiliationGate,
  isParentCompanyAsCurrentBrand,
  validateCurrentBrandSemantics,
  CHOICE_URL_BRAND_SLUG_MAP,
  inferChoiceBrandFromOfficialPropertyUrl,
  lookupBrandRegistry,
} from "./current-affiliation.js";

export const GOLDEN_QUALITY_MODEL_VERSION = "golden-quality-model-v1";
export const V4_QUALITY_GATE_VERSION = "v4-future-quality-gate-v1";

export const CITY_STATUS = Object.freeze({
  VALID: "VALID",
  UNKNOWN: "UNKNOWN",
  CONFLICT: "CONFLICT",
  INVALID: "INVALID",
  BLANK: "BLANK",
});

export const ADDRESS_STATUS = Object.freeze({
  ADDRESS_VERIFIED: "ADDRESS_VERIFIED",
  ADDRESS_RIGHTS_BLOCKED: "ADDRESS_RIGHTS_BLOCKED",
  ADDRESS_NOT_FOUND: "ADDRESS_NOT_FOUND",
  ADDRESS_CONFLICT: "ADDRESS_CONFLICT",
  ADDRESS_PRESENT: "ADDRESS_PRESENT",
  ADDRESS_BLANK: "ADDRESS_BLANK",
});

export const SUBMARKET_STATUS = Object.freeze({
  MATCHED: "MATCHED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  UNRESOLVED: "UNRESOLVED",
});

export const MUTATION_CLASS = Object.freeze({
  SAFE_INVALID_VALUE_CORRECTION: "SAFE_INVALID_VALUE_CORRECTION",
  SAFE_BRAND_CORRECTION: "SAFE_BRAND_CORRECTION",
  SAFE_BLANK_FILL: "SAFE_BLANK_FILL",
  SAFE_DERIVED_GEOGRAPHY: "SAFE_DERIVED_GEOGRAPHY",
  TEMPORAL_AFFILIATION_UPDATE: "TEMPORAL_AFFILIATION_UPDATE",
  STEWARD_REVIEW: "STEWARD_REVIEW",
  RIGHTS_BLOCKED: "RIGHTS_BLOCKED",
  NO_CHANGE: "NO_CHANGE",
});

/** Countries that must not appear as City. */
const COUNTRY_AS_CITY = new Set(
  [
    "Mexico",
    "Brazil",
    "Argentina",
    "Jamaica",
    "Barbados",
    "Costa Rica",
    "Dominican Republic",
    "Colombia",
    "Panama",
    "Chile",
    "Peru",
  ].map((s) => s.toLowerCase())
);

/**
 * Semantic City validation.
 */
export function validateCitySemantics(city, country = null) {
  const raw = String(city || "").trim();
  if (!raw) return { status: CITY_STATUS.BLANK, ok: false, reason: "blank" };
  if (/^unknown$/i.test(raw)) {
    return { status: CITY_STATUS.UNKNOWN, ok: false, reason: "unknown_placeholder" };
  }
  if (isDescriptorCity(raw) || /adults?\s*only|all[-\s]?inclusive/i.test(raw)) {
    return {
      status: CITY_STATUS.INVALID,
      ok: false,
      reason: "marketing_or_descriptor_text",
      value: raw,
    };
  }
  if (COUNTRY_AS_CITY.has(raw.toLowerCase())) {
    return {
      status: CITY_STATUS.INVALID,
      ok: false,
      reason: "country_as_city",
      value: raw,
    };
  }
  if (country && raw.toLowerCase() === String(country).toLowerCase()) {
    return {
      status: CITY_STATUS.INVALID,
      ok: false,
      reason: "country_equals_city",
      value: raw,
    };
  }
  // Riviera Maya / market-like labels — treat as QUESTIONABLE locality (invalid for City)
  if (/^riviera maya$/i.test(raw) || /^caribbean$/i.test(raw)) {
    return {
      status: CITY_STATUS.INVALID,
      ok: false,
      reason: "market_or_region_as_city",
      value: raw,
    };
  }
  return { status: CITY_STATUS.VALID, ok: true, value: raw };
}

/**
 * Validate Choice URL slug against canonical registry / known map only.
 */
export function validateChoiceUrlBrandCorrection(url, proposedBrand) {
  const u = String(url || "");
  const m = u.match(/\/([a-z0-9-]+)-hotels\/[a-z]{2}\d+/i);
  if (!m) {
    return {
      ok: false,
      reason: "no_choice_brand_slug_in_url",
      auto_correct: false,
    };
  }
  const slug = m[1].toLowerCase();
  const mapped = CHOICE_URL_BRAND_SLUG_MAP[slug];
  if (!mapped) {
    return {
      ok: false,
      reason: "unknown_choice_brand_slug",
      slug,
      auto_correct: false,
    };
  }
  const reg = lookupBrandRegistry(mapped);
  if (proposedBrand && proposedBrand !== mapped) {
    return {
      ok: false,
      reason: "proposed_brand_mismatch_registry_map",
      slug,
      mapped,
      proposedBrand,
      auto_correct: false,
    };
  }
  return {
    ok: true,
    slug,
    canonical_brand: mapped,
    registry_hit: Boolean(reg),
    auto_correct: true,
    parent_company: "Choice Hotels International",
  };
}

/**
 * Golden Quality score (0–100) — validity-weighted, not mere completeness.
 */
export function scoreGoldenQuality(dims = {}) {
  const weights = {
    field_completeness: 0.15,
    semantic_validity: 0.3,
    identity_confidence: 0.15,
    source_eligibility: 0.1,
    geography_coherence: 0.15,
    affiliation_confidence: 0.1,
    freshness: 0.05,
  };
  let total = 0;
  let wsum = 0;
  for (const [k, w] of Object.entries(weights)) {
    const v = Number(dims[k]);
    if (!Number.isFinite(v)) continue;
    total += w * Math.max(0, Math.min(100, v));
    wsum += w;
  }
  return wsum ? Math.round((10 * total) / wsum) / 10 : 0;
}

/**
 * V4 pre-mutation quality gate.
 */
export function evaluateV4QualityGate(ctx = {}) {
  const failures = [];
  if (!ctx.property_identity_ok) failures.push("PROPERTY_IDENTITY");
  if (!ctx.field_semantics_ok) failures.push("FIELD_SEMANTICS");
  if (!ctx.source_eligibility_ok) failures.push("SOURCE_ELIGIBILITY");
  if (!ctx.cross_field_consistency_ok) failures.push("CROSS_FIELD_CONSISTENCY");
  if (!ctx.current_affiliation_ok) failures.push("CURRENT_AFFILIATION");
  if (!ctx.geography_coherence_ok) failures.push("GEOGRAPHY_COHERENCE");
  if (!ctx.write_safety_ok) failures.push("WRITE_SAFETY");

  return {
    version: V4_QUALITY_GATE_VERSION,
    pass: failures.length === 0,
    failures,
    principle: "Completeness comes AFTER validity. Blank preferred over wrong.",
    circuit_break_on_semantic_violation: true,
  };
}

export function classifySubmarketStatus(submarket, applicability) {
  if (
    applicability === "NOT_APPLICABLE" ||
    /not applicable|n\/a/i.test(String(submarket || ""))
  ) {
    return SUBMARKET_STATUS.NOT_APPLICABLE;
  }
  if (submarket && String(submarket).trim()) return SUBMARKET_STATUS.MATCHED;
  return SUBMARKET_STATUS.UNRESOLVED;
}

export {
  AFFILIATION_STATUS,
  evaluateCurrentAffiliationGate,
  isParentCompanyAsCurrentBrand,
  validateCurrentBrandSemantics,
  inferChoiceBrandFromOfficialPropertyUrl,
  classifyAndNormalizeCityState,
};
