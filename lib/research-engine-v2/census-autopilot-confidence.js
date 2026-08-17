/**
 * Unified Census Autopilot confidence system.
 * High = production-writable; Medium/Low/Hold = no default write.
 */

export const CONFIDENCE_LEVELS = Object.freeze(["High", "Medium", "Low", "Hold"]);

export const FIELD_CONFIDENCE_RULES = Object.freeze({
  coordinates: {
    high_requires: ["official_coords_or_approved_address_geocode", "no_city_centroid", "no_zero_zero"],
    never: ["city_centroid", "fabricated", "unapproved_provider"],
  },
  descriptions: {
    high_requires: ["grounded_in_official_or_public_source_text"],
    never: ["unsourced_ai", "booking_boilerplate"],
  },
  amenities: {
    high_requires: ["explicitly_supported_on_source"],
    never: ["inferred_without_support"],
  },
  rooms_keys: {
    high_requires: ["official_source_states_hotel_rooms_or_keys", "hotel_only_count"],
    never: ["residences", "villas", "apartments", "units_ambiguous", "brand_average", "fabricated"],
  },
  radar: {
    high_requires: ["clear_identity", "source_support", "not_held"],
    never: ["held_record_public", "brand_unconfirmed_public"],
  },
});

/**
 * Normalize any confidence-like value to High|Medium|Low|Hold.
 * @param {unknown} value
 */
export function normalizeConfidence(value) {
  const s = String(value || "").trim();
  if (/^exact$/i.test(s)) return "High";
  if (/^high$/i.test(s)) return "High";
  if (/^medium$/i.test(s)) return "Medium";
  if (/^low$/i.test(s)) return "Low";
  if (/^hold$/i.test(s)) return "Hold";
  if (/^insufficient$/i.test(s)) return "Hold";
  if (/^unknown$/i.test(s)) return "Low";
  return "Low";
}

/**
 * Rank for sorting (Higher = better).
 * @param {string} confidence
 */
export function confidenceRank(confidence) {
  const n = normalizeConfidence(confidence);
  return { High: 4, Medium: 3, Low: 2, Hold: 1 }[n] || 0;
}

/**
 * Whether a field write is allowed under Autopilot apply rules.
 * Default: High only.
 * @param {string} confidence
 * @param {{ threshold?: string }} [opts]
 */
export function isWritableConfidence(confidence, opts = {}) {
  const threshold = normalizeConfidence(opts.threshold || "High");
  const c = normalizeConfidence(confidence);
  if (c === "Hold" || c === "Low") return false;
  if (threshold === "High") return c === "High";
  if (threshold === "Medium") return c === "High" || c === "Medium";
  return false;
}

/**
 * Classify a proposed field update into a confidence bucket with reason.
 * @param {{ field: string, confidence?: string, reasons?: string[], hold?: boolean, mixed_use?: boolean, held_record?: boolean, brand_unconfirmed?: boolean, source_missing?: boolean, conflicting?: boolean }} input
 */
export function classifyFieldConfidence(input) {
  if (input.held_record || input.brand_unconfirmed) {
    return { confidence: "Hold", reason: input.held_record ? "human_review_required" : "brand_unconfirmed" };
  }
  if (input.mixed_use || input.hold) {
    return { confidence: "Hold", reason: input.mixed_use ? "mixed_use_ambiguity" : "explicit_hold" };
  }
  if (input.conflicting) {
    return { confidence: "Hold", reason: "source_conflict" };
  }
  if (input.source_missing) {
    return { confidence: "Low", reason: "source_support_missing" };
  }
  const c = normalizeConfidence(input.confidence);
  return {
    confidence: c,
    reason: (input.reasons && input.reasons[0]) || `classified_${c.toLowerCase()}`,
    writable: isWritableConfidence(c),
  };
}

/**
 * Aggregate proposal list into High/Medium/Low/Hold counts.
 * @param {Array<{ confidence?: string }>} proposals
 */
export function tallyConfidence(proposals = []) {
  const out = { High: 0, Medium: 0, Low: 0, Hold: 0 };
  for (const p of proposals) {
    const c = normalizeConfidence(p.confidence);
    out[c] = (out[c] || 0) + 1;
  }
  return out;
}
