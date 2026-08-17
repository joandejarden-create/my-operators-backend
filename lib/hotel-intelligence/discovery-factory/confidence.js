/**
 * Discovery Factory confidence tiers — maximize high-confidence auto-stage, minimize review.
 */

export const DISCOVERY_FACTORY_VERSION = "discovery-factory-v1";

export const DISCOVERY_TIER = Object.freeze({
  A: "TIER_A",
  B: "TIER_B",
  C: "TIER_C",
});

export const STAGE_STATUS = Object.freeze({
  READY_FOR_IMPORT: "READY_FOR_IMPORT",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  REJECTED: "REJECTED",
  MATCHED_EXISTING: "MATCHED_EXISTING",
});

export const TIER_THRESHOLDS = Object.freeze({
  tier_a_min: 0.9,
  tier_b_min: 0.7,
});

const STRONG_NAME_RE =
  /\b(hotel|resort|inn|suites?|palace|lodge|hostel|posada|pousada|hostal|hyatt|marriott|hilton|ihg|accor|radisson|sheraton|westin|four seasons|intercontinental|novotel|ibis|mercure|sofitel|fairmont|ritz|st\.?\s*regis|w hotel|andaz|kimpton|curio|autograph|tribute|moxy|aloft|element|residence inn|courtyard|hampton|garden inn|doubletree|embassy|homewood|home2|holiday inn|crowne plaza|indigo|voco|even|avid|staybridge|candlewood|wyndham|ramada|days inn|super 8|la quinta|best western|choice|comfort|quality|clarion|sleep inn|econo|rodeo|ascend|radisson|collection)\b/i;

const WEAK_NAME_RE =
  /\b(center|centre|centro|plaza events?|salon|salão|salao|ballroom|auditorium|stadium|arena|cowork|office|apartment|apto|airbnb|vrbo|condo)\b/i;

/**
 * Score hotel-name strength 0–1.
 */
export function scoreHotelNameStrength(name) {
  const n = String(name || "").trim();
  if (!n || n.length < 3) return 0;
  if (WEAK_NAME_RE.test(n) && !STRONG_NAME_RE.test(n)) return 0.35;
  let s = 0.55;
  if (STRONG_NAME_RE.test(n)) s += 0.25;
  if (n.split(/\s+/).length >= 2) s += 0.1;
  if (n.length >= 12) s += 0.05;
  if (/^\d+$/.test(n)) s = 0.1;
  return Math.min(1, s);
}

/**
 * Compute discovery identity confidence and tier.
 * @param {object} input
 * @param {object} input.cityResult — from resolveDiscoveryCity
 * @param {object} input.resolveResult — from resolveHotelIdentity
 * @param {string} input.name
 * @param {string} input.country
 */
export function assignDiscoveryConfidence(input = {}) {
  const reasons = [];
  const name = String(input.name || "").trim();
  const country = String(input.country || "").trim();
  const cityResult = input.cityResult || {};
  const resolved = input.resolveResult || {};
  const matchStatus = String(resolved.match_status || "").toLowerCase();

  // Existing matches → not import candidates
  if (["exact", "strong", "probable"].includes(matchStatus)) {
    return {
      identity_confidence: Number(resolved.match_score) || 0.95,
      tier: DISCOVERY_TIER.C,
      stage_status: STAGE_STATUS.MATCHED_EXISTING,
      reasons: ["matched_existing_census", ...(resolved.matching_reasons || [])],
      review_required: false,
    };
  }

  if (matchStatus === "ambiguous") {
    return {
      identity_confidence: Math.min(0.65, Number(resolved.match_score) || 0.5),
      tier: DISCOVERY_TIER.C,
      stage_status: STAGE_STATUS.REJECTED,
      reasons: ["ambiguous_identity", "duplicate_risk"],
      review_required: false,
    };
  }

  if (!name || !country) {
    return {
      identity_confidence: 0.2,
      tier: DISCOVERY_TIER.C,
      stage_status: STAGE_STATUS.REJECTED,
      reasons: [!name ? "missing_name" : null, !country ? "missing_country" : null].filter(
        Boolean
      ),
      review_required: false,
    };
  }

  if (!cityResult.city) {
    return {
      identity_confidence: 0.35,
      tier: DISCOVERY_TIER.C,
      stage_status: STAGE_STATUS.REJECTED,
      reasons: ["city_unresolved"],
      review_required: false,
    };
  }

  if (cityResult.multi_city) {
    reasons.push("multi_city_conflict");
  }

  const nameStrength = scoreHotelNameStrength(name);
  reasons.push(`name_strength_${nameStrength.toFixed(2)}`);

  let confidence = 0.4;
  confidence += nameStrength * 0.3;
  confidence += (Number(cityResult.confidence) || 0) * 0.35;

  if (cityResult.method === "explicit") {
    confidence += 0.05;
    reasons.push("explicit_city");
  }
  if (cityResult.method === "cvent_url_and_name_agree") {
    confidence += 0.06;
    reasons.push("city_url_name_agree");
  }
  if (cityResult.known_city) {
    confidence += 0.03;
    reasons.push("known_city_alias");
  }
  if (cityResult.multi_city) {
    confidence -= 0.18;
  }

  // Soft duplicate pressure: only when NEW still has near-dupe census hits.
  // Do NOT penalize merely because the scorer returned 3 weak pool rows.
  const candidates = resolved.candidate_matches || [];
  const nearDupes = candidates.filter((c) => {
    const score = Number(c.match_score);
    const status = String(c.match_status || "").toLowerCase();
    return (
      (Number.isFinite(score) && score >= 0.55) ||
      ["probable", "strong", "exact"].includes(status)
    );
  });
  if (nearDupes.length >= 2 && matchStatus === "new") {
    confidence -= 0.08;
    reasons.push("soft_duplicate_pressure");
  }

  if (matchStatus === "insufficient") {
    confidence = Math.min(confidence, 0.55);
    reasons.push("insufficient_identity_signals");
  }

  if (input.source_type === "independent_discovery") {
    confidence += 0.04;
    reasons.push("independent_verified_source");
  }

  confidence = Math.max(0, Math.min(0.99, Math.round(confidence * 1000) / 1000));

  let tier;
  let stage_status;
  let review_required = false;

  if (
    confidence >= TIER_THRESHOLDS.tier_a_min &&
    nameStrength >= 0.7 &&
    !cityResult.multi_city &&
    (Number(cityResult.confidence) || 0) >= 0.85 &&
    matchStatus === "new"
  ) {
    tier = DISCOVERY_TIER.A;
    stage_status = STAGE_STATUS.READY_FOR_IMPORT;
  } else if (confidence >= TIER_THRESHOLDS.tier_b_min && matchStatus === "new") {
    tier = DISCOVERY_TIER.B;
    stage_status = STAGE_STATUS.REVIEW_REQUIRED;
    review_required = true;
  } else if (confidence >= TIER_THRESHOLDS.tier_b_min) {
    tier = DISCOVERY_TIER.B;
    stage_status = STAGE_STATUS.REVIEW_REQUIRED;
    review_required = true;
  } else {
    tier = DISCOVERY_TIER.C;
    stage_status = STAGE_STATUS.REJECTED;
  }

  return {
    identity_confidence: confidence,
    name_strength: nameStrength,
    city_confidence: Number(cityResult.confidence) || 0,
    tier,
    stage_status,
    reasons,
    review_required,
  };
}
