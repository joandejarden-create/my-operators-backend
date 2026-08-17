/**
 * Operator Intelligence — Market Presence types & geographic eligibility helpers.
 * Country lists alone are not sufficient for Fit eligibility.
 */

export const MARKET_PRESENCE_TYPE = Object.freeze({
  CURRENT_MANAGED_PROPERTY: "Current Managed Property",
  CURRENT_OPERATING_PORTFOLIO: "Current Operating Portfolio",
  REGIONAL_OFFICE_OR_TEAM: "Regional Office or Team",
  ACTIVE_DEVELOPMENT: "Active Development",
  HISTORICAL_PRESENCE: "Historical Presence",
  STRATEGIC_INTEREST: "Strategic Interest",
  CLAIMED_CAPABILITY: "Claimed Capability",
  UNKNOWN: "Unknown",
});

/** Establishes current geographic eligibility for a country. */
export const STRONG_GEOGRAPHIC_SUPPORT = Object.freeze([
  MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY,
  MARKET_PRESENCE_TYPE.CURRENT_OPERATING_PORTFOLIO,
  MARKET_PRESENCE_TYPE.REGIONAL_OFFICE_OR_TEAM,
]);

/** May support Conditional eligibility depending on project context. */
export const CONDITIONAL_GEOGRAPHIC_SUPPORT = Object.freeze([
  MARKET_PRESENCE_TYPE.ACTIVE_DEVELOPMENT,
]);

/** Must never establish current geographic eligibility alone. */
export const NON_ELIGIBLE_PRESENCE = Object.freeze([
  MARKET_PRESENCE_TYPE.HISTORICAL_PRESENCE,
  MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST,
  MARKET_PRESENCE_TYPE.CLAIMED_CAPABILITY,
  MARKET_PRESENCE_TYPE.UNKNOWN,
]);

export function normalizePresenceType(value) {
  const s = String(value || "").trim();
  if (!s) return MARKET_PRESENCE_TYPE.UNKNOWN;
  for (const v of Object.values(MARKET_PRESENCE_TYPE)) {
    if (v.toLowerCase() === s.toLowerCase()) return v;
  }
  if (/current managed/i.test(s)) return MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY;
  if (/current operating|operating portfolio/i.test(s)) {
    return MARKET_PRESENCE_TYPE.CURRENT_OPERATING_PORTFOLIO;
  }
  if (/regional office|regional team/i.test(s)) return MARKET_PRESENCE_TYPE.REGIONAL_OFFICE_OR_TEAM;
  if (/active development|pipeline|under development/i.test(s)) {
    return MARKET_PRESENCE_TYPE.ACTIVE_DEVELOPMENT;
  }
  if (/historical/i.test(s)) return MARKET_PRESENCE_TYPE.HISTORICAL_PRESENCE;
  if (/strategic interest/i.test(s)) return MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST;
  if (/claimed capability/i.test(s)) return MARKET_PRESENCE_TYPE.CLAIMED_CAPABILITY;
  return MARKET_PRESENCE_TYPE.UNKNOWN;
}

export function isStrongGeographicSupport(presenceType) {
  return STRONG_GEOGRAPHIC_SUPPORT.includes(normalizePresenceType(presenceType));
}

export function isConditionalGeographicSupport(presenceType) {
  return CONDITIONAL_GEOGRAPHIC_SUPPORT.includes(normalizePresenceType(presenceType));
}

export function establishesCurrentGeographicEligibility(presenceType) {
  return isStrongGeographicSupport(presenceType);
}

/**
 * Countries with at least one strong presence record.
 * @param {Array<{ country?: string, presenceType?: string }>} records
 */
export function countriesWithStrongPresence(records = []) {
  const out = new Set();
  for (const r of records || []) {
    if (!r?.country) continue;
    if (establishesCurrentGeographicEligibility(r.presenceType)) out.add(String(r.country));
  }
  return [...out];
}

/**
 * Countries with only non-eligible presence (historical / strategic / claimed / unknown).
 */
export function countriesWithNonEligiblePresenceOnly(records = []) {
  const byCountry = new Map();
  for (const r of records || []) {
    if (!r?.country) continue;
    const c = String(r.country);
    if (!byCountry.has(c)) byCountry.set(c, []);
    byCountry.get(c).push(normalizePresenceType(r.presenceType));
  }
  const out = [];
  for (const [country, types] of byCountry) {
    const hasStrong = types.some((t) => establishesCurrentGeographicEligibility(t));
    const hasConditional = types.some((t) => isConditionalGeographicSupport(t));
    if (!hasStrong && !hasConditional && types.length) out.push(country);
  }
  return out;
}

/**
 * Evaluate geographic eligibility for a project country against presence records.
 * Prefer presence records over raw Active Countries when records exist.
 *
 * @returns {{
 *   status: 'match'|'conditional'|'conflict'|'unknown_operator'|'unknown_project',
 *   matchingTypes: string[],
 *   reasons: string[],
 *   conditions: string[],
 *   hardConflicts: string[],
 *   unknowns: string[],
 *   usedPresenceRecords: boolean,
 * }}
 */
export function evaluateGeographicEligibilityFromPresence(projectCountry, records = [], fallbackCountries = []) {
  const reasons = [];
  const conditions = [];
  const hardConflicts = [];
  const unknowns = [];
  const country = String(projectCountry || "").trim();
  if (!country) {
    return {
      status: "unknown_project",
      matchingTypes: [],
      reasons,
      conditions: ["Complete project geography to tighten eligibility."],
      hardConflicts,
      unknowns: ["Project country is unknown."],
      usedPresenceRecords: false,
    };
  }

  const rows = (records || []).filter(
    (r) =>
      r?.country &&
      (String(r.country).toLowerCase() === country.toLowerCase() ||
        String(r.country).toLowerCase().includes(country.toLowerCase()) ||
        country.toLowerCase().includes(String(r.country).toLowerCase()))
  );

  if (rows.length) {
    const types = rows.map((r) => normalizePresenceType(r.presenceType));
    const strong = types.filter((t) => establishesCurrentGeographicEligibility(t));
    const conditional = types.filter((t) => isConditionalGeographicSupport(t));
    const nonEligible = types.filter((t) => NON_ELIGIBLE_PRESENCE.includes(t));

    if (strong.length) {
      reasons.push(
        `Current geographic support in ${country} (${[...new Set(strong)].join("; ")}).`
      );
      return {
        status: "match",
        matchingTypes: [...new Set(strong)],
        reasons,
        conditions,
        hardConflicts,
        unknowns,
        usedPresenceRecords: true,
      };
    }
    if (conditional.length) {
      conditions.push(
        `Active Development documented in ${country} — not equivalent to current operating presence; validate before ranking.`
      );
      return {
        status: "conditional",
        matchingTypes: [...new Set(conditional)],
        reasons,
        conditions,
        hardConflicts,
        unknowns,
        usedPresenceRecords: true,
      };
    }
    if (nonEligible.length) {
      conditions.push(
        `${country} presence is documented as ${[...new Set(nonEligible)].join(" / ")} — does not establish current geographic eligibility.`
      );
      conditions.push(
        `Do not treat Strategic Interest, Historical Presence, or Claimed Capability as current operating presence in ${country}.`
      );
      return {
        status: "conditional",
        matchingTypes: [...new Set(nonEligible)],
        reasons,
        conditions,
        hardConflicts,
        unknowns,
        usedPresenceRecords: true,
      };
    }
  }

  // Fallback: legacy Active Countries only when no presence records exist for operator at all
  const anyRecords = (records || []).length > 0;
  if (anyRecords) {
    // Presence model exists but no row for this country
    hardConflicts.push(
      `No documented Market Presence for project country (${country}).`
    );
    return {
      status: "conflict",
      matchingTypes: [],
      reasons,
      conditions,
      hardConflicts,
      unknowns,
      usedPresenceRecords: true,
    };
  }

  const fb = (fallbackCountries || []).map(String);
  if (!fb.length) {
    unknowns.push("Operator geographic coverage is unknown.");
    conditions.push("Validate geographic eligibility during outreach.");
    return {
      status: "unknown_operator",
      matchingTypes: [],
      reasons,
      conditions,
      hardConflicts,
      unknowns,
      usedPresenceRecords: false,
    };
  }
  const hit = fb.some(
    (x) =>
      x.toLowerCase() === country.toLowerCase() ||
      x.toLowerCase().includes(country.toLowerCase()) ||
      country.toLowerCase().includes(x.toLowerCase())
  );
  if (hit) {
    reasons.push(`Geographic presence includes project country (${country}) via Active Countries fallback.`);
    conditions.push(
      "Market Presence records missing — Active Countries used as interim fallback; migrate to presence types."
    );
    return {
      status: "match",
      matchingTypes: [],
      reasons,
      conditions,
      hardConflicts,
      unknowns,
      usedPresenceRecords: false,
    };
  }
  hardConflicts.push(`No documented active country overlap with project country (${country}).`);
  return {
    status: "conflict",
    matchingTypes: [],
    reasons,
    conditions,
    hardConflicts,
    unknowns,
    usedPresenceRecords: false,
  };
}

export const MARKET_PRESENCE_TABLE = "Operator Intelligence - Market Presence";

export const map_marketPresenceFields = Object.freeze({
  operator: "Operator",
  country: "Country",
  region: "Region",
  presenceType: "Market Presence Type",
  currentOrHistorical: "Current / Historical",
  effectiveDate: "Effective Date",
  verificationDate: "Verification Date",
  sourceUrls: "Source URLs",
  claimId: "Claim ID",
  evidenceClass: "Evidence Class",
  publicationStatus: "Publication Status",
  confidence: "Confidence",
  notes: "Notes",
  limitations: "Limitations",
});
