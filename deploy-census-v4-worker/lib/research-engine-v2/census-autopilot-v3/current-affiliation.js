/**
 * Current Affiliation gate + Current Brand semantic validation (Census Autopilot V3/V4).
 *
 * Permanent separation:
 *   Physical Property Identity ≠ Current Brand ≠ Parent Company ≠ Collection ≠
 *   Distribution Platform ≠ Historical Brand ≠ Operator
 *
 * Auto-write Current Brand only when:
 *   CURRENT_AFFILIATION_CONFIRMED + Exact/High physical identity.
 *
 * Never default Current Brand from source-adapter family / parent company.
 */

import { createAffiliationPeriod } from "../clean-census/temporal-affiliation.js";

export const CURRENT_AFFILIATION_GATE_VERSION = "current-affiliation-gate-v1";

export const AFFILIATION_STATUS = Object.freeze({
  CONFIRMED: "CURRENT_AFFILIATION_CONFIRMED",
  PROBABLE: "CURRENT_AFFILIATION_PROBABLE",
  CONFLICT: "CURRENT_AFFILIATION_CONFLICT",
  UNKNOWN: "CURRENT_AFFILIATION_UNKNOWN",
});

export const CONTRADICTION_CLASS = Object.freeze({
  CONFIRMED: "CONFIRMED",
  REFLAGGED: "REFLAGGED",
  RENAMED_SAME_BRAND: "RENAMED_SAME_BRAND",
  COLLECTION_CHANGE: "COLLECTION_CHANGE",
  PARENT_CHANGE: "PARENT_CHANGE",
  CONFLICT: "CONFLICT",
  UNKNOWN: "UNKNOWN",
});

export const BRAND_CORRECTION_CLASS = Object.freeze({
  SAFE_BRAND_CORRECTION: "SAFE_BRAND_CORRECTION",
  SAFE_PARENT_CORRECTION: "SAFE_PARENT_CORRECTION",
  REFLAG_REQUIRES_TEMPORAL_UPDATE: "REFLAG_REQUIRES_TEMPORAL_UPDATE",
  STEWARD_REVIEW: "STEWARD_REVIEW",
  NO_CHANGE: "NO_CHANGE",
});

/**
 * Parent / platform labels that must NEVER auto-populate Current Brand
 * (unless a future registry entry explicitly marks same-as-brand — none today).
 */
export const PARENT_COMPANY_NEVER_CURRENT_BRAND = Object.freeze([
  "Choice",
  "Choice Hotels",
  "Choice Hotels International",
  "Choice Hotels International, Inc.",
  "Choice Hotels International, Inc",
  "Marriott International",
  "Marriott Bonvoy",
  "Hilton Worldwide",
  "Hilton",
  "IHG",
  "InterContinental Hotels Group",
  "IHG Hotels & Resorts",
  "Hyatt",
  "Hyatt Hotels",
  "Hyatt Hotels Corporation",
  "Accor",
  "Accor Hotels",
  "Accor S.A.",
  "Wyndham",
  "Wyndham Hotels & Resorts",
  "Wyndham Hotels and Resorts",
  "Minor",
  "Minor Hotels",
  "Minor International",
  "Radisson Hotel Group",
  "Radisson Hotels",
]);

/** Source-family / adapter tokens that are parents, not hotel brands. */
export const SOURCE_FAMILY_NEVER_CURRENT_BRAND = Object.freeze([
  "Choice",
  "Marriott",
  "Hilton",
  "IHG",
  "Hyatt",
  "Accor",
  "Wyndham",
  "Minor",
  "Preferred",
]);

/**
 * Choice URL brand-slug → canonical hotel-level brand.
 * Property URL pattern: choicehotels.com/{geo}/{city}/{brand-slug}-hotels/{id}
 */
export const CHOICE_URL_BRAND_SLUG_MAP = Object.freeze({
  "sleep-inn": "Sleep Inn",
  comfort: "Comfort",
  "comfort-inn": "Comfort Inn",
  "comfort-suites": "Comfort Suites",
  quality: "Quality Inn",
  "quality-inn": "Quality Inn",
  clarion: "Clarion",
  cambria: "Cambria",
  ascend: "Ascend Hotel Collection",
  "country-inn": "Country Inn & Suites",
  "country-inn-suites": "Country Inn & Suites",
  radisson: "Radisson",
  "radisson-blu": "Radisson Blu",
  "radisson-red": "Radisson RED",
  "radisson-individuals": "Radisson Individuals",
  park: "Park Inn",
  "park-inn": "Park Inn",
  "park-plaza": "Park Plaza",
  "econo-lodge": "Econo Lodge",
  "rodeway-inn": "Rodeway Inn",
  "mainstay-suites": "MainStay Suites",
  suburban: "Suburban Studios",
  "woodspring-suites": "WoodSpring Suites",
});

/**
 * Canonical brand registry seed for affiliation / parent separation (Census Autopilot).
 * Not a static truth table for every property — property evidence still wins.
 */
export const BRAND_NORMALIZATION_REGISTRY_SEED = Object.freeze([
  {
    canonical_brand_id: "sleep-inn",
    canonical_name: "Sleep Inn",
    aliases: ["Sleep Inn & Suites"],
    parent_company: "Choice Hotels International",
    collection_status: "hard_brand",
    regional_structure: "choice_global",
    active_date_range: null,
  },
  {
    canonical_brand_id: "comfort",
    canonical_name: "Comfort",
    aliases: ["Comfort Inn", "Comfort Suites", "Comfort Inn & Suites"],
    parent_company: "Choice Hotels International",
    collection_status: "hard_brand_family",
    regional_structure: "choice_global",
    active_date_range: null,
  },
  {
    canonical_brand_id: "quality-inn",
    canonical_name: "Quality Inn",
    aliases: ["Quality", "Quality Inn & Suites"],
    parent_company: "Choice Hotels International",
    collection_status: "hard_brand",
    regional_structure: "choice_global",
    active_date_range: null,
  },
  {
    canonical_brand_id: "cambria",
    canonical_name: "Cambria",
    aliases: ["Cambria Hotels", "Cambria Hotel"],
    parent_company: "Choice Hotels International",
    collection_status: "hard_brand",
    regional_structure: "choice_global",
    active_date_range: null,
  },
  {
    canonical_brand_id: "ascend-hotel-collection",
    canonical_name: "Ascend Hotel Collection",
    aliases: ["Ascend", "Ascend Collection"],
    parent_company: "Choice Hotels International",
    collection_status: "soft_collection",
    regional_structure: "choice_global",
    active_date_range: null,
  },
  {
    canonical_brand_id: "country-inn-suites",
    canonical_name: "Country Inn & Suites",
    aliases: ["Country Inn & Suites by Radisson", "Country Inn"],
    parent_company: "Choice Hotels International",
    collection_status: "hard_brand",
    regional_structure: "radisson_americas_under_choice",
    active_date_range: null,
  },
  {
    canonical_brand_id: "radisson",
    canonical_name: "Radisson",
    aliases: ["Radisson Hotels"],
    parent_company: "Choice Hotels International",
    collection_status: "hard_brand",
    regional_structure: "radisson_americas_under_choice",
    active_date_range: { note: "Americas affiliation under Choice where evidence indicates" },
  },
  {
    canonical_brand_id: "radisson-blu",
    canonical_name: "Radisson Blu",
    aliases: [],
    parent_company: "Choice Hotels International",
    collection_status: "hard_brand",
    regional_structure: "radisson_americas_under_choice",
    active_date_range: null,
  },
  {
    canonical_brand_id: "radisson-individuals",
    canonical_name: "Radisson Individuals",
    aliases: [
      "Radisson Individuals Americas",
      "Radisson Individuals by Choice",
      "a member of Radisson Individuals",
    ],
    parent_company: "Choice Hotels International",
    collection_status: "soft_collection",
    regional_structure: "radisson_individuals_americas_under_choice",
    active_date_range: null,
  },
  {
    canonical_brand_id: "clarion",
    canonical_name: "Clarion",
    aliases: ["Clarion Hotel", "Clarion Inn"],
    parent_company: "Choice Hotels International",
    collection_status: "hard_brand",
    regional_structure: "choice_global",
    active_date_range: null,
  },
]);

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

const PARENT_NEVER_SET = new Set(PARENT_COMPANY_NEVER_CURRENT_BRAND.map(norm));
const FAMILY_NEVER_SET = new Set(SOURCE_FAMILY_NEVER_CURRENT_BRAND.map(norm));

/**
 * True when value is a parent/platform label, not a hotel-level Current Brand.
 */
export function isParentCompanyAsCurrentBrand(value) {
  const n = norm(value);
  if (!n) return false;
  if (PARENT_NEVER_SET.has(n)) return true;
  if (FAMILY_NEVER_SET.has(n)) return true;
  return false;
}

/**
 * Reject Current Brand values that are semantically not hotel brands.
 */
export function validateCurrentBrandSemantics(value, opts = {}) {
  const raw = String(value || "").trim();
  if (!raw) {
    return {
      ok: false,
      reason: "blank",
      code: "CURRENT_BRAND_BLANK",
    };
  }
  if (isParentCompanyAsCurrentBrand(raw)) {
    return {
      ok: false,
      reason: "parent_company_or_source_family_as_brand",
      code: "CURRENT_BRAND_EQUALS_PARENT_COMPANY",
      value: raw,
    };
  }
  if (/^(hotel|hotels|unknown|n\/?a|independent\s*\/\s*unknown)$/i.test(raw)) {
    return {
      ok: false,
      reason: "generic_or_unknown_label",
      code: "CURRENT_BRAND_GENERIC",
      value: raw,
    };
  }
  if (opts.reject_operator_labels && /^(managed by|operator)/i.test(raw)) {
    return {
      ok: false,
      reason: "operator_label",
      code: "CURRENT_BRAND_OPERATOR",
      value: raw,
    };
  }
  return { ok: true, value: raw };
}

/**
 * Infer Choice hotel-level brand from official property URL brand slug.
 * Does not invent brand from hostname alone (choicehotels.com ≠ brand).
 */
export function inferChoiceBrandFromOfficialPropertyUrl(url) {
  const u = String(url || "");
  if (!/choicehotels\.com/i.test(u)) return null;
  if (/regional-hotels/i.test(u)) return null;
  const m = u.match(/\/([a-z0-9-]+)-hotels\/[a-z]{2}\d+/i);
  if (!m) return null;
  const slug = m[1].toLowerCase();
  if (CHOICE_URL_BRAND_SLUG_MAP[slug]) return CHOICE_URL_BRAND_SLUG_MAP[slug];
  // Title-case unknown Choice brand slug (still property-level evidence from URL structure)
  return slug
    .split("-")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

/**
 * Resolve property-level Current Brand candidate without parent/family default.
 *
 * @param {object} input
 * @returns {{ brand: string|null, parent_company: string|null, evidence: object, gate: string }}
 */
export function resolvePropertyLevelCurrentBrand(input = {}) {
  const identityOk =
    input.identity_confidence === "Exact" ||
    input.identity_confidence === "High" ||
    input.match_class === "NEW_INSERT" ||
    input.match_class === "EXACT_EXISTING_MATCH";

  const parent =
    String(input.parent_company || input.brand_family || "").trim() || null;
  const family = String(input.source_family || input.family || "").trim() || null;

  const candidates = [];

  if (input.explicit_brand) {
    candidates.push({
      value: String(input.explicit_brand).trim(),
      source_rank: 1,
      source: "explicit_property_brand",
      source_url: input.official_property_url || input.source_url || null,
    });
  }
  if (input.directory_brand) {
    candidates.push({
      value: String(input.directory_brand).trim(),
      source_rank: 2,
      source: "official_brand_directory_property_record",
      source_url: input.official_property_url || input.source_url || null,
    });
  }
  const urlBrand = inferChoiceBrandFromOfficialPropertyUrl(
    input.official_property_url || input.source_url
  );
  if (urlBrand) {
    candidates.push({
      value: urlBrand,
      source_rank: 2,
      source: "official_choice_property_url_brand_slug",
      source_url: input.official_property_url || input.source_url || null,
    });
  }
  if (input.hotel_website_brand) {
    candidates.push({
      value: String(input.hotel_website_brand).trim(),
      source_rank: 4,
      source: "official_hotel_website",
      source_url: input.hotel_website_url || null,
    });
  }

  // Strip parent/family contamination from candidates
  const clean = candidates
    .map((c) => ({ ...c, semantic: validateCurrentBrandSemantics(c.value) }))
    .filter((c) => c.semantic.ok);

  // Explicit conflict: multiple distinct clean brands
  const distinct = [...new Set(clean.map((c) => norm(c.value)))];
  if (distinct.length > 1) {
    return {
      brand: null,
      parent_company: parent || (family && !isParentCompanyAsCurrentBrand(family) ? null : family),
      collection: input.collection || null,
      distribution_platform: input.distribution_platform || null,
      regional_operating_structure: input.regional_operating_structure || null,
      gate: AFFILIATION_STATUS.CONFLICT,
      evidence: { candidates: clean, reason: "multiple_current_brand_claims" },
      auto_write_allowed: false,
    };
  }

  const best = clean.sort((a, b) => a.source_rank - b.source_rank)[0] || null;

  // Hard ban: never promote family/parent into brand
  if (
    !best &&
    (input.fallback_brand_from_family === true ||
      isParentCompanyAsCurrentBrand(input.brand) ||
      isParentCompanyAsCurrentBrand(family))
  ) {
    return {
      brand: null,
      parent_company: parent || (isParentCompanyAsCurrentBrand(family) ? expandParentLabel(family) : family),
      collection: input.collection || null,
      distribution_platform: input.distribution_platform || null,
      regional_operating_structure: input.regional_operating_structure || null,
      gate: AFFILIATION_STATUS.UNKNOWN,
      evidence: {
        candidates: [],
        blocked_family_default: family || parent,
        reason: "no_property_level_brand_evidence",
      },
      auto_write_allowed: false,
    };
  }

  if (!best) {
    return {
      brand: null,
      parent_company: parent || expandParentLabel(family),
      collection: input.collection || null,
      distribution_platform: input.distribution_platform || null,
      regional_operating_structure: input.regional_operating_structure || null,
      gate: AFFILIATION_STATUS.UNKNOWN,
      evidence: { candidates: [], reason: "no_brand_candidate" },
      auto_write_allowed: false,
    };
  }

  const gate =
    best.source_rank <= 2 && identityOk
      ? AFFILIATION_STATUS.CONFIRMED
      : best.source_rank <= 4 && identityOk
        ? AFFILIATION_STATUS.PROBABLE
        : AFFILIATION_STATUS.UNKNOWN;

  const auto_write_allowed = gate === AFFILIATION_STATUS.CONFIRMED && identityOk;

  return {
    brand: best.value,
    parent_company: parent || expandParentLabel(family),
    collection: input.collection || null,
    distribution_platform: input.distribution_platform || null,
    regional_operating_structure: input.regional_operating_structure || null,
    gate,
    evidence: { selected: best, candidates: clean },
    auto_write_allowed,
  };
}

function expandParentLabel(family) {
  const f = norm(family);
  if (f === "choice") return "Choice Hotels International";
  if (f === "marriott") return "Marriott International";
  if (f === "hilton") return "Hilton";
  if (f === "ihg") return "IHG";
  if (f === "hyatt") return "Hyatt";
  if (f === "accor") return "Accor";
  if (f === "wyndham") return "Wyndham Hotels & Resorts";
  if (f === "minor") return "Minor Hotels";
  return family || null;
}

/**
 * Gate check for production Current Brand write.
 */
export function evaluateCurrentAffiliationGate(args = {}) {
  const resolved = resolvePropertyLevelCurrentBrand(args);
  const identity =
    args.identity_confidence === "Exact" ||
    args.identity_confidence === "High" ||
    args.match_class === "NEW_INSERT" ||
    args.match_class === "EXACT_EXISTING_MATCH";

  if (args.match_confidence === "Medium" || args.match_class === "PROBABLE_MATCH") {
    return {
      ...resolved,
      gate: AFFILIATION_STATUS.PROBABLE,
      auto_write_allowed: false,
      write_policy: "staging_review_only",
      reason: "medium_identity_match_cannot_write_current_brand",
    };
  }
  if (args.match_confidence === "Low" || args.match_class === "LOW_MATCH") {
    return {
      ...resolved,
      gate: AFFILIATION_STATUS.UNKNOWN,
      auto_write_allowed: false,
      write_policy: "reject",
      reason: "low_identity_match_rejects_brand_write",
    };
  }
  if (!identity) {
    return {
      ...resolved,
      auto_write_allowed: false,
      write_policy: "reject",
      reason: "identity_not_exact_or_high",
    };
  }
  if (resolved.gate === AFFILIATION_STATUS.CONFLICT) {
    return {
      ...resolved,
      auto_write_allowed: false,
      write_policy: "no_automatic_current_brand_write",
    };
  }
  if (resolved.gate === AFFILIATION_STATUS.CONFIRMED && resolved.brand) {
    return {
      ...resolved,
      auto_write_allowed: true,
      write_policy: "auto_write_current_brand",
    };
  }
  if (resolved.gate === AFFILIATION_STATUS.PROBABLE) {
    return {
      ...resolved,
      auto_write_allowed: false,
      write_policy: "staging_review_only",
    };
  }
  return {
    ...resolved,
    auto_write_allowed: false,
    write_policy: "leave_blank",
  };
}

/**
 * Canonical affiliation claim shape (current vs historical).
 */
export function buildAffiliationClaim(input = {}) {
  const period = createAffiliationPeriod({
    brand: input.affiliation_brand || input.brand || null,
    parent: input.parent_company || input.parent || null,
    affiliation_start: input.valid_from || null,
    affiliation_end: input.valid_to || null,
    current: input.current_flag !== false,
    evidence: input.source ? [{ source: input.source, url: input.source_url || null }] : [],
    evidence_date: input.verified_at || null,
    confidence: input.confidence || "Medium",
  });
  return {
    affiliation_brand: period.brand,
    valid_from: period.affiliation_start,
    valid_to: period.affiliation_end,
    source: input.source || null,
    verified_at: input.verified_at || null,
    current_flag: period.current === true,
    confidence: period.confidence,
    parent_company: period.parent,
    collection: input.collection || null,
    distribution_platform: input.distribution_platform || null,
    regional_operating_structure: input.regional_operating_structure || null,
    period,
  };
}

/**
 * Hard regression: source adapter family must not become Current Brand.
 */
export function assertNoFamilyDefaultCurrentBrand(sourceFamily, currentBrand) {
  const family = String(sourceFamily || "").trim();
  const brand = String(currentBrand || "").trim();
  if (!family || !brand) {
    return { pass: true, reason: "blank_ok" };
  }
  if (isParentCompanyAsCurrentBrand(brand) && norm(brand) === norm(family)) {
    return {
      pass: false,
      reason: "source_family_defaulted_to_current_brand",
      source_family: family,
      current_brand: brand,
    };
  }
  if (isParentCompanyAsCurrentBrand(brand)) {
    return {
      pass: false,
      reason: "parent_company_used_as_current_brand",
      source_family: family,
      current_brand: brand,
    };
  }
  return { pass: true, source_family: family, current_brand: brand };
}

/**
 * Cross-family parent ≠ brand regression matrix.
 */
export function runParentVsBrandRegressionMatrix() {
  const families = [
    "Choice",
    "IHG",
    "Hilton",
    "Marriott",
    "Hyatt",
    "Accor",
    "Wyndham",
    "Minor",
  ];
  const cases = [];
  for (const f of families) {
    const bad = assertNoFamilyDefaultCurrentBrand(f, f);
    const badParent = assertNoFamilyDefaultCurrentBrand(
      f,
      expandParentLabel(f)
    );
    const okExample =
      f === "Choice"
        ? assertNoFamilyDefaultCurrentBrand(f, "Sleep Inn")
        : assertNoFamilyDefaultCurrentBrand(f, `${f} Property Brand Placeholder`);
    cases.push({
      family: f,
      family_as_brand_blocked: bad.pass === false,
      expanded_parent_as_brand_blocked: badParent.pass === false,
      property_level_brand_allowed:
        f === "Choice" ? okExample.pass === true : true,
    });
  }
  const pass = cases.every(
    (c) => c.family_as_brand_blocked && c.expanded_parent_as_brand_blocked
  );
  return { pass, cases, version: CURRENT_AFFILIATION_GATE_VERSION };
}

/**
 * Classify a proposed correction vs production Current Brand (dry-run only).
 */
export function classifyBrandCorrection(productionBrand, bestClaim, opts = {}) {
  const prod = String(productionBrand || "").trim();
  const best = String(bestClaim || "").trim();
  if (!best && !prod) return { class: BRAND_CORRECTION_CLASS.NO_CHANGE };
  if (!best && prod && isParentCompanyAsCurrentBrand(prod)) {
    return {
      class: BRAND_CORRECTION_CLASS.STEWARD_REVIEW,
      reason: "parent_as_brand_without_property_level_claim",
    };
  }
  if (best && !prod) {
    return {
      class: BRAND_CORRECTION_CLASS.SAFE_BRAND_CORRECTION,
      reason: "blank_fill_with_confirmed_property_brand",
      proposed: best,
    };
  }
  if (best && isParentCompanyAsCurrentBrand(prod) && !isParentCompanyAsCurrentBrand(best)) {
    return {
      class: BRAND_CORRECTION_CLASS.SAFE_BRAND_CORRECTION,
      reason: "replace_parent_contamination_with_property_brand",
      proposed: best,
      before: prod,
    };
  }
  if (best && prod && norm(best) !== norm(prod) && opts.reflag_suspected) {
    return {
      class: BRAND_CORRECTION_CLASS.REFLAG_REQUIRES_TEMPORAL_UPDATE,
      reason: "possible_reflag",
      proposed: best,
      before: prod,
    };
  }
  if (best && prod && norm(best) !== norm(prod)) {
    return {
      class: BRAND_CORRECTION_CLASS.STEWARD_REVIEW,
      reason: "populated_value_differs_requires_stronger_evidence",
      proposed: best,
      before: prod,
    };
  }
  return { class: BRAND_CORRECTION_CLASS.NO_CHANGE, before: prod, proposed: best || prod };
}

export function lookupBrandRegistry(name) {
  const n = norm(name);
  if (!n) return null;
  for (const row of BRAND_NORMALIZATION_REGISTRY_SEED) {
    if (norm(row.canonical_name) === n) return row;
    if ((row.aliases || []).some((a) => norm(a) === n)) return row;
  }
  return null;
}
