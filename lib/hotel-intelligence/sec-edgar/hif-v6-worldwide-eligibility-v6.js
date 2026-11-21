/**
 * HIF V6 — Permanent worldwide discovery eligibility (founder 2026-11-21).
 *
 * HIF is a worldwide hotel development opportunity discovery platform.
 * US / MX / BR / GB / CA are initial priority markets for commercial critical mass.
 * They are NOT geographic restrictions on research, discovery, qualification, or storage.
 *
 * Country concentration controls research prioritization, not research eligibility.
 * Physical hotel country verification is a data-quality requirement — never an allowlist.
 */

import {
  PRIORITY_COUNTRIES_V6,
  COUNTRY_PROSPECT_AMBITIONS_V6,
  COUNTRY_CONCENTRATION_RULES_V6,
} from "./hif-v6-country-concentration-v6.js";

export const HIF_V6_WORLDWIDE_ELIGIBILITY = "hif-v6-worldwide-eligibility-v6";

export const WORLDWIDE_ELIGIBILITY_POLICY_V6 = Object.freeze({
  platformScope: "WORLDWIDE",
  geographicEligibilityExclusions: "NONE",
  countryAllowlists: "PROHIBITED",
  countryRejectionGates: "PROHIBITED",
  countrySpecificOpportunityCeilings: "PROHIBITED",
  priorityMarketsControl: "PRIORITIZATION_ONLY_NOT_ELIGIBILITY",
  physicalCountryVerification: "DATA_QUALITY_REQUIRED_NOT_RESTRICTION",
  preserveIncidentalNonPriorityFinds: true,
  internationalIssuersWithForeignPhysicalHotels: "ELIGIBLE",
  freePermittedResearchExpansion: "NO_NEW_COUNTRY_AUTHORIZATION_REQUIRED",
  milestoneCeilings: "NONE",
  progressMilestones: Object.freeze([1000, 2500, 5000, 10000]),
  priorityMarkets: PRIORITY_COUNTRIES_V6,
  concentrationRules: COUNTRY_CONCENTRATION_RULES_V6,
});

/** Always true for any non-empty country code — eligibility is universal. */
export function isCountryEligibleForDiscovery(_countryCode) {
  return true;
}

/** Priority market = concentration / depth priority, not eligibility. */
export function isPriorityMarket(countryCode) {
  const c = String(countryCode || "").toUpperCase();
  if (c === "UK") return true; // GB alias
  return PRIORITY_COUNTRIES_V6.includes(c);
}

/**
 * Never quarantine or discard solely because the hotel is outside the five priority markets.
 * Returns null when country is not a valid rejection reason.
 */
export function countryBasedRejectionReason(_countryCode) {
  return null;
}

export function shouldDiscardForNonPriorityCountry(_countryCode) {
  return false;
}

export function normalizeHotelCountryCode(countryCode) {
  const c = String(countryCode || "")
    .trim()
    .toUpperCase();
  if (!c) return "UNATTRIBUTED";
  if (c === "UK" || c === "UNITED KINGDOM") return "GB";
  if (c === "USA" || c === "UNITED STATES") return "US";
  if (c === "MEXICO") return "MX";
  if (c === "BRAZIL" || c === "BRASIL") return "BR";
  if (c === "CANADA") return "CA";
  return c.length <= 3 ? c : c.slice(0, 3);
}

/**
 * Dual scorecard views required by permanent founder directive.
 */
export function buildDualGeographyViews(byCountry = {}) {
  const allCountries = {};
  for (const [raw, cell] of Object.entries(byCountry || {})) {
    const code = normalizeHotelCountryCode(raw);
    if (!allCountries[code]) {
      allCountries[code] = {
        countryCode: code,
        isPriorityMarket: isPriorityMarket(code),
        prospects: 0,
        hotelsResearched: 0,
        ...(typeof cell === "object" && cell ? cell : {}),
      };
    }
    const n =
      Number(cell?.distinctEvidenceBackedProspects) ||
      Number(cell?.prospects) ||
      Number(cell) ||
      0;
    const h = Number(cell?.distinctHotelsResearched) || Number(cell?.hotelsResearched) || 0;
    allCountries[code].prospects = (allCountries[code].prospects || 0) + (Number.isFinite(n) ? n : 0);
    allCountries[code].hotelsResearched =
      (allCountries[code].hotelsResearched || 0) + (Number.isFinite(h) ? h : 0);
    allCountries[code].isPriorityMarket = isPriorityMarket(code);
  }

  const globalCountries = Object.values(allCountries).sort((a, b) => b.prospects - a.prospects);
  const priority = {};
  for (const code of PRIORITY_COUNTRIES_V6) {
    const row = allCountries[code] || {
      countryCode: code,
      isPriorityMarket: true,
      prospects: 0,
      hotelsResearched: 0,
    };
    const ambition = COUNTRY_PROSPECT_AMBITIONS_V6[code];
    priority[code] = {
      ...row,
      planningAmbitionMin: ambition?.min ?? null,
      planningAmbitionMax: ambition?.max ?? null,
      countryLabel: ambition?.label || code,
    };
  }

  const otherCountries = globalCountries.filter((c) => !isPriorityMarket(c.countryCode));

  return {
    policy: WORLDWIDE_ELIGIBILITY_POLICY_V6,
    globalOpportunityInventory: {
      view: "GLOBAL_OPPORTUNITY_INVENTORY",
      description:
        "All countries, all qualified opportunities, global discovery yield — unlimited expansion",
      countryCount: globalCountries.length,
      totalProspects: globalCountries.reduce((s, c) => s + (c.prospects || 0), 0),
      byCountry: Object.fromEntries(globalCountries.map((c) => [c.countryCode, c])),
      otherThanPriorityMarkets: otherCountries,
    },
    priorityMarketPerformance: {
      view: "PRIORITY_MARKET_PERFORMANCE",
      description:
        "Enhanced coverage and commercial concentration for US, Mexico, Brazil, UK, Canada",
      priorityMarkets: PRIORITY_COUNTRIES_V6,
      byCountry: priority,
      totalPriorityProspects: Object.values(priority).reduce((s, c) => s + (c.prospects || 0), 0),
      note: "Priority depth does not exclude non-priority countries from the global inventory",
    },
  };
}

export function assertWorldwideEligibilityInvariants() {
  const violations = [];
  if (WORLDWIDE_ELIGIBILITY_POLICY_V6.geographicEligibilityExclusions !== "NONE") {
    violations.push("geographicEligibilityExclusions_must_be_NONE");
  }
  if (WORLDWIDE_ELIGIBILITY_POLICY_V6.countryAllowlists !== "PROHIBITED") {
    violations.push("countryAllowlists_must_be_PROHIBITED");
  }
  if (shouldDiscardForNonPriorityCountry("JP")) {
    violations.push("must_not_discard_japan");
  }
  if (shouldDiscardForNonPriorityCountry("ES")) {
    violations.push("must_not_discard_spain");
  }
  if (!isCountryEligibleForDiscovery("DO")) {
    violations.push("dominican_republic_must_be_eligible");
  }
  if (!isPriorityMarket("US") || isPriorityMarket("JP")) {
    violations.push("priority_market_set_incorrect");
  }
  if (countryBasedRejectionReason("AE") !== null) {
    violations.push("must_not_reject_uae_for_country");
  }
  return { ok: violations.length === 0, violations };
}
