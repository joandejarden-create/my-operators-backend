/**
 * DealSignal — CALA-first research-processing priority (V1).
 *
 * Operational queue preference only — NOT commercial classification bias.
 * Reuses Track C CALA geography helpers; hotel geography (not HQ) is authoritative.
 *
 * Queue order:
 *  1. Time-critical live opportunities globally
 *  2. High-value CALA owner decisions / early-engagement
 *  3. Mexico-specific when otherwise comparable
 *  4. Other CALA
 *  5. Remaining global
 */
import {
  CALA_COUNTRY_LABELS,
  isCalaCountry,
  normalizeCountry,
} from "../sec-edgar/hif-track-c-universe-expansion-v1.js";

export const DEALSIGNAL_CALA_PRIORITY_VERSION = "dealsignal-cala-research-priority-v1";

export const RESEARCH_QUEUE_TIER = Object.freeze({
  T1_TIME_CRITICAL_GLOBAL: "T1_TIME_CRITICAL_GLOBAL",
  T2_HIGH_VALUE_CALA: "T2_HIGH_VALUE_CALA",
  T3_MEXICO_COMPARABLE: "T3_MEXICO_COMPARABLE",
  T4_OTHER_CALA: "T4_OTHER_CALA",
  T5_REMAINING_GLOBAL: "T5_REMAINING_GLOBAL",
});

export const MEXICO_FIRST_WITHIN_CALA = true;

/** Preferential research attention — not eligibility. */
export const CALA_PRIORITY_BANDS = Object.freeze({
  A_MEXICO: Object.freeze(["Mexico"]),
  B_CARIBBEAN: Object.freeze([
    "Dominican Republic",
    "Jamaica",
    "Puerto Rico",
    "Aruba",
    "Curaçao",
    "Bahamas",
    "Barbados",
  ]),
  C_CENTRAL_AMERICA: Object.freeze([
    "Costa Rica",
    "Panama",
    "Guatemala",
    "Belize",
    "Honduras",
    "El Salvador",
    "Nicaragua",
  ]),
  D_SOUTH_AMERICA: Object.freeze([
    "Brazil",
    "Colombia",
    "Chile",
    "Peru",
    "Argentina",
    "Ecuador",
    "Uruguay",
    "Bolivia",
    "Paraguay",
  ]),
});

export const FAIRNESS_CONTROLS = Object.freeze({
  doNotHideNonCala: true,
  doNotDowngradeNonCalaCommercialStatus: true,
  doNotInflateCalaEvidenceStrength: true,
  doNotInflateCalaCommercialConfidence: true,
  urgencyOverridesGeography: true,
  classifyByHotelGeographyNotOwnerHq: true,
  portfolioParentsPreserveFootprint: true,
  sameEvidenceStandardsGlobally: true,
});

/**
 * @param {object} opp
 * @param {string} [opp.hotelCountry] property geography
 * @param {boolean} [opp.timeCritical]
 * @param {boolean} [opp.highValueCalaOwnerSignal]
 * @returns {string} RESEARCH_QUEUE_TIER value
 */
export function assignResearchQueueTier(opp = {}) {
  const country = normalizeCountry(opp.hotelCountry || opp.country || "");
  const timeCritical = Boolean(opp.timeCritical || opp.engagementPriority === "ENGAGE_NOW");
  if (timeCritical) return RESEARCH_QUEUE_TIER.T1_TIME_CRITICAL_GLOBAL;

  const cala = isCalaCountry(country);
  const mexico = country === "Mexico";
  const highValue = Boolean(opp.highValueCalaOwnerSignal || opp.earlyEngagement);

  if (cala && highValue) return RESEARCH_QUEUE_TIER.T2_HIGH_VALUE_CALA;
  if (mexico) return RESEARCH_QUEUE_TIER.T3_MEXICO_COMPARABLE;
  if (cala) return RESEARCH_QUEUE_TIER.T4_OTHER_CALA;
  return RESEARCH_QUEUE_TIER.T5_REMAINING_GLOBAL;
}

/**
 * Sort key: lower = process earlier. Does not change commercial status fields.
 */
export function researchQueueSortKey(opp = {}) {
  const tier = assignResearchQueueTier(opp);
  const order = {
    [RESEARCH_QUEUE_TIER.T1_TIME_CRITICAL_GLOBAL]: 1,
    [RESEARCH_QUEUE_TIER.T2_HIGH_VALUE_CALA]: 2,
    [RESEARCH_QUEUE_TIER.T3_MEXICO_COMPARABLE]: 3,
    [RESEARCH_QUEUE_TIER.T4_OTHER_CALA]: 4,
    [RESEARCH_QUEUE_TIER.T5_REMAINING_GLOBAL]: 5,
  };
  return order[tier] || 5;
}

export function sortOpportunitiesForResearch(opps = []) {
  return [...opps].sort((a, b) => {
    const d = researchQueueSortKey(a) - researchQueueSortKey(b);
    if (d !== 0) return d;
    // Within same tier, Mexico slightly ahead of other CALA when comparable
    const aMx = normalizeCountry(a.hotelCountry || a.country) === "Mexico" ? 0 : 1;
    const bMx = normalizeCountry(b.hotelCountry || b.country) === "Mexico" ? 0 : 1;
    return aMx - bMx;
  });
}

export function calaGeographyContract() {
  return {
    version: DEALSIGNAL_CALA_PRIORITY_VERSION,
    authoritativeGeography: "HOTEL_PROPERTY_NOT_OWNER_HQ",
    calaCountryLabels: CALA_COUNTRY_LABELS,
    priorityBands: CALA_PRIORITY_BANDS,
    queueTiers: RESEARCH_QUEUE_TIER,
    fairness: FAIRNESS_CONTROLS,
    mexicoFirstWithinCala: MEXICO_FIRST_WITHIN_CALA,
    metricsRequired: ["cala_processed", "mexico_processed", "non_cala_processed", "time_critical_overrides"],
  };
}

export { isCalaCountry, normalizeCountry, CALA_COUNTRY_LABELS };
