/**
 * Shared helpers for Caribbean island countrywide radar builds.
 */

import { getPointTypeDefaults } from "../demand-anchors/point-type-defaults.js";
import { getCountryConfig } from "./country-configs.js";

function resolveRadarRegion(countryDisplay, fallback = "Caribbean") {
  return getCountryConfig(countryDisplay)?.region || fallback;
}

/** @type {Record<string, string[]>} */
export const ISLAND_POINT_TYPE_USE_CASE_TAGS = {
  "Convention Center": ["Group / Convention", "Urban / Corporate"],
  "Medical Campus": ["Medical / Education", "Urban / Corporate"],
  "University / College": ["Medical / Education", "Government / Institutional"],
  "Sports Venue": ["Group / Convention", "Resort / Leisure"],
  "Entertainment District": ["Resort / Leisure", "Mixed-Use / Growth"],
  "Tourist Attraction": ["Resort / Leisure", "Nature / Eco-Tourism"],
  "Beach / Waterfront": ["Resort / Leisure", "Cruise / Port"],
  "Business District": ["Urban / Corporate", "Mixed-Use / Growth"],
  "Industrial / Logistics Zone": ["Industrial / Logistics", "Airport / Transit"],
  "Government / Civic Center": ["Government / Institutional", "Urban / Corporate"],
  "Mixed-Use Development": ["Mixed-Use / Growth", "Resort / Leisure"],
  "Future Growth Node": ["Future Growth", "Airport / Transit"],
};

export const ISLAND_TIER_1_POINT_TYPES = new Set([
  "Convention Center",
  "Business District",
  "Tourist Attraction",
  "Mixed-Use Development",
  "Future Growth Node",
  "Beach / Waterfront",
]);

/**
 * @param {string} countryDisplay
 * @param {object} point
 * @param {object} [overrides]
 */
export function applyIslandGovernanceDefaults(countryDisplay, point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

  return {
    ...point,
    scopeLevel: overrides.scopeLevel || "Country",
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    useCaseTags,
    defaultMapVisibility: overrides.defaultMapVisibility || "Visible",
    externalVisibilityLevel: overrides.externalVisibilityLevel || "Member",
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      `${countryDisplay} countrywide build — ${submarket} ${pointType} anchor for hotel demand.`,
    dealSpecificNotes: overrides.dealSpecificNotes || "",
  };
}

/**
 * @param {string} countryDisplay
 * @param {string} region
 * @param {(point: object, overrides?: object) => object} applyGovernance
 */
export function createIslandCandidateBuilder(countryDisplay, region, applyGovernance) {
  return function pt(v) {
    const defaults = getPointTypeDefaults(v.pointType);
    const rationale =
      v.hotelDemandNote ||
      defaults.hotelDemandRationale ||
      `Supports identifiable hotel demand in this ${countryDisplay} corridor.`;
    const base = {
      name: v.name,
      pointType: v.pointType,
      city: v.city,
      country: countryDisplay,
      region,
      submarket: v.submarket,
      latitude: v.latitude,
      longitude: v.longitude,
      source: "Public Source",
      sourceReference: v.sourceReference,
      dataConfidence: v.dataConfidence || "Medium",
      notes:
        v.notes ||
        `Submarket: ${v.submarket}. ${rationale} Candidate pending Google pre-import verification.`,
    };
    if (v.googleSearchQuery) base.googleSearchQuery = v.googleSearchQuery;
    if (v.manuallyVerified) {
      base.notes = `${base.notes} Manually verified using official/public source; Google Maps match was not used as final authority.`;
      base.dataConfidence = v.dataConfidence || "High";
      base.manuallyVerified = true;
    }
    return applyGovernance(base, v.governance || {});
  };
}

/**
 * @param {string} countryDisplay
 * @param {string} marketLabel
 */
export function createIslandTiBuilder(countryDisplay, marketLabel) {
  const region = resolveRadarRegion(countryDisplay);
  return function ti(v) {
    return {
      name: v.name,
      pointType: v.pointType,
      pointSubtype: v.pointSubtype || "",
      city: v.city,
      country: countryDisplay,
      region,
      submarket: v.submarket || "Other",
      latitude: v.latitude,
      longitude: v.longitude,
      source: "Public Source",
      sourceReference: v.sourceReference,
      dataConfidence: v.dataConfidence || "High",
      demandRelevance: v.demandRelevance || "High",
      includeOnRadarMap: true,
      notes: v.notes || "",
      scopeLevel: "Country",
      relevanceTier: v.relevanceTier || "Tier 1",
      useCaseTags: v.useCaseTags || ["Airport / Transit", "Resort / Leisure"],
      defaultMapVisibility: "Visible",
      externalVisibilityLevel: "Member",
      projectRelevanceLogic: `${marketLabel} island TI build — ${v.name}.`,
    };
  };
}

export function buildIslandTiDeltaFixture(countryDisplay, marketLabel, records) {
  return {
    market: marketLabel,
    country: countryDisplay,
    region: resolveRadarRegion(countryDisplay),
    buildBatch: "delta",
    status: "verified_ready",
    generatedAt: new Date().toISOString(),
    verification: {
      method: "Source-backed Travel Infrastructure delta; no Google fields on points",
      verifiedAt: new Date().toISOString(),
      verifiedRecords: records.length,
      manuallyVerifiedRecords: records.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: `Airport, cruise, marina, ferry, and logistics gap fill for ${countryDisplay} countrywide pass.`,
    },
    corrections: [],
    summary: {
      totalPoints: records.length,
      byPointType: records.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: records.map((p) =>
      p.country === countryDisplay && p.region !== resolveRadarRegion(countryDisplay)
        ? { ...p, region: resolveRadarRegion(countryDisplay) }
        : p
    ),
  };
}

export const REVIEW_TAG = "[Google review correction applied]";

/**
 * @param {Record<string, object>} corrections
 */
export function createPlaceReviewApplier(corrections) {
  return function applyPlaceReviewCorrection(point) {
    const fix = corrections[point.name];
    if (!fix) return point;
    const merged = { ...point, ...fix };
    if (fix.manuallyVerified) {
      merged.manuallyVerified = true;
      merged.dataConfidence = fix.dataConfidence || "High";
    }
    merged.notes = `${point.notes || ""} ${REVIEW_TAG}`.trim();
    return merged;
  };
}
