/**
 * Compact factory for growth signal records.
 */

import { getGrowthSignalTypeMeta } from "./growth-signal-types.js";

/**
 * @param {object} v
 */
export function growthSignal(v) {
  const meta = getGrowthSignalTypeMeta(v.signalType);
  return {
    signalType: v.signalType,
    direction: v.direction || "unknown",
    buildImplication: v.buildImplication || meta?.buildImplication || "",
    recommendedHotelTypes: v.recommendedHotelTypes || meta?.defaultHotelTypes || [],
    timeHorizon: v.timeHorizon || "",
    summary: v.summary || "",
    ownerBrandTakeaway: v.ownerBrandTakeaway || "",
    linkedAnchorNames: v.linkedAnchorNames || (v.linkedAnchorName ? [v.linkedAnchorName] : []),
    source: v.source || "Public Source",
    sourceReference: v.sourceReference || "",
    dataConfidence: v.dataConfidence || "Medium",
    lastReviewed: v.lastReviewed || "2026-06-24",
    id: v.id,
  };
}

/**
 * @param {string} country
 * @param {string} region
 * @param {object} v
 */
export function submarketProfile(country, region, v) {
  return {
    country,
    region,
    submarket: v.submarket,
    profileStatus: v.profileStatus || "skeleton",
    earlyEntryOpportunity: v.earlyEntryOpportunity || "unknown",
    primaryBuildProducts: v.primaryBuildProducts || [],
    ownerBrandSummary: v.ownerBrandSummary || "",
    lastReviewed: v.lastReviewed || "2026-06-24",
    signals: (v.signals || []).map(growthSignal),
  };
}
