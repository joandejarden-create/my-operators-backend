/**
 * Manual lead-asset overrides for owner outreach (CoStar rollups, SPV noise).
 */
import { normalizeOwnerKey } from "./normalize.js";

/** @typedef {{ buildingMatch?: string, deprioritizePatterns?: string[] }} LeadAssetOverride */

/** ownerKey → override */
export const MANUAL_LEAD_ASSET = {
  "park mizgal s c": {
    buildingMatch: "sunscape",
    deprioritizePatterns: ["ibis"],
  },
};

/**
 * Pick the best lead property for pitch / queue display.
 * @param {object[]} candidates properties with buildingName (+ optional operatorAligned, propertyScore)
 * @param {string} ownerName
 */
export function pickLeadProperty(candidates, ownerName) {
  const pool = (candidates || []).filter((p) => p?.buildingName);
  if (!pool.length) return null;

  const key = normalizeOwnerKey(ownerName);
  const manual = MANUAL_LEAD_ASSET[key];

  if (manual?.buildingMatch) {
    const matchNorm = normalizeOwnerKey(manual.buildingMatch);
    const hit = pool.find((p) => normalizeOwnerKey(p.buildingName).includes(matchNorm));
    if (hit) return hit;
  }

  let working = [...pool];
  if (manual?.deprioritizePatterns?.length) {
    const deprioritized = manual.deprioritizePatterns.map((p) => normalizeOwnerKey(p));
    const preferred = working.filter(
      (p) => !deprioritized.some((d) => normalizeOwnerKey(p.buildingName).includes(d))
    );
    if (preferred.length) working = preferred;
  }

  working.sort((a, b) => {
    const opA = Boolean(a.operatorAligned);
    const opB = Boolean(b.operatorAligned);
    if (opB !== opA) return Number(opB) - Number(opA);
    return (b.propertyScore || 0) - (a.propertyScore || 0);
  });

  return working[0];
}
