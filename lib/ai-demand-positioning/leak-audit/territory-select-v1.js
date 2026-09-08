/**
 * Select 5–6 demand territories for ADP Lite free leak audit (not the full ADP prompt set).
 */

import {
  DEMAND_SEGMENT_ALIASES,
  FREE_AUDIT_SCOPE,
  LEAK_AUDIT_DEMAND_TERRITORIES,
} from "./schema-v1.js";

const DEFAULT_PRIORITY = [
  "leisure",
  "couples",
  "business",
  "meetings_groups",
  "wellness",
  "family",
  "celebration",
];

export function normalizeDemandSegmentKey(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return null;
  if (LEAK_AUDIT_DEMAND_TERRITORIES[raw]) return raw;
  return DEMAND_SEGMENT_ALIASES[raw] || null;
}

/**
 * @param {string} demandSegmentOfInterest
 * @param {object} [hotelContext]
 * @returns {{ keys: string[], labels: string[] }}
 */
export function selectDemandTerritories(demandSegmentOfInterest, hotelContext = {}) {
  const selected = [];
  const primary = normalizeDemandSegmentKey(demandSegmentOfInterest);
  if (primary) selected.push(primary);

  const hotelBlob = [hotelContext.hotelName, hotelContext.notes, hotelContext.positioning]
    .join(" ")
    .toLowerCase();
  const preferCelebration =
    /wedding|celebration|honeymoon|adults.?only|couples.?only/.test(hotelBlob);

  const priority = DEFAULT_PRIORITY.filter((k) => {
    if (preferCelebration && k === "family") return false;
    if (!preferCelebration && k === "celebration" && selected.length >= 5) return false;
    return true;
  });

  for (const key of priority) {
    if (selected.length >= FREE_AUDIT_SCOPE.maxDemandTerritories) break;
    if (!selected.includes(key)) selected.push(key);
  }

  while (selected.length > FREE_AUDIT_SCOPE.maxDemandTerritories) selected.pop();
  while (
    selected.length < FREE_AUDIT_SCOPE.minDemandTerritories &&
    selected.length < DEFAULT_PRIORITY.length
  ) {
    for (const key of DEFAULT_PRIORITY) {
      if (!selected.includes(key)) {
        selected.push(key);
        break;
      }
    }
  }

  return {
    keys: selected,
    labels: selected.map((k) => LEAK_AUDIT_DEMAND_TERRITORIES[k] || k),
  };
}

export function territoryLabel(key) {
  return LEAK_AUDIT_DEMAND_TERRITORIES[key] || key;
}
