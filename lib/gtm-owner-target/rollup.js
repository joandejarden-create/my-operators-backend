import {
  inferOwnerType,
  inferPriorityTier,
  normalizeOwnerKey,
} from "./normalize.js";

function uniqueSorted(values) {
  return [...new Set(values.map((v) => String(v || "").trim()).filter(Boolean))].sort((a, b) =>
    a.localeCompare(b)
  );
}

/**
 * Compute owner-level aggregates from parsed CoStar property rows.
 * @param {{ ownerName: string, properties: object[] }} group
 */
export function computeOwnerRollup(group) {
  const properties = group.properties || [];
  const propertyCount = properties.length;
  const totalRbaSf = properties.reduce((sum, p) => sum + (p.rbaGlaSf || 0), 0);

  const markets = uniqueSorted(properties.map((p) => p.submarket || p.market));
  const countries = uniqueSorted(properties.map((p) => p.country));
  const sampleProperties = properties
    .slice()
    .sort((a, b) => (b.rbaGlaSf || 0) - (a.rbaGlaSf || 0))
    .slice(0, 8)
    .map((p) => {
      const loc = [p.city, p.submarket || p.market, p.country].filter(Boolean).join(", ");
      const rba = p.rbaGlaSf != null ? `${Math.round(p.rbaGlaSf).toLocaleString("en-US")} SF` : "—";
      return `${p.buildingName || "Unnamed"} (${loc}; ${rba})`;
    })
    .join("\n");

  return {
    ownerName: group.ownerName,
    ownerNameNormalized: normalizeOwnerKey(group.ownerName),
    ownerType: inferOwnerType(group.ownerName),
    priorityTier: inferPriorityTier(propertyCount, totalRbaSf),
    propertyCount,
    totalRbaSf: totalRbaSf || null,
    marketsSummary: markets.join("; "),
    countriesSummary: countries.join("; "),
    sampleProperties,
  };
}

/**
 * @param {object[]} groups from groupRowsByOwner
 */
export function computeOwnerRollups(groups) {
  return groups.map(computeOwnerRollup);
}
