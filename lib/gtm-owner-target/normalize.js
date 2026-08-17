/**
 * Owner name normalization for GTM target rollups.
 */

export function normalizeOwnerKey(name) {
  return String(name || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

export function normalizeHeaderKey(header) {
  return String(header || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

/** CoStar / export header aliases → canonical keys used by import parser. */
const HEADER_ALIASES = new Map([
  ["true owner", "trueOwner"],
  ["building name", "buildingName"],
  ["property name", "buildingName"],
  ["property id", "costarPropertyId"],
  ["submarket", "submarket"],
  ["market", "market"],
  ["country", "country"],
  ["city", "city"],
  ["zip code", "zipCode"],
  ["zip", "zipCode"],
  ["star rating", "starRating"],
  ["rba gla", "rbaGlaSf"],
  ["rba", "rbaGlaSf"],
  ["gla", "rbaGlaSf"],
  ["year built", "yearBuilt"],
  ["month year renov", "yearRenovated"],
  ["built renov", "builtRenovText"],
  ["type", "propertyType"],
  ["affiliation", "brandAffiliation"],
  ["brand", "brandAffiliation"],
  ["chain", "brandAffiliation"],
]);

export function mapCostarHeaderToCanonical(header) {
  const key = normalizeHeaderKey(header);
  return HEADER_ALIASES.get(key) || null;
}

export function parseNumber(value) {
  if (value == null || value === "") return null;
  const n = Number(String(value).replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : null;
}

export function parseYear(value) {
  if (value == null || value === "") return null;
  const s = String(value).trim();
  const m = s.match(/\b(19|20)\d{2}\b/);
  if (m) return Number(m[0]);
  const n = parseNumber(s);
  return n != null && n >= 1800 && n <= 2100 ? Math.trunc(n) : null;
}

export function inferOwnerType(ownerName) {
  const key = normalizeOwnerKey(ownerName);
  if (!key) return "unknown";
  if (/\b(spe|ltda|s\.r\.l|llc|inc|holdings?|invest|capital|reit|fundo)\b/.test(key)) {
    if (/\bspe\b/.test(key)) return "spv";
    if (/\breit\b|\bfundo\b/.test(key)) return "reit";
    if (/\bcapital\b|\binvest/.test(key)) return "institutional";
  }
  if (/\b(wind creek|gaming|casino)\b/.test(key)) return "gaming_hospitality";
  if (/\b(hoteis|hotels|resorts|hospitality|gestion|operadores)\b/.test(key)) {
    return "integrated_operator";
  }
  if (key.split(" ").length <= 3 && !/\b(ltd|sa|inc|llc)\b/.test(key)) {
    return "individual";
  }
  return "regional_operator";
}

export function inferPriorityTier(propertyCount, totalRbaSf) {
  const count = propertyCount || 0;
  const rba = totalRbaSf || 0;
  if (count >= 3 || rba >= 400000) return "A";
  if (count >= 2 || rba >= 150000) return "B";
  return "C";
}

export function buildSourceRowKey(row) {
  const parts = [
    row.trueOwner || "",
    row.buildingName || "",
    row.costarPropertyId || "",
    row.city || "",
    row.submarket || "",
  ];
  return normalizeOwnerKey(parts.join("|"));
}
