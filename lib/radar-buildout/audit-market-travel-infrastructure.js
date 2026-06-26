/**
 * Audit Travel Infrastructure records for a country/market corridor.
 */

import { getMarketTiAuditConfig } from "./market-travel-infrastructure-audit-configs.js";

const DEFAULT_CANCUN_KEYWORDS = [
  "cancun",
  "cancún",
  "cozumel",
  "tulum",
  "playa del carmen",
  "riviera maya",
  "isla mujeres",
  "puerto aventuras",
  "akumal",
  "mayakoba",
  "playa mujeres",
  "costa mujeres",
  "quintana roo",
  "felipe carrillo",
  "puerto morelos",
];

const EXPECTED_CANCUN_TI_PATTERNS = [
  { label: "Cancún International Airport", patterns: [/canc[uú]n.*international.*airport/i, /\bCUN\b/] },
  { label: "Tulum International Airport", patterns: [/tulum.*international/i, /felipe carrillo/i, /\bTQO\b/] },
  { label: "Cozumel International Airport", patterns: [/cozumel.*international/i, /\bCZM\b/] },
  { label: "Playa del Carmen ferry terminal", patterns: [/playa del carmen.*ferry/i, /playa del carmen.*maritime/i] },
  { label: "Cozumel ferry terminal", patterns: [/cozumel.*ferry/i] },
  { label: "Isla Mujeres ferry terminal", patterns: [/isla mujeres.*ferry/i, /gran puerto.*ferry/i, /ultramar/i] },
  { label: "Tren Maya / rail corridor node", patterns: [/tren maya/i, /maya train/i] },
  { label: "Cancún cruise / maritime terminal", patterns: [/cruise.*canc[uú]n/i, /puerto.*canc[uú]n.*cruise/i] },
];

function norm(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function haystack(record) {
  return [
    record.name,
    record.city,
    record.submarket,
    record.notes,
    record.pointType,
    record.iataCode,
    record.address,
  ]
    .map(norm)
    .join(" | ");
}

function matchesKeywords(record, keywords) {
  const h = haystack(record);
  return keywords.some((kw) => h.includes(norm(kw)));
}

function matchesPattern(record, regex) {
  return regex.test(haystack(record));
}

function weakSubmarket(record) {
  const sm = String(record.submarket || "").trim();
  return !sm || sm === "Other" || sm.length < 3;
}

function missingGovernance(record) {
  return !String(record.dataConfidence || "").trim() || record.includeOnRadarMap == null;
}

function duplicateKey(record) {
  return [norm(record.name), norm(record.city), norm(record.pointType || record.type)].join("|");
}

/**
 * @param {object[]} records — normalized TI radar points
 * @param {object} options
 */
export function auditMarketTravelInfrastructure(records, options = {}) {
  const countryLabel = options.country || "Mexico";
  const country = norm(countryLabel);
  const market = options.market || "Cancún / Riviera Maya";
  const marketConfig = getMarketTiAuditConfig(countryLabel);
  const keywords = (options.keywords || marketConfig?.keywords || DEFAULT_CANCUN_KEYWORDS).map(norm);
  const expectedPatterns =
    options.expectedPatterns || marketConfig?.expectedPatterns || EXPECTED_CANCUN_TI_PATTERNS;
  const submarketFilter = (options.submarkets || []).map(norm).filter(Boolean);

  const countryRecords = records.filter((r) => norm(r.country) === country);
  const marketRecords = countryRecords.filter((r) => {
    if (submarketFilter.length) {
      return submarketFilter.includes(norm(r.submarket));
    }
    return matchesKeywords(r, keywords);
  });

  const seen = new Map();
  const duplicateCandidates = [];
  for (const r of marketRecords) {
    const key = duplicateKey(r);
    if (seen.has(key)) duplicateCandidates.push({ name: r.name, city: r.city, duplicateOf: seen.get(key) });
    else seen.set(key, r.name);
  }

  const existingMatches = [];
  const likelyMissing = [];
  for (const expected of expectedPatterns) {
    const hit = marketRecords.find((r) => expected.patterns.some((rx) => matchesPattern(r, rx)));
    if (hit) {
      existingMatches.push({
        label: expected.label,
        name: hit.name,
        city: hit.city,
        submarket: hit.submarket || "",
        pointType: hit.pointType || hit.type || "",
      });
    } else {
      likelyMissing.push(expected.label);
    }
  }

  const weakSubmarketRecords = marketRecords
    .filter(weakSubmarket)
    .map((r) => ({ name: r.name, city: r.city, submarket: r.submarket || "" }));

  const governanceBackfill = marketRecords
    .filter(missingGovernance)
    .map((r) => ({ name: r.name, city: r.city, issues: ["dataConfidence or includeOnRadarMap missing"] }));

  const additionalFixtureNeeded =
    likelyMissing.length > 0 ||
    weakSubmarketRecords.length > 3 ||
    marketRecords.length < 12;

  return {
    auditedAt: new Date().toISOString(),
    country: countryLabel,
    market,
    keywords,
    summary: {
      countryTravelInfrastructureTotal: countryRecords.length,
      marketMatchedRecords: marketRecords.length,
      existingExpectedNodes: existingMatches.length,
      likelyMissingNodes: likelyMissing.length,
      duplicateCandidates: duplicateCandidates.length,
      weakSubmarketRecords: weakSubmarketRecords.length,
      governanceBackfillCandidates: governanceBackfill.length,
      additionalFixtureRecommended: additionalFixtureNeeded,
      /** @deprecated use countryTravelInfrastructureTotal */
      mexicoTravelInfrastructureTotal: countryRecords.length,
    },
    existingMatchingRecords: marketRecords.map((r) => ({
      name: r.name,
      pointType: r.pointType || r.type,
      city: r.city,
      submarket: r.submarket || "",
      latitude: r.latitude ?? r.lat,
      longitude: r.longitude ?? r.lng,
      sourceReference: r.sourceReference || r.sourceUrl || "",
      dataConfidence: r.dataConfidence || "",
    })),
    expectedNodeCoverage: existingMatches,
    likelyMissingRecords: likelyMissing,
    duplicateCandidates,
    weakSubmarketRecords,
    governanceBackfillCandidates: governanceBackfill,
    notes: [
      "Demand anchors should not duplicate TI airports/ferries unless explicitly DA-relevant.",
      "Do not import TI from this audit unless gaps are confirmed and fixture is prepared.",
    ],
  };
}

export { DEFAULT_CANCUN_KEYWORDS };
