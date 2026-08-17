/**
 * Match CALA IHG Hotel Census rows to official ihg.com directory (fill-blank Website + Property ID).
 */

import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import {
  mapCensusRowForDirectoryMatch,
  scoreDirectoryAgainstCensus,
} from "./match-brand-directory-to-census.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hilton-property-id-contract.js";
import { isCalaCountry } from "../design-hotels-census-enrichment.js";
import { nameSimilarity, citiesMatch, countriesMatch } from "../independent-census/match-current-census.js";
import { ihgBrandFromUrl } from "../ihg-brand-directory-extract.js";

export const IHG_PARENT_FORMULA = `OR(FIND("IHG", {${CENSUS_FIELDS.parentCompany}}), FIND("InterContinental Hotels Group", {${CENSUS_FIELDS.parentCompany}}), FIND("InterContinental Hotel Group", {${CENSUS_FIELDS.parentCompany}}))`;

export const MAP_IHG_CENSUS_BACKFILL = {
  website: "Website",
  propertyId: CENSUS_PROPERTY_ID_FIELD,
  amenities: "Amenities",
};

/** Conservative brand family alignment (census name ↔ URL brand segment). */
const IHG_BRAND_FAMILIES = [
  { key: "holidayinnexpress", re: /\bholiday inn express\b/i, brands: ["holidayinnexpress"] },
  { key: "holidayinnresorts", re: /\bholiday inn resort\b/i, brands: ["holidayinnresorts"] },
  { key: "holidayinnclub", re: /\bholiday inn club\b/i, brands: ["holidayinnclubvacations"] },
  { key: "holidayinn", re: /\bholiday inn\b/i, brands: ["holidayinn", "holidayinntheniu"] },
  { key: "crowneplaza", re: /\bcrowne plaza\b/i, brands: ["crowneplaza"] },
  { key: "intercontinental", re: /\bintercontinental\b/i, brands: ["intercontinental"] },
  { key: "hotelindigo", re: /\bindigo\b/i, brands: ["hotelindigo"] },
  { key: "kimpton", re: /\bkimpton\b/i, brands: ["kimptonhotels", "kimptonclub"] },
  { key: "staybridge", re: /\bstaybridge\b/i, brands: ["staybridge"] },
  { key: "candlewood", re: /\bcandlewood\b/i, brands: ["candlewood"] },
  { key: "voco", re: /\bvoco\b/i, brands: ["voco"] },
  { key: "even", re: /\beven (hotels|hotel)\b/i, brands: ["evenhotels"] },
  { key: "avid", re: /\bavid\b/i, brands: ["avidhotels"] },
  { key: "atwell", re: /\batwell\b/i, brands: ["atwellsuites"] },
  { key: "regent", re: /\bregent\b/i, brands: ["regent"] },
  { key: "vignette", re: /\bvignette\b/i, brands: ["vignettecollection"] },
  { key: "garner", re: /\bgarner\b/i, brands: ["garnerhotels"] },
  { key: "iberostar", re: /\biberostar\b/i, brands: ["iberostarselection", "iberostarwaves", "iberostarjoia", "joia-iberostar"] },
];

/**
 * First matching brand family for a hotel display name (most-specific regex wins).
 * @param {string} name
 */
export function ihgBrandFamilyFromName(name) {
  return IHG_BRAND_FAMILIES.find((b) => b.re.test(String(name || ""))) || null;
}

/**
 * Prefer directory card name for brand family when present (URL path can lag rebrands).
 * @param {string} censusName
 * @param {string} propertyUrl
 * @param {string} [directoryName]
 */
export function ihgBrandFamiliesAlign(censusName, propertyUrl, directoryName = "") {
  const censusFamily = ihgBrandFamilyFromName(censusName);
  if (!censusFamily) return true;
  const dirNameFamily = directoryName ? ihgBrandFamilyFromName(directoryName) : null;
  if (dirNameFamily) return censusFamily.key === dirNameFamily.key;
  const brand = ihgBrandFromUrl(propertyUrl) || "";
  const urlFamily = IHG_BRAND_FAMILIES.find((b) => b.brands.includes(brand));
  if (!urlFamily) return true;
  return censusFamily.key === urlFamily.key;
}

/**
 * @param {string} name
 */
export function normalizeIhgHotelNameForMatch(name) {
  return String(name || "")
    .replace(/\u200b/g, "")
    .replace(/[-–—]/g, " ")
    .replace(/\s*,\s*a member of .*$/i, "")
    .replace(/\s+an ihg hotel\b/gi, "")
    .replace(/\s+by ihg\b/gi, "")
    .replace(/\bavid hotels?\b/gi, "avid")
    .replace(/\bciudad de mexico\b/gi, "mexico city")
    .replace(/\bmexico norte\b/gi, "mexico north")
    .replace(/\bnorte\b/gi, "north")
    .replace(/\bcd\.\s*/gi, "")
    .replace(/\bst\.?\s+vincent(?:\s+and\s+the\s+grenadines)?\b/gi, "st vincent grenadines")
    .replace(/\s+\b(DO|JAL|CDM|QRO|MX)\b\s*$/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

/** City slugs too broad to confirm a property match on their own. */
const IHG_GENERIC_CITY_SLUGS = new Set([
  "mexico",
  "panama",
  "brazil",
  "chile",
  "peru",
  "colombia",
  "argentina",
  "ecuador",
  "uruguay",
  "jamaica",
  "bahamas",
  "aruba",
  "barbados",
  "grenada",
  "dominica",
]);

function isDistinctiveIhgCitySlug(slug) {
  const s = String(slug || "")
    .toLowerCase()
    .replace(/\s+/g, "-")
    .trim();
  if (!s || s.length < 4) return false;
  if (IHG_GENERIC_CITY_SLUGS.has(s)) return false;
  return true;
}

/**
 * Score IHG directory hotel vs census row (conservative).
 * @param {object} dir
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 */
export function scoreIhgDirectoryAgainstCensus(dir, censusRow) {
  const directoryName = normalizeIhgHotelNameForMatch(dir.inferredHotelName || dir.name);
  const censusName = normalizeIhgHotelNameForMatch(censusRow.name);
  let nameSim = nameSimilarity(directoryName, censusName);
  const citySlug = String(dir.citySlug || "").replace(/-/g, " ");
  const slugDistinctive = isDistinctiveIhgCitySlug(dir.citySlug || citySlug);
  const cityOk =
    citiesMatch(dir.city, censusRow.city) === true ||
    (slugDistinctive && citiesMatch(citySlug, censusRow.city) === true) ||
    (slugDistinctive && citiesMatch(citySlug, censusRow.name) === true);
  const countryOk =
    Boolean(dir.country) &&
    Boolean(censusRow.country) &&
    countriesMatch(dir.country, censusRow.country) === true;
  const brandOk = ihgBrandFamiliesAlign(censusName, dir.propertyUrl || "", directoryName);

  // City+country+brand confirmed with partial name overlap (e.g. Diamond ↔ St. Vincent listing)
  if (brandOk && cityOk && countryOk && nameSim >= 0.5 && nameSim < 0.6) {
    nameSim = 0.6;
  }

  const scored = scoreDirectoryAgainstCensus(
    {
      name: directoryName,
      city: dir.city || citySlug,
      country: dir.country || "",
      website: dir.propertyUrl,
      brandPropertyCode: String(dir.propertyId || "").toUpperCase(),
      source: dir.source || "ihg_destination_directory",
    },
    censusRow
  );

  let score = Math.round(Math.max(nameSim * 72, scored.score * 0.65));
  if (nameSim >= 0.88) score = Math.max(score, 90);
  if (nameSim >= 0.8 && countryOk) score = Math.max(score, 82);
  if (nameSim >= 0.72 && cityOk && countryOk) score = Math.max(score, 80);
  if (nameSim >= 0.72 && countryOk) score = Math.max(score, 72);
  if (nameSim >= 0.55 && cityOk && countryOk) score = Math.max(score, 70);
  if (nameSim >= 0.6 && cityOk && countryOk) score = Math.max(score, 70);
  if (nameSim >= 0.55 && countryOk && !cityOk) score = Math.max(score, 62);
  score = Math.min(100, score);

  let confidence = "none";
  if (nameSim >= 0.88 || (nameSim >= 0.75 && cityOk && countryOk)) confidence = "high";
  else if (nameSim >= 0.82 && countryOk) confidence = "high";
  else if (nameSim >= 0.55 && cityOk && countryOk) confidence = "medium";
  else if (nameSim >= 0.72 && countryOk) confidence = "medium";
  else if (nameSim >= 0.55 && countryOk) confidence = "low";

  if (!brandOk) {
    score = Math.min(score, 42);
    confidence = "none";
  }
  if (!countryOk) {
    score = Math.min(score, 40);
    confidence = "none";
  }

  return {
    score,
    confidence,
    nameSim,
    cityOk,
    countryOk,
    reason: `IHG name match (${nameSim.toFixed(2)}); ${scored.reason}`,
  };
}

/**
 * Greedy one-to-one match.
 * @param {object[]} directoryRows
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>[]} censusRows
 * @param {object} [opts]
 */
export function matchIhgDirectoryToCensus(directoryRows, censusRows, opts = {}) {
  const minScore = opts.minScore ?? 62;
  const minNameSim = opts.minNameSim ?? 0.55;
  const minConfidence = opts.minConfidence || "low";
  const confRank = { none: 0, low: 1, medium: 2, high: 3 };
  const minConfRank = confRank[minConfidence] ?? 1;

  /** @type {{ censusRow: object, dir: object, scored: object }[]} */
  const candidates = [];
  for (const censusRow of censusRows) {
    for (const dir of directoryRows) {
      if (
        !dir.country ||
        !censusRow.country ||
        !countriesMatch(dir.country, censusRow.country)
      ) {
        continue;
      }
      const scored = scoreIhgDirectoryAgainstCensus(dir, censusRow);
      if (scored.score < minScore || scored.nameSim < minNameSim) continue;
      if ((confRank[scored.confidence] ?? 0) < minConfRank) continue;
      candidates.push({ censusRow, dir, scored });
    }
  }

  candidates.sort((a, b) => b.scored.score - a.scored.score || b.scored.nameSim - a.scored.nameSim);

  const usedCensus = new Set();
  const usedDir = new Set();
  /** @type {object[]} */
  const matches = [];
  for (const c of candidates) {
    const cid = c.censusRow.recordId;
    const did = c.dir.propertyId;
    if (usedCensus.has(cid) || usedDir.has(did)) continue;
    usedCensus.add(cid);
    usedDir.add(did);
    matches.push(c);
  }

  return {
    matches,
    unmatchedCensus: censusRows.filter((r) => !usedCensus.has(r.recordId)),
    unmatchedDirectory: directoryRows.filter((r) => !usedDir.has(r.propertyId)),
  };
}

/**
 * @param {object} [opts]
 * @param {object[]} [opts.directoryRows]
 * @param {number} [opts.minScore]
 * @param {number} [opts.minNameSim]
 * @param {string} [opts.minConfidence]
 * @param {boolean} [opts.calaOnly]
 */
export async function planIhgCensusDirectoryMatch(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const directoryRows = opts.directoryRows || [];
  if (!directoryRows.length) {
    throw new Error("No IHG directory rows. Run extract-ihg-cala-directory.mjs first.");
  }

  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of [MAP_IHG_CENSUS_BACKFILL.website, MAP_IHG_CENSUS_BACKFILL.propertyId, MAP_IHG_CENSUS_BACKFILL.amenities]) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: IHG_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  const calaOnly = opts.calaOnly !== false;
  let censusRows = records.map(mapCensusRowForDirectoryMatch);
  if (calaOnly) {
    censusRows = censusRows.filter((r) => isCalaCountry(r.fields?.[CENSUS_FIELDS.country] || r.country));
  }

  const websiteField = MAP_IHG_CENSUS_BACKFILL.website;
  const propField = MAP_IHG_CENSUS_BACKFILL.propertyId;

  // Directory hotels already claimed on census (any IHG row) must not be reassigned.
  const claimedPropertyIds = new Set();
  for (const r of records.map(mapCensusRowForDirectoryMatch)) {
    const existing = r.fields?.[propField];
    if (!isBlankCensusValue(existing)) claimedPropertyIds.add(String(existing).toUpperCase());
  }
  const availableDirectory = directoryRows.filter(
    (d) => d?.propertyId && !claimedPropertyIds.has(String(d.propertyId).toUpperCase())
  );

  // Only rows with at least one blank fill-blank field compete for matches.
  const fillableCensus = censusRows.filter(
    (r) =>
      isBlankCensusValue(r.fields?.[websiteField]) || isBlankCensusValue(r.fields?.[propField])
  );

  const { matches, unmatchedCensus, unmatchedDirectory } = matchIhgDirectoryToCensus(
    availableDirectory,
    fillableCensus,
    {
      // Discover down to low; apply gate is medium+ below
      minScore: Math.min(opts.minScore ?? 62, 62),
      minNameSim: Math.min(opts.minNameSim ?? 0.55, 0.55),
      minConfidence: "low",
    }
  );

  const applyMinScore = opts.minScore ?? 68;
  const applyMinNameSim = opts.minNameSim ?? 0.6;
  const applyMinConfidence = opts.minConfidence || "medium";
  const confRank = { none: 0, low: 1, medium: 2, high: 3 };
  const applyConfRank = confRank[applyMinConfidence] ?? 2;

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];
  /** @type {object[]} */
  const stewardReview = [];

  for (const { censusRow, dir, scored } of matches) {
    const applyFields = {};
    const websiteField = MAP_IHG_CENSUS_BACKFILL.website;
    const propField = MAP_IHG_CENSUS_BACKFILL.propertyId;

    if (isBlankCensusValue(censusRow.fields?.[websiteField]) && dir.propertyUrl) {
      applyFields[websiteField] = dir.propertyUrl;
    }
    if (isBlankCensusValue(censusRow.fields?.[propField]) && dir.propertyId) {
      applyFields[propField] = String(dir.propertyId).toUpperCase();
    }

    const row = {
      censusRecordId: censusRow.recordId,
      censusName: censusRow.name,
      censusCountry: censusRow.country,
      censusCity: censusRow.city,
      propertyId: dir.propertyId,
      propertyUrl: dir.propertyUrl,
      directoryName: dir.inferredHotelName || dir.name,
      matchScore: scored.score,
      matchConfidence: scored.confidence,
      nameSim: scored.nameSim,
      matchReason: scored.reason,
      applyFields,
      status: Object.keys(applyFields).length ? "ready" : "no_blank_fields",
    };

    if (!Object.keys(applyFields).length) {
      skipped.push({ ...row, reason: "no_blank_fields_to_fill" });
      continue;
    }

    const passesApplyGate =
      scored.score >= applyMinScore &&
      scored.nameSim >= applyMinNameSim &&
      (confRank[scored.confidence] ?? 0) >= applyConfRank;

    if (!passesApplyGate) {
      stewardReview.push({ ...row, status: "steward_review", reason: "below_apply_gate" });
      skipped.push({ ...row, reason: "below_apply_gate_steward_only" });
      continue;
    }

    planRows.push(row);
  }

  for (const c of unmatchedCensus) {
    skipped.push({
      censusRecordId: c.recordId,
      censusName: c.name,
      censusCountry: c.country,
      reason: "no_directory_match",
    });
  }

  return {
    fieldMapping: MAP_IHG_CENSUS_BACKFILL,
    directoryRowsLoaded: directoryRows.length,
    censusRowsScanned: censusRows.length,
    censusRowsTotalIhg: records.length,
    readyToApply: planRows.length,
    planRows,
    stewardReview,
    skipped,
    unmatchedDirectoryCount: unmatchedDirectory.length,
    unmatchedDirectorySample: unmatchedDirectory.slice(0, 40).map((d) => ({
      propertyId: d.propertyId,
      name: d.inferredHotelName || d.name,
      country: d.country,
      propertyUrl: d.propertyUrl,
    })),
  };
}

/**
 * Validate apply payload before Airtable write.
 * @param {object} planRow
 */
export function validateIhgCensusApplyRow(planRow) {
  const errors = [];
  if (!planRow?.censusRecordId) errors.push("missing censusRecordId");
  if (!planRow?.applyFields || !Object.keys(planRow.applyFields).length) {
    errors.push("no applyFields");
  }
  const website = planRow.applyFields?.[MAP_IHG_CENSUS_BACKFILL.website];
  const propId = planRow.applyFields?.[MAP_IHG_CENSUS_BACKFILL.propertyId];
  if (website != null) {
    if (typeof website !== "string" || !/^https:\/\/www\.ihg\.com\//i.test(website)) {
      errors.push("Website must be https://www.ihg.com/… hoteldetail URL");
    }
    if (!/\/hoteldetail\/?$/i.test(website.replace(/\/$/, ""))) {
      errors.push("Website must end with /hoteldetail");
    }
  }
  if (propId != null) {
    if (!/^[A-Z0-9]{4,6}$/.test(String(propId))) {
      errors.push("Property ID must be 4–6 alphanumeric mnemonic");
    }
  }
  if (planRow.matchConfidence === "none") errors.push("match confidence too low");
  return { pass: errors.length === 0, errors };
}
