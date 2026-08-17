/**
 * Plan Hyatt CALA Hotel Census enrichment (Website + Property ID fill-blank).
 * Directory source: official hyatt.com hotel URLs (sitemap / Wayback).
 */

import { readFileSync, existsSync } from "node:fs";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import {
  mapCensusRowForDirectoryMatch,
  scoreDirectoryAgainstCensus,
} from "./match-brand-directory-to-census.js";
import {
  isBlankCensusValue,
  MAP_DIRECTORY_ENRICHMENT,
} from "./brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hilton-property-id-contract.js";
import {
  isCalaCountry,
  nameFromHyattSlug,
  parseHyattHotelUrl,
} from "../hyatt-brand-directory-extract.js";
import {
  citiesMatch,
  countriesMatch,
  nameSimilarity,
  normalizeKey,
  normalizeText,
} from "../independent-census/match-current-census.js";

export const HYATT_PARENT_FORMULA = `FIND("Hyatt", {${CENSUS_FIELDS.parentCompany}})`;
export const DEFAULT_HYATT_DIRECTORY_JSON = "reports/hyatt-cala-directory-extract.json";

const HYATT_BRAND_TOKENS = [
  { key: "hyatt regency", re: /\bhyatt regency\b/i, slug: /hyatt-regency/i },
  { key: "hyatt place", re: /\bhyatt place\b/i, slug: /hyatt-place/i },
  { key: "hyatt house", re: /\bhyatt house\b/i, slug: /hyatt-house/i },
  { key: "hyatt centric", re: /\bhyatt centric\b/i, slug: /hyatt-centric/i },
  { key: "grand hyatt", re: /\bgrand hyatt\b/i, slug: /grand-hyatt/i },
  { key: "park hyatt", re: /\bpark hyatt\b/i, slug: /park-hyatt/i },
  { key: "andaz", re: /\bandaz\b/i, slug: /andaz/i },
  { key: "thompson", re: /\bthompson\b/i, slug: /thompson/i },
  { key: "alila", re: /\balila\b/i, slug: /alila/i },
  { key: "miraval", re: /\bmiraval\b/i, slug: /miraval/i },
  { key: "caption", re: /\bcaption\b/i, slug: /caption/i },
  { key: "hyatt vivid", re: /\bvivid\b/i, slug: /vivid/i },
  { key: "hyatt ziva", re: /\bziva\b/i, slug: /ziva/i },
  { key: "hyatt zilara", re: /\bzilara\b/i, slug: /zilara/i },
  { key: "secrets", re: /\bsecrets\b/i, slug: /secrets/i },
  { key: "dreams", re: /\bdreams?\b/i, slug: /dreams?/i },
  { key: "breathless", re: /\bbreathless\b/i, slug: /breathless/i },
  { key: "sunscape", re: /\bsunscape\b/i, slug: /sunscape/i },
  { key: "zoetry", re: /\bzoetry\b/i, slug: /zoetry/i },
  { key: "impression", re: /\bimpressions?\b/i, slug: /impression/i },
  { key: "now resorts", re: /\bnow\b/i, slug: /\/now-|\bnow-/i },
  { key: "unbound", re: /\bunbound\b/i, slug: /unbound/i },
  { key: "destination", re: /\bdestination\b/i, slug: /destination/i },
  { key: "jdv", re: /\bjdv\b|\bjoie de vivre\b/i, slug: /jdv|joie-de-vivre/i },
  { key: "hyatt", re: /\bhyatt\b/i, slug: /hyatt/i },
];

/**
 * @param {string} jsonPath
 */
export function loadHyattDirectoryRows(jsonPath = DEFAULT_HYATT_DIRECTORY_JSON) {
  if (!existsSync(jsonPath)) return [];
  const data = JSON.parse(readFileSync(jsonPath, "utf8"));
  const rows = Array.isArray(data.propertyRows) ? data.propertyRows : [];
  return rows.filter(
    (r) =>
      String(r.propertyId || "").trim() &&
      String(r.propertyUrl || "").trim() &&
      (r.isCala !== false)
  );
}

/** Inclusive Collection Property ID prefixes → brand family (official Hyatt codes). */
const HYATT_INCLUSIVE_PID_PREFIX_BRAND = [
  { prefix: /^SE/i, brand: "secrets" },
  { prefix: /^DR/i, brand: "dreams" },
  { prefix: /^BR/i, brand: "breathless" },
  { prefix: /^ZO/i, brand: "zoetry" },
  { prefix: /^SU/i, brand: "sunscape" },
  { prefix: /^IM/i, brand: "impression" },
  { prefix: /^NO/i, brand: "now resorts" },
];

/**
 * Brand tokens for URL/slug checks must ignore the hyatt.com domain.
 * Inclusive Collection pages often omit brand from the path (e.g. /vallarta-bay-resort-spa/drvpv);
 * matching "hyatt" from the host would falsely conflict with Dreams/Secrets/etc.
 * When the path has no brand token, infer family from the official Property ID prefix.
 * @param {string} censusName
 * @param {string} propertyUrl
 * @param {string} directoryName
 */
export function hyattBrandsAlign(censusName, propertyUrl, directoryName = "") {
  const parsed = parseHyattHotelUrl(propertyUrl);
  const pathHay = [
    parsed?.slug || "",
    parsed?.region || "",
    String(propertyUrl || "").replace(/^https?:\/\/[^/]+/i, ""),
    directoryName,
  ]
    .join(" ")
    .trim();
  const censusFamilies = HYATT_BRAND_TOKENS.filter((b) => b.re.test(censusName)).map((b) => b.key);
  /** @type {string[]} */
  let urlFamilies = HYATT_BRAND_TOKENS.filter((b) => b.slug.test(pathHay)).map((b) => b.key);
  const specificFromPath = urlFamilies.filter((k) => k !== "hyatt");
  // Path often omits Inclusive brand; Property ID prefix is authoritative on hyatt.com.
  if (!specificFromPath.length && parsed?.propertyId) {
    const pidBrand = HYATT_INCLUSIVE_PID_PREFIX_BRAND.find((p) => p.prefix.test(parsed.propertyId));
    if (pidBrand) urlFamilies = [...urlFamilies, pidBrand.brand];
  }
  if (!censusFamilies.length || !urlFamilies.length) return true;
  // Prefer specific brand over generic "hyatt"
  const specificCensus = censusFamilies.filter((k) => k !== "hyatt");
  const specificUrl = urlFamilies.filter((k) => k !== "hyatt");
  if (specificCensus.length && specificUrl.length) {
    return specificCensus.some((k) => specificUrl.includes(k));
  }
  return censusFamilies.some((k) => urlFamilies.includes(k));
}

/**
 * @param {string} name
 */
export function normalizeHyattHotelNameForMatch(name) {
  return normalizeText(name)
    .replace(/\s*,\s*a member of[^,]*/gi, "")
    .replace(/\s+by hyatt\b/gi, "")
    .replace(/\//g, " ")
    .replace(/\bst\.?\s+/gi, "saint ")
    .replace(/\s+resort\s*&\s*spa\b/gi, " resort and spa")
    .replace(/\s+resort and spa\b/gi, " resort and spa")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Hard steward exclusions — do not auto-apply even if score looks medium+.
 * Cariari vs Pinares are distinct San Jose Hyatt Place properties.
 * @param {string} censusName
 * @param {string} directoryNameOrSlug
 */
export function hyattMatchIsHardExcluded(censusName, directoryNameOrSlug) {
  const c = normalizeKey(censusName);
  const d = normalizeKey(directoryNameOrSlug);
  if (!c || !d) return false;
  const censusCariari = /\bcariari\b/.test(c);
  const censusPinares = /\bpinares\b/.test(c);
  const dirCariari = /\bcariari\b/.test(d);
  const dirPinares = /\bpinares\b/.test(d);
  if (censusCariari && dirPinares) return true;
  if (censusPinares && dirCariari) return true;
  // Insurgentes census must not bind to generic Hyatt Regency Mexico City (MEXHR).
  if (/\binsurgentes\b/.test(c) && /hyatt.?regency.?mexico.?city/.test(d) && !/\binsurgentes\b/.test(d)) {
    return true;
  }
  return false;
}

/**
 * @param {object} directoryHotel
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 */
export function scoreHyattDirectoryAgainstCensus(directoryHotel, censusRow) {
  const directoryName = normalizeHyattHotelNameForMatch(
    directoryHotel.name || nameFromHyattSlug(directoryHotel.slug)
  );
  const censusName = normalizeHyattHotelNameForMatch(censusRow.name);
  const nameSim = nameSimilarity(directoryName, censusName);
  const slugName = normalizeHyattHotelNameForMatch(nameFromHyattSlug(directoryHotel.slug));
  const slugSim = nameSimilarity(slugName, censusName);
  const bestNameSim = Math.max(nameSim, slugSim);

  const countryOk = countriesMatch(
    directoryHotel.censusCountry || directoryHotel.country,
    censusRow.country
  );
  if (
    directoryHotel.censusCountry ||
    directoryHotel.country ||
    censusRow.country
  ) {
    if (!countryOk) {
      return {
        score: 0,
        confidence: "none",
        reason: "Country mismatch",
        nameSim: bestNameSim,
      };
    }
  }

  if (!hyattBrandsAlign(censusName, directoryHotel.propertyUrl || "", directoryName)) {
    return {
      score: Math.round(bestNameSim * 40),
      confidence: "none",
      reason: "Brand family mismatch",
      nameSim: bestNameSim,
    };
  }

  if (
    hyattMatchIsHardExcluded(
      censusName,
      `${directoryName} ${directoryHotel.slug || ""} ${directoryHotel.propertyUrl || ""}`
    )
  ) {
    return {
      score: 0,
      confidence: "none",
      reason: "Hard steward exclusion (distinct properties)",
      nameSim: bestNameSim,
    };
  }

  const directoryRow = {
    name: directoryName,
    matchName: directoryName,
    city: directoryHotel.city || "",
    country: directoryHotel.censusCountry || directoryHotel.country || "",
    website: directoryHotel.propertyUrl,
    brandPropertyCode: String(directoryHotel.propertyId || "").toUpperCase(),
    latitude: directoryHotel.latitude ?? null,
    longitude: directoryHotel.longitude ?? null,
    source: directoryHotel.source || "hyatt_official_sitemap",
  };

  const scored = scoreDirectoryAgainstCensus(directoryRow, censusRow);

  // Boost when slug tokens heavily overlap census name (Hyatt URLs encode hotel name).
  let score = scored.score;
  if (bestNameSim >= 0.85 && countryOk) score = Math.min(100, Math.max(score, 88));
  else if (bestNameSim >= 0.7 && countryOk) score = Math.min(100, Math.max(score, 72));
  else if (bestNameSim >= 0.55 && countryOk) score = Math.min(100, Math.max(score, 60));

  const cityFromSlug = extractCityHintFromSlug(directoryHotel.slug, censusRow);
  if (cityFromSlug === true) score = Math.min(100, score + 8);

  let confidence = scored.confidence;
  if (score >= 80 && bestNameSim >= 0.65) confidence = "high";
  else if (score >= 65 && bestNameSim >= 0.5) confidence = "medium";
  else if (score >= 50 && bestNameSim >= 0.4) confidence = "low";
  else confidence = "none";

  return {
    score,
    confidence,
    reason: scored.reason || "Hyatt name + country match",
    nameSim: bestNameSim,
    cityHint: cityFromSlug,
  };
}

/**
 * @param {string} slug
 * @param {object} censusRow
 */
function extractCityHintFromSlug(slug, censusRow) {
  const city = normalizeKey(censusRow.city || "");
  if (!city || city.length < 3) return null;
  const slugKey = normalizeKey(String(slug || "").replace(/-/g, " "));
  if (slugKey.includes(city) || city.includes(slugKey)) return true;
  const cityResult = citiesMatch(censusRow.city, slugKey);
  return cityResult;
}

/**
 * Greedy 1:1 assignment of directory hotels to census rows.
 * @param {object[]} directoryHotels
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>[]} censusRows
 * @param {{ minScore?: number, minNameSim?: number, minConfidence?: string, onlyBlankWebsiteOrPid?: boolean }} [opts]
 */
export function matchHyattDirectoryToCensus(directoryHotels, censusRows, opts = {}) {
  const minScore = opts.minScore ?? 58;
  const minNameSim = opts.minNameSim ?? 0.45;
  const minConfidence = opts.minConfidence ?? "low";
  const onlyBlank = opts.onlyBlankWebsiteOrPid !== false;
  const rank = { high: 3, medium: 2, low: 1, none: 0 };
  const needRank = rank[minConfidence] ?? 1;

  /** @type {object[]} */
  const pairs = [];

  for (const directoryHotel of directoryHotels) {
    for (const censusRow of censusRows) {
      if (onlyBlank) {
        const webBlank = isBlankCensusValue(censusRow.fields?.[MAP_DIRECTORY_ENRICHMENT.website]);
        const pidBlank = isBlankCensusValue(censusRow.fields?.[CENSUS_PROPERTY_ID_FIELD]);
        if (!webBlank && !pidBlank) continue;
      }

      if (
        directoryHotel.censusCountry &&
        censusRow.country &&
        !countriesMatch(directoryHotel.censusCountry, censusRow.country)
      ) {
        continue;
      }

      const scored = scoreHyattDirectoryAgainstCensus(directoryHotel, censusRow);
      if (scored.score < minScore || scored.nameSim < minNameSim) continue;
      if ((rank[scored.confidence] ?? 0) < needRank) continue;

      pairs.push({ directoryHotel, censusRow, ...scored });
    }
  }

  pairs.sort((a, b) => b.score - a.score || b.nameSim - a.nameSim);

  const usedCensus = new Set();
  const usedDir = new Set();
  /** @type {object[]} */
  const assigned = [];

  for (const p of pairs) {
    const pid = String(p.directoryHotel.propertyId || "").toUpperCase();
    if (!pid || usedCensus.has(p.censusRow.recordId) || usedDir.has(pid)) continue;
    usedCensus.add(p.censusRow.recordId);
    usedDir.add(pid);
    assigned.push(p);
  }

  return assigned;
}

/**
 * @param {object[]} assigned
 */
export function buildHyattApplyPlan(assigned) {
  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const stewardReview = [];

  for (const row of assigned) {
    const f = row.censusRow.fields || {};
    const cat = row.directoryHotel;
    const applyFields = {};

    const websiteField = MAP_DIRECTORY_ENRICHMENT.website;
    if (isBlankCensusValue(f[websiteField]) && cat.propertyUrl) {
      applyFields[websiteField] = cat.propertyUrl;
    }
    if (isBlankCensusValue(f[CENSUS_PROPERTY_ID_FIELD]) && cat.propertyId) {
      applyFields[CENSUS_PROPERTY_ID_FIELD] = String(cat.propertyId).toUpperCase();
    }

    if (!Object.keys(applyFields).length) continue;

    // Validate URL encodes the same property id we plan to write.
    const parsed = parseHyattHotelUrl(cat.propertyUrl);
    if (!parsed || parsed.propertyId !== String(cat.propertyId).toUpperCase()) {
      stewardReview.push({
        censusRecordId: row.censusRow.recordId,
        censusName: row.censusRow.name,
        reason: "property_id_url_mismatch",
        propertyUrl: cat.propertyUrl,
        propertyId: cat.propertyId,
      });
      continue;
    }

    const planRow = {
      censusRecordId: row.censusRow.recordId,
      censusName: row.censusRow.name,
      censusCity: row.censusRow.city,
      censusCountry: row.censusRow.country,
      propertyId: String(cat.propertyId).toUpperCase(),
      propertyUrl: cat.propertyUrl,
      directoryHotelName: cat.name || nameFromHyattSlug(cat.slug),
      matchScore: row.score,
      nameSim: row.nameSim,
      matchConfidence: row.confidence,
      matchReason: row.reason,
      applyFields,
      fieldMapping: {
        Website: websiteField,
        "Property ID": CENSUS_PROPERTY_ID_FIELD,
      },
      status: row.confidence === "low" ? "steward_review" : "ready",
    };

    if (row.confidence === "low") {
      stewardReview.push(planRow);
    } else {
      planRows.push(planRow);
    }
  }

  return { planRows, stewardReview };
}

/**
 * @param {object} [opts]
 */
export async function planHyattCensusEnrichment(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const jsonPath = opts.jsonPath || DEFAULT_HYATT_DIRECTORY_JSON;
  const directoryAll = loadHyattDirectoryRows(jsonPath);
  if (!directoryAll.length) {
    throw new Error(
      `No Hyatt directory rows at ${jsonPath}. Run extract first (sitemap/wayback).`
    );
  }

  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of [MAP_DIRECTORY_ENRICHMENT.website, CENSUS_PROPERTY_ID_FIELD, "Amenities"]) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: HYATT_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  const censusAll = records.map(mapCensusRowForDirectoryMatch);
  const censusRows = censusAll.filter((r) => isCalaCountry(r.country || r.fields?.[CENSUS_FIELDS.country]));

  const directoryHotels = directoryAll.filter(
    (r) => r.isCala !== false && isCalaCountry(r.censusCountry || r.country)
  );

  // Do not re-bind Property IDs already written on any census row (blocks duplicate 1:1 collisions).
  const usedPropertyIds = new Set();
  for (const row of censusAll) {
    const pid = String(row.fields?.[CENSUS_PROPERTY_ID_FIELD] || "")
      .trim()
      .toUpperCase();
    if (pid) usedPropertyIds.add(pid);
  }

  const directoryAvailable = directoryHotels.filter((r) => {
    const pid = String(r.propertyId || "").trim().toUpperCase();
    return pid && !usedPropertyIds.has(pid);
  });

  const assigned = matchHyattDirectoryToCensus(directoryAvailable, censusRows, {
    minScore: opts.minScore ?? 58,
    minNameSim: opts.minNameSim ?? 0.45,
    minConfidence: opts.minConfidence ?? "low",
    onlyBlankWebsiteOrPid: opts.onlyBlankWebsiteOrPid !== false,
  });

  const { planRows, stewardReview } = buildHyattApplyPlan(assigned);

  const matchedIds = new Set(assigned.map((a) => a.censusRow.recordId));
  const unmatchedCensus = censusRows
    .filter((r) => !matchedIds.has(r.recordId))
    .filter((r) => {
      const webBlank = isBlankCensusValue(r.fields?.[MAP_DIRECTORY_ENRICHMENT.website]);
      const pidBlank = isBlankCensusValue(r.fields?.[CENSUS_PROPERTY_ID_FIELD]);
      return webBlank || pidBlank;
    })
    .map((r) => ({
      censusRecordId: r.recordId,
      censusName: r.name,
      censusCity: r.city,
      censusCountry: r.country,
      blankWebsite: isBlankCensusValue(r.fields?.[MAP_DIRECTORY_ENRICHMENT.website]),
      blankPropertyId: isBlankCensusValue(r.fields?.[CENSUS_PROPERTY_ID_FIELD]),
    }));

  return {
    jsonPath,
    directoryRowsLoaded: directoryHotels.length,
    hyattParentRows: censusAll.length,
    censusRowsScanned: censusRows.length,
    matched: assigned.length,
    readyToApply: planRows.length,
    stewardReviewCount: stewardReview.length,
    unmatchedBlankCount: unmatchedCensus.length,
    planRows,
    stewardReview,
    unmatchedCensus,
    fieldMapping: {
      Website: MAP_DIRECTORY_ENRICHMENT.website,
      "Property ID": CENSUS_PROPERTY_ID_FIELD,
    },
  };
}

/**
 * Rows with low confidence for steward CSV export.
 * @param {object[]} stewardReview
 */
export function hyattStewardReviewRows(stewardReview) {
  return (stewardReview || []).map((r) => ({
    censusRecordId: r.censusRecordId,
    censusName: r.censusName,
    censusCity: r.censusCity || "",
    censusCountry: r.censusCountry || "",
    propertyId: r.propertyId || "",
    propertyUrl: r.propertyUrl || "",
    directoryHotelName: r.directoryHotelName || "",
    matchScore: r.matchScore ?? "",
    nameSim: r.nameSim ?? "",
    matchConfidence: r.matchConfidence || "",
    matchReason: r.matchReason || r.reason || "",
    status: r.status || "steward_review",
  }));
}
