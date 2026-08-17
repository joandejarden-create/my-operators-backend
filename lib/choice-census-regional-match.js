/**
 * Match Choice regional directory hotels (official page names) to Hotel Census rows.
 */

import {
  citiesMatch,
  countriesMatch,
  nameSimilarity,
  normalizeKey,
  normalizeText,
} from "./independent-census/match-current-census.js";
import { isBlankCensusValue } from "./hotel-census/brand-directory-enrichment-contract.js";
import { scoreDirectoryAgainstCensus } from "./hotel-census/match-brand-directory-to-census.js";
import { mapExtractRowToDirectoryMatchRow } from "./hotel-census/plan-brand-census-directory-match.js";
import { choiceCitySlugFromPropertyUrl } from "./choice-regional-directory-extract.js";

const STOP_TOKENS = new Set([
  "hotel",
  "hotels",
  "inn",
  "suites",
  "collection",
  "country",
  "club",
  "beach",
  "the",
  "and",
  "by",
  "de",
  "del",
  "la",
  "el",
  "real",
]);

const BRAND_FAMILIES = [
  { key: "quality", re: /\bquality\b/i, slug: /quality-inn|quality-hotel/i },
  { key: "comfort", re: /\bcomfort\b/i, slug: /comfort-inn|comfort-hotel|comfort-suites/i },
  { key: "sleep", re: /\bsleep\b/i, slug: /sleep-inn/i },
  { key: "radisson", re: /\bradisson\b/i, slug: /radisson/i },
  { key: "ascend", re: /\bascend\b/i, slug: /ascend/i },
  { key: "clarion", re: /\bclarion\b/i, slug: /clarion/i },
  { key: "park inn", re: /\bpark inn\b/i, slug: /park-inn/i },
  { key: "econo", re: /\becono\b/i, slug: /econo-lodge/i },
  { key: "rodeway", re: /\brodeway\b/i, slug: /rodeway/i },
  { key: "cambria", re: /\bcambria\b/i, slug: /cambria/i },
];

/**
 * @param {string} censusName
 * @param {string} propertyUrl
 */
export function choiceBrandFamiliesAlign(censusName, propertyUrl) {
  const censusFamilies = BRAND_FAMILIES.filter((b) => b.re.test(censusName)).map((b) => b.key);
  const urlFamilies = BRAND_FAMILIES.filter((b) => b.slug.test(propertyUrl)).map((b) => b.key);
  if (!censusFamilies.length || !urlFamilies.length) return true;
  return censusFamilies.some((k) => urlFamilies.includes(k));
}

/**
 * @param {string} name
 */
export function normalizeChoiceHotelNameForMatch(name) {
  return normalizeText(name)
    .replace(/\s*\(choice\)\s*/gi, " ")
    .replace(/\s*,\s*a member of radisson[^,]*/gi, "")
    .replace(/\s+by faranda[^,]*/gi, "")
    .replace(/\s*&\s*country club\s*/gi, " ")
    .replace(/\s+by radisson\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {object} regionalHotel
 * @param {ReturnType<import("./hotel-census/match-brand-directory-to-census.js").mapCensusRowForDirectoryMatch>} censusRow
 * @param {string} [regionalCountry]
 */
export function scoreChoiceRegionalAgainstCensus(regionalHotel, censusRow, regionalCountry = "") {
  const directoryName = normalizeChoiceHotelNameForMatch(regionalHotel.name);
  const censusName = normalizeChoiceHotelNameForMatch(censusRow.name);
  const nameSim = nameSimilarity(directoryName, censusName);

  const cityFromUrl = regionalHotel.citySlug?.replace(/-/g, " ") || "";
  const cityOk =
    citiesMatch(cityFromUrl, censusRow.city) ||
    citiesMatch(regionalHotel.citySlug, censusRow.city);

  const countryOk =
    !regionalCountry ||
    countriesMatch(regionalCountry, censusRow.country) ||
    countriesMatch(regionalHotel.regionalCountry, censusRow.country);

  const dirMatch = mapExtractRowToDirectoryMatchRow(
    {
      inferredHotelName: directoryName,
      city: cityFromUrl,
      country: regionalCountry || regionalHotel.regionalCountry || censusRow.country,
      propertyId: regionalHotel.propertyId,
      propertyUrl: regionalHotel.propertyUrl,
      source: "choice_regional_jsonld",
    },
    { scoringProfile: "accor" }
  );
  const scored = scoreDirectoryAgainstCensus(dirMatch, censusRow);

  let score = Math.round(Math.max(nameSim * 70, scored.score * 0.6));
  if (nameSim >= 0.85) score = Math.max(score, 88);
  if (nameSim >= 0.7 && cityOk === true) score = Math.max(score, 78);
  if (nameSim >= 0.55 && cityOk === true && countryOk) score = Math.max(score, 68);
  score = Math.min(100, score);

  let confidence = "none";
  if (nameSim >= 0.85 || (nameSim >= 0.7 && cityOk === true)) confidence = "high";
  else if (nameSim >= 0.6 && cityOk === true && countryOk) confidence = "medium";
  else if (nameSim >= 0.5 && cityOk === true) confidence = "low";
  if (
    regionalHotel.source === "choice_sitemap_only" &&
    cityOk === true &&
    countryOk &&
    nameSim >= 0.35
  ) {
    confidence = "low";
    score = Math.max(score, 65);
  }

  if (!choiceBrandFamiliesAlign(censusName, regionalHotel.propertyUrl || "")) {
    score = Math.min(score, 40);
    confidence = "none";
  }

  return {
    score,
    confidence,
    nameSim,
    cityOk,
    countryOk,
    reason: `Regional name match (${nameSim.toFixed(2)}); ${scored.reason}`,
  };
}

/**
 * Greedy one-to-one assignment.
 * @param {object[]} regionalHotels
 * @param {ReturnType<import("./hotel-census/match-brand-directory-to-census.js").mapCensusRowForDirectoryMatch>[]} censusRows
 * @param {object} [opts]
 */
export function matchChoiceRegionalToCensus(regionalHotels, censusRows, opts = {}) {
  const minScore = opts.minScore ?? 65;
  const minNameSim = opts.minNameSim ?? 0.55;
  const minConfidence = opts.minConfidence ?? "medium";
  const rank = { high: 3, medium: 2, low: 1, none: 0 };
  const needRank = rank[minConfidence] ?? 2;
  const onlyBlankWebsite = opts.onlyBlankWebsite !== false;

  /** @type {object[]} */
  const pairs = [];
  for (const regionalHotel of regionalHotels) {
    for (const censusRow of censusRows) {
      if (onlyBlankWebsite && !isBlankCensusValue(censusRow.fields?.Website)) continue;

      const regionalCountry = opts.regionalCountry || regionalHotel.regionalCountry || "";
      if (
        regionalCountry &&
        censusRow.country &&
        !countriesMatch(regionalCountry, censusRow.country)
      ) {
        continue;
      }

      const scored = scoreChoiceRegionalAgainstCensus(
        regionalHotel,
        censusRow,
        regionalCountry
      );
      if (!choiceBrandFamiliesAlign(censusRow.name, regionalHotel.propertyUrl || "")) continue;
      if (scored.score < minScore || scored.nameSim < minNameSim) continue;
      if (rank[scored.confidence] < needRank) continue;

      pairs.push({
        regionalHotel,
        censusRow,
        ...scored,
      });
    }
  }

  pairs.sort((a, b) => b.score - a.score || b.nameSim - a.nameSim);

  const usedCensus = new Set();
  const usedRegional = new Set();
  /** @type {object[]} */
  const assigned = [];

  for (const p of pairs) {
    const pid = p.regionalHotel.propertyId;
    if (usedCensus.has(p.censusRow.recordId) || usedRegional.has(pid)) continue;
    usedCensus.add(p.censusRow.recordId);
    usedRegional.add(pid);
    assigned.push(p);
  }

  return assigned;
}

/**
 * @param {object[]} assigned
 */
export function buildChoiceRegionalApplyPlan(assigned) {
  const plan = [];
  for (const row of assigned) {
    const f = row.censusRow.fields || {};
    const cat = row.regionalHotel;
    const applyFields = {};

    if (isBlankCensusValue(f.Website) && cat.propertyUrl) applyFields.Website = cat.propertyUrl;
    if (isBlankCensusValue(f["Property ID"]) && cat.propertyId) {
      applyFields["Property ID"] = cat.propertyId;
    }

    const needsAmenities = isBlankCensusValue(f.Amenities);
    if (!Object.keys(applyFields).length && !needsAmenities) continue;

    plan.push({
      censusRecordId: row.censusRow.recordId,
      censusName: row.censusRow.name,
      censusCity: row.censusRow.city,
      censusCountry: row.censusRow.country,
      propertyId: cat.propertyId,
      propertyUrl: cat.propertyUrl,
      regionalName: cat.name,
      matchScore: row.score,
      nameSim: row.nameSim,
      matchConfidence: row.confidence,
      matchReason: row.reason,
      applyFields,
      needsAmenities,
    });
  }
  return plan;
}

/**
 * Steward-review rows: medium confidence or nameSim 0.5–0.65
 * @param {object[]} assigned
 */
export function choiceRegionalStewardReview(assigned) {
  return assigned
    .filter((r) => r.confidence === "low" || (r.nameSim >= 0.5 && r.nameSim < 0.65))
    .map((r) => ({
      censusRecordId: r.censusRow.recordId,
      censusName: r.censusRow.name,
      regionalName: r.regionalHotel.name,
      propertyId: r.regionalHotel.propertyId,
      propertyUrl: r.regionalHotel.propertyUrl,
      score: r.score,
      nameSim: r.nameSim,
      confidence: r.confidence,
    }));
}
