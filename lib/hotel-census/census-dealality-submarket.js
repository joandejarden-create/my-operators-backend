/**
 * Dealality corridor resolution for Hotel Census `Submarket` backfill.
 * Replaces STR *Regional buckets and normalizes city-level labels to radar corridors.
 */

import { CENSUS_FIELDS } from "./fields.js";
import {
  GEOGRAPHY_SOURCE,
  isBlankGeoValue,
  COUNTRY_TO_SUB_CONTINENT,
} from "./geography-enrichment-contract.js";
import {
  getSubmarketOptionsForCountry,
  normalizeSubmarketLabel,
} from "../radar-submarket.js";
import {
  inferTravelInfrastructureSubmarket,
  inferCensusSubmarketCorridor,
  meetsMinConfidence,
} from "../radar-buildout/travel-infrastructure-submarket-inference.js";
import {
  resolveCensusCountryKey,
  resolveStrSubmarketToCorridor,
} from "./census-str-submarket-corridors.js";

/** Airtable column updated by backfill scripts. */
export const CENSUS_SUBMARKET_BACKFILL_FIELD = CENSUS_FIELDS.submarket;

const CONFIDENCE_RANK = { High: 3, Medium: 2, Low: 1, "No Match": 0 };

/** CALA countries for audit scope (Sub-Continent map + Mexico). */
export const CALA_CENSUS_COUNTRIES = [
  ...new Set(Object.keys(COUNTRY_TO_SUB_CONTINENT)),
];

/**
 * @param {string} [value]
 */
export function isStrRegionalSubmarket(value) {
  const label = String(value || "").trim();
  if (!label) return false;
  return /\bregional$/i.test(label);
}

/**
 * @param {string} country
 */
export function isCalaCensusCountry(country) {
  const key = String(country || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/&/g, "and");
  if (!key) return false;
  return CALA_CENSUS_COUNTRIES.some(
    (name) =>
      name.toLowerCase().replace(/&/g, "and") === key ||
      key.includes(name.toLowerCase())
  );
}

/**
 * @param {string} current
 * @param {string} country
 * @param {string[]} optionsForCountry
 */
function isCanonicalCorridor(current, country, optionsForCountry) {
  const label = String(current || "").trim();
  if (!label || label === "Other") return false;
  return optionsForCountry.includes(label);
}

/**
 * @param {object} row
 * @param {object} [options]
 * @param {boolean} [options.force]
 * @param {boolean} [options.overwriteRegional] — replace STR *Regional in Submarket
 * @param {boolean} [options.normalizeLabels] — replace city/STR labels with canonical corridor
 * @param {string} [options.minConfidence] — High | Medium | Low
 */
export function proposeCensusSubmarketCorridor(row, options = {}) {
  const country = resolveCensusCountryKey(
    String(row[CENSUS_FIELDS.country] ?? row.country ?? "").trim()
  );
  const city = String(row[CENSUS_FIELDS.city] ?? row.city ?? "").trim();
  const name = String(row[CENSUS_FIELDS.name] ?? row.name ?? "").trim();
  const market = String(row[CENSUS_FIELDS.market] ?? row.market ?? "").trim();
  const current = String(row[CENSUS_FIELDS.submarket] ?? row.Submarket ?? "").trim();
  const minConfidence = options.minConfidence || "Medium";

  const base = {
    submarket: null,
    confidence: "No Match",
    reason: "no_match",
    source: null,
    skipped: true,
  };

  if (!country) {
    return { ...base, reason: "missing_country", skipped: true };
  }

  const optionsForCountry = getSubmarketOptionsForCountry(country);
  if (!optionsForCountry.length || (optionsForCountry.length === 1 && optionsForCountry[0] === "Other")) {
    return { ...base, reason: "country_not_in_corridor_registry", skipped: true };
  }

  const regional = isStrRegionalSubmarket(current);
  const canonical = isCanonicalCorridor(current, country, optionsForCountry);

  if (canonical && !options.force && !regional) {
    if (!options.normalizeLabels) {
      return {
        submarket: current,
        confidence: "High",
        reason: "existing_canonical_corridor",
        source: "existing",
        skipped: true,
      };
    }
    const normalized = normalizeSubmarketLabel(current, country);
    if (!normalized || normalized === "Other" || normalized === current) {
      return {
        submarket: current,
        confidence: "High",
        reason: "already_canonical",
        source: "existing",
        skipped: true,
      };
    }
  }

  if (!isBlankGeoValue(current) && !options.force) {
    if (regional && !options.overwriteRegional) {
      return { ...base, reason: "regional_requires_overwrite_flag", skipped: true };
    }
    if (!regional && !options.normalizeLabels && !isBlankGeoValue(current)) {
      return { ...base, reason: "already_populated", skipped: true };
    }
    if (!regional && options.overwriteRegional && !options.normalizeLabels) {
      return { ...base, reason: "overwrite_regional_only_skipped", skipped: true };
    }
  }

  /** @type {{ submarket: string, confidence: string, reason: string, source: string } | null} */
  let hit = null;

  if (current && !regional && options.normalizeLabels) {
    const normalized = normalizeSubmarketLabel(current, country);
    if (normalized && normalized !== "Other" && optionsForCountry.includes(normalized)) {
      hit = {
        submarket: normalized,
        confidence: "Medium",
        reason: "str_submarket_normalized_to_corridor",
        source: GEOGRAPHY_SOURCE.dealalitySubmarketStrNormalized,
      };
    }
  }

  if (!hit && current && !regional && !options.normalizeLabels) {
    const normalized = normalizeSubmarketLabel(current, country);
    if (normalized && normalized !== "Other" && optionsForCountry.includes(normalized)) {
      hit = {
        submarket: normalized,
        confidence: "Medium",
        reason: "str_submarket_normalized_to_corridor",
        source: GEOGRAPHY_SOURCE.dealalitySubmarketStrNormalized,
      };
    }
  }

  if (!hit) {
    const inferred = inferCensusSubmarketCorridor(
      {
        id: row.id || row.recordId || "",
        name,
        city,
        country,
        submarket: regional ? "" : current,
        notes: "",
      },
      { minConfidence }
    );
    if (inferred.inferredSubmarket && meetsMinConfidence(inferred.confidence, minConfidence)) {
      const source =
        inferred.reason === "exact_city_map" || inferred.reason === "pr_city_inference_map"
          ? GEOGRAPHY_SOURCE.dealalitySubmarketCityMap
          : GEOGRAPHY_SOURCE.dealalitySubmarketKeyword;
      hit = {
        submarket: inferred.inferredSubmarket,
        confidence: inferred.confidence,
        reason: inferred.reason,
        source,
      };
    }
  }

  if (!hit) {
    const inferred = inferTravelInfrastructureSubmarket(
      {
        id: row.id || row.recordId || "",
        name,
        city,
        country,
        submarket: regional ? "" : current,
        notes: "",
      },
      { force: true, minConfidence }
    );
    if (
      inferred.inferredSubmarket &&
      meetsMinConfidence(inferred.confidence, minConfidence)
    ) {
      const source =
        inferred.reason === "exact_city_map" || inferred.reason === "pr_city_inference_map"
          ? GEOGRAPHY_SOURCE.dealalitySubmarketCityMap
          : GEOGRAPHY_SOURCE.dealalitySubmarketKeyword;
      hit = {
        submarket: inferred.inferredSubmarket,
        confidence: inferred.confidence,
        reason: inferred.reason,
        source,
      };
    }
  }

  if (!hit) {
    const fromStr = resolveStrSubmarketToCorridor({
      country,
      strSubmarket: current,
      city,
      name,
      market,
    });
    if (fromStr?.submarket && meetsMinConfidence(fromStr.confidence, minConfidence)) {
      hit = {
        submarket: fromStr.submarket,
        confidence: fromStr.confidence,
        reason: fromStr.reason,
        source: fromStr.source,
      };
    }
  }

  if (!hit && options.assignUnmappedOther) {
    const optionsForOther = getSubmarketOptionsForCountry(country);
    if (optionsForOther.includes("Other")) {
      hit = {
        submarket: "Other",
        confidence: "Low",
        reason: "unmapped_fallback_other",
        source: GEOGRAPHY_SOURCE.dealalitySubmarketKeyword,
      };
    }
  }

  if (!hit?.submarket) {
    return { ...base, reason: "no_corridor_match", skipped: false };
  }

  if (current === hit.submarket) {
    return {
      submarket: current,
      confidence: hit.confidence,
      reason: "unchanged",
      source: "existing",
      skipped: true,
    };
  }

  return {
    submarket: hit.submarket,
    confidence: hit.confidence,
    reason: hit.reason,
    source: hit.source,
    skipped: false,
  };
}

/** @deprecated Use proposeCensusSubmarketCorridor */
export const proposeCensusDealalitySubmarket = proposeCensusSubmarketCorridor;

/**
 * @param {Record<string, string>} fields
 */
export function validateSubmarketCorridorProposal(fields, country = "") {
  const errors = [];
  const value = fields[CENSUS_SUBMARKET_BACKFILL_FIELD];
  if (!value) return { pass: true, errors };
  const countryKey = resolveCensusCountryKey(country);
  const options = getSubmarketOptionsForCountry(countryKey);
  if (!options.includes(value)) {
    errors.push(
      `Submarket "${value}" is not in corridor registry for ${countryKey || country || "country"}`
    );
  }
  return { pass: errors.length === 0, errors };
}

/** @deprecated */
export const validateDealalitySubmarketProposal = validateSubmarketCorridorProposal;

/**
 * Client/server display helper when Submarket still holds STR regional bucket.
 * @param {object} row
 */
export function resolveEffectiveSubmarket(row) {
  const current = String(row.submarket ?? row[CENSUS_FIELDS.submarket] ?? "").trim();
  if (current && !isStrRegionalSubmarket(current)) {
    const country = String(row.country ?? row[CENSUS_FIELDS.country] ?? "").trim();
    const normalized = normalizeSubmarketLabel(current, country);
    if (normalized && normalized !== "Other") return normalized;
    return current;
  }
  const proposal = proposeCensusSubmarketCorridor(
    {
      country: row.country ?? row[CENSUS_FIELDS.country],
      city: row.city ?? row[CENSUS_FIELDS.city],
      name: row.name ?? row[CENSUS_FIELDS.name],
      submarket: current,
    },
    { overwriteRegional: true, minConfidence: "Medium" }
  );
  return proposal.submarket || current || "";
}

/** @deprecated */
export const resolveCensusDealalitySubmarketForDisplay = resolveEffectiveSubmarket;

export { CONFIDENCE_RANK };
