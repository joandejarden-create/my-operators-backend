/**
 * AI Visibility geography model v1.
 *
 * Canonical sources (read-only reuse — no parallel taxonomy):
 * - Country → radar subregion: lib/radar-buildout/country-configs.js (Caribbean, North America,
 *   Central America, South America for CALA buildout countries)
 * - Commercial Region for those countries: CALA (Caribbean & Latin America) — product/Explorer convention
 * - Dealality Market / Submarket: Hotel Census / Radar (market-level; not invented here)
 *
 * Gaps: Europe / US-Canada-as-North-America commercial regions are not in COUNTRY_CONFIGS.
 * Global is an explicit prompt scope — never an average of regions.
 */

import { COUNTRY_CONFIGS } from "../radar-buildout/country-configs.js";
import {
  resolveCommercialRegionForCountry,
  HEADLINE_REGION_METRIC_COHORT_RULE,
} from "./commercial-geography.js";

export const GEOGRAPHY_MODEL_VERSION = "ai_visibility_geography_v1";

export const GEOGRAPHY_SCOPES = Object.freeze([
  "global",
  "region",
  "subregion",
  "country",
  "market",
]);

/** Product commercial region for all Radar CALA buildout countries. */
export const CALA_COMMERCIAL_REGION = "CALA";

export { HEADLINE_REGION_METRIC_COHORT_RULE };

/**
 * Country display name → radar subregion + commercial region.
 * Commercial region uses AI Visibility commercial geography (CALA/Europe/NA).
 */
export function resolveCountryGeography(countryName) {
  const commercial = resolveCommercialRegionForCountry(countryName);
  if (!commercial.countryName) {
    return { countryName: null, radarSubregion: null, commercialRegion: null };
  }
  if (!commercial.known) {
    return {
      countryName: commercial.countryName,
      radarSubregion: null,
      commercialRegion: null,
    };
  }
  // Prefer radar subregion from COUNTRY_CONFIGS when CALA
  let radarSubregion = commercial.radarSubregion;
  if (!radarSubregion && COUNTRY_CONFIGS[commercial.countryName]) {
    radarSubregion = COUNTRY_CONFIGS[commercial.countryName].region || null;
  }
  return {
    countryName: commercial.countryName,
    radarSubregion,
    commercialRegion: commercial.commercialRegion,
  };
}

/**
 * Normalize prompt geography into structural scope fields.
 * Prefer explicit prompt fields; do not invent Europe/Global without metadata.
 * @param {object} prompt
 */
export function normalizePromptGeography(prompt = {}) {
  const countryRaw = prompt.country || null;
  const regionRaw = prompt.region || null;
  const geographyRaw = prompt.geography || null;
  const marketRaw = prompt.market || prompt.marketName || null;

  let geographyScope = prompt.geographyScope || null;
  let regionName = regionRaw;
  let subregionName = prompt.subregion || prompt.subregionName || null;
  let countryName = countryRaw;
  let marketName = marketRaw;

  const countryResolved = countryName ? resolveCountryGeography(countryName) : null;
  if (countryResolved?.countryName) {
    countryName = countryResolved.countryName;
    if (!subregionName && countryResolved.radarSubregion) {
      subregionName = countryResolved.radarSubregion;
    }
    if (!regionName && countryResolved.commercialRegion) {
      regionName = countryResolved.commercialRegion;
    }
  }

  // Latin America / Caribbean labels on prompts → region CALA (product)
  const geoLabel = String(geographyRaw || "").trim().toLowerCase();
  const regionLabel = String(regionRaw || "").trim().toLowerCase();
  if (geoLabel === "caribbean" || regionLabel === "caribbean") {
    if (!regionName) regionName = CALA_COMMERCIAL_REGION;
    if (!subregionName) subregionName = "Caribbean";
  } else if (!regionName) {
    if (geoLabel === "latin america" || geoLabel === "latam" || geoLabel === "cala") {
      regionName = CALA_COMMERCIAL_REGION;
    } else if (regionLabel === "latin america" || regionLabel === "latam" || regionLabel === "cala") {
      regionName = CALA_COMMERCIAL_REGION;
    } else if (geoLabel === "global" || geoLabel === "worldwide") {
      regionName = null;
    }
  }

  if (!geographyScope) {
    if (marketName) geographyScope = "market";
    else if (countryName) geographyScope = "country";
    // Prefer subregion when Caribbean (or other) subregion is explicit and no country
    else if (subregionName && !countryName) geographyScope = "subregion";
    else if (regionName) geographyScope = "region";
    else if (geoLabel === "global" || geoLabel === "worldwide") geographyScope = "global";
    else geographyScope = "unknown";
  }

  return {
    geographyModelVersion: GEOGRAPHY_MODEL_VERSION,
    geographyScope,
    regionId: null,
    regionName: regionName || null,
    subregionId: null,
    subregionName: subregionName || null,
    countryCode: null,
    countryName: countryName || null,
    marketId: null,
    marketName: marketName || null,
    sourceLabels: {
      geography: geographyRaw || null,
      region: regionRaw || null,
      country: countryRaw || null,
    },
  };
}

/**
 * Filter observations by geography cohort.
 * Global scope = only observations with geographyScope === "global" (never a rollup average).
 * Region scope = regionName match OR (optional) country rollup into that region.
 */
export function filterObservationsByGeography(observations, filter = {}) {
  const list = Array.isArray(observations) ? observations : [];
  const scope = filter.geographyScope || null;
  const region = filter.region || filter.regionName || null;
  const subregion = filter.subregion || filter.subregionName || null;
  const country = filter.country || filter.countryName || null;
  const market = filter.market || filter.marketName || null;
  const allowCountryRollup = filter.allowCountryRollup !== false;

  return list.filter((o) => {
    const g = o.geography || {};
    if (scope === "global") {
      return g.geographyScope === "global";
    }
    if (market) {
      return String(g.marketName || "").toLowerCase() === String(market).toLowerCase();
    }
    if (country) {
      return String(g.countryName || "").toLowerCase() === String(country).toLowerCase();
    }
    if (subregion) {
      return String(g.subregionName || "").toLowerCase() === String(subregion).toLowerCase();
    }
    if (region) {
      if (String(g.regionName || "").toLowerCase() === String(region).toLowerCase()) {
        return true;
      }
      if (allowCountryRollup && g.countryName) {
        const mapped = resolveCountryGeography(g.countryName);
        return (
          String(mapped.commercialRegion || "").toLowerCase() ===
          String(region).toLowerCase()
        );
      }
      return false;
    }
    if (scope && g.geographyScope !== scope) return false;
    return true;
  });
}

/**
 * Internal peer-set shape (no Airtable table yet).
 */
export function buildPeerSetDescriptor(args = {}) {
  return {
    peerSetId: args.peerSetId || null,
    name: args.name || null,
    entityType: args.entityType || null,
    geographyScope: args.geographyScope || null,
    region: args.region || args.regionName || null,
    chainScale: args.chainScale || null,
    entityIds: [...(args.entityIds || [])],
  };
}

/**
 * Calculate visibility metrics over a geography-filtered cohort (same formulas).
 */
export function calculateVisibilityMetrics(args = {}) {
  const {
    entityId,
    peerSetId = null,
    geographyScope = null,
    region = null,
    subregion = null,
    country = null,
    market = null,
    intentTerritory = null,
    observations = [],
    computeAiPresenceRate,
    computeRecommendationShare,
    computeFirstRecommendationRate,
    computeQuestionsWon,
    computeQuestionsMissing,
    computeCompetitivePosition,
    computeCitationRate,
  } = args;

  let cohort = filterObservationsByGeography(observations, {
    geographyScope,
    region,
    subregion,
    country,
    market,
  });
  if (intentTerritory) {
    cohort = cohort.filter(
      (o) =>
        !o.intentTerritory ||
        String(o.intentTerritory).toLowerCase() === String(intentTerritory).toLowerCase()
    );
  }

  const promptIds = [...new Set(cohort.map((o) => o.promptId).filter(Boolean))];
  const peerIds =
    args.peerEntityIds ||
    (peerSetId && args.peerSets
      ? args.peerSets.find((p) => p.peerSetId === peerSetId)?.entityIds
      : null) ||
    null;

  return {
    geographyModelVersion: GEOGRAPHY_MODEL_VERSION,
    filter: { geographyScope, region, subregion, country, market, intentTerritory },
    observationCount: cohort.length,
    presence: computeAiPresenceRate?.(cohort, entityId) ?? null,
    recommendationShare: computeRecommendationShare?.(cohort, entityId) ?? null,
    firstRecommendationRate: computeFirstRecommendationRate?.(cohort, entityId) ?? null,
    questionsWon: computeQuestionsWon?.(cohort, entityId, promptIds) ?? null,
    questionsMissing: computeQuestionsMissing?.(cohort, entityId, promptIds) ?? null,
    citationRate: computeCitationRate?.(cohort, entityId) ?? null,
    competitivePosition: peerIds
      ? computeCompetitivePosition?.(cohort, peerIds) ?? null
      : null,
  };
}

export function auditCanonicalGeographySources() {
  const countries = Object.keys(COUNTRY_CONFIGS).sort();
  const radarSubregions = [
    ...new Set(countries.map((c) => COUNTRY_CONFIGS[c].region).filter(Boolean)),
  ].sort();
  return {
    CANONICAL_GEOGRAPHY_SOURCE:
      "lib/radar-buildout/country-configs.js (+ product CALA commercial region convention)",
    GEOGRAPHY_SCOPES_AVAILABLE: GEOGRAPHY_SCOPES,
    REGIONS_FOUND: [CALA_COMMERCIAL_REGION],
    SUBREGIONS_FOUND: radarSubregions,
    COUNTRY_MAPPING_SOURCE: "COUNTRY_CONFIGS[country].region → radar subregion; commercial Region=CALA",
    MARKET_MAPPING_SOURCE:
      "Dealality Hotel Census / Radar Market + corridor Submarket (not wired into AI Visibility prompts yet)",
    COUNTRY_COUNT: countries.length,
    GAPS: [
      "Europe commercial region not in COUNTRY_CONFIGS",
      "North America (US/Canada) commercial region not in COUNTRY_CONFIGS (Mexico radar subregion is North America but product Region=CALA)",
      "No explicit Global prompt in Phase 2A cohort",
      "Market-level AI Visibility prompts not yet defined",
    ],
  };
}
