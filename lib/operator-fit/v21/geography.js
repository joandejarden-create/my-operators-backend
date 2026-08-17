/**
 * Operator Fit v2.1 — Geography Relevance Score (0–100) inside weight-22 factor.
 * Formula: balanced_depth_v1
 */

import {
  MARKET_PRESENCE_TYPE,
  normalizePresenceType,
  establishesCurrentGeographicEligibility,
} from "../../operator-intelligence/market-presence.js";
import { listValue, scalarValue } from "../adapters/field-state.js";
import {
  V21_PRESENCE_STRENGTH_SCORES,
  V21_GEO_DEPTH,
  V21_GEOGRAPHY_FORMULA_ID,
} from "./config.js";

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function countryRows(records, country) {
  const c = norm(country);
  if (!c) return [];
  return (records || []).filter(
    (r) =>
      r?.country &&
      (norm(r.country) === c || norm(r.country).includes(c) || c.includes(norm(r.country)))
  );
}

/**
 * @returns {{ score: number, breakdown: object, positiveEvidence: string[], negativeEvidence: string[], unknownNotes: string[], state: string }}
 */
export function calculateGeographyRelevanceScore(project, operator) {
  const country = scalarValue(project.geography?.country);
  const city = scalarValue(project.geography?.city);
  const presenceRecords =
    operator.geography?.marketPresence ||
    operator.geography?.presenceRecords ||
    operator.marketPresence ||
    [];
  const opCountries = listValue(operator.geography?.countries);
  const opMarkets = listValue(operator.geography?.markets);

  if (!country && !city) {
    return {
      score: 0,
      state: "unknown",
      breakdown: { formula: V21_GEOGRAPHY_FORMULA_ID, reason: "project_geo_unknown" },
      positiveEvidence: [],
      negativeEvidence: [],
      unknownNotes: ["Project geography is unknown."],
    };
  }
  if (!presenceRecords.length && !opCountries.length && !opMarkets.length) {
    return {
      score: 0,
      state: "unknown",
      breakdown: { formula: V21_GEOGRAPHY_FORMULA_ID, reason: "operator_geo_unknown" },
      positiveEvidence: [],
      negativeEvidence: [],
      unknownNotes: ["Operator geographic coverage is unknown."],
    };
  }

  const rows = countryRows(presenceRecords, country);
  const types = rows.map((r) => normalizePresenceType(r.presenceType));
  const strongTypes = types.filter((t) => establishesCurrentGeographicEligibility(t));
  const bestType =
    strongTypes[0] ||
    types.find((t) => t === MARKET_PRESENCE_TYPE.ACTIVE_DEVELOPMENT) ||
    types[0] ||
    null;

  let presenceStrength = bestType ? V21_PRESENCE_STRENGTH_SCORES[bestType] ?? 0 : 0;
  const positive = [];
  const negative = [];

  // Operating depth from property counts on strong presence rows
  const currentPropCount = rows.filter((r) => {
    const t = normalizePresenceType(r.presenceType);
    return (
      t === MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY ||
      t === MARKET_PRESENCE_TYPE.CURRENT_OPERATING_PORTFOLIO
    );
  }).length;
  // If portfolio type without count, treat as multi when type is Current Operating Portfolio
  const portfolioType = types.includes(MARKET_PRESENCE_TYPE.CURRENT_OPERATING_PORTFOLIO);
  const multi =
    currentPropCount >= 2 ||
    (portfolioType && currentPropCount >= 1) ||
    rows.some((r) => Number(r.propertyCount || r.count || 0) >= 2);

  let depthBonus = 0;
  if (multi && strongTypes.length) {
    depthBonus += V21_GEO_DEPTH.multipleCurrentProperties;
    positive.push(`Multiple / portfolio current operations in ${country}`);
  } else if (strongTypes.includes(MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY) || currentPropCount === 1) {
    depthBonus += V21_GEO_DEPTH.singleCurrentProperty;
    positive.push(`Current operating presence in ${country}`);
  } else if (strongTypes.includes(MARKET_PRESENCE_TYPE.REGIONAL_OFFICE_OR_TEAM)) {
    depthBonus += V21_GEO_DEPTH.regionalOfficeWithCountryOps;
    positive.push(`Regional office / team in ${country}`);
  }

  const cityHit =
    city &&
    (opMarkets.some((m) => norm(m).includes(norm(city)) || norm(city).includes(norm(m))) ||
      rows.some(
        (r) =>
          r.city &&
          (norm(r.city).includes(norm(city)) || norm(city).includes(norm(r.city)))
      ));
  if (cityHit) {
    depthBonus += V21_GEO_DEPTH.cityMatch;
    positive.push(`Market-level presence aligns with ${city}`);
  }

  // Currentness: historical/strategic alone already low presenceStrength
  if (bestType === MARKET_PRESENCE_TYPE.HISTORICAL_PRESENCE) {
    negative.push("Presence is historical — not equal to current operations");
  }
  if (
    bestType === MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST ||
    bestType === MARKET_PRESENCE_TYPE.CLAIMED_CAPABILITY
  ) {
    negative.push(`${bestType} does not establish current operating depth`);
  }

  // Fallback when no presence rows: Active Countries / markets (interim)
  let usedFallback = false;
  if (!rows.length) {
    usedFallback = true;
    const countryHit =
      country &&
      (opCountries.some((c) => norm(c) === norm(country) || norm(c).includes(norm(country))) ||
        opMarkets.some((m) => norm(m).includes(norm(country))));
    if (cityHit) {
      presenceStrength = 90;
      positive.push(`Active market overlap: ${city} (countries/markets fallback)`);
    } else if (countryHit) {
      presenceStrength = 78;
      positive.push(`Active country: ${country} (countries/markets fallback)`);
    } else {
      presenceStrength = 12;
      negative.push("No documented geographic overlap with the project market");
    }
  } else if (bestType) {
    positive.push(`Market Presence: ${bestType} in ${country}`);
  }

  let score = Math.max(0, Math.min(100, Math.round(presenceStrength + depthBonus)));
  // Cap strategic/claimed/historical even with bonuses
  if (
    bestType === MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST ||
    bestType === MARKET_PRESENCE_TYPE.CLAIMED_CAPABILITY
  ) {
    score = Math.min(score, 22);
  }
  if (bestType === MARKET_PRESENCE_TYPE.HISTORICAL_PRESENCE) {
    score = Math.min(score, 40);
  }

  return {
    score,
    state: "known",
    breakdown: {
      formula: V21_GEOGRAPHY_FORMULA_ID,
      presenceStrength,
      depthBonus,
      bestType,
      currentPropCount,
      usedFallback,
      cityHit: Boolean(cityHit),
    },
    positiveEvidence: positive,
    negativeEvidence: negative,
    unknownNotes: [],
  };
}
