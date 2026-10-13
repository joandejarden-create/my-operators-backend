/**
 * Hotel ↔ Demand Generator fit + relevance classification.
 * No opaque score-only; persist factor rationale.
 * No hotel/city hardcodes.
 */

import { createHash } from "node:crypto";
import {
  FIT_ID_PREFIX,
  GENERATOR_RELEVANCE_CLASS,
  GENERATOR_PRIORITY,
  GENERATOR_STATUS,
  RESEARCH_CADENCE_DAYS,
  DG_SCHEMA_VERSION,
} from "./constants.js";

function clean(s) {
  return String(s || "").trim();
}

function scoreFactor(value) {
  const v = clean(value).toUpperCase();
  if (v === "HIGH" || v === "STRONG" || v === "CORE") return 3;
  if (v === "MEDIUM" || v === "MODERATE" || v === "COMPETITIVE") return 2;
  if (v === "LOW" || v === "WEAK" || v === "STRETCH") return 1;
  if (v === "NONE" || v === "OUTSIDE") return 0;
  return 1;
}

export function computeHotelGeneratorFitId({ hotelId, demandGeneratorId } = {}) {
  const seed = `${clean(hotelId)}|${clean(demandGeneratorId)}`;
  return `${FIT_ID_PREFIX}${createHash("sha256").update(seed).digest("hex").slice(0, 14)}`;
}

/**
 * Evaluate explicit relevance factors → class + priority + rationale.
 */
export function evaluateGeneratorRelevance({
  generatorRelevance = "MEDIUM",
  recurrence = "UNKNOWN",
  marketRelevance = "MEDIUM",
  likelyTravelCreation = "MEDIUM",
  programObservability = "MEDIUM",
  buyerAccessibility = "MEDIUM",
  evidenceQuality = "MEDIUM",
} = {}) {
  const factors = {
    generatorRelevance: clean(generatorRelevance).toUpperCase() || "MEDIUM",
    recurrence: clean(recurrence).toUpperCase() || "UNKNOWN",
    marketRelevance: clean(marketRelevance).toUpperCase() || "MEDIUM",
    likelyTravelCreation: clean(likelyTravelCreation).toUpperCase() || "MEDIUM",
    programObservability: clean(programObservability).toUpperCase() || "MEDIUM",
    buyerAccessibility: clean(buyerAccessibility).toUpperCase() || "MEDIUM",
    evidenceQuality: clean(evidenceQuality).toUpperCase() || "MEDIUM",
  };

  const total =
    scoreFactor(factors.generatorRelevance) +
    scoreFactor(factors.marketRelevance) +
    scoreFactor(factors.likelyTravelCreation) +
    scoreFactor(factors.programObservability) +
    scoreFactor(factors.buyerAccessibility) +
    scoreFactor(factors.evidenceQuality) +
    (factors.recurrence === "RECURRING_CONFIRMED"
      ? 3
      : factors.recurrence === "RECURRING_HISTORICAL" ||
          factors.recurrence === "POSSIBLE_RECURRING"
        ? 2
        : factors.recurrence === "ONE_TIME"
          ? 0
          : 1);

  let relevanceClass = GENERATOR_RELEVANCE_CLASS.WATCH_GENERATOR;
  let generatorPriority = GENERATOR_PRIORITY.WATCH;
  if (total >= 18) {
    relevanceClass = GENERATOR_RELEVANCE_CLASS.HIGH_VALUE_GENERATOR;
    generatorPriority = GENERATOR_PRIORITY.HIGH;
  } else if (total >= 14) {
    relevanceClass = GENERATOR_RELEVANCE_CLASS.MEDIUM_VALUE_GENERATOR;
    generatorPriority = GENERATOR_PRIORITY.MEDIUM;
  } else if (total <= 8) {
    relevanceClass = GENERATOR_RELEVANCE_CLASS.LOW_VALUE_GENERATOR;
    generatorPriority = GENERATOR_PRIORITY.LOW;
  }

  const rationaleParts = [
    `market=${factors.marketRelevance}`,
    `travel=${factors.likelyTravelCreation}`,
    `recurrence=${factors.recurrence}`,
    `observability=${factors.programObservability}`,
    `buyer=${factors.buyerAccessibility}`,
    `evidence=${factors.evidenceQuality}`,
    `sum=${total}`,
  ];

  return {
    factors,
    factorScoreSum: total,
    relevanceClass,
    generatorPriority,
    fitRationale: rationaleParts.join("; "),
  };
}

export function buildHotelDemandGeneratorFit(input = {}) {
  const hotelId = clean(input.hotelId);
  const demandGeneratorId = clean(input.demandGeneratorId);
  if (!hotelId || !demandGeneratorId) {
    const err = new Error("hotel_generator_fit_requires_ids");
    err.code = "hotel_generator_fit_requires_ids";
    throw err;
  }
  const evaluated = evaluateGeneratorRelevance(input.factors || input);
  const now = new Date().toISOString();
  return {
    fitId:
      clean(input.fitId) ||
      computeHotelGeneratorFitId({ hotelId, demandGeneratorId }),
    hotelId,
    hotelName: clean(input.hotelName) || null,
    demandGeneratorId,
    organizationName: clean(input.organizationName) || null,
    marketRelevance:
      evaluated.factors.marketRelevance || clean(input.marketRelevance) || null,
    productFit: clean(input.productFit).toUpperCase() || "MEDIUM",
    travelDemandPotential:
      evaluated.factors.likelyTravelCreation ||
      clean(input.travelDemandPotential) ||
      null,
    recurrencePotential:
      evaluated.factors.recurrence || clean(input.recurrencePotential) || null,
    buyerAccessibility:
      evaluated.factors.buyerAccessibility ||
      clean(input.buyerAccessibility) ||
      null,
    generatorPriority:
      input.generatorPriority || evaluated.generatorPriority,
    relevanceClass: input.relevanceClass || evaluated.relevanceClass,
    factors: evaluated.factors,
    fitRationale: clean(input.fitRationale) || evaluated.fitRationale,
    lastEvaluatedAt: input.lastEvaluatedAt || now,
    firstEvaluatedAt: input.firstEvaluatedAt || now,
    schemaVersion: DG_SCHEMA_VERSION,
  };
}

export function suggestNextResearchAt(generatorStatus, fromDate = new Date()) {
  const days =
    RESEARCH_CADENCE_DAYS[generatorStatus] ||
    RESEARCH_CADENCE_DAYS[GENERATOR_STATUS.VERIFIED];
  const d = new Date(fromDate);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString();
}

/**
 * Map hotel profile segments → likely organization types (portable).
 */
export function organizationTypesForHotelProfile(hotelProfile = {}) {
  const segments = [
    ...(hotelProfile.commercialPriorities?.targetSegments || []),
    ...(hotelProfile.commercialPriorities?.demandAnchorFocus || []),
  ]
    .map((s) => String(s || "").toLowerCase())
    .join(" ");
  const types = new Set();
  if (/associat/.test(segments)) types.add("ASSOCIATION");
  if (/medical|health|scientific|nih/.test(segments)) {
    types.add("HOSPITAL_HEALTH_SYSTEM");
    types.add("RESEARCH_INSTITUTION");
    types.add("PROFESSIONAL_BODY");
  }
  if (/government|federal|contractor/.test(segments)) {
    types.add("GOVERNMENT_AGENCY");
    types.add("GOVERNMENT_CONTRACTOR");
  }
  if (/corporate|employer/.test(segments)) types.add("LARGE_EMPLOYER");
  if (/universit|education/.test(segments)) types.add("UNIVERSITY");
  if (/sport/.test(segments)) types.add("SPORTS_ORGANIZATION");
  if (/social|nonprofit/.test(segments)) types.add("NONPROFIT");
  if (/consult/.test(segments)) types.add("CONSULTING_FIRM");
  if (/training/.test(segments)) types.add("TRAINING_PROVIDER");
  if (types.size === 0) {
    types.add("ASSOCIATION");
    types.add("LARGE_EMPLOYER");
    types.add("TRAINING_PROVIDER");
  }
  return [...types];
}
