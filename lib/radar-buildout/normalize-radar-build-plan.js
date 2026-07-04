/**
 * Normalize CALA Radar Build Plans Airtable records.
 */

import { RADAR_BUILD_PLANS_FIELDS as F } from "./airtable-radar-build-plans-fields.js";

function strVal(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  return String(v).trim();
}

function numVal(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseJsonField(v) {
  if (v == null || v === "") return null;
  if (typeof v === "object") return v;
  try {
    return JSON.parse(String(v));
  } catch {
    return null;
  }
}

function parseSubmarkets(v) {
  if (Array.isArray(v)) return v.map((x) => strVal(x)).filter(Boolean);
  const s = strVal(v);
  if (!s) return [];
  if (s.startsWith("[")) {
    const parsed = parseJsonField(s);
    if (Array.isArray(parsed)) return parsed.map((x) => strVal(x)).filter(Boolean);
  }
  return s.split(/\n|;|,/).map((x) => x.trim()).filter(Boolean);
}

/**
 * @param {{ id?: string, fields?: Record<string, unknown> } | null | undefined} record
 */
export function normalizeRadarBuildPlan(record) {
  const f = record?.fields || {};
  return {
    id: record?.id || "",
    country: strVal(f[F.country]),
    region: strVal(f[F.region]),
    buildStrategy: strVal(f[F.buildStrategy]),
    priorityTier: strVal(f[F.priorityTier]),
    buildStatus: strVal(f[F.buildStatus]),
    targets: {
      demandAnchors: numVal(f[F.targetDemandAnchors]),
      travelInfrastructure: numVal(f[F.targetTravelInfrastructure]),
      totalRadarPoints: numVal(f[F.targetTotalRadarPoints]),
    },
    current: {
      demandAnchors: numVal(f[F.currentDemandAnchors]),
      travelInfrastructure: numVal(f[F.currentTravelInfrastructure]),
      totalRadarPoints: numVal(f[F.currentTotalRadarPoints]),
    },
    submarkets: parseSubmarkets(f[F.submarketsCorridors]),
    primaryHotelDemandProfile: strVal(f[F.primaryHotelDemandProfile]),
    coverage: {
      sourceCoveragePct: numVal(f[F.sourceCoveragePct]),
      coordinateCoveragePct: numVal(f[F.coordinateCoveragePct]),
      dataConfidenceMix: parseJsonField(f[F.dataConfidenceMix]) || {},
    },
    lastBuildDate: strVal(f[F.lastBuildDate]),
    lastQaDate: strVal(f[F.lastQaDate]),
    nextRecommendedAction: strVal(f[F.nextRecommendedAction]),
    notes: strVal(f[F.notes]),
    recommendedBuildSequence: numVal(f[F.recommendedBuildSequence]),
    nextBuildMarket: strVal(f[F.nextBuildMarket]),
    buildApproachNotes: strVal(f[F.buildApproachNotes]),
    firstPassTargetDescription: strVal(f[F.firstPassTargetDescription]),
  };
}

/**
 * @param {object} plan — normalized or partial plan payload
 */
export function toApiCountryBuildPlan(plan) {
  return {
    ok: true,
    country: plan.country,
    region: plan.region,
    buildStrategy: plan.buildStrategy,
    priorityTier: plan.priorityTier,
    buildStatus: plan.buildStatus,
    targets: plan.targets,
    current: plan.current,
    coverage: plan.coverage,
    submarkets: plan.submarkets || [],
    primaryHotelDemandProfile: plan.primaryHotelDemandProfile || "",
    lastBuildDate: plan.lastBuildDate || "",
    lastQaDate: plan.lastQaDate || "",
    nextRecommendedAction: plan.nextRecommendedAction || "",
    notes: plan.notes || "",
    recommendedBuildSequence: plan.recommendedBuildSequence ?? null,
    nextBuildMarket: plan.nextBuildMarket || "",
    buildApproachNotes: plan.buildApproachNotes || "",
    firstPassTargetDescription: plan.firstPassTargetDescription || "",
  };
}
