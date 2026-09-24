/**
 * Build real multi-hotel Jev shadow cohorts from registry targets and/or
 * verified GDI opportunities (no fabrication).
 */

import { loadOpportunities } from "../repository.js";

export const HOTEL_ARCHETYPE = Object.freeze({
  recLuxvwwxID7U2B8: "suburban_full_service",
  recG66DQJKP2c0UNh: "urban_times_square",
  recIwaP1etgx2g9nA: "resort_destination",
});

export const HOTEL_IDS = Object.freeze({
  BETHESDA: "recLuxvwwxID7U2B8",
  RENAISSANCE: "recG66DQJKP2c0UNh",
  CAMBRIDGE: "recIwaP1etgx2g9nA",
});

function inferMissingFields(opp = {}) {
  const missing = [];
  const lodging =
    opp.roomDemandStatus ||
    opp.lodgingEvidence ||
    opp.roomDemandStatusLabel ||
    "";
  if (
    !lodging ||
    /UNKNOWN|NONE|UNCONFIRMED/i.test(String(lodging)) ||
    opp.opportunityType === "OVERFLOW_HOUSING"
  ) {
    if (opp.opportunityType === "OVERFLOW_HOUSING" || !lodging) {
      missing.push("lodgingEvidence");
    }
  }
  const fut = opp.futureCycleEvidenceState || "";
  if (/UNCONFIRMED|UNKNOWN|NONE/i.test(String(fut)) || !opp.eventStartDate) {
    missing.push("futureCycle");
  }
  if (!opp.eventLocationStatus || /UNKNOWN|UNVERIFIED/i.test(String(opp.eventLocationStatus))) {
    missing.push("geo");
  }
  const sources = Array.isArray(opp.sources) ? opp.sources : [];
  if (!sources.length) missing.push("source");
  return [...new Set(missing)];
}

function evidenceSummaryFromOpp(opp) {
  const bits = [
    `opportunityType=${opp.opportunityType || "n/a"}`,
    `demandType=${opp.demandType || "n/a"}`,
    `futureCycle=${opp.futureCycleEvidenceState || "n/a"}`,
    `roomDemand=${opp.roomDemandStatus || "n/a"}`,
    `venueSourcing=${opp.venueSourcingStatus || "n/a"}`,
    `geo=${opp.eventLocationStatus || "n/a"}`,
    `qualification=${opp.opportunityQualification || "n/a"}`,
    `priority=${opp.priority || "n/a"}`,
  ];
  return bits.join("; ").slice(0, 600);
}

function hardPolicyFromOpp(opp) {
  const h = {};
  if (opp.opportunityType === "CLOSED_DISQUALIFIED") h.PAST_EVENT = false;
  if (/PAST|HISTORICAL/i.test(String(opp.futureCycleEvidenceState || ""))) {
    h.PAST_EVENT = true;
  }
  if (/FULLY_PLACED|EXCLUSIVE/i.test(String(opp.venueSourcingStatus || ""))) {
    h.FULLY_PLACED = true;
  }
  if (opp.qualificationGatePassed === false && /PRIVACY/i.test(String(opp.qualificationFailureReason || ""))) {
    h.PRIVACY_BLOCK = true;
  }
  return h;
}

function playbookExistingFromOpp(opp) {
  if (opp.opportunityType === "OVERFLOW_HOUSING") return "LODGING_HOUSING";
  if (opp.opportunityType === "FUTURE_CYCLE" || opp.opportunityType === "FUTURE_WATCH") {
    return "EVENT_FUTURE_CYCLE";
  }
  if (opp.opportunityType === "PRIMARY_PURSUIT") return "DEMAND_GENERATOR";
  return "GENERAL_FOLLOWUP";
}

function priorityExistingFromOpp(opp) {
  const p = String(opp.priority || "");
  if (/HIGH/i.test(p)) return "RESEARCH_NOW";
  if (/LOW/i.test(p)) return "LOWER_PRIORITY";
  return "DEFER";
}

function cadenceExistingFromOpp(opp) {
  void opp;
  return "KEEP_CADENCE";
}

function followupExistingFromOpp(opp) {
  const missing = inferMissingFields(opp);
  if (missing.includes("lodgingEvidence")) return "LODGING";
  if (missing.includes("futureCycle")) return "FUTURE_CYCLE";
  if (missing.includes("geo")) return "GEO";
  if (missing.includes("source")) return "SOURCE";
  return "NONE";
}

/**
 * Derive shadow cohort targets from FS/Airtable-backed opportunities.
 * Strips hotel/org names from Jev context.
 */
export function deriveCohortFromOpportunities(hotelId, { limit = 25 } = {}) {
  const doc = loadOpportunities(hotelId);
  const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
  const archetype = HOTEL_ARCHETYPE[hotelId] || "unknown";
  const out = [];
  for (const opp of ops) {
    if (!opp || !opp.id) continue;
    if (opp.opportunityType === "CLOSED_DISQUALIFIED") continue;
    const missingFields = inferMissingFields(opp);
    const context = {
      targetType: "OPPORTUNITY_CANDIDATE",
      entityType: opp.demandType || opp.segment || "opportunity",
      programType: opp.demandType || null,
      opportunityType: opp.opportunityType || null,
      priority: opp.priority || "MEDIUM",
      futureDateStatus: opp.eventStartDate
        ? "CONFIRMED_FUTURE"
        : opp.futureCycleEvidenceState || "UNKNOWN",
      futureCycleEvidenceState: opp.futureCycleEvidenceState || null,
      lodgingEvidence: opp.roomDemandStatus || "unknown",
      roomDemandStatus: opp.roomDemandStatus || null,
      sourcingStatus: opp.venueSourcingStatus || opp.sourcingStatus || null,
      geoOk: /VERIFIED|CITY|MARKET/i.test(String(opp.eventLocationStatus || "")),
      geoEvidence: opp.eventLocationStatus || null,
      missingFields,
      sourceCount: Array.isArray(opp.sources) ? opp.sources.length : 0,
      sourceTypes: (Array.isArray(opp.sources) ? opp.sources : [])
        .map((s) => s.sourceType || s.supportsFact || "source")
        .slice(0, 5),
      evidenceSummary: evidenceSummaryFromOpp(opp),
      hardPolicyContext: hardPolicyFromOpp(opp),
      archetype,
      researchReason: "shadow_cohort",
      consecutiveNoChangeRuns: 0,
      lastResult: null,
      qualification: opp.opportunityQualification || null,
    };
    out.push({
      cohortSource: "gdi_opportunity",
      hotelId,
      targetId: `opp_${opp.id}`,
      opportunityId: opp.id,
      archetype,
      context,
      existing: {
        TARGET_RESEARCH_PRIORITY: priorityExistingFromOpp(opp),
        RESEARCH_PLAYBOOK: playbookExistingFromOpp(opp),
        FOLLOWUP_TYPE: followupExistingFromOpp(opp),
        GENERATOR_CADENCE: cadenceExistingFromOpp(opp),
      },
      policyContext: {
        pastEvent: Boolean(context.hardPolicyContext?.PAST_EVENT),
        fullyPlaced: Boolean(context.hardPolicyContext?.FULLY_PLACED),
        privacyReject: Boolean(context.hardPolicyContext?.PRIVACY_BLOCK),
      },
      researchOutcomeHint: {
        // Used for offline outcome comparison — not factual invention
        lodgingRelevant: opp.opportunityType === "OVERFLOW_HOUSING",
        futureCycleRelevant:
          opp.opportunityType === "FUTURE_CYCLE" ||
          opp.opportunityType === "FUTURE_WATCH",
      },
    });
    if (out.length >= limit) break;
  }
  return out;
}

/**
 * Map registry research targets into the same cohort shape.
 */
export function mapRegistryTargetsToCohort(hotelId, targets = [], { limit = 30 } = {}) {
  const archetype = HOTEL_ARCHETYPE[hotelId] || "unknown";
  return targets.slice(0, limit).map((t) => {
    const missingFields = [];
    // Only add lodging gap when last result / type implies it — do NOT force
    if (t.targetType === "PROGRAM" && (!t.lastResult || t.lastResult === "NO_CHANGE")) {
      // leave empty unless payload says otherwise
    }
    const context = {
      targetType: t.targetType,
      entityType: t.entityType,
      programType: t.entityType,
      priority: t.priority,
      status: t.status,
      researchCadence: t.researchCadence,
      consecutiveNoChangeRuns: t.consecutiveNoChangeRuns || 0,
      signalsFound: t.signalsFound,
      opportunitiesCreated: t.opportunitiesCreated,
      noChangeRuns: t.noChangeRuns,
      lastResult: t.lastResult,
      missingFields,
      reasonMonitored: t.reasonMonitored
        ? String(t.reasonMonitored).slice(0, 200)
        : null,
      archetype,
      researchReason: "registry_due_or_sample",
      evidenceSummary: [
        `targetType=${t.targetType}`,
        `priority=${t.priority}`,
        `cadence=${t.researchCadence}`,
        `noChange=${t.consecutiveNoChangeRuns || 0}`,
        `lastResult=${t.lastResult || "n/a"}`,
        `signals=${t.signalsFound || 0}`,
      ].join("; "),
      hardPolicyContext: {},
    };
    const existingPriority =
      (t.consecutiveNoChangeRuns || 0) >= 3
        ? "DEFER"
        : t.priority === "HIGH"
          ? "RESEARCH_NOW"
          : t.priority === "LOW"
            ? "LOWER_PRIORITY"
            : "DEFER";
    const existingPlaybook =
      t.targetType === "PRIVATE_EVENT_VENUE"
        ? "PRIVATE_EVENT_SIGNAL"
        : t.targetType === "PROGRAM"
          ? "EVENT_FUTURE_CYCLE"
          : t.targetType === "DEMAND_GENERATOR"
            ? "DEMAND_GENERATOR"
            : "GENERAL_FOLLOWUP";
    return {
      cohortSource: "research_registry",
      hotelId,
      targetId: t.targetId,
      opportunityId: null,
      archetype,
      context,
      existing: {
        TARGET_RESEARCH_PRIORITY: existingPriority,
        RESEARCH_PLAYBOOK: existingPlaybook,
        FOLLOWUP_TYPE:
          t.targetType === "PRIVATE_EVENT_VENUE"
            ? "VENUE"
            : t.targetType === "PROGRAM"
              ? "FUTURE_CYCLE"
              : "PROGRAM",
        GENERATOR_CADENCE:
          (t.consecutiveNoChangeRuns || 0) >= 4 ? "RELAX_CADENCE" : "KEEP_CADENCE",
      },
      policyContext: {},
      researchOutcomeHint: {
        lodgingRelevant: false,
        futureCycleRelevant: t.targetType === "PROGRAM",
      },
    };
  });
}

/**
 * Prefer registry targets; if empty/short, fill from opportunities.
 */
export function buildHotelShadowCohort(
  hotelId,
  registryTargets = [],
  { registryLimit = 30, opportunityLimit = 25, minSize = 15 } = {}
) {
  const fromRegistry = mapRegistryTargetsToCohort(hotelId, registryTargets, {
    limit: registryLimit,
  });
  if (fromRegistry.length >= minSize) {
    return {
      hotelId,
      archetype: HOTEL_ARCHETYPE[hotelId],
      source: "research_registry",
      targets: fromRegistry,
    };
  }
  const fromOpp = deriveCohortFromOpportunities(hotelId, {
    limit: Math.max(opportunityLimit, minSize - fromRegistry.length),
  });
  const merged = [...fromRegistry];
  const seen = new Set(merged.map((t) => t.targetId));
  for (const t of fromOpp) {
    if (seen.has(t.targetId)) continue;
    merged.push(t);
    if (merged.length >= Math.max(minSize, registryLimit)) break;
  }
  return {
    hotelId,
    archetype: HOTEL_ARCHETYPE[hotelId],
    source:
      fromRegistry.length && fromOpp.length
        ? "registry_plus_opportunities"
        : fromRegistry.length
          ? "research_registry"
          : "gdi_opportunities",
    targets: merged,
  };
}
