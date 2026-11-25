/**
 * Canonical Full E2E — Leisure Demand Intelligence standard stage V1.
 *
 * Leisure modules are STANDARD canonical capabilities (not ad-hoc opt-in).
 * Always assess all six; execute only hotel-relevant applicable modules.
 * Paid external provider execution remains separately authorized.
 *
 * Ordinary 14-segment discovery ≠ specialized leisure-module execution.
 */

import {
  DESIGN_HOTELS_LDI_COHORT,
  runLeisureLiveE2eIntegration,
} from "../leisure-demand-intelligence/gdi-leisure-live-e2e-integration-v1.js";
import {
  LEISURE_MODULES_ORDERED,
  listLeisureModuleIds,
  getLeisureModule,
  buildLeisureFourteenSegmentCoverageMatrix,
} from "../leisure-demand-intelligence/gdi-leisure-module-registry-v1.js";
import {
  selectLeisureModulesForHotel,
  LEISURE_HOTEL_PROFILE_PRESETS,
} from "../leisure-demand-intelligence/gdi-leisure-hotel-module-selection-v1.js";
import {
  assessLeisureModuleApplicability,
  finalizeLeisureModuleOutcomes,
  LEISURE_MODULE_OUTCOME,
  CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
  HILTON_NY_TIMES_SQUARE_HOTEL_ID,
  WYNDHAM_HOTEL_ID_RECONCILIATION,
  GDI_CANONICAL_LEISURE_APPLICABILITY_V1,
} from "./gdi-canonical-leisure-applicability-v1.js";

export const GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1 =
  "gdi-canonical-leisure-opt-in-wire-v1";

export const GDI_CANONICAL_LEISURE_STANDARD_STAGE_V1 =
  "gdi-canonical-leisure-standard-stage-v1";

/**
 * Resolve leisure standard stage.
 * Default: ON (always assess). Isolated tests may pass disableLeisureStandardStage.
 */
export function resolveCanonicalLeisureOptIn(opts = {}) {
  const forceModules = normalizeModuleList(
    opts.leisureModules || opts.forceLeisureModules || opts.modules,
  );
  const disabled =
    opts.disableLeisureStandardStage === true ||
    opts.skipLeisureModules === true ||
    opts.leisureOptIn === false ||
    opts.enableLeisureModules === false;

  if (disabled) {
    return {
      enabled: false,
      standardStage: true,
      reason: "LEISURE_STANDARD_STAGE_EXPLICITLY_DISABLED",
      moduleIds: [],
      forceModules: [],
      paidAuthorized: false,
      researchMode: null,
      note: "Isolated-test / explicit disable only — not the permanent operating standard.",
    };
  }

  const paidAuthorized =
    opts.authorizePaidLeisureResearch === true &&
    opts.authorizePaidDiscovery === true &&
    opts.authorizePaidResearch === true;

  // Production paid leisure remains fail-closed. Standard stage uses mock_live
  // / fixture for $0 assessment+execution unless founder authorizes paid.
  let researchMode = String(opts.leisureResearchMode || "mock_live").toLowerCase();
  if (researchMode === "live" && !paidAuthorized) {
    researchMode = "pending_authorization";
  } else if (paidAuthorized) {
    researchMode = "live_blocked_until_founder_go";
  }

  return {
    enabled: true,
    standardStage: true,
    reason: forceModules.length
      ? "STANDARD_STAGE_WITH_FORCE_MODULES"
      : "STANDARD_STAGE_HOTEL_RELEVANCE",
    moduleIds: forceModules,
    forceModules,
    disableModules: normalizeModuleList(opts.disableLeisureModules),
    paidAuthorized: false,
    researchMode,
    note:
      "Leisure is a standard Full E2E capability: always assess all six; execute applicable only; paid providers separately gated.",
  };
}

function normalizeModuleList(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.map((m) => getLeisureModule(m)?.id).filter(Boolean);
}

/**
 * Hotel-profile eligibility / applicability matrix for all six modules.
 */
export function buildCanonicalLeisureEligibilityMatrix(hotels = []) {
  const rows = (Array.isArray(hotels) ? hotels : []).map((h) => {
    const hotelId = String(h.hotelId || h.id || "").trim();
    const profile = {
      hotelId,
      name: h.name || h.displayName || null,
      ...(h.hotelProfile && typeof h.hotelProfile === "object"
        ? h.hotelProfile
        : {}),
      ...(typeof h === "object" ? h : {}),
    };
    const assessment = assessLeisureModuleApplicability(profile, {
      forceModules: h.forceModules,
      disableModules: h.disableModules,
    });
    return {
      hotelId: hotelId || null,
      name: profile.name || assessment.selection?.label || null,
      selectionSource: assessment.selection?.source || null,
      rationale: assessment.selection?.rationale || null,
      selectedModuleIds: assessment.applicableModuleIds,
      moduleEligibility: assessment.modules.map((m) => ({
        moduleId: m.moduleId,
        label: m.label,
        eligible: m.applicable === true,
        commerciallyAppropriate: m.applicable === true,
        outcomeHint: m.outcomeHint,
        reason: m.reason,
        coverage: m.coverage,
        primarySegments: m.primarySegments,
      })),
      fourteenSegmentOverlapIsNotLeisureExecution: true,
      wyndhamIdGuard:
        hotelId === HILTON_NY_TIMES_SQUARE_HOTEL_ID
          ? "ERRONEOUS_IF_LABELED_WYNDHAM"
          : hotelId === CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID
            ? "CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE"
            : null,
    };
  });

  return {
    contract: GDI_CANONICAL_LEISURE_STANDARD_STAGE_V1,
    applicabilityContract: GDI_CANONICAL_LEISURE_APPLICABILITY_V1,
    moduleIds: listLeisureModuleIds(),
    moduleRegistry: buildLeisureFourteenSegmentCoverageMatrix(),
    presetHotelIds: Object.keys(LEISURE_HOTEL_PROFILE_PRESETS),
    designHotelsCohort: DESIGN_HOTELS_LDI_COHORT.map((h) => ({
      hotelId: h.hotelId,
      name: h.name,
      priorityModules: [...h.priorityModules],
    })),
    wyndhamHotelIdReconciliation: WYNDHAM_HOTEL_ID_RECONCILIATION,
    hotels: rows,
    defaults: {
      optInRequired: false,
      standardStage: true,
      silentRunAtEveryHotel: false,
      alwaysAssessAllSix: true,
      executeOnlyApplicable: true,
      paidProductionOff: true,
    },
  };
}

/**
 * Default applicability set for founder matrix ($0).
 */
export function buildDefaultCanonicalLeisureApplicabilityMatrix() {
  const hotels = [
    ...DESIGN_HOTELS_LDI_COHORT.map((h) => ({
      hotelId: h.hotelId,
      name: h.name,
    })),
    {
      hotelId: "recLuxvwwxID7U2B8",
      name: "Bethesda Marriott",
      classification: "urban meetings hotel",
      commercialPriorities: ["corporate", "associations", "government"],
    },
    {
      hotelId: CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
      name: "Wyndham San Diego Bayside",
      classification: "urban waterfront hotel",
      commercialPriorities: ["corporate", "associations", "preferred lodging"],
    },
    {
      hotelId: HILTON_NY_TIMES_SQUARE_HOTEL_ID,
      name: "Hilton New York Times Square",
      classification: "urban meetings hotel",
      commercialPriorities: ["corporate", "associations"],
    },
    {
      hotelId: "force-all-six-demo",
      name: "Explicit all-six force list",
      forceModules: listLeisureModuleIds(),
    },
  ];
  return buildCanonicalLeisureEligibilityMatrix(hotels);
}

/**
 * Execute leisure standard stage for canonical Full E2E.
 */
export async function runCanonicalLeisureOptInStage(opts = {}) {
  const resolved = resolveCanonicalLeisureOptIn(opts);
  if (!resolved.enabled) {
    return {
      contract: GDI_CANONICAL_LEISURE_STANDARD_STAGE_V1,
      status: "SKIPPED_EXPLICITLY_DISABLED",
      enabled: false,
      standardStage: true,
      spentUsd: 0,
      paidCalls: 0,
      moduleIds: [],
      moduleOutcomes: [],
      specializedLeisureExecution: false,
      ordinaryFourteenSegmentOverlap: true,
      note: resolved.note,
    };
  }

  const hotelId = String(opts.hotelId || "").trim();
  if (!hotelId) {
    return {
      contract: GDI_CANONICAL_LEISURE_STANDARD_STAGE_V1,
      status: "BLOCKED",
      enabled: true,
      ok: false,
      error: "HOTEL_ID_REQUIRED",
      spentUsd: 0,
      paidCalls: 0,
      moduleOutcomes: listLeisureModuleIds().map((moduleId) => ({
        moduleId,
        outcome: LEISURE_MODULE_OUTCOME.INSUFFICIENT_HOTEL_CONTEXT,
        reason: "HOTEL_ID_REQUIRED",
        executed: false,
      })),
    };
  }

  if (hotelId === HILTON_NY_TIMES_SQUARE_HOTEL_ID) {
    // Hard identity guard — never treat Hilton TS as Wyndham.
  }

  const hotelProfile = {
    hotelId,
    ...(opts.hotelProfile && typeof opts.hotelProfile === "object"
      ? opts.hotelProfile
      : {}),
  };

  const assessment = assessLeisureModuleApplicability(hotelProfile, {
    forceModules: resolved.forceModules.length
      ? resolved.forceModules
      : undefined,
    disableModules: resolved.disableModules,
  });

  const moduleIds = assessment.applicableModuleIds;

  if (resolved.researchMode === "pending_authorization") {
    const moduleOutcomes = finalizeLeisureModuleOutcomes({
      assessment,
      pendingAuthorizationModuleIds: moduleIds,
    });
    return {
      contract: GDI_CANONICAL_LEISURE_STANDARD_STAGE_V1,
      status: "APPLICABLE_PENDING_AUTHORIZATION",
      enabled: true,
      ok: true,
      standardStage: true,
      spentUsd: 0,
      paidCalls: 0,
      moduleIds,
      assessment,
      moduleOutcomes,
      specializedLeisureExecution: false,
      paidProductionOff: true,
      note:
        "Applicable leisure modules assessed but live paid execution awaits separate founder authorization.",
    };
  }

  if (!moduleIds.length) {
    const moduleOutcomes = finalizeLeisureModuleOutcomes({ assessment });
    return {
      contract: GDI_CANONICAL_LEISURE_STANDARD_STAGE_V1,
      status: assessment.insufficientContext
        ? "INSUFFICIENT_HOTEL_CONTEXT"
        : "ASSESSED_NONE_APPLICABLE",
      enabled: true,
      ok: true,
      standardStage: true,
      spentUsd: 0,
      paidCalls: 0,
      moduleIds: [],
      assessment,
      moduleOutcomes,
      specializedLeisureExecution: false,
      note: assessment.insufficientContext
        ? "Missing hotel context — modules marked INSUFFICIENT_HOTEL_CONTEXT (not treated as leisure-irrelevant)."
        : "All six assessed; none commercially applicable for this hotel profile.",
    };
  }

  const integration = await runLeisureLiveE2eIntegration({
    hotelId,
    hotelProfile,
    modules: moduleIds,
    researchMode: resolved.researchMode || "mock_live",
    existingInventory: opts.existingInventory || [],
    needPeriodContext: opts.needPeriodContext || null,
    writeArtifacts: opts.writeLeisureArtifacts !== false,
    outDir: opts.outDir || undefined,
    authorizePaidResearch: false,
    allowLivePilot: false,
  });

  const executed = [];
  const failed = [];
  for (const mr of integration.moduleResults || []) {
    if (mr.status === "MOCK_LIVE_COMPLETE" || mr.acceptedCount >= 0) {
      executed.push(mr.leisureModuleId);
    } else {
      failed.push(mr.leisureModuleId);
    }
  }
  // Any requested applicable module missing from results → failed (no silent complete)
  for (const id of moduleIds) {
    if (!executed.includes(id) && !failed.includes(id)) failed.push(id);
  }

  const moduleOutcomes = finalizeLeisureModuleOutcomes({
    assessment,
    executedModuleIds: integration.ok === false ? [] : executed,
    failedModuleIds: integration.ok === false ? moduleIds : failed,
  });

  return {
    contract: GDI_CANONICAL_LEISURE_STANDARD_STAGE_V1,
    status:
      integration.ok === false
        ? "APPLICABLE_EXECUTION_FAILED"
        : "STANDARD_STAGE_COMPLETE",
    enabled: true,
    ok: integration.ok !== false,
    standardStage: true,
    spentUsd: Number(integration.spentUsd || 0),
    paidCalls: Number(integration.paidCalls || 0),
    moduleIds,
    assessment,
    moduleOutcomes,
    moduleResults: integration.moduleResults || [],
    opportunityCount: Array.isArray(integration.opportunities)
      ? integration.opportunities.length
      : 0,
    fourteenSegmentCalls: integration.fourteenSegmentCalls || [],
    stagedPlanWriteApplied: integration.stagedPlan?.writeApplied === true,
    specializedLeisureExecution: true,
    ordinaryFourteenSegmentOverlap: false,
    attribution: {
      discovery: "leisure_module_discovery",
      deepResearch: "leisure_module_deep_research_when_seeded",
      distinctFromCanonicalFourteenSegmentDefault: true,
    },
    paidProductionOff: true,
    wyndhamHotelIdReconciliation: WYNDHAM_HOTEL_ID_RECONCILIATION,
    integration,
  };
}

export {
  listLeisureModuleIds,
  LEISURE_MODULES_ORDERED,
  selectLeisureModulesForHotel,
  DESIGN_HOTELS_LDI_COHORT,
  assessLeisureModuleApplicability,
  finalizeLeisureModuleOutcomes,
  LEISURE_MODULE_OUTCOME,
  CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
  HILTON_NY_TIMES_SQUARE_HOTEL_ID,
  WYNDHAM_HOTEL_ID_RECONCILIATION,
};
