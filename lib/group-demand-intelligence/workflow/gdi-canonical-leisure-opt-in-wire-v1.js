/**
 * Canonical Full E2E — Leisure Demand Intelligence opt-in wire V1.
 *
 * Connects existing LDI modules (#577/#581) to `runCanonicalFullE2eResearch`
 * without running them by default and without inventing a parallel research engine.
 *
 * Paid production leisure research remains OFF unless separately authorized.
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

export const GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1 =
  "gdi-canonical-leisure-opt-in-wire-v1";

/**
 * Resolve whether leisure modules should run on this canonical Full E2E call.
 * Default: OFF. Explicit enableLeisureModules / leisureModules required.
 */
export function resolveCanonicalLeisureOptIn(opts = {}) {
  const forceModules = normalizeModuleList(
    opts.leisureModules || opts.forceLeisureModules || opts.modules,
  );
  const enabled =
    opts.enableLeisureModules === true ||
    opts.leisureOptIn === true ||
    forceModules.length > 0;

  if (!enabled) {
    return {
      enabled: false,
      reason: "LEISURE_OPT_IN_OFF_BY_DEFAULT",
      moduleIds: [],
      forceModules: [],
      paidAuthorized: false,
      researchMode: null,
      note: "Ordinary 14-segment discovery is not specialized leisure-module execution.",
    };
  }

  const paidAuthorized =
    opts.authorizePaidLeisureResearch === true &&
    opts.authorizePaidDiscovery === true &&
    opts.authorizePaidResearch === true;

  // Production paid leisure remains OFF in this wire — mock_live / fixture only
  // unless a future founder GO flips authorizePaidLeisureResearch.
  const researchMode = paidAuthorized
    ? "live_blocked_until_founder_go"
    : String(opts.leisureResearchMode || "mock_live").toLowerCase();

  return {
    enabled: true,
    reason: forceModules.length
      ? "EXPLICIT_LEISURE_MODULES"
      : "ENABLE_LEISURE_MODULES_FLAG",
    moduleIds: forceModules,
    forceModules,
    disableModules: normalizeModuleList(opts.disableLeisureModules),
    paidAuthorized: false,
    researchMode: researchMode === "live" ? "mock_live" : researchMode,
    note:
      "Leisure modules run only when explicitly enabled; paid production path stays fail-closed ($0 mock/fixture).",
  };
}

function normalizeModuleList(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.map((m) => getLeisureModule(m)?.id).filter(Boolean);
}

/**
 * Hotel-profile eligibility matrix — which modules would be selected if opted in.
 * Does not execute research.
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
    const selection = selectLeisureModulesForHotel(profile, {
      forceModules: h.forceModules,
      disableModules: h.disableModules,
    });
    const moduleEligibility = listLeisureModuleIds().map((moduleId) => {
      const eligible = selection.moduleIds.includes(moduleId);
      const mod = getLeisureModule(moduleId);
      return {
        moduleId,
        label: mod?.label || moduleId,
        eligible,
        commerciallyAppropriate: eligible,
        coverage: mod?.coverage || null,
        primarySegments: mod ? [...mod.primarySegments] : [],
      };
    });
    return {
      hotelId: hotelId || null,
      name: profile.name || selection.label || null,
      selectionSource: selection.source,
      rationale: selection.rationale,
      selectedModuleIds: selection.moduleIds,
      moduleEligibility,
      fourteenSegmentOverlapIsNotLeisureExecution: true,
    };
  });

  return {
    contract: GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
    moduleIds: listLeisureModuleIds(),
    moduleRegistry: buildLeisureFourteenSegmentCoverageMatrix(),
    presetHotelIds: Object.keys(LEISURE_HOTEL_PROFILE_PRESETS),
    designHotelsCohort: DESIGN_HOTELS_LDI_COHORT.map((h) => ({
      hotelId: h.hotelId,
      name: h.name,
      priorityModules: [...h.priorityModules],
    })),
    hotels: rows,
    defaults: {
      optInRequired: true,
      silentRunAtEveryHotel: false,
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
      hotelId: "rec35fExUxCClpOP6",
      name: "Wyndham San Diego Bayside",
      classification: "urban waterfront hotel",
      commercialPriorities: ["corporate", "associations", "preferred lodging"],
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
 * Execute leisure opt-in stage for canonical Full E2E ($0 mock/fixture by default).
 */
export async function runCanonicalLeisureOptInStage(opts = {}) {
  const resolved = resolveCanonicalLeisureOptIn(opts);
  if (!resolved.enabled) {
    return {
      contract: GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
      status: "SKIPPED_OPT_IN_OFF",
      enabled: false,
      spentUsd: 0,
      paidCalls: 0,
      moduleIds: [],
      specializedLeisureExecution: false,
      ordinaryFourteenSegmentOverlap: true,
      note: resolved.note,
    };
  }

  const hotelId = String(opts.hotelId || "").trim();
  if (!hotelId) {
    return {
      contract: GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
      status: "BLOCKED",
      enabled: true,
      ok: false,
      error: "HOTEL_ID_REQUIRED",
      spentUsd: 0,
      paidCalls: 0,
    };
  }

  const hotelProfile = {
    hotelId,
    ...(opts.hotelProfile && typeof opts.hotelProfile === "object"
      ? opts.hotelProfile
      : {}),
  };

  const selection = selectLeisureModulesForHotel(hotelProfile, {
    forceModules: resolved.forceModules.length
      ? resolved.forceModules
      : undefined,
    disableModules: resolved.disableModules,
  });

  const moduleIds = resolved.forceModules.length
    ? resolved.forceModules
    : selection.moduleIds;

  if (!moduleIds.length) {
    return {
      contract: GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
      status: "SKIPPED_NO_ELIGIBLE_MODULES",
      enabled: true,
      ok: true,
      spentUsd: 0,
      paidCalls: 0,
      moduleIds: [],
      selection,
      specializedLeisureExecution: false,
      note: "Leisure opt-in enabled but hotel profile selected zero modules.",
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

  return {
    contract: GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
    status: integration.ok ? "MOCK_LIVE_COMPLETE" : "FAILED",
    enabled: true,
    ok: integration.ok !== false,
    spentUsd: Number(integration.spentUsd || 0),
    paidCalls: Number(integration.paidCalls || 0),
    moduleIds,
    selection,
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
    integration,
  };
}

export {
  listLeisureModuleIds,
  LEISURE_MODULES_ORDERED,
  selectLeisureModulesForHotel,
  DESIGN_HOTELS_LDI_COHORT,
};
