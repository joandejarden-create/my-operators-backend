/**
 * Canonical leisure module applicability contract V1.
 *
 * Always assess all six LDI modules on a standard Full E2E.
 * Execute only commercially applicable missions (hotel-relevant).
 * Never silently mark applicable modules COMPLETE without execution.
 * Missing hotel-profile data → INSUFFICIENT_HOTEL_CONTEXT, not "not leisure".
 */

import {
  listLeisureModuleIds,
  getLeisureModule,
} from "../leisure-demand-intelligence/gdi-leisure-module-registry-v1.js";
import {
  selectLeisureModulesForHotel,
  LEISURE_HOTEL_PROFILE_PRESETS,
} from "../leisure-demand-intelligence/gdi-leisure-hotel-module-selection-v1.js";

export const GDI_CANONICAL_LEISURE_APPLICABILITY_V1 =
  "gdi-canonical-leisure-applicability-v1";

export const LEISURE_MODULE_OUTCOME = Object.freeze({
  APPLICABLE_EXECUTED: "APPLICABLE_EXECUTED",
  APPLICABLE_PENDING_AUTHORIZATION: "APPLICABLE_PENDING_AUTHORIZATION",
  APPLICABLE_EXECUTION_FAILED: "APPLICABLE_EXECUTION_FAILED",
  NOT_APPLICABLE_WITH_REASON: "NOT_APPLICABLE_WITH_REASON",
  INSUFFICIENT_HOTEL_CONTEXT: "INSUFFICIENT_HOTEL_CONTEXT",
});

/** Certified Wyndham San Diego Bayside HPC id (not Hilton Times Square). */
export const CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID =
  "recnPB3Q02NO8Ma1o";

/** Hilton New York Times Square — previously mislabeled as Wyndham in #607 matrix. */
export const HILTON_NY_TIMES_SQUARE_HOTEL_ID = "rec35fExUxCClpOP6";

export const WYNDHAM_HOTEL_ID_RECONCILIATION = Object.freeze({
  certifiedWyndhamSanDiegoBayside: CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
  erroneousCloseoutId: HILTON_NY_TIMES_SQUARE_HOTEL_ID,
  erroneousCloseoutActualHotel: "Hilton New York Times Square",
  relationship: "SEPARATE_HOTELS_NOT_ALIAS",
  note:
    "rec35fExUxCClpOP6 is Hilton New York Times Square. Certified Wyndham San Diego Bayside is recnPB3Q02NO8Ma1o. Never apply Hilton profile as Wyndham.",
});

/**
 * Detect whether hotel profile has enough context for leisure applicability.
 */
export function assessHotelLeisureContextSufficiency(hotelProfile = {}) {
  const hotelId = String(hotelProfile.hotelId || hotelProfile.id || "").trim();
  if (LEISURE_HOTEL_PROFILE_PRESETS[hotelId]) {
    return {
      sufficient: true,
      source: "hpc_preset",
      reason: null,
    };
  }
  const blob = JSON.stringify(hotelProfile || {}).toLowerCase();
  const hasIdentity = Boolean(
    hotelId ||
      hotelProfile.name ||
      hotelProfile.displayName ||
      hotelProfile.city ||
      hotelProfile.market,
  );
  const hasLeisureSignal =
    /leisure|resort|boutique|villa|beach|island|urban|city|university|festival|attraction|dmc|destination|sports|vfr|graduation/.test(
      blob,
    ) ||
    Boolean(hotelProfile.classification) ||
    Boolean(hotelProfile.hotelClass) ||
    Boolean(hotelProfile.propertyType) ||
    Boolean(hotelProfile.commercialPriorities) ||
    Number(hotelProfile.meetingRooms) > 0;

  if (!hasIdentity) {
    return {
      sufficient: false,
      source: "insufficient_identity",
      reason: "MISSING_HOTEL_IDENTITY",
    };
  }
  if (!hasLeisureSignal && !hotelId) {
    return {
      sufficient: false,
      source: "insufficient_signals",
      reason: "MISSING_POSITIONING_AND_TERRITORY_SIGNALS",
    };
  }
  // Known HPC id alone is enough to attempt heuristic/preset selection;
  // empty heuristic → still assessed (may be NOT_APPLICABLE with reason).
  return {
    sufficient: true,
    source: hasLeisureSignal ? "profile_signals" : "hotel_id_only",
    reason: null,
  };
}

/**
 * Assess all six modules for a hotel. Does not execute research.
 */
export function assessLeisureModuleApplicability(hotelProfile = {}, opts = {}) {
  const hotelId = String(hotelProfile.hotelId || hotelProfile.id || "").trim();
  const context = assessHotelLeisureContextSufficiency(hotelProfile);
  const forceModules = new Set(
    (opts.forceModules || [])
      .map((m) => getLeisureModule(m)?.id)
      .filter(Boolean),
  );
  const disableModules = new Set(
    (opts.disableModules || [])
      .map((m) => getLeisureModule(m)?.id)
      .filter(Boolean),
  );

  const selection = selectLeisureModulesForHotel(hotelProfile, {
    forceModules: forceModules.size ? [...forceModules] : undefined,
    disableModules: [...disableModules],
  });

  const selected = new Set(selection.moduleIds || []);
  const modules = listLeisureModuleIds().map((moduleId) => {
    const mod = getLeisureModule(moduleId);
    if (disableModules.has(moduleId)) {
      return {
        moduleId,
        label: mod?.label || moduleId,
        applicable: false,
        outcomeHint: LEISURE_MODULE_OUTCOME.NOT_APPLICABLE_WITH_REASON,
        reason: "EXPLICITLY_DISABLED",
        primarySegments: mod ? [...mod.primarySegments] : [],
        coverage: mod?.coverage || null,
      };
    }
    if (forceModules.has(moduleId) || selected.has(moduleId)) {
      return {
        moduleId,
        label: mod?.label || moduleId,
        applicable: true,
        outcomeHint: LEISURE_MODULE_OUTCOME.APPLICABLE_PENDING_AUTHORIZATION,
        reason: forceModules.has(moduleId)
          ? "FORCE_MODULE"
          : selection.source || "HOTEL_RELEVANCE",
        primarySegments: mod ? [...mod.primarySegments] : [],
        coverage: mod?.coverage || null,
      };
    }
    if (!context.sufficient) {
      return {
        moduleId,
        label: mod?.label || moduleId,
        applicable: null,
        outcomeHint: LEISURE_MODULE_OUTCOME.INSUFFICIENT_HOTEL_CONTEXT,
        reason: context.reason || "INSUFFICIENT_HOTEL_CONTEXT",
        primarySegments: mod ? [...mod.primarySegments] : [],
        coverage: mod?.coverage || null,
      };
    }
    return {
      moduleId,
      label: mod?.label || moduleId,
      applicable: false,
      outcomeHint: LEISURE_MODULE_OUTCOME.NOT_APPLICABLE_WITH_REASON,
      reason: `NOT_SELECTED_BY_${String(selection.source || "HEURISTIC").toUpperCase()}`,
      primarySegments: mod ? [...mod.primarySegments] : [],
      coverage: mod?.coverage || null,
    };
  });

  return {
    contract: GDI_CANONICAL_LEISURE_APPLICABILITY_V1,
    hotelId: hotelId || null,
    context,
    selection,
    modules,
    applicableModuleIds: modules
      .filter((m) => m.applicable === true)
      .map((m) => m.moduleId),
    insufficientContext: context.sufficient !== true,
  };
}

/**
 * Finalize per-module outcomes after an execution attempt.
 */
export function finalizeLeisureModuleOutcomes({
  assessment,
  executedModuleIds = [],
  failedModuleIds = [],
  pendingAuthorizationModuleIds = [],
} = {}) {
  const executed = new Set(executedModuleIds);
  const failed = new Set(failedModuleIds);
  const pending = new Set(pendingAuthorizationModuleIds);
  return (assessment?.modules || []).map((row) => {
    if (row.applicable === false) {
      return {
        ...row,
        outcome: LEISURE_MODULE_OUTCOME.NOT_APPLICABLE_WITH_REASON,
        executed: false,
      };
    }
    if (row.applicable == null) {
      return {
        ...row,
        outcome: LEISURE_MODULE_OUTCOME.INSUFFICIENT_HOTEL_CONTEXT,
        executed: false,
      };
    }
    if (failed.has(row.moduleId)) {
      return {
        ...row,
        outcome: LEISURE_MODULE_OUTCOME.APPLICABLE_EXECUTION_FAILED,
        executed: false,
      };
    }
    if (executed.has(row.moduleId)) {
      return {
        ...row,
        outcome: LEISURE_MODULE_OUTCOME.APPLICABLE_EXECUTED,
        executed: true,
      };
    }
    if (pending.has(row.moduleId)) {
      return {
        ...row,
        outcome: LEISURE_MODULE_OUTCOME.APPLICABLE_PENDING_AUTHORIZATION,
        executed: false,
      };
    }
    // Applicable but not executed and not pending → treat as failed silent-skip prevention
    return {
      ...row,
      outcome: LEISURE_MODULE_OUTCOME.APPLICABLE_EXECUTION_FAILED,
      executed: false,
      reason: row.reason || "APPLICABLE_BUT_NOT_EXECUTED",
    };
  });
}
