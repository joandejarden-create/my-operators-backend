/**
 * Canonical Full E2E — Commercial Priorities + Need Dates run-context V1.
 *
 * Resolves the hotel's authorized Tab 2 commercial profile and active need
 * periods into a versioned snapshot that guides (does not limit) research.
 *
 * Missing fields = neutral. Suggestions ≠ hotel-confirmed. Soft ranking only.
 * Does not invent demand, remove segments, auto-reject, or authorize paid spend.
 */

import {
  resolveHotelConfigWithSavedCommercialProfile,
  HOTEL_SAVED_COMMERCIAL_PROFILE_RESEARCH_BRIDGE_V1,
} from "../need-date-demand/hotel-saved-commercial-profile-research-bridge-v1.js";
import { listNeedPeriodsForHotel } from "../need-date-demand/need-period-hotel-store-v1.js";
import {
  isNeedPeriodCommerciallyOpen,
  isNeedPeriodInactiveForProspecting,
} from "../need-date-demand/need-period-lifecycle-v1.js";
import {
  attachHotelPreferenceMatchExplanations,
  buildHotelPreferenceMatchExplanations,
} from "../need-date-demand/hotel-preference-match-explanation-v1.js";
import { evaluateNeedPeriodTimingOverlap } from "../need-date-demand/need-period-timing-match-v1.js";
import {
  CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
  HILTON_NY_TIMES_SQUARE_HOTEL_ID,
  WYNDHAM_HOTEL_ID_RECONCILIATION,
} from "./gdi-canonical-leisure-applicability-v1.js";

export const GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1 =
  "gdi-canonical-commercial-priorities-run-context-v1";

function text(v) {
  return String(v == null ? "" : v).trim();
}

function list(v) {
  return (Array.isArray(v) ? v : []).filter(Boolean);
}

/**
 * Build versioned commercial-priorities run context for one hotel.
 * Local store by default ($0). Never mutates Airtable / STAGED / CURRENT.
 */
export async function buildCanonicalCommercialPrioritiesRunContext(
  hotelId,
  opts = {},
) {
  const id = text(hotelId);
  if (!id) {
    return {
      contract: GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1,
      ok: false,
      error: "HOTEL_ID_REQUIRED",
      hotelId: null,
    };
  }

  if (
    opts.assertNotCrossHotel !== false &&
    id === HILTON_NY_TIMES_SQUARE_HOTEL_ID &&
    opts.expectWyndham === true
  ) {
    return {
      contract: GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1,
      ok: false,
      error: "CROSS_HOTEL_IDENTITY_REFUSED",
      hotelId: id,
      wyndhamHotelIdReconciliation: WYNDHAM_HOTEL_ID_RECONCILIATION,
      note: "Refuse to apply Hilton Times Square preferences as Wyndham.",
    };
  }

  const hotelConfig =
    opts.hotelConfig && typeof opts.hotelConfig === "object"
      ? { ...opts.hotelConfig }
      : { hotelId: id };

  const bridged = await resolveHotelConfigWithSavedCommercialProfile(
    id,
    hotelConfig,
    {
      useAirtable: opts.useAirtable === true,
      allowLocalFallback: opts.allowLocalFallback !== false,
      commercialProfile: opts.commercialProfile || null,
      skipLoad: opts.skipLoad === true,
      env: opts.env,
    },
  );

  const profile = bridged.commercialProfile || null;
  const cp = bridged.hotelConfig?.commercialPriorities || {};
  const savedApplied = bridged.savedProfileApplied === true;

  let periods = [];
  try {
    const listed = listNeedPeriodsForHotel(id, {
      isolation: opts.needPeriodIsolation || undefined,
    });
    periods = Array.isArray(listed.periods) ? listed.periods : [];
  } catch {
    periods = [];
  }

  const activeNeedPeriods = periods.filter((p) => isNeedPeriodCommerciallyOpen(p));
  const historicalOrReplacedNeedPeriods = periods.filter((p) =>
    isNeedPeriodInactiveForProspecting(p),
  );

  const snapshot = {
    contract: GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1,
    bridge: HOTEL_SAVED_COMMERCIAL_PROFILE_RESEARCH_BRIDGE_V1,
    hotelId: id,
    at: new Date().toISOString(),
    profileId: text(profile?.profileId || cp.profileId) || null,
    currentVersion:
      profile?.currentVersion != null
        ? Number(profile.currentVersion)
        : cp.currentVersion != null
          ? Number(cp.currentVersion)
          : null,
    source: savedApplied
      ? "HOTEL_CONFIRMED_TAB2_PROFILE"
      : cp.source || "CONFIG_OR_EMPTY",
    provenance: cp.provenance || profile?.provenance || null,
    confirmationStatus: savedApplied
      ? "HOTEL_CONFIRMED"
      : periods.length || Object.keys(cp).length
        ? "PARTIAL_OR_CONFIG"
        : "NO_STATED_PREFERENCES",
    softSignalOnly: true,
    doesNotLimitDiscovery: true,
    missingFieldsAreNeutral: true,
    revenuePriorities: list(cp.revenuePriorities || profile?.revenuePriorities),
    activeNeedPeriods: activeNeedPeriods.map((p) => ({
      needPeriodId: p.needPeriodId || p.id || null,
      periodLabel: p.periodLabel || p.label || null,
      status: p.status || null,
      startDate: p.startDate || p.periodStart || null,
      endDate: p.endDate || p.periodEnd || null,
      revenueNeed: list(p.revenueNeed),
    })),
    historicalOrReplacedNeedPeriodCount: historicalOrReplacedNeedPeriods.length,
    preferredRoomBlockSize: {
      preset: cp.peakRoomsPreset || null,
      min: cp.coreTargetPeakRoomsMin ?? null,
      max: cp.coreTargetPeakRoomsMax ?? null,
    },
    preferredBookingWindow: cp.bookingWindow || null,
    meetingSpaceNeed: cp.meetingSpaceNeed || null,
    preferredStayDuration: cp.stayDuration || null,
    preferredWeekdays: {
      dayPreference: cp.dayPreference || null,
      specificDays: list(profile?.specificDays),
      weekendFocus: cp.weekendFocus === true,
      weekdayFocus: cp.weekdayFocus === true,
    },
    targetSegments: list(cp.targetSegments),
    excludedSegments: list(cp.excludedSegments),
    localDemandGeneratorCategories: list(cp.demandAnchorFocus),
    confirmedKnownAccounts: list(cp.knownAccounts),
    hotelConsiderations: list(cp.hotelConsiderations),
    businessRestrictions: list(cp.businessToAvoid),
    restrictionNotes: text(cp.notes) || null,
    lastConfirmedAt: text(cp.lastConfirmedAt || profile?.lastConfirmedAt) || null,
    zeroActiveNeedPeriodsMeansNeutralTiming: activeNeedPeriods.length === 0,
    wyndhamHotelIdReconciliation:
      id === CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID ||
      id === HILTON_NY_TIMES_SQUARE_HOTEL_ID
        ? WYNDHAM_HOTEL_ID_RECONCILIATION
        : null,
  };

  return {
    contract: GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1,
    ok: true,
    hotelId: id,
    savedProfileApplied: savedApplied,
    doesNotLimitDiscovery: true,
    softSignalOnly: true,
    missingFieldsAreNeutral: true,
    hotelConfig: bridged.hotelConfig,
    commercialProfile: profile,
    snapshot,
    activeNeedPeriods,
    historicalOrReplacedNeedPeriods,
  };
}

/**
 * Soft commercial-alignment rank for one opportunity.
 * Never rejects; never fabricates demand; preference miss stays visible.
 */
export function scoreCommercialAlignmentForOpportunity(
  opportunity = {},
  runContext = null,
) {
  const snap = runContext?.snapshot || runContext || null;
  const profile = runContext?.commercialProfile || {
    revenuePriorities: snap?.revenuePriorities || [],
    prioritySegments: snap?.targetSegments || [],
    dayPreference: snap?.preferredWeekdays?.dayPreference || null,
    specificDays: snap?.preferredWeekdays?.specificDays || [],
  };

  const activePeriods = list(
    runContext?.activeNeedPeriods || snap?.activeNeedPeriods,
  );
  let bestTiming = null;
  for (const period of activePeriods) {
    try {
      const timing = evaluateNeedPeriodTimingOverlap(period, opportunity);
      if (
        !bestTiming ||
        String(timing?.overlapStatus || "").includes("CONFIRMED") ||
        String(timing?.overlapStatus || "").includes("SUPPORTED")
      ) {
        bestTiming = timing;
      }
    } catch {
      /* timing helper optional */
    }
  }

  const explanations = buildHotelPreferenceMatchExplanations({
    commercialProfile: profile,
    needPeriod: activePeriods[0] || null,
    opportunity,
    timingMatch: bestTiming,
  });

  const restrictions = list(snap?.businessRestrictions);
  const oppBlob = `${opportunity.title || ""} ${opportunity.organizationName || ""} ${opportunity.summary || ""}`.toLowerCase();
  const restrictionHits = restrictions.filter((r) =>
    oppBlob.includes(String(r).toLowerCase()),
  );

  const alignment =
    explanations.length === 0 && restrictionHits.length === 0
      ? "NEUTRAL"
      : restrictionHits.length
        ? "CONFLICTS_WITH_RESTRICTION"
        : explanations.length
          ? "SUPPORTS_STATED_PRIORITIES"
          : "NEUTRAL";

  const needDateMatch =
    bestTiming &&
    /CONFIRMED_PERIOD_OVERLAP|SUPPORTED_PROBABLE_OVERLAP/.test(
      String(bestTiming.overlapStatus || ""),
    )
      ? "ACTIVE_NEED_PERIOD_OVERLAP"
      : activePeriods.length === 0
        ? "NEUTRAL_NO_ACTIVE_NEED_PERIODS"
        : "NO_VERIFIED_NEED_DATE_MATCH";

  return {
    commercialAlignment: alignment,
    needDateMatch,
    hotelPreferenceMatchExplanations: explanations,
    hotelPreferenceMatchLine: explanations[0] || null,
    restrictionConflicts: restrictionHits,
    supportsRevenuePriorities: explanations.some((l) =>
      /priority/i.test(l),
    ),
    estimatedWindowIsNotOpenContract: true,
    doesNotRejectForPreferenceMiss: true,
  };
}

/**
 * Attach soft commercial alignment onto opportunity rows (copy; no mutation of demand claims).
 */
export function applyCommercialPrioritiesToOpportunityRows(
  rows = [],
  runContext = null,
) {
  if (!runContext?.ok && !runContext?.snapshot) {
    return Array.isArray(rows) ? rows.map((r) => ({ ...r })) : [];
  }
  return (Array.isArray(rows) ? rows : []).map((row) => {
    const scored = scoreCommercialAlignmentForOpportunity(row, runContext);
    const withPref = attachHotelPreferenceMatchExplanations(row, {
      commercialProfile: runContext.commercialProfile || {
        revenuePriorities: runContext.snapshot?.revenuePriorities || [],
        prioritySegments: runContext.snapshot?.targetSegments || [],
        dayPreference: runContext.snapshot?.preferredWeekdays?.dayPreference,
      },
      needPeriod: (runContext.activeNeedPeriods || [])[0] || null,
    });
    return {
      ...withPref,
      commercialPrioritiesAlignment: scored,
      // Preserve independent FQO — preference miss never clears qualification
      finalizedQualifiedOpportunity:
        row.finalizedQualifiedOpportunity === true ? true : row.finalizedQualifiedOpportunity,
    };
  });
}

/**
 * Detect preference-version change between weekly runs (no paid rediscovery required).
 */
export function classifyCommercialPriorityChangeImpact({
  priorSnapshot = null,
  currentSnapshot = null,
} = {}) {
  const prior = priorSnapshot || {};
  const curr = currentSnapshot || {};
  const versionChanged =
    Number(prior.currentVersion || 0) !== Number(curr.currentVersion || 0) ||
    text(prior.profileId) !== text(curr.profileId);
  const revenueChanged =
    JSON.stringify(prior.revenuePriorities || []) !==
    JSON.stringify(curr.revenuePriorities || []);
  const needPeriodsChanged =
    JSON.stringify(prior.activeNeedPeriods || []) !==
    JSON.stringify(curr.activeNeedPeriods || []);

  return {
    priorityChangeAloneIsNotNewDiscovery: true,
    versionChanged,
    revenueChanged,
    needPeriodsChanged,
    weeklyChangeLabels: [
      versionChanged || revenueChanged ? "CHANGED_HOTEL_PRIORITY" : null,
      needPeriodsChanged ? "NEW_NEED_PERIOD_MATCH_CONTEXT" : null,
      versionChanged || revenueChanged || needPeriodsChanged
        ? "CHANGED_COMMERCIAL_FIT"
        : null,
    ].filter(Boolean),
    reEvaluateQualifiedInventoryWithoutPaidDiscovery: true,
  };
}

export {
  CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
  HILTON_NY_TIMES_SQUARE_HOTEL_ID,
  WYNDHAM_HOTEL_ID_RECONCILIATION,
};
