/**
 * Qualify Demand Generator triggers → GDI opportunity promotion bar.
 * Generator alone NEVER creates an opportunity.
 * No NEW_TO_HOTEL. No contact enrichment. No invented futures.
 */

import { createHash } from "node:crypto";
import {
  SIGNAL_ID_PREFIX,
  SIGNAL_STATUS,
  TRIGGER_TYPE,
  DG_SCHEMA_VERSION,
} from "./constants.js";
import { assertNoInventedFuture } from "./entities.js";

function clean(s) {
  return String(s || "").trim();
}

export function computeSignalId({
  demandGeneratorId,
  programId,
  triggerType,
  sourceUrl,
  eventStartDate,
} = {}) {
  const seed = [
    clean(demandGeneratorId),
    clean(programId),
    clean(triggerType),
    clean(sourceUrl),
    clean(eventStartDate),
  ].join("|");
  return `${SIGNAL_ID_PREFIX}${createHash("sha256").update(seed).digest("hex").slice(0, 14)}`;
}

function isFutureOrCurrentDate(iso, now = new Date()) {
  if (!iso) return false;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return false;
  const floor = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())
  );
  return d.getTime() >= floor.getTime() - 24 * 60 * 60 * 1000;
}

/**
 * Qualify a generator-driven trigger. Returns status + reasons.
 * TRUE_ACTIONABLE requires lodging thesis + hotel fit + future timing + source.
 */
export function qualifyGeneratorSignal(input = {}) {
  const reasons = [];
  const fail = [];

  const inventGuard = assertNoInventedFuture({
    recurrenceStatus: input.recurrenceStatus,
    confirmedFutureCycle: input.confirmedFutureCycle,
    inventFromHistoryOnly: input.inventFromHistoryOnly === true,
  });
  if (!inventGuard.ok) {
    return {
      status: SIGNAL_STATUS.REJECTED,
      failReasons: [inventGuard.code],
      reasons: [inventGuard.message],
      qualify: false,
      trueActionable: false,
    };
  }

  if (!clean(input.demandGeneratorId)) fail.push("MISSING_GENERATOR");
  if (!clean(input.sourceUrl) && !(input.sourceUrls || []).length) {
    fail.push("MISSING_SOURCE");
  }
  if (!clean(input.triggerType)) fail.push("MISSING_TRIGGER");

  const hasFutureTiming =
    isFutureOrCurrentDate(input.eventStartDate) ||
    input.timingStatus === "FUTURE_CONFIRMED" ||
    input.timingStatus === "CURRENT";
  if (!hasFutureTiming) fail.push("NO_FUTURE_TIMING");

  const lodgingThesis =
    input.lodgingDemandThesis === true ||
    input.hasCredibleLodgingDemand === true ||
    (input.lodgingDemandThesis !== false &&
      input.hasCredibleLodgingDemand !== false &&
      /room block|hotel block|stay-to-play|lodging|housing|overnight|peak rooms/i.test(
        String(input.hotelDemandThesis || "")
      ));
  if (!lodgingThesis) fail.push("NO_LODGING_THESIS");

  const hotelFitOk =
    input.hotelFitOk === true ||
    ["HIGH", "MEDIUM", "STRONG", "CORE", "COMPETITIVE"].includes(
      clean(input.marketRelevance || input.productFit).toUpperCase()
    );
  if (!hotelFitOk) fail.push("WEAK_HOTEL_FIT");

  const commercialGeoOk =
    input.commercialGeographyOk !== false &&
    clean(input.marketRelevance).toUpperCase() !== "OUTSIDE";
  if (!commercialGeoOk) fail.push("OUTSIDE_COMMERCIAL_GEOGRAPHY");

  const actionPath =
    clean(input.defensibleActionPath) ||
    clean(input.recommendedAction) ||
    null;
  if (!actionPath) fail.push("NO_ACTION_PATH");

  // Generator-only or historical-only must never promote
  if (input.generatorOnly === true) fail.push("GENERATOR_ONLY_NOT_OPPORTUNITY");
  if (input.fromHistoryWithoutEvidence === true) {
    fail.push("HISTORY_WITHOUT_FUTURE_EVIDENCE");
  }

  if (fail.includes("MISSING_GENERATOR") || fail.includes("MISSING_SOURCE")) {
    return {
      status: SIGNAL_STATUS.REJECTED,
      failReasons: fail,
      reasons,
      qualify: false,
      trueActionable: false,
    };
  }

  if (fail.length === 0) {
    reasons.push("meets_true_actionable_bar");
    return {
      status: SIGNAL_STATUS.TRUE_ACTIONABLE,
      failReasons: [],
      reasons,
      qualify: true,
      trueActionable: true,
      actionPath,
    };
  }

  // Partial: future signal but incomplete lodging/fit → WATCH or QUALIFIED
  const softFails = new Set([
    "NO_LODGING_THESIS",
    "WEAK_HOTEL_FIT",
    "NO_ACTION_PATH",
  ]);
  const onlySoft = fail.every((f) => softFails.has(f) || f === "NO_FUTURE_TIMING");
  if (fail.includes("NO_FUTURE_TIMING") && !fail.includes("GENERATOR_ONLY_NOT_OPPORTUNITY")) {
    return {
      status: SIGNAL_STATUS.WATCH,
      failReasons: fail,
      reasons: ["monitor_for_future_cycle"],
      qualify: false,
      trueActionable: false,
    };
  }
  if (onlySoft && hasFutureTiming) {
    return {
      status: SIGNAL_STATUS.QUALIFIED,
      failReasons: fail,
      reasons: ["future_signal_incomplete_lodging_or_fit"],
      qualify: true,
      trueActionable: false,
      actionPath,
    };
  }

  return {
    status: SIGNAL_STATUS.REJECTED,
    failReasons: fail,
    reasons,
    qualify: false,
    trueActionable: false,
  };
}

export function buildDemandGeneratorSignal(input = {}) {
  const demandGeneratorId = clean(input.demandGeneratorId);
  const triggerType =
    TRIGGER_TYPE[clean(input.triggerType).toUpperCase()] ||
    Object.values(TRIGGER_TYPE).find(
      (v) => v === clean(input.triggerType).toUpperCase()
    ) ||
    TRIGGER_TYPE.OTHER;
  const sourceUrl =
    clean(input.sourceUrl) ||
    (Array.isArray(input.sourceUrls) ? input.sourceUrls[0] : "") ||
    "";
  const qualification = qualifyGeneratorSignal({ ...input, triggerType, sourceUrl });
  const signalId =
    clean(input.signalId) ||
    computeSignalId({
      demandGeneratorId,
      programId: input.programId,
      triggerType,
      sourceUrl,
      eventStartDate: input.eventStartDate,
    });
  const now = new Date().toISOString();
  return {
    signalId,
    demandGeneratorId,
    programId: clean(input.programId) || null,
    seriesId: clean(input.seriesId) || null,
    cycleId: clean(input.cycleId) || null,
    hotelId: clean(input.hotelId) || null,
    hotelGeneratorFitId: clean(input.hotelGeneratorFitId) || null,
    triggerType,
    title: clean(input.title || input.eventName) || null,
    eventStartDate: input.eventStartDate || null,
    eventEndDate: input.eventEndDate || null,
    market: clean(input.market) || null,
    hotelDemandThesis: clean(input.hotelDemandThesis) || null,
    sourceUrl: sourceUrl || null,
    sourceUrls: Array.isArray(input.sourceUrls)
      ? input.sourceUrls.map(clean).filter(Boolean)
      : sourceUrl
        ? [sourceUrl]
        : [],
    signalStatus: qualification.status,
    qualify: qualification.qualify,
    trueActionable: qualification.trueActionable,
    failReasons: qualification.failReasons,
    reasons: qualification.reasons,
    recommendedAction: qualification.actionPath || clean(input.recommendedAction) || null,
    gdiOpportunityId: clean(input.gdiOpportunityId) || null,
    firstSeenAt: input.firstSeenAt || now,
    lastSeenAt: input.lastSeenAt || now,
    schemaVersion: DG_SCHEMA_VERSION,
    /** Never expose as customer "Demand Generator" card */
    isCustomerFacing: false,
  };
}

/**
 * Build promotion payload for canonical GDI opportunity.
 * Does not write. Caller must still pass normal GDI bar.
 */
export function buildGeneratorDrivenOpportunityDraft(signal, generator, program, fit) {
  if (!signal?.trueActionable) {
    return {
      ok: false,
      code: "NOT_TRUE_ACTIONABLE",
      message: "Only TRUE_ACTIONABLE generator signals may promote",
    };
  }
  const opportunityId = `gdi_dg_${createHash("sha256")
    .update(`${signal.signalId}|${fit?.hotelId || signal.hotelId || ""}`)
    .digest("hex")
    .slice(0, 16)}`;

  return {
    ok: true,
    opportunity: {
      opportunityId,
      title: signal.title || `${generator?.organizationName || "Organization"} — demand signal`,
      organizationName: generator?.organizationName || null,
      organizationId: generator?.demandGeneratorId || null,
      hotelId: fit?.hotelId || signal.hotelId,
      demandFamily: "GROUP_MEETINGS",
      demandSignalType: mapTriggerToDemandSignalType(signal.triggerType, program),
      eventStartDate: signal.eventStartDate,
      eventEndDate: signal.eventEndDate,
      summaryWhat: signal.hotelDemandThesis || signal.title,
      recommendedAction: signal.recommendedAction,
      sources: (signal.sourceUrls || []).map((url) => ({ url, authority: "OFFICIAL_ORG" })),
      dgGeneratorId: generator?.demandGeneratorId || signal.demandGeneratorId,
      dgProgramId: program?.programId || signal.programId,
      dgSignalId: signal.signalId,
      hotelGeneratorFitId: fit?.fitId || signal.hotelGeneratorFitId,
      recurrenceContextLabel: "Recurring demand source",
      isTestData: false,
      customerVisible: true,
      newnessStatus: "NEW_TO_GDI",
      /** Explicitly forbidden inference */
      newToHotel: undefined,
    },
  };
}

function mapTriggerToDemandSignalType(triggerType, program) {
  const t = clean(triggerType).toUpperCase();
  const pt = clean(program?.programType).toUpperCase();
  if (pt.includes("TRAINING") || t.includes("TRAINING")) return "TRAINING_PROGRAM";
  if (pt.includes("SPORTS") || t.includes("SPORTS")) return "SPORTS_ACADEMIC_COMPETITION";
  if (pt.includes("UNIVERSITY") || t.includes("ACADEMIC")) return "UNIVERSITY_ACADEMIC_PROGRAM";
  if (pt.includes("GOVERNMENT") || t.includes("CONTRACT")) return "GOVERNMENT_CONTRACTOR_PROGRAM";
  if (pt.includes("BOARD") || t.includes("BOARD")) return "BOARD_COMMITTEE_MEETING";
  if (t.includes("RELOCATION") || t.includes("OFFICE")) return "CORPORATE_RELOCATION";
  return "EVENT";
}
