/**
 * Future watch record builder + history append (immutable history entries).
 */

import {
  FUTURE_WATCH_VERSION,
  WATCH_STATUS,
  TRIGGER_TYPE,
  DATE_PROVENANCE,
} from "./constants.js";
import { assignMarketArchetypes } from "./market-archetype-v1.js";
import {
  deriveNextResearchSchedule,
  inferTriggerFromBlocker,
} from "./trigger-policy-v1.js";

/**
 * Build or upgrade a FUTURE_WATCH record from a gap-resolution / corpus row.
 */
export function buildFutureWatchRecord(raw = {}, opts = {}) {
  const now = opts.now || new Date();
  const hotelShort = raw.hotelShort || opts.hotelShort || "";
  const hpc = raw.hpc || raw.hotelId || opts.hpc || null;
  const archetype = assignMarketArchetypes({
    hpc,
    hotelShort,
    hotelType: opts.hotelType,
    market: opts.market,
    region: opts.region,
    positioning: opts.positioning,
    tags: opts.tags,
  });

  const isCeiling =
    raw.stateAfter === "PUBLIC_DATA_CEILING" ||
    raw.watchStatus === WATCH_STATUS.PUBLIC_DATA_CEILING ||
    raw.outcome === "PUBLIC_DATA_CEILING";

  const triggerType =
    raw.nextTriggerType && raw.nextTriggerType !== "OTHER"
      ? raw.nextTriggerType
      : inferTriggerFromBlocker(raw.primaryBlocker, raw.outcome || raw.note);

  const schedule = deriveNextResearchSchedule({
    triggerType: isCeiling ? "PUBLIC_DATA_CEILING" : triggerType,
    candidate: raw,
    pageText: opts.pageText || "",
    now,
    watchStatus: isCeiling ? WATCH_STATUS.PUBLIC_DATA_CEILING : null,
  });

  const history = Array.isArray(raw.watchHistory) ? [...raw.watchHistory] : [];

  return {
    watchId: raw.watchId || `gdi_fw_${raw.candidateId || raw.id || Date.now()}`,
    candidateId: raw.candidateId || raw.id || null,
    hotel: raw.hotel || null,
    hotelShort,
    hpc,
    event: raw.event || raw.eventResolved || null,
    seriesId: raw.eventSeries || raw.seriesId || null,
    cycleId: raw.futureCycle || raw.cycleId || null,
    organizer: raw.organizer || raw.organizerResolved || null,
    market: opts.market || (hotelShort === "AC" ? "A Coruña / Galicia" : hotelShort === "SPICE" ? "Grenada" : null),
    dates: raw.dates || raw.futureCycle || null,
    primarySource: raw.primarySource || raw.sourceUrl || null,
    sourceFamily: raw.sourceFamily || null,
    surfaceEligibility: raw.surfaceEligibility || raw.evaluation?.eligibility || null,
    lodgingRelationship: raw.lodgingRelationship || raw.evaluation?.lodgingRelationship || null,
    lodgingGrade: raw.lodgingEvidenceGrade || raw.evaluation?.lodgingGrade || null,
    commercialStatus: raw.commercialStatus || raw.evaluation?.commercialStatus || null,
    winnability: raw.winnability || null,
    hotelFit: raw.hotelFit || null,
    whoState: raw.whoStatus || raw.whoState || null,
    primaryBlocker: raw.primaryBlocker || null,
    secondaryBlocker: raw.secondaryBlocker || null,
    watchStatus: isCeiling
      ? WATCH_STATUS.PUBLIC_DATA_CEILING
      : raw.watchStatus || WATCH_STATUS.ACTIVE,
    watchReason: raw.watchReason || raw.nextTriggerNote || "awaiting_commercial_trigger",
    nextTriggerType: schedule.nextTriggerType || triggerType || TRIGGER_TYPE.HOUSING_OPEN,
    nextTriggerCondition: schedule.nextTriggerCondition,
    nextResearchDate: schedule.nextResearchDate,
    researchWindowStart: schedule.researchWindowStart,
    researchWindowEnd: schedule.researchWindowEnd,
    dateProvenance: schedule.dateProvenance || DATE_PROVENANCE.HEURISTIC,
    lastCheckedAt: raw.lastCheckedAt || raw.jevActions?.slice(-1)?.[0]?.completed || null,
    lastMaterialChangeAt: raw.lastMaterialChangeAt || null,
    sourceLastVerifiedAt: raw.sourceLastVerifiedAt || null,
    lastResearchDate: raw.lastResearchDate || raw.lastCheckedAt || null,
    lastResearchOutcome: raw.lastResearchOutcome || raw.outcome || null,
    watchVersion: FUTURE_WATCH_VERSION,
    marketArchetype: archetype.primary,
    marketArchetypeSecondary: archetype.secondary,
    marketArchetypeConfidence: archetype.confidence,
    preferredResearchActions: archetype.preferredActions,
    stopConditions: schedule.stopConditions || [],
    sourceFingerprint: raw.sourceFingerprint || null,
    watchHistory: history,
    createdAt: raw.createdAt || new Date(now).toISOString(),
    updatedAt: new Date(now).toISOString(),
  };
}

/**
 * Append a recheck history entry without overwriting prior entries.
 */
export function appendWatchHistory(watch, entry = {}) {
  const history = Array.isArray(watch.watchHistory) ? [...watch.watchHistory] : [];
  history.push({
    at: entry.at || new Date().toISOString(),
    priorState: entry.priorState || watch.watchStatus,
    trigger: entry.trigger || watch.nextTriggerType,
    jevAction: entry.jevAction || null,
    defaultAction: entry.defaultAction || null,
    queries: entry.queries || 0,
    fetches: entry.fetches || 0,
    evidenceFound: entry.evidenceFound === true,
    result: entry.result || null,
    stateAfter: entry.stateAfter || null,
    nextTrigger: entry.nextTrigger || null,
    nextResearchDate: entry.nextResearchDate || null,
    targetRunId: entry.targetRunId || null,
  });
  return {
    ...watch,
    watchHistory: history,
    updatedAt: new Date().toISOString(),
  };
}
