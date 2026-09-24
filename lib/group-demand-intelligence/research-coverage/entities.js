/**
 * Pure research-coverage entity builders + cadence / due / productivity logic.
 */

import crypto from "crypto";
import {
  RESEARCH_COVERAGE_VERSION,
  TARGET_ID_PREFIX,
  RUN_ID_PREFIX,
  TARGET_RUN_ID_PREFIX,
  TARGET_TYPE,
  TARGET_STATUS,
  TARGET_PRIORITY,
  RESEARCH_CADENCE,
  RUN_TYPE,
  RUN_STATUS,
  EXECUTION_STATUS,
  RESULT_TYPE,
  CADENCE_DAYS_BY_PRIORITY,
  NO_CHANGE_RELAX_THRESHOLD,
} from "./constants.js";

function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function shortHash(s) {
  return crypto.createHash("sha256").update(String(s || "")).digest("hex").slice(0, 10);
}

export function buildTargetId({ hotelId, targetType, entityKey }) {
  const key = `${hotelId}|${targetType}|${entityKey}`;
  return `${TARGET_ID_PREFIX}${slug(targetType)}_${shortHash(key)}`;
}

export function buildRunId({ hotelId, runType = RUN_TYPE.WEEKLY, at = new Date() }) {
  const d = new Date(at);
  const ymd = `${d.getUTCFullYear()}_${String(d.getUTCMonth() + 1).padStart(2, "0")}_${String(d.getUTCDate()).padStart(2, "0")}`;
  const hotelSlug = slug(String(hotelId || "").replace(/^rec/, "")).slice(0, 12) || "hotel";
  const short = shortHash(`${hotelId}|${runType}|${ymd}|${Date.now()}|${Math.random()}`).slice(0, 6);
  return `${RUN_ID_PREFIX}${hotelSlug}_${ymd}_${String(runType).toLowerCase()}_${short}`;
}

export function buildTargetRunId({ runId, targetId }) {
  return `${TARGET_RUN_ID_PREFIX}${shortHash(`${runId}|${targetId}`)}`;
}

export function cadenceDaysFor(priority, researchCadence, consecutiveNoChangeRuns = 0) {
  let days =
    researchCadence === RESEARCH_CADENCE.WEEKLY
      ? 7
      : researchCadence === RESEARCH_CADENCE.BIWEEKLY
        ? 14
        : researchCadence === RESEARCH_CADENCE.MONTHLY
          ? 30
          : researchCadence === RESEARCH_CADENCE.QUARTERLY
            ? 90
            : CADENCE_DAYS_BY_PRIORITY[priority] || 14;
  const noChange = Number(consecutiveNoChangeRuns) || 0;
  if (noChange >= NO_CHANGE_RELAX_THRESHOLD * 2) days = Math.max(days, 90);
  else if (noChange >= NO_CHANGE_RELAX_THRESHOLD) days = Math.max(days, days === 7 ? 14 : days === 14 ? 30 : days);
  return days;
}

export function suggestNextResearchAt({
  priority = TARGET_PRIORITY.MEDIUM,
  researchCadence,
  consecutiveNoChangeRuns = 0,
  from = new Date(),
} = {}) {
  const days = cadenceDaysFor(priority, researchCadence, consecutiveNoChangeRuns);
  const d = new Date(from);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString();
}

export function defaultCadenceForPriority(priority) {
  if (priority === TARGET_PRIORITY.HIGH) return RESEARCH_CADENCE.WEEKLY;
  if (priority === TARGET_PRIORITY.LOW) return RESEARCH_CADENCE.MONTHLY;
  return RESEARCH_CADENCE.BIWEEKLY;
}

export function isTargetDue(target, { now = new Date() } = {}) {
  if (!target) return false;
  if (target.status === TARGET_STATUS.PAUSED || target.status === TARGET_STATUS.RETIRED) {
    return false;
  }
  if (target.status === TARGET_STATUS.LOW_PRIORITY && target.priority === TARGET_PRIORITY.LOW) {
    // still due by nextResearchAt
  }
  if (!target.nextResearchAt) return target.status === TARGET_STATUS.ACTIVE;
  return new Date(target.nextResearchAt).getTime() <= new Date(now).getTime();
}

export function mapFitPriorityToTargetPriority(fitPriority) {
  const p = String(fitPriority || "").toUpperCase();
  if (p === "HIGH" || p === "HIGH_PRIORITY") return TARGET_PRIORITY.HIGH;
  if (p === "LOW" || p === "LOW_PRIORITY" || p === "WATCH") return TARGET_PRIORITY.LOW;
  return TARGET_PRIORITY.MEDIUM;
}

export function buildResearchTarget(raw = {}) {
  const hotelId = String(raw.hotelId || "").trim();
  const targetType = raw.targetType || TARGET_TYPE.OTHER_MONITORED_SOURCE;
  const entityKey =
    raw.entityKey ||
    raw.entityId ||
    raw.demandGeneratorId ||
    raw.programId ||
    raw.venueId ||
    raw.organizationId ||
    raw.canonicalName ||
    "unknown";
  const targetId =
    raw.targetId ||
    buildTargetId({ hotelId, targetType, entityKey });
  const priority = raw.priority || TARGET_PRIORITY.MEDIUM;
  const researchCadence = raw.researchCadence || defaultCadenceForPriority(priority);
  const now = raw.createdAt || new Date().toISOString();
  return {
    targetId,
    hotelId,
    hotelName: raw.hotelName || null,
    targetType,
    entityType: raw.entityType || targetType,
    canonicalName: raw.canonicalName || String(entityKey),
    entityId: raw.entityId || String(entityKey),
    programId: raw.programId || null,
    venueId: raw.venueId || null,
    organizationId: raw.organizationId || null,
    seriesId: raw.seriesId || null,
    demandGeneratorId: raw.demandGeneratorId || null,
    officialDomain: raw.officialDomain || null,
    primarySourceUrl: raw.primarySourceUrl || null,
    researchPlaybook: raw.researchPlaybook || "official_source_monitor_v1",
    priority,
    status: raw.status || TARGET_STATUS.ACTIVE,
    reasonMonitored: raw.reasonMonitored || null,
    researchCadence,
    nextResearchAt: raw.nextResearchAt || suggestNextResearchAt({ priority, researchCadence }),
    lastResearchedAt: raw.lastResearchedAt || null,
    lastSuccessfulResearchAt: raw.lastSuccessfulResearchAt || null,
    lastMaterialChangeAt: raw.lastMaterialChangeAt || null,
    lastResult: raw.lastResult || null,
    lastRunId: raw.lastRunId || null,
    consecutiveNoChangeRuns: Number(raw.consecutiveNoChangeRuns) || 0,
    sourceFamilies: Array.isArray(raw.sourceFamilies) ? raw.sourceFamilies : [],
    firstSeenAt: raw.firstSeenAt || now,
    createdAt: raw.createdAt || now,
    updatedAt: raw.updatedAt || now,
    confidence: raw.confidence != null ? Number(raw.confidence) : null,
    signalsFound: Number(raw.signalsFound) || 0,
    opportunitiesCreated: Number(raw.opportunitiesCreated) || 0,
    opportunitiesUpdated: Number(raw.opportunitiesUpdated) || 0,
    successfulRuns: Number(raw.successfulRuns) || 0,
    noChangeRuns: Number(raw.noChangeRuns) || 0,
    schemaVersion: RESEARCH_COVERAGE_VERSION,
    airtableRecordId: raw.airtableRecordId || null,
    payload: raw.payload || null,
  };
}

export function buildResearchRun(raw = {}) {
  const hotelId = String(raw.hotelId || "").trim();
  const runType = raw.runType || RUN_TYPE.WEEKLY;
  const runId = raw.runId || buildRunId({ hotelId, runType });
  const now = new Date().toISOString();
  return {
    runId,
    hotelId,
    hotelName: raw.hotelName || null,
    runType,
    startedAt: raw.startedAt || now,
    completedAt: raw.completedAt || null,
    status: raw.status || RUN_STATUS.PLANNED,
    targetsDue: Number(raw.targetsDue) || 0,
    targetsAttempted: Number(raw.targetsAttempted) || 0,
    targetsCompleted: Number(raw.targetsCompleted) || 0,
    targetsMissed: Number(raw.targetsMissed) || 0,
    targetsFailed: Number(raw.targetsFailed) || 0,
    targetsNoChange: Number(raw.targetsNoChange) || 0,
    newSignals: Number(raw.newSignals) || 0,
    updatedSignals: Number(raw.updatedSignals) || 0,
    newOpportunities: Number(raw.newOpportunities) || 0,
    updatedOpportunities: Number(raw.updatedOpportunities) || 0,
    queries: Number(raw.queries) || 0,
    fetches: Number(raw.fetches) || 0,
    estimatedCost: Number(raw.estimatedCost) || 0,
    codeVersion: raw.codeVersion || RESEARCH_COVERAGE_VERSION,
    gitSha: raw.gitSha || null,
    researchVersion: raw.researchVersion || RESEARCH_COVERAGE_VERSION,
    notes: raw.notes || null,
    schemaVersion: RESEARCH_COVERAGE_VERSION,
    airtableRecordId: raw.airtableRecordId || null,
    payload: raw.payload || null,
  };
}

export function buildTargetRun(raw = {}) {
  const runId = String(raw.runId || "").trim();
  const targetId = String(raw.targetId || "").trim();
  const targetRunId =
    raw.targetRunId || buildTargetRunId({ runId, targetId });
  const now = new Date().toISOString();
  return {
    targetRunId,
    hotelId: raw.hotelId || null,
    targetId,
    runId,
    scheduledAt: raw.scheduledAt || now,
    startedAt: raw.startedAt || null,
    completedAt: raw.completedAt || null,
    executionStatus: raw.executionStatus || EXECUTION_STATUS.DUE,
    resultType: raw.resultType || null,
    playbook: raw.playbook || null,
    researchExecuted: raw.researchExecuted === true,
    baselineExisting: raw.baselineExisting === true,
    executionStatusSemantic: raw.executionStatusSemantic || null,
    queriesUsed: Number(raw.queriesUsed) || 0,
    fetchesUsed: Number(raw.fetchesUsed) || 0,
    sourceCount: Number(raw.sourceCount) || 0,
    officialSourceCount: Number(raw.officialSourceCount) || 0,
    materialChange: raw.materialChange === true,
    failureReason: raw.failureReason || null,
    newSignalCount: Number(raw.newSignalCount) || 0,
    updatedSignalCount: Number(raw.updatedSignalCount) || 0,
    newOpportunityCount: Number(raw.newOpportunityCount) || 0,
    updatedOpportunityCount: Number(raw.updatedOpportunityCount) || 0,
    lastResultSummary: raw.lastResultSummary || null,
    sourceUrls: Array.isArray(raw.sourceUrls) ? raw.sourceUrls : [],
    schemaVersion: RESEARCH_COVERAGE_VERSION,
    researchRunRecordId: raw.researchRunRecordId || null,
    researchTargetRecordId: raw.researchTargetRecordId || null,
    airtableRecordId: raw.airtableRecordId || null,
    payload: raw.payload || null,
  };
}

/**
 * Apply one researched outcome onto a target (pure).
 * Ledger-only checks (SKIPPED / researchExecuted=false) do not count as RESEARCHED
 * and do not advance lastResearchedAt / cadence (Coverage is not fake research).
 */
export function applyTargetRunOutcome(target, targetRun, { now = new Date() } = {}) {
  const t = { ...target };
  const iso = new Date(now).toISOString();
  const researched =
    targetRun.executionStatus === EXECUTION_STATUS.RESEARCHED ||
    targetRun.researchExecuted === true;

  t.lastRunId = targetRun.runId;
  t.lastResult = targetRun.resultType;
  t.updatedAt = iso;

  if (!researched) {
    return {
      target: t,
      learning: {
        priorityChanged: null,
        cadenceRelaxed: false,
        previousCadence: t.researchCadence,
        nextCadence: t.researchCadence,
        researched: false,
      },
    };
  }

  t.lastResearchedAt = iso;
  t.lastSuccessfulResearchAt = iso;
  t.successfulRuns = (Number(t.successfulRuns) || 0) + 1;

  const noChange =
    targetRun.resultType === RESULT_TYPE.NO_MATERIAL_CHANGE ||
    targetRun.baselineExisting === true;
  if (noChange) {
    t.consecutiveNoChangeRuns = (Number(t.consecutiveNoChangeRuns) || 0) + 1;
    t.noChangeRuns = (Number(t.noChangeRuns) || 0) + 1;
  } else if (targetRun.materialChange) {
    t.consecutiveNoChangeRuns = 0;
    t.lastMaterialChangeAt = iso;
  }

  t.signalsFound =
    (Number(t.signalsFound) || 0) +
    (Number(targetRun.newSignalCount) || 0) +
    (Number(targetRun.updatedSignalCount) || 0);
  t.opportunitiesCreated =
    (Number(t.opportunitiesCreated) || 0) + (Number(targetRun.newOpportunityCount) || 0);
  t.opportunitiesUpdated =
    (Number(t.opportunitiesUpdated) || 0) + (Number(targetRun.updatedOpportunityCount) || 0);

  let priorityChanged = null;
  let cadenceRelaxed = false;
  const prevCadence = t.researchCadence;
  if (t.consecutiveNoChangeRuns >= NO_CHANGE_RELAX_THRESHOLD) {
    if (t.researchCadence === RESEARCH_CADENCE.WEEKLY) {
      t.researchCadence = RESEARCH_CADENCE.BIWEEKLY;
      cadenceRelaxed = true;
    } else if (t.researchCadence === RESEARCH_CADENCE.BIWEEKLY) {
      t.researchCadence = RESEARCH_CADENCE.MONTHLY;
      cadenceRelaxed = true;
    }
    if (
      t.priority === TARGET_PRIORITY.HIGH &&
      t.consecutiveNoChangeRuns >= NO_CHANGE_RELAX_THRESHOLD * 2 &&
      (Number(t.opportunitiesCreated) || 0) === 0
    ) {
      t.priority = TARGET_PRIORITY.MEDIUM;
      priorityChanged = "DECREASED";
    }
  }
  if (
    (Number(targetRun.newOpportunityCount) || 0) > 0 ||
    (Number(targetRun.newSignalCount) || 0) > 0
  ) {
    if (t.priority === TARGET_PRIORITY.LOW) {
      t.priority = TARGET_PRIORITY.MEDIUM;
      priorityChanged = "INCREASED";
    } else if (
      t.priority === TARGET_PRIORITY.MEDIUM &&
      (Number(targetRun.newOpportunityCount) || 0) > 0
    ) {
      t.priority = TARGET_PRIORITY.HIGH;
      priorityChanged = "INCREASED";
    }
  }

  t.nextResearchAt = suggestNextResearchAt({
    priority: t.priority,
    researchCadence: t.researchCadence,
    consecutiveNoChangeRuns: t.consecutiveNoChangeRuns,
    from: now,
  });

  return {
    target: t,
    learning: {
      priorityChanged,
      cadenceRelaxed: cadenceRelaxed && prevCadence !== t.researchCadence,
      previousCadence: prevCadence,
      nextCadence: t.researchCadence,
      researched: true,
    },
  };
}

/**
 * NEW opportunity provenance guard — firstDiscoveredRunId must equal current run.
 */
export function isTrulyNewOpportunity(opp, runId) {
  if (!opp || !runId) return false;
  const first = opp.firstDiscoveredRunId || opp.firstSeenRunId || null;
  return String(first) === String(runId);
}

/**
 * Attach discovery provenance for a newly created opportunity.
 */
export function attachDiscoveryProvenance(opp, { runId, targetId, targetRunId, method, playbook, source, at } = {}) {
  const iso = at || new Date().toISOString();
  return {
    ...opp,
    firstDiscoveredAt: opp.firstDiscoveredAt || iso,
    firstDiscoveredRunId: runId,
    firstSeenRunId: opp.firstSeenRunId || runId,
    discoveryTargetId: targetId || null,
    discoveryTargetRunId: targetRunId || null,
    discoveryMethod: method || "research_target_coverage",
    discoveryPlaybook: playbook || null,
    discoverySource: source || null,
    lastMaterialChangeAt: iso,
    lastMaterialChangeRunId: runId,
  };
}

export {
  TARGET_TYPE,
  TARGET_STATUS,
  TARGET_PRIORITY,
  RESEARCH_CADENCE,
  RUN_TYPE,
  RUN_STATUS,
  EXECUTION_STATUS,
  RESULT_TYPE,
  NO_CHANGE_RELAX_THRESHOLD,
};
