/**
 * Thin Weekly Discovery Orchestrator V1.1
 *
 * Role: read due research targets → route to bounded playbook → execute
 * real fetch/check → update Target Run ledger. Does NOT own qualification.
 *
 * Coverage remains the ledger; this module performs research execution.
 */

import {
  TARGET_TYPE,
  EXECUTION_STATUS,
  RESULT_TYPE,
  RUN_TYPE,
  RUN_STATUS,
} from "./research-coverage/constants.js";
import {
  planCoverage,
  buildResearchRun,
  buildTargetRun,
  applyTargetRunOutcome,
} from "./research-coverage/index.js";

export const PLAYBOOK = Object.freeze({
  DEMAND_GENERATOR: "DEMAND_GENERATOR",
  EVENT_FUTURE_CYCLE: "EVENT_FUTURE_CYCLE",
  LODGING_HOUSING: "LODGING_HOUSING",
  PRIVATE_EVENT_SIGNAL: "PRIVATE_EVENT_SIGNAL",
  VENUE_PARTNERSHIP: "VENUE_PARTNERSHIP",
  SPORTS_HOUSING: "SPORTS_HOUSING",
  TRAINING_PROGRAM: "TRAINING_PROGRAM",
  GOVERNMENT_PROJECT: "GOVERNMENT_PROJECT",
  GENERAL_FOLLOWUP: "GENERAL_FOLLOWUP",
});

/**
 * Map registry target type → existing bounded playbook id.
 */
export function routeTargetToPlaybook(target = {}) {
  const tt = String(target.targetType || "");
  const et = String(target.entityType || target.programType || "").toUpperCase();
  if (tt === TARGET_TYPE.DEMAND_GENERATOR) return PLAYBOOK.DEMAND_GENERATOR;
  if (tt === TARGET_TYPE.PROGRAM) {
    if (/SPORT|SOCCER|ATHLETIC/.test(et)) return PLAYBOOK.SPORTS_HOUSING;
    if (/TRAIN|CERTIF/.test(et)) return PLAYBOOK.TRAINING_PROGRAM;
    if (/GOV|FEDERAL|NIH|NIST|NRC/.test(et)) return PLAYBOOK.GOVERNMENT_PROJECT;
    return PLAYBOOK.EVENT_FUTURE_CYCLE;
  }
  if (tt === TARGET_TYPE.PRIVATE_EVENT_VENUE) return PLAYBOOK.VENUE_PARTNERSHIP;
  if (tt === TARGET_TYPE.SPORTS_SERIES) return PLAYBOOK.SPORTS_HOUSING;
  if (tt === TARGET_TYPE.TRAINING_PROGRAM) return PLAYBOOK.TRAINING_PROGRAM;
  if (tt === TARGET_TYPE.GOVERNMENT_PROGRAM) return PLAYBOOK.GOVERNMENT_PROJECT;
  if (tt === TARGET_TYPE.HOUSING_PAGE) return PLAYBOOK.LODGING_HOUSING;
  if (tt === TARGET_TYPE.OFFICIAL_CALENDAR) return PLAYBOOK.EVENT_FUTURE_CYCLE;
  if (tt === TARGET_TYPE.EVENT_SERIES) return PLAYBOOK.EVENT_FUTURE_CYCLE;
  return PLAYBOOK.GENERAL_FOLLOWUP;
}

async function fetchSource(url) {
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, error: "no_url", queriesUsed: 0, fetchesUsed: 0 };
  }
  try {
    const r = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(20000),
      headers: { "User-Agent": "DealalityGDI-WeeklyDiscovery/1.1" },
    });
    const text = await r.text();
    const hasLodging = /hotel|lodging|accommodat|housing|room.?block|travel/i.test(
      text
    );
    const hasFuture = /2026|2027|2028|2029/i.test(text);
    return {
      ok: r.status >= 200 && r.status < 400,
      status: r.status,
      finalUrl: r.url,
      hasLodging,
      hasFuture,
      bytes: text.length,
      queriesUsed: 0,
      fetchesUsed: 1,
    };
  } catch (err) {
    return {
      ok: false,
      error: err.message || String(err),
      queriesUsed: 0,
      fetchesUsed: 1,
    };
  }
}

/**
 * Execute bounded research for one due target (official URL check).
 * Does not invent opportunities; returns Target Run fields for ledger.
 */
export async function executeBoundedTargetResearch(target, { runId, now = new Date() } = {}) {
  const playbook = routeTargetToPlaybook(target);
  const url = target.primarySourceUrl || null;
  const fetchResult = await fetchSource(url);

  const materialChange = Boolean(
    fetchResult.ok && (fetchResult.hasLodging || fetchResult.hasFuture)
  );
  const resultType = !fetchResult.ok
    ? RESULT_TYPE.INSUFFICIENT_EVIDENCE
    : materialChange
      ? RESULT_TYPE.SIGNAL_UPDATED
      : RESULT_TYPE.NO_MATERIAL_CHANGE;

  return {
    playbook,
    executionStatus: EXECUTION_STATUS.RESEARCHED,
    researchExecuted: true,
    resultType,
    materialChange,
    newSignalCount: 0,
    updatedSignalCount: materialChange ? 1 : 0,
    newOpportunityCount: 0,
    updatedOpportunityCount: 0,
    sourceCount: url ? 1 : 0,
    officialSourceCount: url ? 1 : 0,
    sourceUrls: url ? [fetchResult.finalUrl || url] : [],
    queriesUsed: fetchResult.queriesUsed || 0,
    fetchesUsed: fetchResult.fetchesUsed || 0,
    lastResultSummary: `PLAYBOOK ${playbook}: fetch ${fetchResult.ok ? "ok" : "fail"} lodging=${Boolean(fetchResult.hasLodging)} future=${Boolean(fetchResult.hasFuture)}`,
    completedAt: new Date(now).toISOString(),
    failureReason: fetchResult.error || null,
    fetchResult,
  };
}

/**
 * Run weekly discovery for due targets (bounded).
 */
export async function runWeeklyDiscoveryOrchestrator({
  hotelId,
  hotelName,
  targets = [],
  limit = 20,
  now = new Date(),
  gitSha = null,
  forceDue = false,
} = {}) {
  const plan = forceDue
    ? {
        due: targets.filter(
          (t) => t && t.status !== "PAUSED" && t.status !== "RETIRED"
        ),
        notDue: [],
        skipped: [],
      }
    : planCoverage({ targets, now });

  const due = plan.due.slice(0, Math.max(0, limit));
  const run = buildResearchRun({
    hotelId,
    hotelName,
    runType: RUN_TYPE.WEEKLY,
    status: RUN_STATUS.RUNNING,
    startedAt: new Date(now).toISOString(),
    targetsDue: due.length,
    gitSha,
    notes: "Weekly Discovery Orchestrator V1.1 — bounded playbook fetches",
  });

  const targetRuns = [];
  const updatedTargets = [];
  let queries = 0;
  let fetches = 0;
  let completed = 0;
  let failed = 0;
  let noChange = 0;
  let updatedSignals = 0;

  for (const t of due) {
    run.targetsAttempted += 1;
    try {
      const research = await executeBoundedTargetResearch(t, {
        runId: run.runId,
        now,
      });
      queries += research.queriesUsed;
      fetches += research.fetchesUsed;
      const tr = buildTargetRun({
        hotelId,
        targetId: t.targetId,
        runId: run.runId,
        scheduledAt: new Date(now).toISOString(),
        startedAt: new Date(now).toISOString(),
        researchTargetRecordId: t.airtableRecordId,
        ...research,
      });
      targetRuns.push(tr);
      const applied = applyTargetRunOutcome(t, tr, { now });
      updatedTargets.push(applied.target);
      completed += 1;
      if (tr.resultType === RESULT_TYPE.NO_MATERIAL_CHANGE) noChange += 1;
      updatedSignals += tr.updatedSignalCount || 0;
    } catch (err) {
      failed += 1;
      targetRuns.push(
        buildTargetRun({
          hotelId,
          targetId: t.targetId,
          runId: run.runId,
          executionStatus: EXECUTION_STATUS.FAILED,
          resultType: RESULT_TYPE.ERROR,
          failureReason: err && err.message ? err.message : String(err),
          researchExecuted: false,
          queriesUsed: 0,
          fetchesUsed: 0,
          scheduledAt: new Date(now).toISOString(),
          completedAt: new Date(now).toISOString(),
        })
      );
      updatedTargets.push(t);
    }
  }

  run.targetsCompleted = completed;
  run.targetsFailed = failed;
  run.targetsMissed = Math.max(0, due.length - completed - failed);
  run.targetsNoChange = noChange;
  run.updatedSignals = updatedSignals;
  run.queries = queries;
  run.fetches = fetches;
  run.completedAt = new Date(now).toISOString();
  run.status =
    failed && completed
      ? RUN_STATUS.PARTIAL
      : failed && !completed
        ? RUN_STATUS.FAILED
        : RUN_STATUS.COMPLETED;

  const coveragePct =
    due.length === 0 ? 100 : Math.round((completed / due.length) * 1000) / 10;

  return {
    plan: { ...plan, due },
    run,
    targetRuns,
    updatedTargets,
    coveragePct,
    metrics: {
      targetsDue: due.length,
      targetsResearched: targetRuns.filter(
        (tr) => tr.executionStatus === EXECUTION_STATUS.RESEARCHED
      ).length,
      queries,
      fetches,
      updatedSignals,
      newSignals: 0,
      true: 0,
      promoted: 0,
      neu: 0,
    },
  };
}
