/**
 * Coverage planning + controlled execution (no broad rediscovery).
 * Classifies due targets against existing signal evidence only.
 */

import {
  RUN_STATUS,
  RUN_TYPE,
  EXECUTION_STATUS,
  RESULT_TYPE,
  TARGET_STATUS,
} from "./constants.js";
import {
  buildResearchRun,
  buildTargetRun,
  isTargetDue,
  applyTargetRunOutcome,
  isTrulyNewOpportunity,
} from "./entities.js";

/**
 * Partition targets into due / not-due / skipped.
 */
export function planCoverage({ targets = [], now = new Date() } = {}) {
  const due = [];
  const notDue = [];
  const skipped = [];
  for (const t of targets) {
    if (!t) continue;
    if (t.status === TARGET_STATUS.PAUSED || t.status === TARGET_STATUS.RETIRED) {
      skipped.push(t);
      continue;
    }
    if (isTargetDue(t, { now })) due.push(t);
    else notDue.push(t);
  }
  return { due, notDue, skipped, now: new Date(now).toISOString() };
}

/**
 * Classify one target against existing signals / opportunities (in-memory evidence).
 * Does not invent opportunities. Does not fetch the web.
 *
 * @param {object} target
 * @param {object} evidence
 * @param {Array} evidence.signals — { signalId, firstSeenAt, lastSeenAt, demandGeneratorId, programId, venueId }
 * @param {Array} evidence.opportunities — GDI opps with firstSeenRunId / discoveryTargetId
 * @param {string} runId
 */
export function classifyTargetAgainstEvidence(target, evidence = {}, { runId, now = new Date() } = {}) {
  const signals = Array.isArray(evidence.signals) ? evidence.signals : [];
  const opportunities = Array.isArray(evidence.opportunities) ? evidence.opportunities : [];
  const firstPass = !target.lastResearchedAt;
  const since = target.lastResearchedAt ? new Date(target.lastResearchedAt).getTime() : 0;
  const runStart = new Date(now).getTime();

  const relatedSignals = signals.filter((s) => signalMatchesTarget(s, target));
  const newSignals = firstPass
    ? [] // baseline pass — existing inventory is not "new this run"
    : relatedSignals.filter((s) => {
        const t = new Date(s.firstSeenAt || s.lastSeenAt || 0).getTime();
        return t > since;
      });
  const updatedSignals = firstPass
    ? []
    : relatedSignals.filter((s) => {
        const first = new Date(s.firstSeenAt || 0).getTime();
        const last = new Date(s.lastSeenAt || 0).getTime();
        return last > since && first <= since;
      });

  const relatedOpps = opportunities.filter((o) => opportunityMatchesTarget(o, target));
  const newOpps = relatedOpps.filter((o) => isTrulyNewOpportunity(o, runId));
  const updatedOpps = relatedOpps.filter((o) => {
    if (isTrulyNewOpportunity(o, runId)) return false;
    const changed = o.lastMaterialChangeRunId === runId || o.weeklyDeltaState === "UPDATED";
    return changed;
  });

  let resultType = RESULT_TYPE.NO_MATERIAL_CHANGE;
  let materialChange = false;
  if (newOpps.length) {
    resultType = RESULT_TYPE.OPPORTUNITY_CREATED;
    materialChange = true;
  } else if (updatedOpps.length) {
    resultType = RESULT_TYPE.OPPORTUNITY_UPDATED;
    materialChange = true;
  } else if (newSignals.length) {
    resultType = RESULT_TYPE.NEW_SIGNAL;
    materialChange = true;
  } else if (updatedSignals.length) {
    resultType = RESULT_TYPE.SIGNAL_UPDATED;
    materialChange = true;
  } else if (firstPass && relatedSignals.length) {
    resultType = RESULT_TYPE.NO_MATERIAL_CHANGE;
    materialChange = false;
  }

  // First-pass baseline: never classify pre-existing inventory as NEW_SIGNAL
  if (firstPass) {
    resultType = RESULT_TYPE.NO_MATERIAL_CHANGE;
    materialChange = false;
  }

  const urls = [];
  for (const s of relatedSignals.slice(0, 5)) {
    if (s.sourceUrl) urls.push(s.sourceUrl);
  }
  if (target.primarySourceUrl) urls.push(target.primarySourceUrl);

  // Ledger-only evidence classification is NOT research execution.
  // Airtable Execution Status = SKIPPED (LEDGER_CHECK semantic flagged below).
  // Result Type stays NO_MATERIAL_CHANGE on baseline (Airtable-safe); summary says BASELINE_EXISTING.
  return {
    executionStatus: EXECUTION_STATUS.SKIPPED,
    executionStatusSemantic: "LEDGER_CHECK",
    resultType: firstPass ? RESULT_TYPE.NO_MATERIAL_CHANGE : resultType,
    baselineExisting: Boolean(firstPass),
    materialChange: firstPass ? false : materialChange,
    newSignalCount: firstPass ? 0 : newSignals.length,
    updatedSignalCount: firstPass ? 0 : updatedSignals.length,
    newOpportunityCount: firstPass ? 0 : newOpps.length,
    updatedOpportunityCount: firstPass ? 0 : updatedOpps.length,
    sourceCount: relatedSignals.length || (target.primarySourceUrl ? 1 : 0),
    officialSourceCount: target.officialDomain || target.primarySourceUrl ? 1 : 0,
    sourceUrls: [...new Set(urls)].slice(0, 10),
    lastResultSummary: firstPass
      ? `BASELINE_EXISTING: verified ${relatedSignals.length} existing signal(s); not weekly NEW`
      : `${resultType}: signals+${newSignals.length}/${updatedSignals.length} opps+${newOpps.length}/${updatedOpps.length}`,
    queriesUsed: 0,
    fetchesUsed: 0,
    completedAt: new Date(now).toISOString(),
    researchExecuted: false,
  };
}

function signalMatchesTarget(s, target) {
  if (!s || !target) return false;
  if (target.demandGeneratorId && s.demandGeneratorId === target.demandGeneratorId) return true;
  if (target.programId && s.programId === target.programId) return true;
  if (target.venueId && (s.venueId === target.venueId || s.peVenueId === target.venueId)) return true;
  if (target.entityId && (s.demandGeneratorId === target.entityId || s.programId === target.entityId)) {
    return true;
  }
  return false;
}

function opportunityMatchesTarget(o, target) {
  if (!o || !target) return false;
  if (o.discoveryTargetId && o.discoveryTargetId === target.targetId) return true;
  if (target.demandGeneratorId && o.dgGeneratorId === target.demandGeneratorId) return true;
  if (target.programId && o.dgProgramId === target.programId) return true;
  if (target.venueId && (o.peVenueId === target.venueId || o.venueId === target.venueId)) return true;
  return false;
}

/**
 * Execute a planned weekly coverage cycle in-memory (persist via stores separately).
 */
export function executeCoverageCycle({
  hotelId,
  hotelName,
  targets = [],
  evidence = {},
  runType = RUN_TYPE.WEEKLY,
  now = new Date(),
  gitSha = null,
  notes = null,
} = {}) {
  const plan = planCoverage({ targets, now });
  const run = buildResearchRun({
    hotelId,
    hotelName,
    runType,
    status: RUN_STATUS.RUNNING,
    startedAt: new Date(now).toISOString(),
    targetsDue: plan.due.length,
    gitSha,
    notes,
  });

  const targetRuns = [];
  const updatedTargets = [];
  const learning = {
    priorityIncreased: 0,
    priorityDecreased: 0,
    cadenceRelaxed: 0,
  };

  for (const t of plan.notDue) {
    targetRuns.push(
      buildTargetRun({
        hotelId,
        targetId: t.targetId,
        runId: run.runId,
        executionStatus: EXECUTION_STATUS.NOT_DUE,
        resultType: null,
        lastResultSummary: "Not due this cycle",
        scheduledAt: new Date(now).toISOString(),
        researchTargetRecordId: t.airtableRecordId,
      })
    );
  }

  for (const t of plan.skipped) {
    targetRuns.push(
      buildTargetRun({
        hotelId,
        targetId: t.targetId,
        runId: run.runId,
        executionStatus: EXECUTION_STATUS.SKIPPED,
        resultType: null,
        lastResultSummary: `Skipped (${t.status})`,
        scheduledAt: new Date(now).toISOString(),
        researchTargetRecordId: t.airtableRecordId,
      })
    );
  }

  let completed = 0;
  let failed = 0;
  let missed = 0;
  let noChange = 0;
  let newSignals = 0;
  let updatedSignals = 0;
  let newOpps = 0;
  let updatedOpps = 0;

  for (const t of plan.due) {
    run.targetsAttempted += 1;
    try {
      const classification = classifyTargetAgainstEvidence(t, evidence, {
        runId: run.runId,
        now,
      });
      const tr = buildTargetRun({
        hotelId,
        targetId: t.targetId,
        runId: run.runId,
        scheduledAt: new Date(now).toISOString(),
        startedAt: new Date(now).toISOString(),
        researchTargetRecordId: t.airtableRecordId,
        ...classification,
      });
      targetRuns.push(tr);
      const applied = applyTargetRunOutcome(t, tr, { now });
      updatedTargets.push(applied.target);
      if (applied.learning.priorityChanged === "INCREASED") learning.priorityIncreased += 1;
      if (applied.learning.priorityChanged === "DECREASED") learning.priorityDecreased += 1;
      if (applied.learning.cadenceRelaxed) learning.cadenceRelaxed += 1;

      completed += 1;
      if (tr.resultType === RESULT_TYPE.NO_MATERIAL_CHANGE) noChange += 1;
      newSignals += tr.newSignalCount;
      updatedSignals += tr.updatedSignalCount;
      newOpps += tr.newOpportunityCount;
      updatedOpps += tr.updatedOpportunityCount;
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
          scheduledAt: new Date(now).toISOString(),
          completedAt: new Date(now).toISOString(),
          researchTargetRecordId: t.airtableRecordId,
        })
      );
      updatedTargets.push(t);
    }
  }

  // Due targets that somehow weren't attempted count as missed (should be 0).
  missed = Math.max(0, plan.due.length - completed - failed);

  run.targetsCompleted = completed;
  run.targetsMissed = missed;
  run.targetsFailed = failed;
  run.targetsNoChange = noChange;
  run.newSignals = newSignals;
  run.updatedSignals = updatedSignals;
  run.newOpportunities = newOpps;
  run.updatedOpportunities = updatedOpps;
  run.completedAt = new Date(now).toISOString();
  run.status =
    failed && completed
      ? RUN_STATUS.PARTIAL
      : failed && !completed
        ? RUN_STATUS.FAILED
        : RUN_STATUS.COMPLETED;

  const coveragePct =
    plan.due.length === 0 ? 100 : Math.round((completed / plan.due.length) * 1000) / 10;

  return {
    plan,
    run,
    targetRuns,
    updatedTargets,
    learning,
    coveragePct,
    researchedTargetRuns: targetRuns.filter(
      (tr) => tr.executionStatus === EXECUTION_STATUS.RESEARCHED
    ),
  };
}

/**
 * Customer-facing summary — only from completed RESEARCHED target runs.
 */
export function buildCustomerResearchSummary({ run, researchedTargetRuns = [], activeTargetCount = 0 } = {}) {
  if (!run || run.status === RUN_STATUS.FAILED || run.status === RUN_STATUS.PLANNED) {
    return null;
  }
  // Grounded only on RESEARCHED target runs (real playbook execution).
  // Ledger-only coverage cycles must not invent a customer research summary.
  const reviewed = researchedTargetRuns.filter(
    (tr) =>
      tr &&
      (tr.executionStatus === EXECUTION_STATUS.RESEARCHED || tr.researchExecuted === true)
  ).length;
  if (!reviewed) return null;
  const last = run.completedAt || run.startedAt || null;
  return {
    lastResearchAt: last,
    monitoredTargetsReviewed: reviewed,
    activeTargets: activeTargetCount || null,
    newDemandSignals: Number(run.newSignals) || 0,
    newOpportunities: Number(run.newOpportunities) || 0,
    updatedOpportunities: Number(run.updatedOpportunities) || 0,
  };
}

export function formatCustomerResearchSummaryText(summary) {
  if (!summary) return "";
  const d = summary.lastResearchAt
    ? new Date(summary.lastResearchAt).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
        timeZone: "UTC",
      })
    : "—";
  return [
    `Last Research: ${d}`,
    `${summary.monitoredTargetsReviewed} monitored demand targets reviewed`,
    `${summary.newDemandSignals} new demand signals identified`,
    `${summary.newOpportunities} new opportunities added`,
    `${summary.updatedOpportunities} existing opportunities materially updated`,
  ].join("\n");
}
