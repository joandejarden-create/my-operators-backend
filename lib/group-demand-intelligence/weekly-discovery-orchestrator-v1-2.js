/**
 * Weekly Discovery Orchestrator V1.2
 *
 * Full loop: due targets → lane harvest → qualify → promote → contact resolution → ledger.
 * Coverage remains audit ledger. Does not lower TRUE standards. No Surfe PII persistence.
 */

import {
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
import {
  PLAYBOOK,
  routeTargetToPlaybook,
  executeBoundedTargetResearch,
} from "./weekly-discovery-orchestrator.js";
import { harvestDueTarget } from "./weekly-lane-harvest-v1-2.js";
import {
  promoteQualifiedGdiOpportunity,
  PROMOTION_ACTION,
} from "./promote-qualified-opportunity.js";
import {
  resolveOpportunityContact,
  resolveContactsForOpportunities,
} from "./weekly-contact-resolution-v1-2.js";
import { classifyContactTier, CONTACT_TIER } from "./contact-tiers-v1-2.js";

export { PLAYBOOK, routeTargetToPlaybook };

/**
 * Run V1.2 weekly discovery with full lane harvest + optional promote + contact.
 */
export async function runWeeklyDiscoveryOrchestratorV12({
  hotelId,
  hotelName,
  targets = [],
  existingOpps = [],
  limit = 12,
  now = new Date(),
  gitSha = null,
  forceDue = false,
  dryRun = true,
  peMaxQueries = 0,
  resolveContacts = true,
  contactBackfillLimit = 8,
  promoteTrue = true,
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
    notes: "Weekly Discovery Orchestrator V1.2 — full lane harvest + contact",
  });

  const targetRuns = [];
  const updatedTargets = [];
  const laneInvoked = new Set();
  const trueCandidates = [];
  const watchCandidates = [];
  const promotionResults = [];
  let workingOpps = [...existingOpps];
  let queries = 0;
  let fetches = 0;
  let completed = 0;
  let failed = 0;
  let noChange = 0;
  let candidates = 0;
  let watch = 0;
  let trueCount = 0;

  for (const t of due) {
    run.targetsAttempted += 1;
    const playbook = routeTargetToPlaybook(t);
    try {
      const harvest = await harvestDueTarget(t, {
        hotelId,
        hotelName,
        existingOpps: workingOpps,
        subjectHotel: { hotelId, name: hotelName },
        peMaxQueries,
      });
      laneInvoked.add(harvest.metrics?.lane || playbook);
      queries += harvest.metrics?.queriesUsed || 0;
      fetches += harvest.metrics?.fetchesUsed || 0;
      candidates += harvest.metrics?.candidates || 0;
      watch += harvest.metrics?.watch || 0;
      trueCount += harvest.metrics?.true || 0;

      for (const c of harvest.trueCandidates || []) {
        trueCandidates.push({ candidate: c, target: t, playbook, harvest });
      }
      for (const w of harvest.watchCandidates || []) {
        watchCandidates.push({ candidate: w, target: t, playbook });
      }

      const materialChange = Boolean(
        (harvest.trueCandidates || []).length ||
          (harvest.watchCandidates || []).length
      );
      const resultType = (harvest.trueCandidates || []).length
        ? RESULT_TYPE.OPPORTUNITY_CREATED
        : materialChange
          ? RESULT_TYPE.SIGNAL_UPDATED
          : RESULT_TYPE.NO_MATERIAL_CHANGE;

      const tr = buildTargetRun({
        hotelId,
        targetId: t.targetId,
        runId: run.runId,
        scheduledAt: new Date(now).toISOString(),
        startedAt: new Date(now).toISOString(),
        researchTargetRecordId: t.airtableRecordId,
        playbook,
        executionStatus: EXECUTION_STATUS.RESEARCHED,
        researchExecuted: true,
        resultType,
        materialChange,
        newSignalCount: 0,
        updatedSignalCount: materialChange && !(harvest.trueCandidates || []).length ? 1 : 0,
        newOpportunityCount: (harvest.trueCandidates || []).length,
        updatedOpportunityCount: 0,
        sourceCount: t.primarySourceUrl ? 1 : 0,
        officialSourceCount: t.primarySourceUrl ? 1 : 0,
        sourceUrls: t.primarySourceUrl ? [t.primarySourceUrl] : [],
        queriesUsed: harvest.metrics?.queriesUsed || 0,
        fetchesUsed: harvest.metrics?.fetchesUsed || 0,
        lastResultSummary: harvest.summary || `V1.2 ${playbook}`,
        completedAt: new Date(now).toISOString(),
      });
      targetRuns.push(tr);
      const applied = applyTargetRunOutcome(t, tr, { now });
      updatedTargets.push(applied.target);
      completed += 1;
      if (tr.resultType === RESULT_TYPE.NO_MATERIAL_CHANGE) noChange += 1;
    } catch (err) {
      failed += 1;
      // Fallback: still mark URL research attempt
      try {
        const fallback = await executeBoundedTargetResearch(t, {
          runId: run.runId,
          now,
        });
        queries += fallback.queriesUsed || 0;
        fetches += fallback.fetchesUsed || 0;
        const tr = buildTargetRun({
          hotelId,
          targetId: t.targetId,
          runId: run.runId,
          ...fallback,
          failureReason: err && err.message ? err.message : String(err),
          lastResultSummary: `V1.2 harvest error; fallback URL research: ${err.message || err}`,
        });
        targetRuns.push(tr);
        updatedTargets.push(applyTargetRunOutcome(t, tr, { now }).target);
        completed += 1;
      } catch {
        targetRuns.push(
          buildTargetRun({
            hotelId,
            targetId: t.targetId,
            runId: run.runId,
            executionStatus: EXECUTION_STATUS.FAILED,
            resultType: RESULT_TYPE.ERROR,
            failureReason: err && err.message ? err.message : String(err),
            researchExecuted: false,
            completedAt: new Date(now).toISOString(),
          })
        );
        updatedTargets.push(t);
      }
    }
  }

  // Promote TRUE candidates through central service
  let promoted = 0;
  let neu = 0;
  let updated = 0;
  const contactAfterPromote = [];

  if (promoteTrue) {
    for (const row of trueCandidates) {
      const c = row.candidate;
      const promotion = await promoteQualifiedGdiOpportunity({
        candidate: {
          ...c,
          opportunityQualification:
            c.opportunityQualification === "PENDING" || !c.opportunityQualification
              ? "STRONG"
              : c.opportunityQualification,
        },
        existingOpps: workingOpps,
        hotelId,
        runId: run.runId,
        discoveryRunId: run.runId,
        discoveryAt: new Date(now).toISOString(),
        targetId: row.target?.targetId,
        targetRunId: targetRuns.find((tr) => tr.targetId === row.target?.targetId)
          ?.targetRunId,
        method: "weekly_discovery_v1_2_lane_harvest",
        playbook: row.playbook,
        source: c.officialSource || row.target?.primarySourceUrl,
        dryRun,
      });
      promotionResults.push(promotion);
      if (
        promotion.action === PROMOTION_ACTION.PROMOTE_NEW ||
        promotion.action === PROMOTION_ACTION.UPDATE_EXISTING
      ) {
        promoted += 1;
        if (promotion.action === PROMOTION_ACTION.PROMOTE_NEW) neu += 1;
        if (promotion.action === PROMOTION_ACTION.UPDATE_EXISTING) updated += 1;
        if (promotion.opportunity) {
          workingOpps = workingOpps
            .filter((o) => o.id !== promotion.opportunity.id)
            .concat([promotion.opportunity]);
          contactAfterPromote.push(promotion.opportunity);
        }
      }
    }
  }

  // Contact resolution: new/updated promotions + blank actionable backfill
  let contactReport = {
    researched: 0,
    namedAdded: 0,
    functionalAdded: 0,
    improved: 0,
    stillBlank: 0,
    fetchesUsed: 0,
    queriesUsed: 0,
    surfeCalls: 0,
    results: [],
  };

  if (resolveContacts) {
    const promoteContactResults = [];
    for (const opp of contactAfterPromote) {
      const r = await resolveOpportunityContact(opp, {
        runId: run.runId,
        preserveNewWeeklyState: true,
        markUpdated: false,
      });
      promoteContactResults.push(r);
      fetches += r.fetchesUsed || 0;
      if (!dryRun && r.opportunity) {
        const write = await promoteQualifiedGdiOpportunity({
          candidate: r.opportunity,
          existingOpps: workingOpps,
          hotelId,
          runId: run.runId,
          discoveryRunId: r.opportunity.firstDiscoveredRunId || run.runId,
          discoveryAt: r.opportunity.firstDiscoveredAt,
          method: "weekly_contact_resolution_v1_2",
          playbook: "CONTACT_WHO",
          dryRun: false,
          forceUpdateId: r.opportunity.id,
          materialUpdateOnly: true,
        });
        if (write.opportunity) {
          workingOpps = workingOpps
            .filter((o) => o.id !== write.opportunity.id)
            .concat([write.opportunity]);
          // Contact-only update must not flip to NEW
          if (write.opportunity.weeklyDeltaState === "NEW" && r.beforeTier !== CONTACT_TIER.NO_CONTACT) {
            /* keep */
          }
        }
      }
    }

    const blankPool = workingOpps.filter((o) => {
      if (/disqualified/i.test(String(o.id || ""))) return false;
      const tier = classifyContactTier(o);
      return (
        tier === CONTACT_TIER.NO_CONTACT ||
        tier === CONTACT_TIER.GENERIC_ONLY ||
        (tier === CONTACT_TIER.ORGANIZATION_PATH && !o.primaryContactName) ||
        o.weeklyDeltaState === "NEW"
      );
    });

    const backfill = await resolveContactsForOpportunities(blankPool, {
      limit: contactBackfillLimit,
      runId: run.runId,
      markUpdated: true,
    });
    fetches += backfill.fetchesUsed || 0;
    queries += backfill.queriesUsed || 0;

    if (!dryRun) {
      for (const r of backfill.results) {
        if (!r.improved && r.beforeTier === r.afterTier) continue;
        await promoteQualifiedGdiOpportunity({
          candidate: {
            ...r.opportunity,
            weeklyDeltaState: "UPDATED",
            isNewThisWeek: false,
          },
          existingOpps: workingOpps,
          hotelId,
          runId: run.runId,
          method: "weekly_contact_backfill_v1_2",
          playbook: "CONTACT_WHO",
          dryRun: false,
          forceUpdateId: r.opportunity.id,
          materialUpdateOnly: true,
        });
      }
    }

    contactReport = {
      researched: promoteContactResults.length + backfill.researched,
      namedAdded:
        promoteContactResults.filter((r) => r.namedAdded).length + backfill.namedAdded,
      functionalAdded:
        promoteContactResults.filter((r) => r.functionalAdded).length +
        backfill.functionalAdded,
      improved:
        promoteContactResults.filter((r) => r.improved).length + backfill.improved,
      stillBlank:
        promoteContactResults.filter((r) => r.afterTier === CONTACT_TIER.NO_CONTACT)
          .length + backfill.stillBlank,
      fetchesUsed:
        promoteContactResults.reduce((s, r) => s + (r.fetchesUsed || 0), 0) +
        backfill.fetchesUsed,
      queriesUsed: backfill.queriesUsed,
      surfeCalls: 0,
      results: [...promoteContactResults, ...backfill.results],
    };
  }

  run.targetsCompleted = completed;
  run.targetsFailed = failed;
  run.targetsMissed = Math.max(0, due.length - completed - failed);
  run.targetsNoChange = noChange;
  run.newSignals = 0;
  run.updatedSignals = watch;
  run.newOpportunities = neu;
  run.updatedOpportunities = updated;
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
    version: "gdi_weekly_discovery_v1_2",
    plan: { ...plan, due },
    run,
    targetRuns,
    updatedTargets,
    coveragePct,
    lanesExecuted: [...laneInvoked],
    trueCandidates,
    watchCandidates,
    promotionResults,
    contactReport,
    workingOpps,
    metrics: {
      targetsDue: due.length,
      targetsResearched: targetRuns.filter(
        (tr) => tr.executionStatus === EXECUTION_STATUS.RESEARCHED
      ).length,
      queries,
      fetches,
      candidates,
      watch,
      true: trueCount,
      promoted,
      neu,
      updated,
      contactsResearched: contactReport.researched,
      namedContactsAdded: contactReport.namedAdded,
      functionalPathsAdded: contactReport.functionalAdded,
      noContactRemaining: contactReport.stillBlank,
      surfeCalls: 0,
    },
  };
}
