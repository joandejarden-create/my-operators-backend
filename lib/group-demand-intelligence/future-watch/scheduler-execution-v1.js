/**
 * Process one due watch: fingerprint → prefilter → Jev (1) → evaluate → persist.
 * Scheduler V1: max 1 Jev action. Never rejects on provider failure.
 */

import {
  WATCH_STATUS,
  DATE_PROVENANCE,
  STOP_CONDITION,
} from "./constants.js";
import {
  buildSourceFingerprint,
  detectSourceMaterialChange,
} from "./source-fingerprint-v1.js";
import { decideMarketAwareNextAction } from "./market-aware-jev-router-v1.js";
import { executeEvidenceGapAction } from "../evidence-gap/evidence-gap-action-executor.js";
import {
  validateMarket,
  ACTION_OUTCOME,
} from "../evidence-gap/evidence-gap-model-v1.js";
import {
  deriveNextResearchSchedule,
  inferTriggerFromBlocker,
  addDays,
} from "./trigger-policy-v1.js";
import { appendWatchHistory } from "./watch-record-v1.js";
import { upsertFutureWatch } from "./watch-store-v1.js";
import { classifyProviderFailure, applyProviderRetry } from "./scheduler-retry-v1.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import {
  upsertTargetRun,
  isResearchCoverageAirtableConfigured,
  EXECUTION_STATUS,
  RESULT_TYPE,
  buildTargetRunId,
} from "../research-coverage/index.js";

async function lightFingerprint(url, prior) {
  if (!url) return { changed: false, reason: "NO_URL", fingerprint: null, text: "" };
  try {
    const page = await fetchResearchPage(url);
    const text = page.ok ? htmlToSearchableText(page.text || "").slice(0, 12000) : "";
    const fp = buildSourceFingerprint({
      url,
      text,
      etag: null,
      lastModified: null,
    });
    const det = detectSourceMaterialChange(prior, fp);
    return { ...det, text, fetchOk: page.ok };
  } catch (err) {
    return {
      changed: false,
      reason: "FINGERPRINT_FETCH_FAILED",
      error: String(err?.message || err).slice(0, 160),
      fingerprint: prior || null,
      text: "",
    };
  }
}

function deterministicPrefilter(watch) {
  const market = validateMarket(
    {
      event: watch.event,
      eventResolved: watch.event,
      sourceUrl: watch.primarySource,
    },
    watch.hotelShort || ""
  );
  if (!market.ok && market.reason === "WRONG_MARKET") {
    return {
      reject: true,
      stop: STOP_CONDITION.WRONG_MARKET,
      detail: market.detail,
    };
  }
  if (/cancel+ed|cancelled event/i.test(String(watch.event || ""))) {
    return { reject: true, stop: STOP_CONDITION.CANCELLED, detail: "event_cancelled_signal" };
  }
  return { reject: false, market };
}

function mapOutcomeToState(outcome, watch) {
  if (outcome === ACTION_OUTCOME.WRONG_MARKET) {
    return { status: WATCH_STATUS.REJECTED, stop: STOP_CONDITION.WRONG_MARKET };
  }
  if (outcome === ACTION_OUTCOME.CURRENT_CYCLE_CLOSED) {
    return { status: WATCH_STATUS.CLOSED, stop: STOP_CONDITION.CURRENT_CYCLE_CLOSED };
  }
  if (outcome === ACTION_OUTCOME.FULLY_PLACED) {
    return { status: WATCH_STATUS.CLOSED, stop: STOP_CONDITION.FULLY_PLACED };
  }
  if (outcome === ACTION_OUTCOME.PUBLIC_DATA_CEILING) {
    return { status: WATCH_STATUS.PUBLIC_DATA_CEILING, stop: STOP_CONDITION.PUBLIC_DATA_CEILING };
  }
  if (outcome === ACTION_OUTCOME.WAIT_TRIGGER || outcome === ACTION_OUTCOME.BLOCKER_RESOLVED_POSITIVE) {
    return { status: WATCH_STATUS.ACTIVE, stop: null };
  }
  // No promotion in scheduler V1 without full readiness gate (out of scope writes)
  return { status: watch.watchStatus === WATCH_STATUS.PUBLIC_DATA_CEILING
    ? WATCH_STATUS.PUBLIC_DATA_CEILING
    : WATCH_STATUS.ACTIVE, stop: null };
}

/**
 * @returns processed result object
 */
export async function processDueWatch({
  watch,
  dueItem = {},
  runId,
  config,
  priorsLedger = null,
  dryRun = true,
  noJev = false,
  applyPersists = false,
} = {}) {
  const stateBefore = watch.watchStatus;
  const result = {
    watchId: watch.watchId,
    candidateId: watch.candidateId,
    hotelShort: watch.hotelShort,
    hpc: watch.hpc,
    dueReason: dueItem.dueReason || null,
    stateBefore,
    stateAfter: stateBefore,
    fingerprint: null,
    prefilter: null,
    jevAction: null,
    defaultAction: null,
    queries: 0,
    fetches: 0,
    jevCalls: 0,
    outcome: null,
    stopReason: null,
    skipped: false,
    skipReason: null,
    providerError: null,
    targetRunId: null,
    historyAppended: false,
    estimatedCostUsd: 0,
  };

  // --- Fingerprint first (apply only; dry-run plans without fetch) ---
  if (dryRun) {
    result.fingerprint = { planned: true, reason: "DRY_RUN_NO_FETCH" };
  } else {
    const fp = await lightFingerprint(watch.primarySource, watch.sourceFingerprint);
    result.fingerprint = { changed: fp.changed, reason: fp.reason };
    result.fetches += fp.fetchOk ? 1 : 0;

    if (
      !fp.changed &&
      watch.dateProvenance === DATE_PROVENANCE.HEURISTIC &&
      dueItem.dueReason === "HEURISTIC_DUE" &&
      !dueItem.dueReason?.includes("FORCE")
    ) {
      // Policy A: skip deeper research; advance nextResearchDate
      const next = addDays(new Date().toISOString().slice(0, 10), 30);
      let updated = {
        ...watch,
        sourceFingerprint: fp.fingerprint || watch.sourceFingerprint,
        lastCheckedAt: new Date().toISOString(),
        nextResearchDate: next,
        researchWindowStart: next,
        researchWindowEnd: addDays(next, 14),
        nextTriggerCondition: "heuristic_due_no_material_source_change — advanced +30d",
      };
      updated = appendWatchHistory(updated, {
        at: new Date().toISOString(),
        priorState: stateBefore,
        trigger: watch.nextTriggerType,
        result: "SKIP_NO_MATERIAL_CHANGE",
        stateAfter: updated.watchStatus,
        nextTrigger: updated.nextTriggerType,
        nextResearchDate: next,
        schedulerRunId: runId,
      });
      if (applyPersists) upsertFutureWatch(watch.hpc, updated);
      result.skipped = true;
      result.skipReason = "NO_MATERIAL_CHANGE_HEURISTIC";
      result.stateAfter = updated.watchStatus;
      result.historyAppended = true;
      result.fetches = 1;
      result.estimatedCostUsd = 0.005;
      return result;
    }
    if (fp.fingerprint) watch = { ...watch, sourceFingerprint: fp.fingerprint };
  }

  // --- Deterministic prefilter ---
  const pre = deterministicPrefilter(watch);
  result.prefilter = pre;
  if (pre.reject) {
    let updated = {
      ...watch,
      watchStatus: WATCH_STATUS.REJECTED,
      stopConditions: [...(watch.stopConditions || []), pre.stop],
      lastCheckedAt: new Date().toISOString(),
      lastResearchOutcome: pre.stop,
    };
    updated = appendWatchHistory(updated, {
      schedulerRunId: runId,
      priorState: stateBefore,
      trigger: watch.nextTriggerType,
      result: pre.stop,
      stateAfter: WATCH_STATUS.REJECTED,
      dueReason: dueItem.dueReason,
      fingerprintResult: result.fingerprint,
      prefilterResult: pre,
    });
    if (!dryRun && applyPersists) upsertFutureWatch(watch.hpc, updated);
    result.stateAfter = WATCH_STATUS.REJECTED;
    result.outcome = pre.stop;
    result.stopReason = pre.stop;
    result.historyAppended = !dryRun;
    return result;
  }

  if (dryRun) {
    // Plan Jev action without calling providers (unless noJev)
    if (noJev || !config.jevEnabled) {
      result.jevAction = "SKIPPED_NO_JEV";
      result.defaultAction = "VERIFY_HOUSING_STATUS";
    } else {
      try {
        const decision = await decideMarketAwareNextAction({
          watch,
          packet: {
            primaryBlocker: watch.primaryBlocker,
            sourceFamily: watch.sourceFamily,
            preferredResearchActions: watch.preferredResearchActions,
            previousResearchActions: (watch.watchHistory || [])
              .map((h) => h.jevAction)
              .filter(Boolean)
              .slice(-5),
          },
          priorsLedger,
          remainingBudget: {
            queries: config.maxQueriesPerAction,
            fetches: config.maxFetchesPerAction,
            jevActions: 1,
          },
          enableSafeApply: true,
        });
        result.jevCalls = 1;
        result.jevAction = decision.appliedAction;
        result.defaultAction = decision.defaultAction;
      } catch (err) {
        result.providerError = classifyProviderFailure(err);
        result.jevAction = "JEV_UNAVAILABLE_FALLBACK";
      }
    }
    result.skipped = false;
    result.stopReason = "DRY_RUN";
    result.estimatedCostUsd = 0;
    return result;
  }

  // --- APPLY path: one Jev action max ---
  let action = null;
  let decision = null;
  if (noJev || !config.jevEnabled) {
    action = "VERIFY_HOUSING_STATUS";
  } else {
    try {
      decision = await decideMarketAwareNextAction({
        watch,
        packet: {
          primaryBlocker: watch.primaryBlocker,
          sourceFamily: watch.sourceFamily,
          preferredResearchActions: watch.preferredResearchActions,
          previousResearchActions: (watch.watchHistory || [])
            .map((h) => h.jevAction)
            .filter(Boolean)
            .slice(-5),
        },
        priorsLedger,
        remainingBudget: {
          queries: config.maxQueriesPerAction,
          fetches: Math.max(0, config.maxFetchesPerAction - result.fetches),
          jevActions: 1,
        },
        enableSafeApply: true,
      });
      result.jevCalls = 1;
      action = decision.appliedAction;
      result.jevAction = action;
      result.defaultAction = decision.defaultAction;
    } catch (err) {
      const classification = classifyProviderFailure(err);
      result.providerError = classification;
      let updated = applyProviderRetry(watch, classification, config);
      updated = appendWatchHistory(updated, {
        schedulerRunId: runId,
        priorState: stateBefore,
        result: classification.class,
        stateAfter: updated.watchStatus,
        nextResearchDate: updated.nextResearchDate,
      });
      if (applyPersists) upsertFutureWatch(watch.hpc, updated);
      result.stateAfter = updated.watchStatus;
      result.historyAppended = true;
      result.stopReason = classification.class;
      // Never reject on provider failure
      return result;
    }
  }

  let exec;
  try {
    exec = await executeEvidenceGapAction({
      action,
      candidate: {
        event: watch.event,
        eventResolved: watch.event,
        sourceUrl: watch.primarySource,
        futureCycle: watch.cycleId,
      },
      hotelShort: watch.hotelShort,
      primaryBlocker: watch.primaryBlocker,
    });
  } catch (err) {
    const classification = classifyProviderFailure(err);
    result.providerError = classification;
    let updated = applyProviderRetry(watch, classification, config);
    updated = appendWatchHistory(updated, {
      schedulerRunId: runId,
      priorState: stateBefore,
      jevAction: action,
      result: classification.class,
      stateAfter: updated.watchStatus,
    });
    if (applyPersists) upsertFutureWatch(watch.hpc, updated);
    result.stateAfter = updated.watchStatus;
    result.historyAppended = true;
    result.stopReason = classification.class;
    return result;
  }

  result.queries += exec.queries || 0;
  result.fetches += exec.fetches || 0;
  result.outcome = exec.outcome;
  result.estimatedCostUsd = Number(
    (result.jevCalls * 0.01 + result.queries * 0.02 + result.fetches * 0.005).toFixed(4)
  );

  const mapped = mapOutcomeToState(exec.outcome, watch);
  const trigger = inferTriggerFromBlocker(watch.primaryBlocker, exec.note);
  const schedule = deriveNextResearchSchedule({
    triggerType: mapped.status === WATCH_STATUS.PUBLIC_DATA_CEILING ? "PUBLIC_DATA_CEILING" : trigger,
    candidate: watch,
    pageText: (exec.evidenceSnippets || []).map((s) => s.excerpt).join(" "),
    watchStatus: mapped.status,
  });

  let updated = {
    ...watch,
    watchStatus: mapped.status,
    primaryBlocker: watch.primaryBlocker,
    nextTriggerType: schedule.nextTriggerType || trigger,
    nextTriggerCondition: schedule.nextTriggerCondition,
    nextResearchDate: schedule.nextResearchDate,
    researchWindowStart: schedule.researchWindowStart,
    researchWindowEnd: schedule.researchWindowEnd,
    dateProvenance: schedule.dateProvenance,
    lastCheckedAt: new Date().toISOString(),
    lastResearchDate: new Date().toISOString(),
    lastResearchOutcome: exec.outcome,
    lastMaterialChangeAt:
      result.fingerprint?.changed ? new Date().toISOString() : watch.lastMaterialChangeAt,
    retryCount: 0,
    stopConditions: mapped.stop
      ? [...new Set([...(watch.stopConditions || []), mapped.stop])]
      : watch.stopConditions || [],
  };

  const targetRunId = buildTargetRunId({
    runId,
    targetId: `gdi_fw_${watch.candidateId || watch.watchId}`,
  });
  result.targetRunId = targetRunId;

  updated = appendWatchHistory(updated, {
    schedulerRunId: runId,
    priorState: stateBefore,
    trigger: watch.nextTriggerType,
    dueReason: dueItem.dueReason,
    fingerprintResult: result.fingerprint,
    prefilterResult: pre,
    jevAction: action,
    defaultAction: decision?.defaultAction || null,
    queries: result.queries,
    fetches: result.fetches,
    result: exec.outcome,
    stateAfter: mapped.status,
    nextTrigger: updated.nextTriggerType,
    nextResearchDate: updated.nextResearchDate,
    targetRunId,
    cost: result.estimatedCostUsd,
  });

  if (applyPersists) {
    upsertFutureWatch(watch.hpc, updated);
    if (isResearchCoverageAirtableConfigured()) {
      try {
        await upsertTargetRun(
          {
            targetRunId,
            hotelId: watch.hpc,
            targetId: `gdi_fw_${watch.candidateId || watch.watchId}`,
            runId,
            executionStatus: EXECUTION_STATUS.RESEARCHED,
            researchExecuted: true,
            resultType:
              exec.outcome === ACTION_OUTCOME.NO_NEW_EVIDENCE
                ? RESULT_TYPE.NO_MATERIAL_CHANGE
                : RESULT_TYPE.SIGNAL_UPDATED,
            playbook: "GDI_FUTURE_WATCH_SCHEDULER_V1",
            queriesUsed: result.queries,
            fetchesUsed: result.fetches,
            sourceUrls: exec.fetchedUrls || [],
            lastResultSummary: `${action}→${exec.outcome}`.slice(0, 500),
            payload: {
              jevAction: action,
              outcome: exec.outcome,
              stateBefore,
              stateAfter: mapped.status,
              dueReason: dueItem.dueReason,
            },
          },
          { dryRun: false }
        );
      } catch (err) {
        result.targetRunError = String(err?.message || err).slice(0, 160);
      }
    }
  }

  result.stateAfter = mapped.status;
  result.stopReason = mapped.stop || exec.outcome;
  result.historyAppended = true;
  return result;
}
