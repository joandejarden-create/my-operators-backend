/**
 * Bridge: canonical fourteen-segment discovery → existing GDI execution ledger.
 *
 * Does not invent provider calls. Distinguishes cache / fixture / live / skip.
 * Apify defaults to honest SKIP (APIFY_NOT_ORCHESTRATED) unless orchestration
 * is explicitly enabled behind existing auth/mock gates.
 */

import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";
import { workflowPrefersApify } from "../research/apify/apify-fulle2e-orchestration-v1.js";
import {
  createEmptyExecutionRun,
  saveExecutionRun,
  loadExecutionRun,
  newFullE2eRunId,
  SEGMENT_EXEC_STATUS,
  PROVIDER_CALL_STATUS,
  APIFY_LIFECYCLE,
  STAGE_STATUS,
  E2E_STAGE,
  GDI_PROVIDER_IDS,
  certifyExecutionRun,
  recomputeSummary,
} from "./gdi-execution-ledger-store-v1.js";
import {
  setActiveExecutionRunContext,
  clearActiveExecutionRunContext,
  getActiveExecutionRunContext,
  observeProviderCall,
  observeApifyLifecycle,
  observeSegmentStatus,
  observeStageStatus,
  observeFinalizeCertificate,
} from "./gdi-execution-run-context-v1.js";

export const GDI_FOURTEEN_SEGMENT_EXECUTION_LEDGER_BRIDGE_V1 =
  "gdi-fourteen-segment-execution-ledger-bridge-v1";

/** Canonical Apify honesty / skip reason codes (compatible with ledger skipReason). */
export const APIFY_SKIP_REASON = Object.freeze({
  APIFY_NOT_ORCHESTRATED: "APIFY_NOT_ORCHESTRATED",
  APIFY_UNAUTHORIZED: "APIFY_UNAUTHORIZED",
  APIFY_DISABLED: "APIFY_DISABLED",
  APIFY_NOT_APPLICABLE: "APIFY_NOT_APPLICABLE",
  APIFY_BUDGET_BLOCKED: "APIFY_BUDGET_BLOCKED",
  APIFY_MOCK_ONLY: "APIFY_MOCK_ONLY",
});

function text(v) {
  return String(v == null ? "" : v).trim();
}

function nowIso() {
  return new Date().toISOString();
}

/**
 * Create durable run + activate process-local context for observability hooks.
 */
export function beginFourteenSegmentExecutionRun({
  hotelId,
  runId = null,
  root = undefined,
  label = null,
  plannedSegmentIds = null,
  researchRunId = null,
} = {}) {
  const id = runId || newFullE2eRunId("gdi_e2e_14seg");
  const planned = plannedSegmentIds || listFourteenWorkflowSegmentIds();
  let run = createEmptyExecutionRun({
    hotelId,
    runId: id,
    label: label || "fourteen-segment-discovery",
    plannedSegmentIds: planned,
  });
  run.startedAt = nowIso();
  run.runStatus = "RUNNING";
  run.lineage = [
    ...(run.lineage || []),
    {
      at: nowIso(),
      kind: "FOURTEEN_SEGMENT_DISCOVERY_START",
      researchRunId: researchRunId || null,
      bridge: GDI_FOURTEEN_SEGMENT_EXECUTION_LEDGER_BRIDGE_V1,
    },
  ];
  saveExecutionRun(run, root);
  setActiveExecutionRunContext({ hotelId, runId: id, root });
  observeStageStatus(E2E_STAGE.E_SUBSTANTIVE_RESEARCH, {
    status: STAGE_STATUS.RUNNING,
  });
  return { hotelId, runId: id, root, researchRunId: researchRunId || null };
}

export function markSegmentStarted(segmentId, patch = {}) {
  return observeSegmentStatus(segmentId, {
    status: SEGMENT_EXEC_STATUS.STARTED,
    discoveryStatus: SEGMENT_EXEC_STATUS.STARTED,
    startedAt: nowIso(),
    ...patch,
  });
}

/**
 * Map a discovery cell trace into truthful segment + provider ledger rows.
 * Never marks EXECUTED without provider/cache/fixture evidence.
 */
export function recordFourteenSegmentCellOutcome(trace = {}) {
  const segmentId = text(trace.segmentId);
  if (!segmentId) return null;

  const finishedAt = trace.finishedAt || nowIso();
  const providerCalls = Number(trace.providerCallCount || 0);
  const fresh = Number(trace.freshRetrievals || 0);
  const candidates = Array.isArray(trace.candidates) ? trace.candidates : [];
  const candidateCount =
    Number(trace.extractedCandidates ?? candidates.length) || 0;
  const qualified =
    Number(
      trace.qualificationHandoff?.qualifiedCount ??
        trace.qualifiedOpportunities ??
        0,
    ) || 0;
  const retrievalMode = String(trace.retrievalMode || "");
  const stopReason = text(trace.stopReason);
  const costUsd = Number(trace.researchCostUsd || 0) || 0;

  if (
    stopReason === "INVALID_ARGS" ||
    stopReason === "MISSING_EXECUTOR_SPEC" ||
    trace.ok === false
  ) {
    observeSegmentStatus(segmentId, {
      status: SEGMENT_EXEC_STATUS.FAILED,
      discoveryStatus: SEGMENT_EXEC_STATUS.FAILED,
      completedAt: finishedAt,
      candidatesFound: candidateCount,
      qualifiedOpportunities: qualified,
      errorOrSkipReason: stopReason || trace.runnerNote || "cell_failed",
      plannedStrategy: trace.strategy || null,
    });
    return { status: SEGMENT_EXEC_STATUS.FAILED, reason: stopReason };
  }

  if (
    stopReason.includes("BUDGET") ||
    stopReason.includes("GLOBAL_BUDGET") ||
    retrievalMode.includes("BLOCKED_BUDGET")
  ) {
    observeProviderCall({
      providerId: GDI_PROVIDER_IDS.EXA_AGENT,
      segmentId,
      researchStage: "DISCOVERY",
      operation: "DISCOVERY",
      status: PROVIDER_CALL_STATUS.BLOCKED,
      skipReason: stopReason || "BLOCKED_BUDGET",
      correlationId: `budget_${trace.researchRunId || "run"}_${segmentId}`,
      candidateCount: 0,
    });
    observeSegmentStatus(segmentId, {
      status: SEGMENT_EXEC_STATUS.BLOCKED_BUDGET,
      discoveryStatus: SEGMENT_EXEC_STATUS.BLOCKED_BUDGET,
      completedAt: finishedAt,
      errorOrSkipReason: stopReason || "BLOCKED_BUDGET",
      plannedStrategy: trace.strategy || null,
    });
    return { status: SEGMENT_EXEC_STATUS.BLOCKED_BUDGET, reason: stopReason };
  }

  const isHistorical =
    retrievalMode.includes("HISTORICAL") ||
    String(trace.sourceFamily || "").includes("historical");
  const isFixture =
    retrievalMode === "FIXTURE" ||
    String(trace.executorId || "").includes("fixture") ||
    String(trace.retrievalMode || "").includes("FIXTURE");
  const isResumed = trace.resumedFromCheckpoint === true;
  const hasActualResearch = providerCalls > 0 || fresh > 0;

  if (hasActualResearch) {
    observeProviderCall({
      providerId: GDI_PROVIDER_IDS.EXA_AGENT,
      segmentId,
      researchStage: "DISCOVERY",
      operation: "DISCOVERY",
      status: PROVIDER_CALL_STATUS.COMPLETED,
      actualCostUsd: costUsd || null,
      candidateCount,
      resultCount: candidateCount,
      providerRequestId: text(trace.researchRunId) || null,
      correlationId: `exa_${text(trace.researchRunId) || "run"}_${segmentId}_disc`,
      cacheHit: false,
    });
    observeSegmentStatus(segmentId, {
      status: SEGMENT_EXEC_STATUS.COMPLETED,
      discoveryStatus: SEGMENT_EXEC_STATUS.COMPLETED,
      completedAt: finishedAt,
      candidatesFound: candidateCount,
      qualifiedOpportunities: qualified,
      plannedStrategy: trace.strategy || null,
      errorOrSkipReason: null,
    });
    return { status: SEGMENT_EXEC_STATUS.COMPLETED, mode: "LIVE_OR_PROVIDER" };
  }

  if (isHistorical || isResumed) {
    observeProviderCall({
      providerId: GDI_PROVIDER_IDS.HISTORICAL_CACHE,
      segmentId,
      researchStage: "DISCOVERY",
      operation: "REPLAY",
      status: PROVIDER_CALL_STATUS.CACHE_HIT,
      cacheHit: true,
      candidateCount,
      resultCount: candidateCount,
      actualCostUsd: 0,
      correlationId: `hist_${text(trace.researchRunId) || "run"}_${segmentId}`,
      skipReason: isResumed
        ? text(trace.liveSkipReason) || "RESUMED_CACHE"
        : null,
    });
    observeSegmentStatus(segmentId, {
      status: SEGMENT_EXEC_STATUS.COMPLETED,
      discoveryStatus: SEGMENT_EXEC_STATUS.COMPLETED,
      completedAt: finishedAt,
      candidatesFound: candidateCount,
      qualifiedOpportunities: qualified,
      plannedStrategy: trace.strategy || null,
      errorOrSkipReason: null,
    });
    return { status: SEGMENT_EXEC_STATUS.COMPLETED, mode: "HISTORICAL_CACHE" };
  }

  if (isFixture || trace.executorInvoked === true) {
    const treatAsFixture =
      isFixture ||
      retrievalMode === "NO_RETRIEVAL" ||
      Number(trace.cacheHits || 0) > 0;

    if (
      treatAsFixture &&
      (isFixture || candidateCount > 0 || retrievalMode === "NO_RETRIEVAL")
    ) {
      observeProviderCall({
        providerId: GDI_PROVIDER_IDS.FIXTURE_DETERMINISTIC,
        segmentId,
        researchStage: "DISCOVERY",
        operation: "FIXTURE_DISCOVERY",
        status: PROVIDER_CALL_STATUS.CACHE_HIT,
        cacheHit: true,
        candidateCount,
        resultCount: candidateCount,
        actualCostUsd: 0,
        correlationId: `fix_${text(trace.researchRunId) || "run"}_${segmentId}`,
        providerDetails: {
          retrievalMode,
          mock: true,
          liveProviderInvocation: false,
        },
      });
      observeSegmentStatus(segmentId, {
        status: SEGMENT_EXEC_STATUS.COMPLETED,
        discoveryStatus: SEGMENT_EXEC_STATUS.COMPLETED,
        completedAt: finishedAt,
        candidatesFound: candidateCount,
        qualifiedOpportunities: qualified,
        plannedStrategy: trace.strategy || null,
        errorOrSkipReason: null,
      });
      return { status: SEGMENT_EXEC_STATUS.COMPLETED, mode: "FIXTURE" };
    }
  }

  observeProviderCall({
    providerId: GDI_PROVIDER_IDS.FIXTURE_DETERMINISTIC,
    segmentId,
    researchStage: "DISCOVERY",
    operation: "NO_INVOCATION",
    status: PROVIDER_CALL_STATUS.SKIPPED,
    skipReason: stopReason || "NO_PROVIDER_OR_CACHE_INVOCATION",
    candidateCount: 0,
    correlationId: `skip_${text(trace.researchRunId) || "run"}_${segmentId}`,
  });
  observeSegmentStatus(segmentId, {
    status: SEGMENT_EXEC_STATUS.SKIPPED,
    discoveryStatus: SEGMENT_EXEC_STATUS.SKIPPED,
    completedAt: finishedAt,
    candidatesFound: candidateCount,
    qualifiedOpportunities: qualified,
    plannedStrategy: trace.strategy || null,
    errorOrSkipReason: stopReason || "NO_PROVIDER_OR_CACHE_INVOCATION",
  });
  return {
    status: SEGMENT_EXEC_STATUS.SKIPPED,
    reason: stopReason || "NO_PROVIDER_OR_CACHE_INVOCATION",
  };
}

/**
 * Honest Apify accounting for a segment on the canonical Full E2E path.
 * Does not submit Actors unless orchestration is explicitly enabled.
 */
export async function accountApifyForFourteenSegmentCell({
  segmentId,
  hotel = {},
  authorizeApifyLive = false,
  enableApifyOrchestration = false,
  forceMockOnly = true,
  apifyDisabled = false,
  orchestrateFn = null,
} = {}) {
  const prefers = workflowPrefersApify(segmentId);
  if (!prefers) {
    observeApifyLifecycle({
      state: APIFY_LIFECYCLE.APIFY_SKIPPED,
      segmentId,
      reason: APIFY_SKIP_REASON.APIFY_NOT_APPLICABLE,
    });
    observeProviderCall({
      providerId: GDI_PROVIDER_IDS.APIFY,
      segmentId,
      researchStage: "DISCOVERY",
      operation: "DISCOVERY",
      status: PROVIDER_CALL_STATUS.SKIPPED,
      skipReason: APIFY_SKIP_REASON.APIFY_NOT_APPLICABLE,
      correlationId: `apify_na_${segmentId}`,
    });
    return {
      status: "SKIPPED",
      reason: APIFY_SKIP_REASON.APIFY_NOT_APPLICABLE,
      dispatched: false,
    };
  }

  if (apifyDisabled === true) {
    observeApifyLifecycle({
      state: APIFY_LIFECYCLE.APIFY_SKIPPED,
      segmentId,
      reason: APIFY_SKIP_REASON.APIFY_DISABLED,
    });
    observeProviderCall({
      providerId: GDI_PROVIDER_IDS.APIFY,
      segmentId,
      operation: "DISCOVERY",
      status: PROVIDER_CALL_STATUS.SKIPPED,
      skipReason: APIFY_SKIP_REASON.APIFY_DISABLED,
      correlationId: `apify_disabled_${segmentId}`,
    });
    return {
      status: "SKIPPED",
      reason: APIFY_SKIP_REASON.APIFY_DISABLED,
      dispatched: false,
    };
  }

  if (enableApifyOrchestration !== true) {
    observeApifyLifecycle({
      state: APIFY_LIFECYCLE.APIFY_CONFIGURED,
      segmentId,
    });
    observeApifyLifecycle({
      state: APIFY_LIFECYCLE.APIFY_SKIPPED,
      segmentId,
      reason: APIFY_SKIP_REASON.APIFY_NOT_ORCHESTRATED,
    });
    observeProviderCall({
      providerId: GDI_PROVIDER_IDS.APIFY,
      segmentId,
      researchStage: "DISCOVERY",
      operation: "DISCOVERY",
      status: PROVIDER_CALL_STATUS.SKIPPED,
      skipReason: APIFY_SKIP_REASON.APIFY_NOT_ORCHESTRATED,
      correlationId: `apify_not_orch_${segmentId}`,
      providerDetails: {
        configured: true,
        orchestratedInFullE2e: false,
        live: false,
      },
    });
    return {
      status: "SKIPPED",
      reason: APIFY_SKIP_REASON.APIFY_NOT_ORCHESTRATED,
      dispatched: false,
    };
  }

  if (authorizeApifyLive !== true && forceMockOnly !== true) {
    observeApifyLifecycle({
      state: APIFY_LIFECYCLE.APIFY_SKIPPED,
      segmentId,
      reason: APIFY_SKIP_REASON.APIFY_UNAUTHORIZED,
    });
    observeProviderCall({
      providerId: GDI_PROVIDER_IDS.APIFY,
      segmentId,
      operation: "DISCOVERY",
      status: PROVIDER_CALL_STATUS.BLOCKED,
      skipReason: APIFY_SKIP_REASON.APIFY_UNAUTHORIZED,
      correlationId: `apify_unauth_${segmentId}`,
    });
    return {
      status: "BLOCKED",
      reason: APIFY_SKIP_REASON.APIFY_UNAUTHORIZED,
      dispatched: false,
    };
  }

  const runApify =
    orchestrateFn ||
    (await import("../research/apify/apify-fulle2e-orchestration-v1.js"))
      .orchestrateApifyDiscoveryForWorkflow;

  const result = await runApify({
    workflow: { id: segmentId },
    hotel,
    authorizeApifyLive: authorizeApifyLive === true && forceMockOnly !== true,
    forceMockOnly: forceMockOnly !== false || authorizeApifyLive !== true,
  });

  return {
    status: result?.status || "UNKNOWN",
    reason: result?.error || result?.status || null,
    dispatched: result?.dispatched === true,
    mock: forceMockOnly !== false || authorizeApifyLive !== true,
    result,
  };
}

/**
 * Record deep-research outcome for a segment on the active run.
 * Does not invent provider calls. Distinguishes fixture/cache/live/skip/fail.
 */
export function recordDeepResearchOutcome({
  segmentId,
  status = null,
  providerId = GDI_PROVIDER_IDS.EXA_AGENT,
  opportunityId = null,
  costUsd = 0,
  cacheHit = false,
  skipped = false,
  blocked = false,
  failed = false,
  reason = null,
  providerRequestId = null,
  mock = false,
} = {}) {
  const sid = text(segmentId);
  if (!sid) return null;

  let callStatus = PROVIDER_CALL_STATUS.COMPLETED;
  let deepStatus = SEGMENT_EXEC_STATUS.COMPLETED;
  if (failed) {
    callStatus = PROVIDER_CALL_STATUS.FAILED;
    deepStatus = SEGMENT_EXEC_STATUS.FAILED;
  } else if (blocked) {
    callStatus = PROVIDER_CALL_STATUS.BLOCKED;
    deepStatus = text(reason || "").toLowerCase().includes("budget")
      ? SEGMENT_EXEC_STATUS.BLOCKED_BUDGET
      : SEGMENT_EXEC_STATUS.BLOCKED_CONFIG;
  } else if (skipped) {
    callStatus = PROVIDER_CALL_STATUS.SKIPPED;
    deepStatus = SEGMENT_EXEC_STATUS.SKIPPED;
  } else if (cacheHit || mock) {
    callStatus = PROVIDER_CALL_STATUS.CACHE_HIT;
    deepStatus = SEGMENT_EXEC_STATUS.COMPLETED;
  }

  const corrBase = `deep_${sid}_${text(opportunityId) || "seg"}`;
  observeProviderCall({
    providerId: text(providerId) || GDI_PROVIDER_IDS.EXA_AGENT,
    segmentId: sid,
    researchStage: "DEEP_RESEARCH",
    operation: "DEEP_RESEARCH",
    status: callStatus,
    skipReason: skipped || blocked ? text(reason) || null : null,
    errorCode: failed ? text(reason) || "deep_research_failed" : null,
    actualCostUsd: Number(costUsd) || 0,
    cacheHit: cacheHit === true || mock === true,
    providerRequestId: text(providerRequestId) || null,
    correlationId: corrBase,
    providerDetails: {
      opportunityId: text(opportunityId) || null,
      mock: mock === true,
      live: mock !== true && cacheHit !== true && !skipped && !blocked,
    },
  });
  observeSegmentStatus(sid, {
    deepResearchStatus: status || deepStatus,
    deepResearchReason: text(reason) || null,
  });
  return { segmentId: sid, deepResearchStatus: status || deepStatus };
}

/**
 * When a hotel run has no deep-research candidates for a segment, mark deep as
 * SKIPPED with an explicit reason so discovery-only cannot look like full complete.
 */
export function markDeepResearchNotApplicableOrSkipped(
  segmentIds = [],
  reason = "NO_DEEP_RESEARCH_CANDIDATES_FOR_SEGMENT",
) {
  const out = [];
  for (const sid of segmentIds) {
    out.push(
      recordDeepResearchOutcome({
        segmentId: sid,
        skipped: true,
        reason,
        providerId: GDI_PROVIDER_IDS.FIXTURE_DETERMINISTIC,
        mock: true,
        cacheHit: true,
      }),
    );
  }
  return out;
}

export function finalizeFourteenSegmentExecutionRun(opts = {}) {
  const ctx = getActiveExecutionRunContext();
  observeStageStatus(E2E_STAGE.E_SUBSTANTIVE_RESEARCH, {
    status: STAGE_STATUS.COMPLETED,
  });
  const certOpts = {
    budgetsRespected: opts.budgetsRespected !== false,
    founderApprovalRequired: true,
    stagedPublicationGenerated: false,
    customerVisibilityGatesPassed: null,
    outputsIngested: opts.outputsIngested !== false,
    blocked: opts.blocked === true,
    discoveryPhaseOnly: opts.discoveryPhaseOnly === true,
    requireDeepResearch: opts.requireDeepResearch,
  };
  const finalized = observeFinalizeCertificate(certOpts);
  clearActiveExecutionRunContext();
  if (!finalized && ctx?.hotelId && ctx?.runId) {
    const run = loadExecutionRun(ctx.hotelId, ctx.runId, ctx.root);
    if (run) {
      const certificate = certifyExecutionRun(run, certOpts);
      const next = recomputeSummary({
        ...run,
        certificate,
        verdict: certificate.verdict,
        completedAt: run.completedAt || nowIso(),
        runStatus:
          certificate.verdict === "FULL_E2E_EXECUTION_COMPLETE"
            ? "COMPLETED"
            : certificate.verdict === "FULL_E2E_FAILED"
              ? "FAILED"
              : certificate.verdict === "FULL_E2E_BLOCKED"
                ? "BLOCKED"
                : "PARTIAL",
      });
      saveExecutionRun(next, ctx.root);
      return next;
    }
  }
  return finalized;
}

export function ensureActiveRunOrNull() {
  return getActiveExecutionRunContext();
}
