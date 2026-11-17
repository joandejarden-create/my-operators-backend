/**
 * Durable GDI Full E2E execution observability ledger store V1.
 * Survives restarts. Does not invent provider calls.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, randomBytes } from "node:crypto";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";
import {
  GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES,
  normalizeCanonicalExaSegmentId,
} from "../research/exa-agent/gdi-exa-segment-request-titles-v1.js";
import {
  E2E_STAGE,
  E2E_STAGE_DEFINITIONS,
} from "../workflow/full-e2e-contract-v1.js";
import {
  GDI_PROVIDER_INVENTORY,
  GDI_PROVIDER_IDS,
  listProviderInventory,
} from "./gdi-provider-inventory-v1.js";

export const GDI_EXECUTION_OBSERVABILITY_V1 =
  "gdi-full-e2e-execution-observability-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const SEGMENT_EXEC_STATUS = Object.freeze({
  PLANNED: "PLANNED",
  STARTED: "STARTED",
  EXECUTED: "EXECUTED",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
  SKIPPED: "SKIPPED",
  DISABLED: "DISABLED",
  BLOCKED_BUDGET: "BLOCKED_BUDGET",
  BLOCKED_CONFIG: "BLOCKED_CONFIG",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  UNKNOWN: "UNKNOWN",
});

export const PROVIDER_CALL_STATUS = Object.freeze({
  SUBMITTED: "SUBMITTED",
  RUNNING: "RUNNING",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
  SKIPPED: "SKIPPED",
  CACHE_HIT: "CACHE_HIT",
  BLOCKED: "BLOCKED",
});

export const APIFY_LIFECYCLE = Object.freeze({
  APIFY_CONFIGURED: "APIFY_CONFIGURED",
  APIFY_ENABLED: "APIFY_ENABLED",
  APIFY_JOB_SUBMITTED: "APIFY_JOB_SUBMITTED",
  APIFY_JOB_RUNNING: "APIFY_JOB_RUNNING",
  APIFY_JOB_COMPLETED: "APIFY_JOB_COMPLETED",
  APIFY_RESULTS_RETRIEVED: "APIFY_RESULTS_RETRIEVED",
  APIFY_RESULTS_INGESTED: "APIFY_RESULTS_INGESTED",
  APIFY_FAILED: "APIFY_FAILED",
  APIFY_SKIPPED: "APIFY_SKIPPED",
});

export const RUN_VERDICT = Object.freeze({
  FULL_E2E_EXECUTION_COMPLETE: "FULL_E2E_EXECUTION_COMPLETE",
  FULL_E2E_PARTIAL: "FULL_E2E_PARTIAL",
  FULL_E2E_BLOCKED: "FULL_E2E_BLOCKED",
  FULL_E2E_FAILED: "FULL_E2E_FAILED",
});

export const STAGE_STATUS = Object.freeze({
  PENDING: "PENDING",
  RUNNING: "RUNNING",
  COMPLETED: "COMPLETED",
  PARTIAL: "PARTIAL",
  FAILED: "FAILED",
  SKIPPED: "SKIPPED",
  BLOCKED: "BLOCKED",
  UNKNOWN: "UNKNOWN",
});

function nowIso() {
  return new Date().toISOString();
}

function text(v) {
  return String(v == null ? "" : v).trim();
}

function atomicWriteJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  try {
    fs.renameSync(tmp, filePath);
  } catch {
    fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
    try {
      fs.unlinkSync(tmp);
    } catch {
      /* best-effort */
    }
  }
}

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

export function defaultExecutionObservabilityRoot(root = ROOT) {
  return path.join(
    root,
    "data",
    "group-demand-intelligence",
    "execution-observability",
  );
}

export function hotelExecutionDir(hotelId, root = ROOT) {
  return path.join(defaultExecutionObservabilityRoot(root), text(hotelId));
}

export function runLedgerPath(hotelId, runId, root = ROOT) {
  return path.join(hotelExecutionDir(hotelId, root), `${text(runId)}.json`);
}

export function newFullE2eRunId(prefix = "gdi_e2e_run") {
  return `${prefix}_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
}

function emptySegmentRow(segmentId) {
  const canonical = normalizeCanonicalExaSegmentId(segmentId) || segmentId;
  return {
    segmentId: canonical,
    displayTitle:
      GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES[canonical] || canonical,
    planned: false,
    status: SEGMENT_EXEC_STATUS.PLANNED,
    discoveryStatus: SEGMENT_EXEC_STATUS.PLANNED,
    deepResearchStatus: SEGMENT_EXEC_STATUS.PLANNED,
    providersUsed: [],
    requestCount: 0,
    costUsd: 0,
    candidatesFound: 0,
    qualifiedOpportunities: 0,
    customerVisibleAdditions: 0,
    errorOrSkipReason: null,
    providerCallIds: [],
  };
}

function emptyStageRow(def) {
  return {
    stageId: def.id,
    letter: def.letter || null,
    title: def.title || def.id,
    status: STAGE_STATUS.PENDING,
    startedAt: null,
    completedAt: null,
    itemCounts: {},
    error: null,
    validationGates: def.evidenceRequired || [],
  };
}

export function createEmptyExecutionRun({
  hotelId,
  runId = null,
  label = null,
  plannedSegmentIds = null,
} = {}) {
  const id = runId || newFullE2eRunId();
  const segments = {};
  const planned = plannedSegmentIds || listFourteenWorkflowSegmentIds();
  for (const sid of listFourteenWorkflowSegmentIds()) {
    segments[sid] = emptySegmentRow(sid);
    if (planned.includes(sid)) {
      segments[sid].planned = true;
      segments[sid].status = SEGMENT_EXEC_STATUS.PLANNED;
    } else {
      segments[sid].planned = false;
      segments[sid].status = SEGMENT_EXEC_STATUS.NOT_APPLICABLE;
      segments[sid].discoveryStatus = SEGMENT_EXEC_STATUS.NOT_APPLICABLE;
      segments[sid].deepResearchStatus = SEGMENT_EXEC_STATUS.NOT_APPLICABLE;
      segments[sid].errorOrSkipReason = "not_in_planned_scope";
    }
  }

  const stages = E2E_STAGE_DEFINITIONS.map((d) => emptyStageRow(d));

  return {
    schema: GDI_EXECUTION_OBSERVABILITY_V1,
    runId: id,
    hotelId: text(hotelId),
    label: label || null,
    createdAt: nowIso(),
    updatedAt: nowIso(),
    startedAt: null,
    completedAt: null,
    runStatus: "PENDING",
    verdict: null,
    certificate: null,
    summary: {
      segmentsCompletedOf14: 0,
      providersInvoked: [],
      providerCalls: 0,
      totalSpendUsd: 0,
      durationMs: null,
      newCandidates: 0,
      qualifiedOpportunities: 0,
      customerVisibleAdditions: 0,
      failedOrBlockedStages: 0,
    },
    segments,
    stages,
    providerCalls: [],
    apifyLifecycle: [],
    errors: [],
    lineage: [],
    historical: false,
    historicalConfidence: null,
  };
}

/**
 * Record a provider invocation. Dedupes by correlationId when present.
 */
export function recordProviderCall(run, call = {}) {
  const next = structuredClone(run);
  const correlationId =
    text(call.correlationId) ||
    `corr_${createHash("sha1")
      .update(
        [
          call.providerId,
          call.providerRequestId,
          call.segmentId,
          call.operation,
          call.startedAt,
        ].join("|"),
      )
      .digest("hex")
      .slice(0, 12)}`;

  const existingIdx = next.providerCalls.findIndex(
    (c) => c.correlationId === correlationId,
  );

  const segmentId = call.segmentId
    ? normalizeCanonicalExaSegmentId(call.segmentId) || text(call.segmentId)
    : "CROSS_SEGMENT";

  const entry = {
    correlationId,
    runId: next.runId,
    hotelId: next.hotelId,
    segmentId,
    researchStage: text(call.researchStage) || null,
    providerId: text(call.providerId),
    providerOperation: text(call.operation || call.providerOperation) || null,
    providerRequestId: text(call.providerRequestId) || null,
    startedAt: call.startedAt || nowIso(),
    completedAt: call.completedAt || null,
    durationMs:
      call.durationMs != null && Number.isFinite(Number(call.durationMs))
        ? Number(call.durationMs)
        : null,
    status: text(call.status || PROVIDER_CALL_STATUS.SUBMITTED).toUpperCase(),
    retryCount: Number(call.retryCount || 0) || 0,
    actualCostUsd:
      call.actualCostUsd == null || !Number.isFinite(Number(call.actualCostUsd))
        ? null
        : Number(call.actualCostUsd),
    estimatedCostUsd:
      call.estimatedCostUsd == null ||
      !Number.isFinite(Number(call.estimatedCostUsd))
        ? null
        : Number(call.estimatedCostUsd),
    cacheHit: call.cacheHit === true,
    candidateCount:
      call.candidateCount == null ? null : Number(call.candidateCount),
    resultCount: call.resultCount == null ? null : Number(call.resultCount),
    errorCode: text(call.errorCode) || null,
    skipReason: text(call.skipReason) || null,
    providerDetails: call.providerDetails || null,
  };

  if (existingIdx >= 0) {
    next.providerCalls[existingIdx] = {
      ...next.providerCalls[existingIdx],
      ...entry,
      startedAt: next.providerCalls[existingIdx].startedAt || entry.startedAt,
    };
  } else {
    next.providerCalls.push(entry);
  }

  // Segment rollups from actual calls only
  if (segmentId !== "CROSS_SEGMENT" && next.segments[segmentId]) {
    const row = next.segments[segmentId];
    if (!row.providerCallIds.includes(correlationId)) {
      row.providerCallIds.push(correlationId);
      row.requestCount = row.providerCallIds.length;
    }
    if (entry.providerId && !row.providersUsed.includes(entry.providerId)) {
      row.providersUsed.push(entry.providerId);
    }
    const cost = entry.actualCostUsd ?? entry.estimatedCostUsd ?? 0;
    if (cost) row.costUsd = Number((row.costUsd + Number(cost)).toFixed(6));
    if (entry.candidateCount != null) {
      row.candidatesFound = Math.max(
        row.candidatesFound,
        Number(entry.candidateCount) || 0,
      );
    }
    if (
      entry.status === PROVIDER_CALL_STATUS.SUBMITTED ||
      entry.status === PROVIDER_CALL_STATUS.RUNNING ||
      entry.status === PROVIDER_CALL_STATUS.COMPLETED ||
      entry.status === PROVIDER_CALL_STATUS.CACHE_HIT
    ) {
      if (
        row.status === SEGMENT_EXEC_STATUS.PLANNED ||
        row.status === SEGMENT_EXEC_STATUS.STARTED
      ) {
        row.status = SEGMENT_EXEC_STATUS.EXECUTED;
      }
      const stageKey =
        String(entry.researchStage || "").toUpperCase().includes("DEEP")
          ? "deepResearchStatus"
          : "discoveryStatus";
      if (
        row[stageKey] === SEGMENT_EXEC_STATUS.PLANNED ||
        row[stageKey] === SEGMENT_EXEC_STATUS.STARTED
      ) {
        row[stageKey] = SEGMENT_EXEC_STATUS.EXECUTED;
      }
      if (entry.status === PROVIDER_CALL_STATUS.COMPLETED || entry.cacheHit) {
        row[stageKey] = SEGMENT_EXEC_STATUS.COMPLETED;
      }
    }
    if (entry.status === PROVIDER_CALL_STATUS.FAILED) {
      row.status = SEGMENT_EXEC_STATUS.FAILED;
      row.errorOrSkipReason = entry.errorCode || entry.skipReason || "provider_failed";
    }
    if (entry.status === PROVIDER_CALL_STATUS.SKIPPED) {
      const alreadyTerminalOk = [
        SEGMENT_EXEC_STATUS.COMPLETED,
        SEGMENT_EXEC_STATUS.EXECUTED,
      ].includes(row.status);
      // Optional-provider skips (e.g. Apify honesty) must not erase a completed
      // primary discovery outcome for the segment.
      if (!alreadyTerminalOk && row.requestCount <= 1) {
        row.status = SEGMENT_EXEC_STATUS.SKIPPED;
        row.errorOrSkipReason =
          entry.skipReason || entry.errorCode || "skipped";
      } else if (!alreadyTerminalOk) {
        row.errorOrSkipReason =
          entry.skipReason || entry.errorCode || row.errorOrSkipReason;
      }
    }
    if (entry.status === PROVIDER_CALL_STATUS.BLOCKED) {
      row.status = entry.skipReason?.includes("budget")
        ? SEGMENT_EXEC_STATUS.BLOCKED_BUDGET
        : SEGMENT_EXEC_STATUS.BLOCKED_CONFIG;
      row.errorOrSkipReason = entry.skipReason || entry.errorCode;
    }
  }

  next.updatedAt = nowIso();
  return recomputeSummary(next);
}

export function recordApifyLifecycle(run, event = {}) {
  const next = structuredClone(run);
  next.apifyLifecycle.push({
    at: event.at || nowIso(),
    state: text(event.state),
    segmentId: event.segmentId
      ? normalizeCanonicalExaSegmentId(event.segmentId) || text(event.segmentId)
      : null,
    actorId: text(event.actorId) || null,
    jobId: text(event.jobId) || null,
    datasetId: text(event.datasetId) || null,
    itemCount: event.itemCount == null ? null : Number(event.itemCount),
    ingestedCount:
      event.ingestedCount == null ? null : Number(event.ingestedCount),
    candidatesProduced:
      event.candidatesProduced == null
        ? null
        : Number(event.candidatesProduced),
    qualifiedContributed:
      event.qualifiedContributed == null
        ? null
        : Number(event.qualifiedContributed),
    cacheHit: event.cacheHit === true,
    costUsd:
      event.costUsd == null || !Number.isFinite(Number(event.costUsd))
        ? null
        : Number(event.costUsd),
    reason: text(event.reason) || null,
  });
  next.updatedAt = nowIso();
  return next;
}

export function updateSegmentStatus(run, segmentId, patch = {}) {
  const next = structuredClone(run);
  const id = normalizeCanonicalExaSegmentId(segmentId) || text(segmentId);
  if (!next.segments[id]) return next;
  Object.assign(next.segments[id], patch);
  next.updatedAt = nowIso();
  return recomputeSummary(next);
}

export function updateStageStatus(run, stageId, patch = {}) {
  const next = structuredClone(run);
  const stage = next.stages.find((s) => s.stageId === stageId);
  if (!stage) return next;
  Object.assign(stage, patch);
  if (patch.status === STAGE_STATUS.RUNNING && !stage.startedAt) {
    stage.startedAt = nowIso();
  }
  if (
    (patch.status === STAGE_STATUS.COMPLETED ||
      patch.status === STAGE_STATUS.FAILED ||
      patch.status === STAGE_STATUS.SKIPPED ||
      patch.status === STAGE_STATUS.BLOCKED) &&
    !stage.completedAt
  ) {
    stage.completedAt = nowIso();
  }
  next.updatedAt = nowIso();
  return recomputeSummary(next);
}

export function recomputeSummary(run) {
  const next = structuredClone(run);
  const segs = Object.values(next.segments || {});
  const completed = segs.filter(
    (s) =>
      s.status === SEGMENT_EXEC_STATUS.COMPLETED ||
      s.status === SEGMENT_EXEC_STATUS.EXECUTED,
  ).length;
  const providers = new Set();
  let calls = 0;
  let spend = 0;
  for (const c of next.providerCalls || []) {
    calls += 1;
    if (c.providerId) providers.add(c.providerId);
    spend += Number(c.actualCostUsd ?? c.estimatedCostUsd ?? 0);
  }
  let candidates = 0;
  let qualified = 0;
  let visible = 0;
  for (const s of segs) {
    candidates += Number(s.candidatesFound || 0);
    qualified += Number(s.qualifiedOpportunities || 0);
    visible += Number(s.customerVisibleAdditions || 0);
  }
  const failedStages = (next.stages || []).filter((s) =>
    [STAGE_STATUS.FAILED, STAGE_STATUS.BLOCKED].includes(s.status),
  ).length;

  let durationMs = null;
  if (next.startedAt) {
    const end = next.completedAt ? Date.parse(next.completedAt) : Date.now();
    durationMs = Math.max(0, end - Date.parse(next.startedAt));
  }

  next.summary = {
    segmentsCompletedOf14: completed,
    providersInvoked: [...providers],
    providerCalls: calls,
    totalSpendUsd: Number(spend.toFixed(6)),
    durationMs,
    newCandidates: candidates,
    qualifiedOpportunities: qualified,
    customerVisibleAdditions: visible,
    failedOrBlockedStages: failedStages,
  };
  return next;
}

export function buildProviderMatrix(run) {
  const calls = run.providerCalls || [];
  return listProviderInventory().map((inv) => {
    const mine = calls.filter((c) => c.providerId === inv.providerId);
    const segments = [
      ...new Set(mine.map((c) => c.segmentId).filter(Boolean)),
    ];
    const completed = mine.filter(
      (c) =>
        c.status === PROVIDER_CALL_STATUS.COMPLETED ||
        c.status === PROVIDER_CALL_STATUS.CACHE_HIT,
    ).length;
    const failed = mine.filter(
      (c) => c.status === PROVIDER_CALL_STATUS.FAILED,
    ).length;
    const cacheHits = mine.filter((c) => c.cacheHit).length;
    const spend = mine.reduce(
      (a, c) => a + Number(c.actualCostUsd ?? c.estimatedCostUsd ?? 0),
      0,
    );
    const last = mine
      .map((c) => c.completedAt || c.startedAt)
      .filter(Boolean)
      .sort()
      .slice(-1)[0] || null;
    let configState = inv.lifecycle;
    let skipReason = null;
    if (mine.length === 0) {
      skipReason =
        inv.fullE2eOrchestratorUsage === "NOT_INVOKED"
          ? "not_invoked_by_full_e2e"
          : inv.fullE2eOrchestratorUsage ===
                "CONFIGURED_NOT_ORCHESTRATED_IN_FULL_E2E" ||
              inv.fullE2eOrchestratorUsage === "WIRED_DEFAULT_MOCK_FAIL_CLOSED" ||
              inv.lifecycle === "CONFIGURED_NOT_ORCHESTRATED"
            ? "configured_not_orchestrated_or_no_calls_in_this_run"
            : "no_calls_recorded";
    }
    return {
      providerId: inv.providerId,
      name: inv.name,
      configurationState: configState,
      enabled:
        inv.lifecycle === "ACTIVE" ||
        inv.lifecycle === "CONFIGURED_NOT_ORCHESTRATED",
      callsSubmitted: mine.length,
      callsCompleted: completed,
      callsFailed: failed,
      cacheHits,
      actualSpendUsd: Number(spend.toFixed(6)),
      segmentsServed: segments,
      lastExecution: last,
      failureOrSkipReason: skipReason,
      fullE2eOrchestratorUsage: inv.fullE2eOrchestratorUsage,
    };
  });
}

export function detectSilentSegmentSkips(run) {
  const findings = [];
  for (const sid of listFourteenWorkflowSegmentIds()) {
    const row = run.segments[sid];
    if (!row) continue;
    if (!row.planned) {
      findings.push({
        segmentId: sid,
        kind: "NEVER_SCHEDULED",
        reason: row.errorOrSkipReason || "not_planned",
      });
      continue;
    }
    if (row.status === SEGMENT_EXEC_STATUS.PLANNED) {
      findings.push({
        segmentId: sid,
        kind: "SCHEDULED_NEVER_STARTED",
        reason: "planned_but_no_start",
      });
      continue;
    }
    if (
      row.status === SEGMENT_EXEC_STATUS.STARTED &&
      row.requestCount === 0
    ) {
      findings.push({
        segmentId: sid,
        kind: "STARTED_WITHOUT_RESEARCH",
        reason: "started_without_provider_calls",
      });
      continue;
    }
    if (row.status === SEGMENT_EXEC_STATUS.FAILED) {
      findings.push({
        segmentId: sid,
        kind: "PROVIDER_FAILED",
        reason: row.errorOrSkipReason || "failed",
      });
      continue;
    }
    if (
      (row.status === SEGMENT_EXEC_STATUS.COMPLETED ||
        row.status === SEGMENT_EXEC_STATUS.EXECUTED) &&
      Number(row.qualifiedOpportunities || 0) === 0
    ) {
      findings.push({
        segmentId: sid,
        kind: "COMPLETED_ZERO_QUALIFIED",
        reason: "completed_with_zero_qualified",
      });
    }
    if (
      row.status === SEGMENT_EXEC_STATUS.BLOCKED_BUDGET ||
      row.status === SEGMENT_EXEC_STATUS.BLOCKED_CONFIG
    ) {
      findings.push({
        segmentId: sid,
        kind: "BLOCKED",
        reason: row.errorOrSkipReason || row.status,
      });
    }
    const onlyCache =
      row.requestCount > 0 &&
      row.providersUsed.length > 0 &&
      row.providersUsed.every(
        (p) =>
          p === GDI_PROVIDER_IDS.HISTORICAL_CACHE ||
          p === GDI_PROVIDER_IDS.FIXTURE_DETERMINISTIC,
      );
    if (onlyCache) {
      findings.push({
        segmentId: sid,
        kind: "CACHE_ONLY",
        reason: "routed_only_through_cached_or_fixture_data",
      });
    }
  }
  return findings;
}

export function certifyExecutionRun(run, opts = {}) {
  const skips = detectSilentSegmentSkips(run);
  const unintentional = skips.filter((s) =>
    [
      "SCHEDULED_NEVER_STARTED",
      "STARTED_WITHOUT_RESEARCH",
      "PROVIDER_FAILED",
    ].includes(s.kind),
  );
  const stagesFailed = (run.stages || []).filter((s) =>
    [STAGE_STATUS.FAILED, STAGE_STATUS.BLOCKED].includes(s.status),
  );
  const planned = Object.values(run.segments || {}).filter((s) => s.planned);
  const executed = planned.filter((s) =>
    [
      SEGMENT_EXEC_STATUS.EXECUTED,
      SEGMENT_EXEC_STATUS.COMPLETED,
      SEGMENT_EXEC_STATUS.FAILED,
      SEGMENT_EXEC_STATUS.SKIPPED,
      SEGMENT_EXEC_STATUS.BLOCKED_BUDGET,
      SEGMENT_EXEC_STATUS.BLOCKED_CONFIG,
    ].includes(s.status),
  );
  const providers = [...new Set((run.providerCalls || []).map((c) => c.providerId))];
  const budgetOk = opts.budgetsRespected !== false;
  const stagedPub = opts.stagedPublicationGenerated === true;
  const founderApprovalRequired = opts.founderApprovalRequired !== false;
  const visibilityGates = opts.customerVisibilityGatesPassed;
  const outputsIngested = opts.outputsIngested !== false;

  let verdict = RUN_VERDICT.FULL_E2E_EXECUTION_COMPLETE;
  if (stagesFailed.length || unintentional.some((u) => u.kind === "PROVIDER_FAILED")) {
    verdict = RUN_VERDICT.FULL_E2E_FAILED;
  } else if (unintentional.length || opts.blocked === true) {
    verdict =
      opts.blocked === true
        ? RUN_VERDICT.FULL_E2E_BLOCKED
        : RUN_VERDICT.FULL_E2E_PARTIAL;
  } else if (executed.length < planned.length) {
    verdict = RUN_VERDICT.FULL_E2E_PARTIAL;
  }

  // Unintentional skips of required research strategy → not COMPLETE
  if (
    verdict === RUN_VERDICT.FULL_E2E_EXECUTION_COMPLETE &&
    unintentional.length
  ) {
    verdict = RUN_VERDICT.FULL_E2E_PARTIAL;
  }

  return {
    schema: `${GDI_EXECUTION_OBSERVABILITY_V1}/certificate`,
    runId: run.runId,
    hotelId: run.hotelId,
    verdict,
    fourteenSegmentPlanEvaluated: planned.length === 14 || planned.length > 0,
    segmentsExecuted: executed.map((s) => s.segmentId),
    segmentsDeliberatelySkipped: skips
      .filter((s) => s.kind === "NEVER_SCHEDULED" || s.kind === "CACHE_ONLY")
      .map((s) => s.segmentId),
    segmentsFailed: skips
      .filter((s) => s.kind === "PROVIDER_FAILED")
      .map((s) => s.segmentId),
    providersExecuted: providers,
    requiredStagesCompleted: stagesFailed.length === 0,
    budgetsRespected: budgetOk,
    outputsIngested,
    customerVisibilityGatesPassed:
      visibilityGates == null ? null : visibilityGates === true,
    stagedPublicationGenerated: stagedPub,
    founderApprovalStillRequired: founderApprovalRequired,
    silentSkipFindings: skips,
    note:
      "Operational execution certificate — not a commercial quality or publish authorization.",
    certifiedAt: nowIso(),
  };
}

export function saveExecutionRun(run, root = ROOT) {
  const file = runLedgerPath(run.hotelId, run.runId, root);
  const next = { ...run, updatedAt: nowIso() };
  atomicWriteJson(file, next);
  // index
  const indexPath = path.join(hotelExecutionDir(run.hotelId, root), "index.json");
  const index = readJson(indexPath) || { schema: GDI_EXECUTION_OBSERVABILITY_V1, runs: [] };
  const summary = {
    runId: next.runId,
    label: next.label,
    startedAt: next.startedAt,
    completedAt: next.completedAt,
    runStatus: next.runStatus,
    verdict: next.verdict || next.certificate?.verdict || null,
    totalSpendUsd: next.summary?.totalSpendUsd ?? null,
    historical: next.historical === true,
  };
  index.runs = [
    summary,
    ...(index.runs || []).filter((r) => r.runId !== next.runId),
  ].sort((a, b) =>
    String(b.startedAt || b.runId).localeCompare(String(a.startedAt || a.runId)),
  );
  atomicWriteJson(indexPath, index);
  return next;
}

export function loadExecutionRun(hotelId, runId, root = ROOT) {
  return readJson(runLedgerPath(hotelId, runId, root));
}

export function listExecutionRuns(hotelId, root = ROOT) {
  const indexPath = path.join(hotelExecutionDir(hotelId, root), "index.json");
  const index = readJson(indexPath);
  if (index?.runs) return index.runs;
  const dir = hotelExecutionDir(hotelId, root);
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((n) => n.endsWith(".json") && n !== "index.json")
    .map((n) => {
      const j = readJson(path.join(dir, n));
      return j
        ? {
            runId: j.runId,
            label: j.label,
            startedAt: j.startedAt,
            completedAt: j.completedAt,
            runStatus: j.runStatus,
            verdict: j.verdict || j.certificate?.verdict || null,
            totalSpendUsd: j.summary?.totalSpendUsd ?? null,
            historical: j.historical === true,
          }
        : null;
    })
    .filter(Boolean)
    .sort((a, b) =>
      String(b.startedAt || b.runId).localeCompare(String(a.startedAt || a.runId)),
    );
}

export function buildMonitorDashboardPayload(run) {
  if (!run) return null;
  const certified = run.certificate || certifyExecutionRun(run);
  return {
    schema: GDI_EXECUTION_OBSERVABILITY_V1,
    run: {
      runId: run.runId,
      hotelId: run.hotelId,
      label: run.label,
      startedAt: run.startedAt,
      completedAt: run.completedAt,
      runStatus: run.runStatus,
      historical: run.historical === true,
      historicalConfidence: run.historicalConfidence,
    },
    summary: run.summary,
    segmentMatrix: listFourteenWorkflowSegmentIds().map((id) => {
      const row = run.segments[id];
      return {
        ...row,
        providerCalls: (run.providerCalls || []).filter(
          (c) => c.segmentId === id,
        ),
      };
    }),
    providerMatrix: buildProviderMatrix(run),
    stageTimeline: run.stages,
    silentSkips: detectSilentSegmentSkips(run),
    certificate: certified,
    apifyLifecycle: run.apifyLifecycle || [],
    errors: run.errors || [],
    lineage: run.lineage || [],
    providerInventory: listProviderInventory(),
  };
}

export {
  GDI_PROVIDER_INVENTORY,
  GDI_PROVIDER_IDS,
  E2E_STAGE,
  listProviderInventory,
};
