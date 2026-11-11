/**
 * Durable FullE2E fourteen-segment discovery research-state V1.
 *
 * Storage convention (existing Aimbridge path):
 *   data/group-demand-intelligence/hotels/{hotelId}/full-e2e-fourteen-segment-state/{segmentId}.json
 *
 * Atomic write pattern matches bethesda-maturation-checkpoint / published-inventory.
 * Does not publish. Does not write Airtable. Does not spend.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, randomBytes } from "node:crypto";
import {
  DISCOVERY_EXECUTION_STATUS,
  SEGMENT_DISCOVERY_EXECUTOR_V1,
} from "./segment-discovery-executor-v1.js";

export const FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_STATE_V1 =
  "gdi-full-e2e-fourteen-segment-discovery-state-v1";

/** Keep as string literal to avoid circular import with execution module. */
const EXECUTION_MODULE_VERSION =
  "gdi-full-e2e-fourteen-segment-discovery-execution-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const LIVE_COMPLETE_STATUSES = new Set([
  DISCOVERY_EXECUTION_STATUS.DISCOVERY_EXECUTED,
  DISCOVERY_EXECUTION_STATUS.DISCOVERY_COMPLETED,
]);

function now() {
  return new Date().toISOString();
}

function text(v) {
  return String(v || "").trim();
}

function sha12(s) {
  return createHash("sha1").update(String(s || "")).digest("hex").slice(0, 12);
}

export function newResearchRunId(prefix = "gdi_disc_run") {
  return `${prefix}_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
}

export function defaultFourteenSegmentStateDir(hotelId, root = ROOT) {
  return path.join(
    root,
    "data",
    "group-demand-intelligence",
    "hotels",
    hotelId,
    "full-e2e-fourteen-segment-state"
  );
}

export function discoveryStatePath(hotelId, segmentId, root = ROOT) {
  return path.join(
    defaultFourteenSegmentStateDir(hotelId, root),
    `${segmentId}.json`
  );
}

export function createEmptyDiscoveryState({
  hotelId,
  segmentId,
  researchRunId = null,
} = {}) {
  const at = now();
  return {
    version: FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_STATE_V1,
    hotelId,
    segmentId,
    applicable: true,
    status: "PENDING",
    coverageStatus: "NOT_RUN",
    completionDecision: "NOT_COMPLETE",
    completionRationale: null,
    executionStatus: DISCOVERY_EXECUTION_STATUS.EXECUTOR_READY,
    executorVersion: SEGMENT_DISCOVERY_EXECUTOR_V1,
    executionModuleVersion: EXECUTION_MODULE_VERSION,
    researchRunId: researchRunId || null,
    researchHorizon: null,
    querySpecs: [],
    providerInvocations: [],
    cacheReferences: [],
    rawCandidateReferences: [],
    candidateIds: [],
    extractedCandidateIds: [],
    deduplicatedCandidateIds: [],
    unresolvedCandidateIds: [],
    completedCandidateIds: [],
    evidenceProvenance: [],
    rejectionReasons: [],
    remainingResearchGaps: [],
    providerUsage: {
      attempts: 0,
      liveCalls: 0,
      cacheHits: 0,
      costUsd: 0,
      byProvider: {},
    },
    stopReason: null,
    lastSuccessfulCheckpoint: null,
    lastCheckpointAt: null,
    researchActions: [],
    publicationOccurred: false,
    airtableWrites: 0,
    createdAt: at,
    updatedAt: at,
  };
}

function atomicWriteJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  const body = `${JSON.stringify(data, null, 2)}\n`;
  fs.writeFileSync(tmp, body, "utf8");
  try {
    fs.renameSync(tmp, filePath);
  } catch (err) {
    if (err?.code === "EPERM" || err?.code === "EEXIST" || err?.code === "EACCES") {
      try {
        fs.writeFileSync(filePath, body, "utf8");
      } finally {
        try {
          fs.rmSync(tmp, { force: true });
        } catch {
          /* ignore */
        }
      }
    } else {
      try {
        fs.rmSync(tmp, { force: true });
      } catch {
        /* ignore */
      }
      throw err;
    }
  }
  return filePath;
}

export function loadDiscoveryState(hotelId, segmentId, opts = {}) {
  const file = discoveryStatePath(hotelId, segmentId, opts.root);
  if (!fs.existsSync(file)) return null;
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (err) {
    return {
      version: FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_STATE_V1,
      hotelId,
      segmentId,
      corrupt: true,
      error: String(err?.message || err),
      path: file,
    };
  }
}

export function saveDiscoveryStateAtomic(state, opts = {}) {
  if (!state?.hotelId || !state?.segmentId) {
    throw new Error("discovery_state_requires_hotelId_and_segmentId");
  }
  const next = {
    ...state,
    version: state.version || FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_STATE_V1,
    updatedAt: now(),
    publicationOccurred: false,
    airtableWrites: 0,
  };
  const file = discoveryStatePath(state.hotelId, state.segmentId, opts.root);
  atomicWriteJson(file, next);
  return { path: file, state: next };
}

/**
 * Whether live provider calls for this cell should be skipped on resume.
 * Historical/fixture replay may still run (zero cost) unless skipAll=true.
 */
export function shouldSkipLiveProviderInvocation(state, opts = {}) {
  if (!state || state.corrupt) {
    return { skip: false, reason: "NO_USABLE_STATE" };
  }
  if (opts.forceRerun === true) {
    return { skip: false, reason: "FORCE_RERUN" };
  }
  const liveCalls = Number(state.providerUsage?.liveCalls || 0);
  const cost = Number(state.providerUsage?.costUsd || 0);
  const status = String(state.executionStatus || "");
  const checkpoint = state.lastSuccessfulCheckpoint;
  const providerRunIds = [
    ...(checkpoint?.providerRunIds || []),
    ...((state.providerInvocations || [])
      .map((p) => p.providerRunId)
      .filter(Boolean)),
  ];
  // Any prior live call with recorded cost/run IDs must not auto-repeat.
  if (liveCalls > 0 && (cost > 0 || providerRunIds.length > 0)) {
    return {
      skip: true,
      reason: "LIVE_PROVIDER_ALREADY_COMPLETED",
      liveCalls,
      costUsd: cost,
      providerRunIds,
      checkpoint,
      priorStatus: status,
    };
  }
  if (
    LIVE_COMPLETE_STATUSES.has(status) ||
    checkpoint?.kind === "LIVE_PROVIDER_COMPLETE"
  ) {
    return {
      skip: true,
      reason: "LIVE_PROVIDER_STATUS_COMPLETE",
      liveCalls,
      costUsd: cost,
      checkpoint,
    };
  }
  if (
    checkpoint?.kind === "LIVE_PROVIDER_COMPLETE" &&
    Array.isArray(checkpoint.providerRunIds) &&
    checkpoint.providerRunIds.length
  ) {
    return {
      skip: true,
      reason: "CHECKPOINT_HAS_PROVIDER_RUN_IDS",
      providerRunIds: checkpoint.providerRunIds,
      costUsd: cost,
    };
  }
  return { skip: false, reason: "NEEDS_EXECUTION", liveCalls, costUsd: cost };
}

function uniq(arr) {
  return [...new Set((arr || []).filter(Boolean).map(String))];
}

/**
 * Merge an execution trace into durable state without double-counting cost
 * when the same researchRunId + provider run IDs are replayed.
 */
export function mergeDiscoveryTraceIntoState(existing, trace, opts = {}) {
  const hotelId = text(trace.hotelId || existing?.hotelId);
  const segmentId = text(trace.segmentId || existing?.segmentId);
  const base =
    existing && !existing.corrupt
      ? { ...existing }
      : createEmptyDiscoveryState({ hotelId, segmentId });

  const researchRunId =
    opts.researchRunId ||
    trace.researchRunId ||
    base.researchRunId ||
    newResearchRunId();

  const candidateIds = uniq([
    ...(base.candidateIds || []),
    ...(trace.candidates || []).map((c) => c.candidateId),
  ]);
  const extracted = uniq([
    ...(base.extractedCandidateIds || []),
    ...(trace.candidates || []).map((c) => c.candidateId),
  ]);
  const withEvidence = (trace.candidates || []).filter((c) => c.hasSourceEvidence);
  const evidenceProvenance = [
    ...(base.evidenceProvenance || []),
    ...withEvidence.map((c) => ({
      candidateId: c.candidateId,
      evidenceUrls: c.evidenceUrls || [],
      provenance: c.provenance || null,
      at: now(),
    })),
  ].slice(-200);

  const priorLive = Number(base.providerUsage?.liveCalls || 0);
  const priorCost = Number(base.providerUsage?.costUsd || 0);
  const priorAttempts = Number(base.providerUsage?.attempts || 0);
  const fresh = Number(trace.freshRetrievals || 0);
  const providerCalls = Number(trace.providerCallCount || 0);
  const costDelta = Number(trace.researchCostUsd || 0);

  // Idempotent cost: only add when this merge introduces new live calls
  // under a new checkpoint fingerprint.
  const invocationFingerprint = sha12(
    JSON.stringify({
      researchRunId,
      providerCalls,
      fresh,
      costDelta,
      stopReason: trace.stopReason,
      retrievalMode: trace.retrievalMode,
      executorId: trace.executorId,
    })
  );
  const alreadyCounted =
    (base.providerInvocations || []).some(
      (p) => p.fingerprint === invocationFingerprint
    ) ||
    (base.lastSuccessfulCheckpoint?.fingerprint === invocationFingerprint);

  const liveDelta = alreadyCounted ? 0 : providerCalls > 0 || fresh > 0 ? Math.max(providerCalls, fresh) : 0;
  const costAdd = alreadyCounted ? 0 : costDelta;
  const attemptAdd = alreadyCounted ? 0 : 1;

  const providerInvocations = [
    ...(base.providerInvocations || []),
    {
      at: now(),
      fingerprint: invocationFingerprint,
      researchRunId,
      providerCallCount: providerCalls,
      freshRetrievals: fresh,
      cacheHits: Number(trace.cacheHits || 0),
      costUsd: costDelta,
      retrievalMode: trace.retrievalMode || null,
      counted: !alreadyCounted,
      stopReason: trace.stopReason || null,
    },
  ].slice(-50);

  const rejectionReasons = [
    ...(base.rejectionReasons || []),
    ...(trace.rejections || []),
  ].slice(-100);

  const querySpecs = Array.isArray(trace.querySpecs)
    ? trace.querySpecs
    : base.querySpecs || [];

  const executionStatus =
    trace.executionStatus ||
    trace.classification ||
    base.executionStatus ||
    DISCOVERY_EXECUTION_STATUS.EXECUTION_PARTIAL;

  const liveCompleted =
    (providerCalls > 0 || fresh > 0) &&
    (executionStatus === DISCOVERY_EXECUTION_STATUS.DISCOVERY_EXECUTED ||
      executionStatus === DISCOVERY_EXECUTION_STATUS.DISCOVERY_COMPLETED);

  const checkpoint = {
    kind: liveCompleted
      ? "LIVE_PROVIDER_COMPLETE"
      : String(trace.retrievalMode || "").includes("HISTORICAL")
        ? "HISTORICAL_REPLAY_CHECKPOINT"
        : String(trace.retrievalMode || "").includes("FIXTURE")
          ? "FIXTURE_CHECKPOINT"
          : "EXECUTION_CHECKPOINT",
    at: now(),
    fingerprint: invocationFingerprint,
    researchRunId,
    executionStatus,
    providerRunIds: (trace.providerRunIds || []).filter(Boolean),
    extractedCount: extracted.length,
    costUsdCumulative: priorCost + costAdd,
  };

  const remainingResearchGaps = [];
  if (!(providerCalls > 0 || fresh > 0)) {
    remainingResearchGaps.push("live_provider_not_executed");
  }
  if (!withEvidence.length) {
    remainingResearchGaps.push("no_http_source_evidence_in_this_pass");
  }
  if (trace.discoveryCompleted !== true) {
    remainingResearchGaps.push("discovery_not_completed");
  }

  return {
    ...base,
    hotelId,
    segmentId,
    researchRunId,
    executorVersion: SEGMENT_DISCOVERY_EXECUTOR_V1,
    executionModuleVersion: EXECUTION_MODULE_VERSION,
    executionStatus,
    status:
      executionStatus === DISCOVERY_EXECUTION_STATUS.DISCOVERY_COMPLETED
        ? "COMPLETE"
        : executionStatus === DISCOVERY_EXECUTION_STATUS.DISCOVERY_EXECUTED
          ? "EXECUTED_PARTIAL_QUALIFICATION_PENDING"
          : executionStatus === DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED
            ? "BLOCKED"
            : "PARTIAL",
    coverageStatus:
      executionStatus === DISCOVERY_EXECUTION_STATUS.DISCOVERY_COMPLETED
        ? "COMPLETE"
        : "PARTIAL",
    completionDecision: "NOT_COMPLETE",
    completionRationale:
      trace.stopReason ||
      "Discovery state checkpointed; qualification remains a separate stage.",
    researchHorizon: trace.researchHorizon || base.researchHorizon || null,
    querySpecs,
    providerInvocations,
    cacheReferences: uniq([
      ...(base.cacheReferences || []),
      trace.historicalLedgerPath,
    ]),
    rawCandidateReferences: uniq([
      ...(base.rawCandidateReferences || []),
      ...(trace.candidates || []).map(
        (c) => c.provenance?.queryId || c.candidateId
      ),
    ]).slice(0, 500),
    candidateIds,
    extractedCandidateIds: extracted,
    deduplicatedCandidateIds: candidateIds,
    unresolvedCandidateIds: candidateIds.filter(
      (id) => !(base.completedCandidateIds || []).includes(id)
    ),
    evidenceProvenance,
    rejectionReasons,
    remainingResearchGaps: uniq([
      ...(base.remainingResearchGaps || []).filter(
        (g) => g !== "live_provider_not_executed" || liveDelta === 0
      ),
      ...remainingResearchGaps,
    ]),
    providerUsage: {
      attempts: priorAttempts + attemptAdd,
      liveCalls: priorLive + liveDelta,
      cacheHits:
        Number(base.providerUsage?.cacheHits || 0) +
        Number(trace.cacheHits || 0),
      costUsd: Number((priorCost + costAdd).toFixed(6)),
      byProvider: {
        ...(base.providerUsage?.byProvider || {}),
        [trace.retrievalMode || "unknown"]: {
          calls:
            Number(
              (base.providerUsage?.byProvider || {})[trace.retrievalMode || "unknown"]
                ?.calls || 0
            ) + (alreadyCounted ? 0 : providerCalls || (fresh ? fresh : 0)),
          costUsd: Number(
            (
              Number(
                (base.providerUsage?.byProvider || {})[
                  trace.retrievalMode || "unknown"
                ]?.costUsd || 0
              ) + costAdd
            ).toFixed(6)
          ),
        },
      },
    },
    stopReason: trace.stopReason || base.stopReason || null,
    lastSuccessfulCheckpoint: checkpoint,
    lastCheckpointAt: checkpoint.at,
    researchActions: [
      ...(base.researchActions || []),
      {
        at: now(),
        action: "discovery_execution_checkpoint",
        executionStatus,
        fingerprint: invocationFingerprint,
        countedNewCost: costAdd,
      },
    ].slice(-100),
    publicationOccurred: false,
    airtableWrites: 0,
  };
}

export function persistDiscoveryTrace(trace, opts = {}) {
  const existing = loadDiscoveryState(trace.hotelId, trace.segmentId, opts);
  const merged = mergeDiscoveryTraceIntoState(existing, trace, opts);
  return saveDiscoveryStateAtomic(merged, opts);
}

/**
 * Build resume plan for hotel×segment matrix.
 */
export function buildDiscoveryResumePlan({
  hotelIds = [],
  segmentIds = [],
  root = ROOT,
  forceRerun = false,
} = {}) {
  const cells = [];
  for (const hotelId of hotelIds) {
    for (const segmentId of segmentIds) {
      const state = loadDiscoveryState(hotelId, segmentId, { root });
      const skipLive = shouldSkipLiveProviderInvocation(state, { forceRerun });
      cells.push({
        hotelId,
        segmentId,
        hasState: Boolean(state) && !state?.corrupt,
        executionStatus: state?.executionStatus || null,
        liveCalls: Number(state?.providerUsage?.liveCalls || 0),
        costUsd: Number(state?.providerUsage?.costUsd || 0),
        candidateCount: (state?.candidateIds || []).length,
        skipLiveProvider: skipLive.skip,
        skipReason: skipLive.reason,
        lastCheckpoint: state?.lastSuccessfulCheckpoint || null,
      });
    }
  }
  return {
    version: FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_STATE_V1,
    at: now(),
    cellCount: cells.length,
    skipLiveCount: cells.filter((c) => c.skipLiveProvider).length,
    needsLiveCount: cells.filter((c) => !c.skipLiveProvider).length,
    cumulativeCostUsd: cells.reduce((n, c) => n + c.costUsd, 0),
    cells,
  };
}

export default {
  FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_STATE_V1,
  defaultFourteenSegmentStateDir,
  loadDiscoveryState,
  saveDiscoveryStateAtomic,
  persistDiscoveryTrace,
  mergeDiscoveryTraceIntoState,
  shouldSkipLiveProviderInvocation,
  buildDiscoveryResumePlan,
  newResearchRunId,
};
