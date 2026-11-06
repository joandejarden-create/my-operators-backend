/**
 * Durable stage result contract shared by GDI and ADP Full E2E.
 * A stage must not mark COMPLETE solely because a function returned successfully —
 * expected vs produced counts must reconcile.
 */

import { createHash } from "node:crypto";
import { EXECUTION_STATE } from "./execution-states.js";

export const STAGE_RESULT_SCHEMA_VERSION = "dealality-stage-result-v1";

/**
 * @typedef {object} StageResult
 */

export function hashArtifactRef(value) {
  const body =
    typeof value === "string" ? value : JSON.stringify(value ?? null);
  return createHash("sha256").update(body).digest("hex").slice(0, 16);
}

/**
 * Build a durable stage result. Incomplete count reconciliation prevents COMPLETE.
 */
export function buildStageResult({
  workflowVersion,
  hotelId,
  product,
  runId,
  stageId,
  state = EXECUTION_STATE.PENDING,
  inputArtifactIds = [],
  outputArtifactIds = [],
  inputHashes = [],
  outputHashes = [],
  expectedInputCount = null,
  actualInputCount = null,
  expectedOutputCount = null,
  actualOutputCount = null,
  evidenceRefs = [],
  providerJobIds = [],
  spendUsd = 0,
  reservedUsd = 0,
  startedAt = null,
  completedAt = null,
  qualityGate = null,
  blockers = [],
  nextAction = null,
  notes = null,
  extras = {},
} = {}) {
  const countMismatch = detectCountMismatch({
    expectedInputCount,
    actualInputCount,
    expectedOutputCount,
    actualOutputCount,
  });

  let resolvedState = state;
  const qualityFailed = qualityGate && qualityGate.passed === false;
  if (
    resolvedState === EXECUTION_STATE.COMPLETE &&
    (countMismatch ||
      (Array.isArray(blockers) && blockers.length > 0) ||
      qualityFailed)
  ) {
    // Fail-closed: COMPLETE is forbidden when qualityGate fails, counts mismatch,
    // or blockers remain — prevents false greens from skipped/stub stages.
    resolvedState = qualityFailed
      ? EXECUTION_STATE.BLOCKED_EVIDENCE
      : EXECUTION_STATE.BLOCKED_EVIDENCE;
  }

  return Object.freeze({
    schemaVersion: STAGE_RESULT_SCHEMA_VERSION,
    workflowVersion: workflowVersion || null,
    hotelId: hotelId || null,
    product: product || null,
    runId: runId || null,
    stageId: stageId || null,
    state: resolvedState,
    inputArtifactIds: Object.freeze([...(inputArtifactIds || [])]),
    outputArtifactIds: Object.freeze([...(outputArtifactIds || [])]),
    inputHashes: Object.freeze([...(inputHashes || [])]),
    outputHashes: Object.freeze([...(outputHashes || [])]),
    expectedInputCount,
    actualInputCount,
    expectedOutputCount,
    actualOutputCount,
    countMismatch,
    evidenceRefs: Object.freeze([...(evidenceRefs || [])]),
    providerJobIds: Object.freeze([...(providerJobIds || [])]),
    spendUsd: Number(spendUsd) || 0,
    reservedUsd: Number(reservedUsd) || 0,
    startedAt: startedAt || null,
    completedAt: completedAt || null,
    qualityGate: qualityGate
      ? Object.freeze({ ...qualityGate })
      : { passed: null, reasons: [] },
    blockers: Object.freeze([...(blockers || [])]),
    nextAction: nextAction || null,
    notes: notes || null,
    extras: Object.freeze({ ...(extras || {}) }),
  });
}

export function detectCountMismatch({
  expectedInputCount,
  actualInputCount,
  expectedOutputCount,
  actualOutputCount,
}) {
  const issues = [];
  if (
    expectedInputCount != null &&
    actualInputCount != null &&
    Number(expectedInputCount) !== Number(actualInputCount)
  ) {
    issues.push({
      axis: "input",
      expected: Number(expectedInputCount),
      actual: Number(actualInputCount),
    });
  }
  if (
    expectedOutputCount != null &&
    actualOutputCount != null &&
    Number(expectedOutputCount) !== Number(actualOutputCount)
  ) {
    issues.push({
      axis: "output",
      expected: Number(expectedOutputCount),
      actual: Number(actualOutputCount),
    });
  }
  // Nonempty source → zero downstream is always a mismatch when expected > 0.
  if (
    expectedInputCount != null &&
    Number(expectedInputCount) > 0 &&
    actualOutputCount != null &&
    Number(actualOutputCount) === 0 &&
    expectedOutputCount != null &&
    Number(expectedOutputCount) > 0
  ) {
    issues.push({
      axis: "handoff_collapse",
      expected: Number(expectedOutputCount),
      actual: 0,
      note: "Nonempty inputs produced zero expected outputs",
    });
  }
  return issues.length ? Object.freeze(issues) : null;
}

/**
 * Guard against historical `{ rows: [...] }` handoff collapse and ready.ok mismatch.
 */
export function assertHandoffIntegrity({
  sourceRows = null,
  downstreamCandidates = null,
  readyOkCount = null,
  hotelFacingReadyCount = null,
} = {}) {
  const blockers = [];
  const sourceCount = Array.isArray(sourceRows)
    ? sourceRows.length
    : sourceRows && Array.isArray(sourceRows.rows)
      ? sourceRows.rows.length
      : null;
  const downCount = Array.isArray(downstreamCandidates)
    ? downstreamCandidates.length
    : null;

  if (sourceCount != null && sourceCount > 0 && downCount === 0) {
    blockers.push({
      code: "HANDOFF_COLLAPSE_ROWS_TO_ZERO",
      message:
        "Nonempty source corpus collapsed to zero downstream candidates — refuse COMPLETE",
      sourceCount,
      downstreamCount: 0,
    });
  }

  if (
    readyOkCount != null &&
    hotelFacingReadyCount != null &&
    Number(hotelFacingReadyCount) > Number(readyOkCount)
  ) {
    blockers.push({
      code: "READY_OK_MISMATCH",
      message:
        "Hotel-facing Ready count exceeds technical ready.ok — refuse publication COMPLETE",
      readyOkCount: Number(readyOkCount),
      hotelFacingReadyCount: Number(hotelFacingReadyCount),
    });
  }

  return {
    ok: blockers.length === 0,
    blockers: Object.freeze(blockers),
    sourceCount,
    downstreamCount: downCount,
  };
}
