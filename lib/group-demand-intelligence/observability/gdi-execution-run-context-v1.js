/**
 * Optional process-local Full E2E run context for observability hooks.
 * When unset, provider adapters do not write ledger rows (no side effects).
 */

import {
  loadExecutionRun,
  saveExecutionRun,
  recordProviderCall,
  recordApifyLifecycle,
  updateSegmentStatus,
  updateStageStatus,
  certifyExecutionRun,
  recomputeSummary,
} from "./gdi-execution-ledger-store-v1.js";

let active = null;

export function getActiveExecutionRunContext() {
  return active;
}

export function setActiveExecutionRunContext(ctx) {
  active = ctx
    ? {
        hotelId: ctx.hotelId,
        runId: ctx.runId,
        root: ctx.root || undefined,
      }
    : null;
  return active;
}

export function clearActiveExecutionRunContext() {
  active = null;
}

function mutateActive(mutator) {
  if (!active?.hotelId || !active?.runId) return null;
  const run = loadExecutionRun(active.hotelId, active.runId, active.root);
  if (!run) return null;
  const next = mutator(run);
  saveExecutionRun(next, active.root);
  return next;
}

export function observeProviderCall(call) {
  return mutateActive((run) => recordProviderCall(run, call));
}

export function observeApifyLifecycle(event) {
  return mutateActive((run) => recordApifyLifecycle(run, event));
}

export function observeSegmentStatus(segmentId, patch) {
  return mutateActive((run) => updateSegmentStatus(run, segmentId, patch));
}

export function observeStageStatus(stageId, patch) {
  return mutateActive((run) => updateStageStatus(run, stageId, patch));
}

export function observeFinalizeCertificate(opts = {}) {
  return mutateActive((run) => {
    const certificate = certifyExecutionRun(run, {
      budgetsRespected: opts.budgetsRespected,
      founderApprovalRequired: opts.founderApprovalRequired,
      stagedPublicationGenerated: opts.stagedPublicationGenerated,
      customerVisibilityGatesPassed: opts.customerVisibilityGatesPassed,
      outputsIngested: opts.outputsIngested,
      blocked: opts.blocked,
      discoveryPhaseOnly: opts.discoveryPhaseOnly,
      requireDeepResearch: opts.requireDeepResearch,
    });
    return recomputeSummary({
      ...run,
      certificate,
      verdict: certificate.verdict,
      completedAt: run.completedAt || new Date().toISOString(),
      runStatus:
        certificate.verdict === "FULL_E2E_EXECUTION_COMPLETE"
          ? "COMPLETED"
          : certificate.verdict === "FULL_E2E_FAILED"
            ? "FAILED"
            : certificate.verdict === "FULL_E2E_BLOCKED"
              ? "BLOCKED"
              : "PARTIAL",
    });
  });
}
