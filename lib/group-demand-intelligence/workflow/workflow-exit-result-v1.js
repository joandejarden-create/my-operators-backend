/**
 * Structured exit results for GDI research-to-page / resume runners.
 *
 * A process exit code 0 must not be treated as workflow advancement unless
 * exitResult.kind indicates meaningful progress.
 *
 * Classification: REUSABLE_PRODUCT_LOGIC
 */

export const WORKFLOW_EXIT_KIND = Object.freeze({
  STAGE_COMPLETED: "STAGE_COMPLETED",
  WAITING_PROVIDER: "WAITING_PROVIDER",
  RESUMABLE_WORK_REMAINS: "RESUMABLE_WORK_REMAINS",
  BLOCKED_WITH_CAUSE: "BLOCKED_WITH_CAUSE",
  BLOCKED_TECHNICAL: "BLOCKED_TECHNICAL",
  RESEARCH_COMPLETE_DELIVERY_BLOCKED: "RESEARCH_COMPLETE_DELIVERY_BLOCKED",
  FULL_E2E_COMPLETE: "FULL_E2E_COMPLETE",
  NO_PROGRESS: "NO_PROGRESS",
});

/**
 * Map exit kind → process exit code.
 * NO_PROGRESS / BLOCKED_TECHNICAL → 2 (not success).
 * WAITING / RESUMABLE / DELIVERY_BLOCKED → 0 with structured payload (monitor continues).
 * FULL_E2E_COMPLETE / STAGE_COMPLETED → 0.
 */
export function exitCodeForKind(kind) {
  switch (kind) {
    case WORKFLOW_EXIT_KIND.NO_PROGRESS:
    case WORKFLOW_EXIT_KIND.BLOCKED_TECHNICAL:
    case WORKFLOW_EXIT_KIND.BLOCKED_WITH_CAUSE:
      return 2;
    default:
      return 0;
  }
}

export function buildWorkflowExitResult({
  kind,
  hotelId = null,
  runId = null,
  stagesAttempted = [],
  stagesCompleted = [],
  pendingWork = null,
  blocker = null,
  meaningfulProgress = false,
  workItemChanged = null,
  notes = null,
  extras = {},
} = {}) {
  const k = String(kind || WORKFLOW_EXIT_KIND.NO_PROGRESS);
  return {
    version: "gdi-workflow-exit-result-v1",
    at: new Date().toISOString(),
    kind: k,
    ok: exitCodeForKind(k) === 0,
    meaningfulProgress: meaningfulProgress === true,
    hotelId,
    runId,
    stagesAttempted,
    stagesCompleted,
    pendingWork,
    blocker,
    workItemChanged,
    notes,
    ...extras,
  };
}

/**
 * Derive exit kind after a resume/coordinator pass from ledger + stage lists.
 */
export function deriveWorkflowExitKind({
  stagesToRun = [],
  stagesCompletedThisPass = [],
  ledger = {},
  paidDispositionRows = 0,
  maturedCount = null,
  hotelFacingCount = null,
  readyCount = null,
  waitingProvider = false,
  authorizePublish = false,
  overallStatus = null,
} = {}) {
  if (waitingProvider) return WORKFLOW_EXIT_KIND.WAITING_PROVIDER;

  const overall = String(overallStatus || ledger.computedOverallStatus || "");
  if (overall === "COMPLETE") return WORKFLOW_EXIT_KIND.FULL_E2E_COMPLETE;

  if (
    stagesToRun.length === 0 &&
    paidDispositionRows > 0 &&
    (maturedCount === 0 || maturedCount == null)
  ) {
    return WORKFLOW_EXIT_KIND.NO_PROGRESS;
  }

  if (stagesToRun.length === 0 && stagesCompletedThisPass.length === 0) {
    // Resume found nothing to do — may be delivery blocked or genuinely idle
    if (
      Number(hotelFacingCount) > 0 &&
      !authorizePublish &&
      Number(readyCount) >= 0
    ) {
      return WORKFLOW_EXIT_KIND.RESEARCH_COMPLETE_DELIVERY_BLOCKED;
    }
    return WORKFLOW_EXIT_KIND.NO_PROGRESS;
  }

  if (stagesCompletedThisPass.length === 0 && stagesToRun.length > 0) {
    return WORKFLOW_EXIT_KIND.NO_PROGRESS;
  }

  const failures = ledger.failures || [];
  if (failures.length) return WORKFLOW_EXIT_KIND.BLOCKED_WITH_CAUSE;

  if (
    Number(maturedCount) > 0 &&
    Number(hotelFacingCount) > 0 &&
    !authorizePublish
  ) {
    // Research advanced; publication separately gated
    if (stagesToRun.every((s) => String(s).includes("delivery") || String(s).includes("evaluate") || String(s).includes("reconcile") || String(s).includes("lessons"))) {
      return WORKFLOW_EXIT_KIND.RESEARCH_COMPLETE_DELIVERY_BLOCKED;
    }
  }

  if (stagesCompletedThisPass.length) {
    return WORKFLOW_EXIT_KIND.STAGE_COMPLETED;
  }

  return WORKFLOW_EXIT_KIND.RESUMABLE_WORK_REMAINS;
}

export default {
  WORKFLOW_EXIT_KIND,
  exitCodeForKind,
  buildWorkflowExitResult,
  deriveWorkflowExitKind,
};
