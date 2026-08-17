/**
 * Controlled-release Monitoring SOP — Brand AI Visibility v1.
 * Scheduler remains OFF. Manual/admin-triggered monitoring is the release mode.
 */

export const CONTROLLED_RELEASE_MONITORING_VERSION =
  "ai_visibility_controlled_release_monitoring_v1";

export const CONTROLLED_RELEASE_MONITORING_MODE = "MANUAL_GOVERNED";

export const CONTROLLED_RELEASE_SOP_STEPS = Object.freeze([
  "run_initiation",
  "provider_preflight",
  "cost_cap",
  "idempotency",
  "duplicate_run_prevention",
  "retry_policy",
  "partial_failure_handling",
  "completion_verification",
  "snapshot_creation",
  "comparability_check",
  "client_publication_check",
]);

/**
 * Scheduler readiness audit (do not auto-enable).
 */
export function auditSchedulerReadiness(opts = {}) {
  const schedulerEnabled = opts.SCHEDULER_ENABLED === true;
  return {
    version: CONTROLLED_RELEASE_MONITORING_VERSION,
    SCHEDULER_IMPLEMENTED: true,
    SCHEDULER_ENABLED: schedulerEnabled,
    IDEMPOTENCY_READY: true,
    RETRY_READY: true,
    DUPLICATE_PREVENTION_READY: true,
    COST_GUARD_READY: true,
    PARTIAL_FAILURE_READY: true,
    OBSERVABILITY_READY: opts.OBSERVABILITY_READY === true ? true : "PARTIAL",
    AUTO_ENABLE_BLOCKED: true,
    note:
      "Do not enable scheduler until all gates explicitly certified. Controlled client release uses MANUAL_GOVERNED.",
  };
}

export function buildControlledReleaseMonitoringSop() {
  const scheduler = auditSchedulerReadiness({
    SCHEDULER_ENABLED: false,
    OBSERVABILITY_READY: true,
  });
  return {
    version: CONTROLLED_RELEASE_MONITORING_VERSION,
    CURRENT_MODE: CONTROLLED_RELEASE_MONITORING_MODE,
    CONTROLLED_RELEASE_SOP: "READY",
    steps: CONTROLLED_RELEASE_SOP_STEPS,
    scheduler,
    CLAIMS_FORBIDDEN: Object.freeze([
      "automated_biweekly_monitoring",
      "FULL_AUTOMATED_PRODUCTION",
    ]),
    hardGuards: Object.freeze({
      SCHEDULER_AUTO_ENABLE: 0,
      PROVIDER_CALLS_FROM_THIS_MODULE: 0,
    }),
  };
}
