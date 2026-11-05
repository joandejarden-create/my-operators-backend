/**
 * GDI 2.0 Phase 2C — adaptive execution controller DESIGN (not enabled).
 *
 * Documents how ORS + strategy ladders + buyer-recovery planner can eventually
 * execute approved next actions under explicit budgets/auth.
 * Do NOT wire as live controller without separate Phase 2D+ authorization.
 */

export const ADAPTIVE_EXECUTION_CONTROLLER_DESIGN_V1 =
  "gdi-adaptive-execution-controller-design-v1";

export const ADAPTIVE_EXECUTION_ENABLED_DEFAULT = false;

/**
 * Frozen design contract for a future bounded controller.
 */
export const ADAPTIVE_EXECUTION_DESIGN = Object.freeze({
  version: ADAPTIVE_EXECUTION_CONTROLLER_DESIGN_V1,
  enabledDefault: ADAPTIVE_EXECUTION_ENABLED_DEFAULT,
  reuse: [
    "opportunity-research-supervisor-v1.js (matureOpportunity / stopWith)",
    "scaling-intelligent-investigation-loop-v1.js",
    "scaling-buyer-research-strategies-v1.js",
    "buyer-recovery-planner-v1.js",
    "research-continuation-disposition-v1.js",
    "deep-research-gdi-live-certification-v1.js ledgers",
    "gdi-provider-billing-guard-v1.js",
  ],
  components: {
    researchObjective: "Explicit gap list from shared eligibility + recovery planner",
    nextActionSelection: "planBuyerRecovery / selectNextBuyerResearchStrategy / ORS heuristic planner",
    strategyHistory: "buyerResearchAttempts + ORS researchAttempts + investigatedUrls",
    deduplication: "strategy id + query fingerprint; reject duplicate_strategy",
    perInvestigationBudget: "ledger ceiling slice; fail-closed when exhausted",
    providerAuthorization: "authorizePaidResearch + billing guard; Surfe/PDL hard-deny unless re-verified",
    modelToolRouting: "public fetch first; optional Exa/Parallel only when authorized",
    durableCheckpoints: "maturation checkpoints / campaign state / investigationLoopDisposition",
    costAndTimeLimits: "iteration ceiling + USD ceiling + wall-clock optional",
    stopJustification: "STOP_REASONS + gdi2ContinuationDispositionV1",
    scheduledRefresh: "weekly-refresh-orchestrator plan-only until paid auth",
    humanEscalation: "REQUEST_HUMAN_REVIEW recommendation + founder approval gates",
  },
  nonGoals: [
    "New agent framework / LangGraph runtime",
    "Unattended paid loops",
    "Auto-publication",
    "Second named buyer as eligibility gate",
  ],
  enablementRequirements: [
    "Founder explicit authorize flag",
    "Per-hotel budget ledger",
    "Shadow disagreement audit below agreed threshold",
    "Offline + authorized live preflight PASS",
  ],
});

export function assertAdaptiveExecutionDisabled(env = process.env) {
  const raw = String(env.GDI_ADAPTIVE_EXECUTION_CONTROLLER_V1 || "0").toLowerCase();
  const enabled = raw === "1" || raw === "true" || raw === "on";
  return {
    enabled: false,
    envRequestedEnabled: enabled,
    blocked: enabled === true,
    note: enabled
      ? "Adaptive execution requested but Phase 2C design forbids enablement — refuse."
      : "Adaptive execution remains disabled (design-only).",
  };
}
