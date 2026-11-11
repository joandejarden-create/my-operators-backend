/**
 * Weekly autonomous operation design — integrate enhanced Research Supervisor
 * with existing weekly GDI orchestration. Does NOT switch the production weekly job.
 */

export const WEEKLY_SUPERVISOR_INTEGRATION_VERSION = "gdi-weekly-supervisor-integration-v2";

/**
 * Proposed weekly cycle contract (design + offline validation only).
 */
export function buildWeeklySupervisorIntegrationDesignV2() {
  return {
    version: WEEKLY_SUPERVISOR_INTEGRATION_VERSION,
    status: "DESIGNED_NOT_SWITCHED",
    productionWeeklyJobSwitched: false,
    soleOrchestrator: "opportunity-research-supervisor-v1 + demand-segment-research-supervisor-v1",
    existingModulesReused: [
      "lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js",
      "lib/group-demand-intelligence/workflow/weekly-refresh-contract-v1.js",
      "lib/group-demand-intelligence/workflow/gdi-global-weekly-feedback-v2.js",
      "lib/group-demand-intelligence/research/demand-segment-research-supervisor-v1.js",
      "lib/group-demand-intelligence/research/hotel-research-orchestrator-v1.js",
      "lib/group-demand-intelligence/research/autonomous-lead-following-v2.js",
      "lib/group-demand-intelligence/research/segment-discovery-coverage-v2.js",
      "lib/group-demand-intelligence/research/hotel-specific-qualification-v2.js",
    ],
    weeklyCycleSteps: [
      {
        id: "discover_new_candidates",
        description: "Discover new candidate organizations and demand signals for applicable segments",
        paidDefault: false,
      },
      {
        id: "update_existing",
        description: "Update existing organizations and meeting cycles from change detection",
        paidDefault: false,
      },
      {
        id: "refresh_sourcing_host_status",
        description: "Refresh sourcing and hotel-host status without inventing open demand",
        paidDefault: false,
      },
      {
        id: "follow_valuable_leads",
        description: "Follow new commercially valuable related-entity leads via autonomous-lead-following-v2",
        paidDefault: false,
      },
      {
        id: "skip_unchanged",
        description: "Avoid rereading unchanged content (evidence acquisition skip-unchanged)",
        paidDefault: false,
      },
      {
        id: "monitor_windows",
        description: "Monitor approaching sourcing or review windows",
        paidDefault: false,
      },
      {
        id: "preserve_completed",
        description: "Preserve completed investigations and research memory",
        paidDefault: false,
      },
      {
        id: "resume_incomplete",
        description: "Resume incomplete investigations from checkpoints",
        paidDefault: false,
      },
      {
        id: "incorporate_sales_feedback",
        description: "Incorporate hotel sales-team feedback into research prioritization",
        paidDefault: false,
      },
    ],
    multilingualAndLocalMarkets: {
      supportedInDesign: true,
      note: "Reuse existing public-retrieval + planner language-agnostic prompts; local-market variations via hotel segment plans — no separate agent framework.",
    },
    integrationHooks: {
      beforePaidDiscovery: "require explicit weekly paid authorization distinct from pilot/benchmark ledgers",
      coverageReporting: "emit buildSegmentDiscoveryCoverageReport per hotel×segment each weekly run",
      publication: "auto-publish remains OFF; publication eligibility unchanged unless separately authorized",
      deepResearchFeatureFlag: "GDI_WEEKLY_DEEP_RESEARCH_ENABLED",
      deepResearchDefaultEnabled: false,
      deepResearchInsertionPoint:
        "runWeeklyRefreshCycle after buildWeeklyWorkQueues — evaluateDeepResearchEligibility for CONTACT_ENRICHMENT / named_buyer_enrichment_pending / high-priority feedback; call runDeepResearchOnExistingGdiOpportunity only when flag=1 AND separate paid auth present",
      deepResearchEntrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
      deepResearchDuplicatePrevention: "persist deepResearchJobId + investigatedUrls + refresh schedule on opportunity; skip if prior job incomplete checkpoint exists unless resume authorized",
    },
    existingModulesReusedExtended: [
      "lib/group-demand-intelligence/research/deep-research-eligibility-v1.js",
      "lib/group-demand-intelligence/research/deep-research-gdi-integration-v1.js",
      "lib/group-demand-intelligence/research/autonomous-deep-research-recovery-v1.js",
    ],
    doNotSwitchProductionWeeklyJobInThisPhase: true,
  };
}

/**
 * Offline validation that design references existing modules and does not enable paid weekly spend.
 */
export function validateWeeklySupervisorIntegrationOfflineV2() {
  const design = buildWeeklySupervisorIntegrationDesignV2();
  const checks = [
    { name: "not_switched", ok: design.productionWeeklyJobSwitched === false },
    { name: "status_designed", ok: design.status === "DESIGNED_NOT_SWITCHED" },
    { name: "has_nine_cycle_steps", ok: design.weeklyCycleSteps.length === 9 },
    { name: "paid_default_false_all_steps", ok: design.weeklyCycleSteps.every((s) => s.paidDefault === false) },
    { name: "reuses_weekly_orchestrator", ok: design.existingModulesReused.some((p) => /weekly-refresh-orchestrator/.test(p)) },
    { name: "reuses_lead_following_v2", ok: design.existingModulesReused.some((p) => /autonomous-lead-following-v2/.test(p)) },
    { name: "auto_publish_off", ok: /auto-publish remains OFF/i.test(design.integrationHooks.publication) },
    { name: "deep_research_flag_default_off", ok: design.integrationHooks.deepResearchDefaultEnabled === false },
    { name: "deep_research_flag_named", ok: design.integrationHooks.deepResearchFeatureFlag === "GDI_WEEKLY_DEEP_RESEARCH_ENABLED" },
  ];
  return {
    version: WEEKLY_SUPERVISOR_INTEGRATION_VERSION,
    offline: true,
    liveCalls: 0,
    spent: false,
    design,
    checks,
    passed: checks.every((c) => c.ok),
  };
}
