/**
 * GDI V37 — canonical Exa → GDI pipeline discard/filter map.
 * Documentation-as-code for founder auditability.
 */

export const PIPELINE_STAGES = Object.freeze([
  {
    stage: "exa_query",
    label: "Exa query",
    canDiscard: false,
    notes: "PowerShell workflow → shared launcher → Exa Agent run (authorizeExaAgentLive).",
  },
  {
    stage: "exa_response",
    label: "Exa response",
    canDiscard: true,
    outcomes: ["PROVIDER_TIMEOUT", "empty structured payload"],
    notes:
      "Adapter returns structured/text/grounding in memory. Four-hotel persist drops text/grounding.",
  },
  {
    stage: "candidate_extraction",
    label: "Candidate extraction",
    canDiscard: true,
    outcomes: ["IDENTITY_MISSING_ORGANIZATION", "PARSING_FAILURE"],
    notes:
      "extractCandidates filters rows without organizationName (silent loss without ledger).",
  },
  {
    stage: "candidate_normalization",
    label: "Candidate normalization",
    canDiscard: false,
    notes: "Org/programme keys; v4 normalizeDiscoveryHit keeps raw — four-hotel path does not.",
  },
  {
    stage: "deduplication",
    label: "Deduplication",
    canDiscard: true,
    outcomes: [
      "DUPLICATE_SAME_ORG_CROSS_PASS",
      "DUPLICATE_SAME_EVENT_FAMILY",
      "DUPLICATE_EXISTING_OPPORTUNITY",
    ],
    notes: "In-memory candidateKey / seen set across discovery passes; acceptance cohort dedupe.",
  },
  {
    stage: "commercial_screening",
    label: "Commercial screening",
    canDiscard: true,
    outcomes: ["SCREENED_OUT_COMMERCIAL", "DEMAND_MECHANISM_UNSUPPORTED"],
    modules: [
      "exa-agent-evidence-validation-v1",
      "exa-agent-discovery-eligibility-v1",
      "exa-agent-hospitality-demand-gates-v2",
    ],
  },
  {
    stage: "deep_research_allocation",
    label: "Deep research allocation",
    canDiscard: false,
    deprioritize: true,
    outcomes: ["RESEARCH_ALLOCATION_CAP", "RESEARCH_BUDGET_LIMIT", "DEEP_RESEARCH_NOT_STARTED"],
    notes:
      "allocateAdaptiveDeepResearchSlots — deferred candidates are NOT commercial rejects.",
  },
  {
    stage: "deep_research_execution",
    label: "Deep research execution",
    canDiscard: true,
    outcomes: ["PROVIDER_TIMEOUT", "RESEARCH_INCOMPLETE"],
  },
  {
    stage: "qualification",
    label: "Qualification / acceptance",
    canDiscard: true,
    outcomes: [
      "HARD_BLOCK_ACCEPTANCE",
      "RESEARCH_CONTINUATION_GAPS",
      "WATCH_INSUFFICIENT_EVIDENCE",
      "REJECTED_COMMERCIAL",
      "SALES_READY (internal)",
    ],
    modules: [
      "fourteen-workflow-commercial-acceptance-gates-v1",
      "commercial-customer-readiness-gate-v1",
    ],
  },
  {
    stage: "staging",
    label: "Staging",
    canDiscard: false,
    outcomes: ["STAGED_INTERNAL_ONLY"],
    notes: "03-STAGED_OPPORTUNITIES.json — local shadow / campaign staging.",
  },
  {
    stage: "customer_facing_acceptance",
    label: "Customer-facing acceptance",
    canDiscard: false,
    outcomes: ["AWAITING_FOUNDER_PROMOTION", "CUSTOMER_FACING_APPROVED"],
    notes: "Founder selective promotion required. Auto-promote disabled.",
  },
  {
    stage: "publication",
    label: "Publication",
    canDiscard: false,
    notes: "Airtable / production deploy remain founder-gated and out of scope for V37.",
  },
]);

export const FOURTEEN_WORKFLOWS = Object.freeze([
  { n: 1, id: "association_events", powershell: "Invoke-GdiAssociationEvents.ps1" },
  { n: 2, id: "scaling_companies", powershell: "Invoke-GdiScalingCompanies.ps1" },
  { n: 3, id: "corporate_meetings_incentives", powershell: "Invoke-GdiMeetingsIncentives.ps1" },
  { n: 4, id: "government_public_sector", powershell: "Invoke-GdiGovernmentPublicSector.ps1" },
  { n: 5, id: "sports_entertainment", powershell: "Invoke-GdiSportsEntertainment.ps1" },
  { n: 6, id: "education_scientific", powershell: "Invoke-GdiEducationScientific.ps1" },
  { n: 7, id: "projects_infrastructure", powershell: "Invoke-GdiProjectsInfrastructure.ps1" },
  { n: 8, id: "social_special_events", powershell: "Invoke-GdiSocialSpecialEvents.ps1" },
  { n: 9, id: "local_catering_day_meetings", powershell: "Invoke-GdiLocalCateringDayMeetings.ps1" },
  {
    n: 10,
    id: "corporate_celebrations_private_events",
    powershell: "Invoke-GdiCorporateCelebrations.ps1",
  },
  {
    n: 11,
    id: "travel_trade_group_intermediaries",
    powershell: "Invoke-GdiTravelTradeIntermediaries.ps1",
  },
  { n: 12, id: "relocation_extended_stay", powershell: "Invoke-GdiRelocationExtendedStay.ps1" },
  { n: 13, id: "preferred_lodging", powershell: "Invoke-GdiPreferredLodgingPartnerships.ps1" },
  {
    n: 14,
    id: "professional_event_planners",
    powershell: "Invoke-GdiProfessionalEventPlanners.ps1",
  },
]);

export const PROVENANCE_LOSS_POINTS = Object.freeze([
  {
    id: "four_hotel_discovery_write",
    severity: "high",
    description:
      "Discovery pass JSON stores entry + runId + candidates only — drops Agent text, grounding, usage, HTTP bodies.",
  },
  {
    id: "extract_candidates_nameless",
    severity: "high",
    description:
      "Rows without organizationName are filtered silently with no rejected-candidate ledger row (pre-V37).",
  },
  {
    id: "screening_rejects_in_memory",
    severity: "medium",
    description:
      "Eligibility/evidence validation rejects often leave only in-memory issue arrays.",
  },
  {
    id: "adaptive_cap_deferrals",
    severity: "medium",
    description:
      "workflow_cap / global_cap deferrals historically looked like disappearance without DEEP_RESEARCH_NOT_STARTED status.",
  },
  {
    id: "research_audit_ui_cost_only",
    severity: "medium",
    description:
      "Research Audit previously showed only run cost/status — no Exa candidate funnel.",
  },
]);
