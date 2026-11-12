/**
 * Opportunity Research Supervisor v1 — shared constants.
 *
 * Aligns with Full E2E / RESEARCH_SEGMENT / maturity funnel.
 * Does not invent Airtable fields. Does not publish.
 */

import { RESEARCH_SEGMENT } from "../workflow/research-segment-ids-v1.js";

export const SUPERVISOR_VERSION = "gdi-opportunity-research-supervisor-v1.1";

/** Terminal research outcomes for one opportunity (process layer, not Ready gate). */
export const RESEARCH_TERMINAL = Object.freeze({
  PUBLISHABLE_PROSPECT: "PUBLISHABLE_PROSPECT",
  EXPLORATORY_PROSPECT: "EXPLORATORY_PROSPECT",
  STOPPED_WITH_LEGITIMATE_REASON: "STOPPED_WITH_LEGITIMATE_REASON",
  IN_PROGRESS: "IN_PROGRESS",
});

/**
 * Prospect maturity labels for disclosure — NOT GDI_MATURITY_STATE.
 * Map downstream: CONFIRMED_DEMAND ≈ strong future+fit evidence;
 * QUALIFIED_PROSPECT ≈ commercially relevant + actionable route;
 * EXPLORATORY_PROSPECT ≈ credible but material uncertainty disclosed.
 * Never implies customer Ready / ACTIONABLE.
 */
export const PROSPECT_MATURITY = Object.freeze({
  CONFIRMED_DEMAND: "CONFIRMED_DEMAND",
  QUALIFIED_PROSPECT: "QUALIFIED_PROSPECT",
  EXPLORATORY_PROSPECT: "EXPLORATORY_PROSPECT",
});

/**
 * Adaptive investigation outcomes. Distinct from STOP_REASONS.
 * A failed call, an empty search, or a spent budget is never RESEARCH_STRATEGIES_EXHAUSTED.
 */
export const INVESTIGATION_DISPOSITION = Object.freeze({
  VERIFIED_RESOLVED: "VERIFIED_RESOLVED",
  EVIDENCE_BACKED_NEGATIVE: "EVIDENCE_BACKED_NEGATIVE",
  RESEARCH_STRATEGIES_EXHAUSTED: "RESEARCH_STRATEGIES_EXHAUSTED",
  SCHEDULED_REFRESH: "SCHEDULED_REFRESH",
  BUDGET_EXHAUSTED: "BUDGET_EXHAUSTED",
  PROVIDER_BLOCKED: "PROVIDER_BLOCKED",
  NEEDS_HUMAN_REVIEW: "NEEDS_HUMAN_REVIEW",
  IN_PROGRESS: "IN_PROGRESS",
  INTERRUPTED: "INTERRUPTED",
});

export const STOP_REASONS = Object.freeze({
  NO_FUTURE_DEMAND_EVIDENCE: "NO_FUTURE_DEMAND_EVIDENCE",
  EVENT_CANCELLED: "EVENT_CANCELLED",
  DATES_ALREADY_PAST: "DATES_ALREADY_PAST",
  OUTSIDE_HOTEL_MARKET: "OUTSIDE_HOTEL_MARKET",
  INSUFFICIENT_DEMAND_SCALE: "INSUFFICIENT_DEMAND_SCALE",
  ALREADY_PLACED_OR_CONTRACTED: "ALREADY_PLACED_OR_CONTRACTED",
  NO_ACTIONABLE_CONTACT_PATH: "NO_ACTIONABLE_CONTACT_PATH",
  SOURCE_CONFLICT_UNRESOLVED: "SOURCE_CONFLICT_UNRESOLVED",
  ORGANIZATION_INACTIVE: "ORGANIZATION_INACTIVE",
  DUPLICATE_OPPORTUNITY: "DUPLICATE_OPPORTUNITY",
  /** Full exhaustion — public + justified paid paths tried, or no remaining EV. */
  RESEARCH_EXHAUSTED: "RESEARCH_EXHAUSTED",
  /** Public/Jev path done; Parallel/Surfe not tried because paidBlocked. */
  PUBLIC_PATH_EXHAUSTED: "PUBLIC_PATH_EXHAUSTED",
  /** Public exhausted; a justified paid follow-up remains available. */
  PAID_FOLLOWUP_AVAILABLE: "PAID_FOLLOWUP_AVAILABLE",
  /** Org route ok; named-buyer depth still unresolved after reasonable public tries. */
  CONTACT_RESEARCH_PENDING: "CONTACT_RESEARCH_PENDING",
  /**
   * High-value related-entity leads remain authorized but not yet retrieved.
   * Not research exhaustion — commercial investigation incomplete.
   */
  RELATED_ENTITY_FOLLOWUPS_PENDING: "RELATED_ENTITY_FOLLOWUPS_PENDING",
  /**
   * Soft iteration/retrieval ceiling hit while commercially material questions remain.
   * Resumable — do not conflate with PUBLIC_PATH_EXHAUSTED or RESEARCH_EXHAUSTED.
   */
  ITERATION_CEILING_HIT: "ITERATION_CEILING_HIT",
  /**
   * Provider permission denied / paid blocked — not the same as exhausting public evidence.
   */
  PROVIDER_RESTRICTED_NOT_EXHAUSTION: "PROVIDER_RESTRICTED_NOT_EXHAUSTION",
  BUDGET_LIMIT_REACHED: "BUDGET_LIMIT_REACHED",
  BUDGET_BLOCKED: "BUDGET_BLOCKED",
  PROVIDER_BLOCKED: "PROVIDER_BLOCKED",
  TEMPORARY_FAILURE: "TEMPORARY_FAILURE",
  /** Evidence-backed reject (not a research-process failure). */
  LEGITIMATELY_REJECTED: "LEGITIMATELY_REJECTED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  HOTEL_CATCHMENT_MISMATCH: "HOTEL_CATCHMENT_MISMATCH",
  BELOW_COMMERCIAL_RELEVANCE: "BELOW_COMMERCIAL_RELEVANCE",
});

/** Soft / resumable stops — orchestrator may continue later. */
export const RESUMABLE_STOP_REASONS = Object.freeze([
  STOP_REASONS.PUBLIC_PATH_EXHAUSTED,
  STOP_REASONS.PAID_FOLLOWUP_AVAILABLE,
  STOP_REASONS.CONTACT_RESEARCH_PENDING,
  STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING,
  STOP_REASONS.ITERATION_CEILING_HIT,
  STOP_REASONS.PROVIDER_RESTRICTED_NOT_EXHAUSTION,
  STOP_REASONS.BUDGET_BLOCKED,
  STOP_REASONS.BUDGET_LIMIT_REACHED,
  STOP_REASONS.PROVIDER_BLOCKED,
  STOP_REASONS.TEMPORARY_FAILURE,
]);

/** Hard terminal — do not auto-retry without new evidence. */
export const HARD_TERMINAL_STOP_REASONS = Object.freeze([
  STOP_REASONS.RESEARCH_EXHAUSTED,
  STOP_REASONS.LEGITIMATELY_REJECTED,
  STOP_REASONS.NO_FUTURE_DEMAND_EVIDENCE,
  STOP_REASONS.EVENT_CANCELLED,
  STOP_REASONS.DATES_ALREADY_PAST,
  STOP_REASONS.OUTSIDE_HOTEL_MARKET,
  STOP_REASONS.INSUFFICIENT_DEMAND_SCALE,
  STOP_REASONS.ALREADY_PLACED_OR_CONTRACTED,
  STOP_REASONS.DUPLICATE_OPPORTUNITY,
  STOP_REASONS.ORGANIZATION_INACTIVE,
  STOP_REASONS.SOURCE_CONFLICT_UNRESOLVED,
  STOP_REASONS.HOTEL_CATCHMENT_MISMATCH,
  STOP_REASONS.BELOW_COMMERCIAL_RELEVANCE,
  STOP_REASONS.NOT_APPLICABLE,
]);

export const ORCHESTRATOR_STOP_REASONS = Object.freeze({
  NO_JUSTIFIED_WORK_REMAINING: "NO_JUSTIFIED_WORK_REMAINING",
  QUEUE_DRAINED: "QUEUE_DRAINED",
  /** Batch/checkpoint window ended — more justified work remains; safe to resume. */
  CHECKPOINT_CONTINUE: "CHECKPOINT_CONTINUE",
  EXECUTION_LIMIT_REACHED: "EXECUTION_LIMIT_REACHED",
  DISCOVERY_INCOMPLETE: "DISCOVERY_INCOMPLETE",
  AUTHORIZATION_EXHAUSTED: "AUTHORIZATION_EXHAUSTED",
  BUDGET_BLOCKED: "BUDGET_BLOCKED",
  PROVIDER_BLOCKED: "PROVIDER_BLOCKED",
  DELIVERY_BLOCKED: "DELIVERY_BLOCKED",
  PERSISTENCE_BLOCKED: "PERSISTENCE_BLOCKED",
  OPERATIONAL_FAILURE: "OPERATIONAL_FAILURE",
  LEASE_HELD_BY_OTHER: "LEASE_HELD_BY_OTHER",
  EXPLICIT_PAUSE: "EXPLICIT_PAUSE",
  /** Never set by orchestrator alone — Full E2E evaluator is SoT. */
  FULL_E2E_COMPLETE: "FULL_E2E_COMPLETE",
});

/** Stops that mean "resume the same queue" without force-lease. */
export const ORCHESTRATOR_RESUMABLE_CONTINUATION = Object.freeze([
  ORCHESTRATOR_STOP_REASONS.CHECKPOINT_CONTINUE,
  ORCHESTRATOR_STOP_REASONS.EXECUTION_LIMIT_REACHED,
  ORCHESTRATOR_STOP_REASONS.EXPLICIT_PAUSE,
  ORCHESTRATOR_STOP_REASONS.DISCOVERY_INCOMPLETE,
]);

export const BLOCKER_KEY = Object.freeze({
  DUPLICATE: "duplicate",
  EVENT_STATUS: "event_status",
  GEOGRAPHY_FIT: "geography_fit",
  FUTURE_DEMAND: "future_demand",
  DEMAND_SCALE: "demand_scale",
  HOTEL_FIT: "hotel_fit",
  BUYER_IDENTITY: "buyer_identity",
  CONTACT_ROUTE: "contact_route",
  CORROBORATION: "corroboration",
  SOURCE_CONFLICT: "source_conflict",
  DRAWER_COMPLETENESS: "drawer_completeness",
});

/** Higher = address first. */
export const BLOCKER_PRIORITY = Object.freeze({
  [BLOCKER_KEY.DUPLICATE]: 100,
  [BLOCKER_KEY.EVENT_STATUS]: 95,
  [BLOCKER_KEY.SOURCE_CONFLICT]: 92,
  [BLOCKER_KEY.GEOGRAPHY_FIT]: 90,
  [BLOCKER_KEY.FUTURE_DEMAND]: 85,
  [BLOCKER_KEY.DEMAND_SCALE]: 80,
  [BLOCKER_KEY.HOTEL_FIT]: 75,
  [BLOCKER_KEY.BUYER_IDENTITY]: 70,
  [BLOCKER_KEY.CONTACT_ROUTE]: 65,
  [BLOCKER_KEY.CORROBORATION]: 50,
  [BLOCKER_KEY.DRAWER_COMPLETENESS]: 40,
});

export const SEGMENT_RUN_STATUS = Object.freeze({
  PENDING: "PENDING",
  IN_PROGRESS: "IN_PROGRESS",
  COMPLETE: "COMPLETE",
  NO_SIGNAL_FOUND_AFTER_SUFFICIENT_RESEARCH: "NO_SIGNAL_FOUND_AFTER_SUFFICIENT_RESEARCH",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  BLOCKED: "BLOCKED",
  FAILED: "FAILED",
  INSUFFICIENT_RESEARCH_DEPTH: "INSUFFICIENT_RESEARCH_DEPTH",
});

/** Segment statuses that satisfy hotel segment-coverage pre-gate. */
export const SEGMENT_TERMINAL_OK = Object.freeze([
  SEGMENT_RUN_STATUS.COMPLETE,
  SEGMENT_RUN_STATUS.NO_SIGNAL_FOUND_AFTER_SUFFICIENT_RESEARCH,
  SEGMENT_RUN_STATUS.NOT_APPLICABLE,
]);

/**
 * Canonical expected segment ids for supervisor coverage
 * (RESEARCH_SEGMENT + planner intermediary axis from Full E2E).
 */
export const SUPERVISOR_EXPECTED_SEGMENTS = Object.freeze([
  RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  RESEARCH_SEGMENT.SPORTS,
  RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING,
  RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION,
  RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST,
  RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS,
  "professional_planners_dmc",
  RESEARCH_SEGMENT.SCALING_COMPANIES,
  RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
  RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS,
]);

/** Fixture / narrative aliases → canonical segment ids. */
export const SEGMENT_ALIAS_TO_CANONICAL = Object.freeze({
  ASSOCIATIONS: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  SPORTS: RESEARCH_SEGMENT.SPORTS,
  CORPORATE_MEETINGS: RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING,
  ENTERTAINMENT: RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION,
  EDUCATION: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  TOURS: RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST,
  WEDDINGS: RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS,
  PLANNERS_DMCS: "professional_planners_dmc",
  PRIVATE_EVENTS: RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS,
  SCALING_COMPANIES: RESEARCH_SEGMENT.SCALING_COMPANIES,
  LOCAL_MEETINGS: RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
  associations_government: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  sports: RESEARCH_SEGMENT.SPORTS,
  corporate_meetings_training: RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING,
  entertainment_production: RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION,
  medical_academic: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  tours_educational_specialist: RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST,
  weddings_social_private_events: RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS,
  professional_planners_dmc: "professional_planners_dmc",
  scaling_companies: RESEARCH_SEGMENT.SCALING_COMPANIES,
  local_meetings_catering: RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
  exhibitor_sponsor_teams: RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS,
  // Display labels from hotel configs / Airtable
  "Scaling Companies / Emerging Corporate Accounts":
    RESEARCH_SEGMENT.SCALING_COMPANIES,
  "Local Meetings & Catering": RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
  "Associations / government": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  Association: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  Associations: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  "Association / Government": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  "Association / Advocacy": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  "Association / Conference": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  "Association / Conference / Events": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  "Association / Conference Group": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  "Event / Association": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  Government: RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  "Government contractor": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  Sports: RESEARCH_SEGMENT.SPORTS,
  "Weekend group": RESEARCH_SEGMENT.SPORTS,
  "Sports / Weekend Group": RESEARCH_SEGMENT.SPORTS,
  "Sports / Academic Competition": RESEARCH_SEGMENT.SPORTS,
  "Corporate meetings, training, and project teams":
    RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING,
  Corporate: RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING,
  // Federal / regulatory conferences are associations_government — not hotel "training product"
  "Training Program": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  "Entertainment / production crews": RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION,
  "Medical / academic": RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  "Medical / Scientific": RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  // Advocacy lodging mechanism → associations_government (medical affinity is secondary only)
  "Medical / Advocacy": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  "Medical / Leadership": RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  Medical: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  Healthcare: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  "Healthcare / Advocacy": RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  Scientific: RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  "Tours / educational / specialist groups":
    RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST,
  "University / education": RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST,
  "Weddings, social groups, and nearby private-event venues":
    RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS,
  "Private Events": RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS,
  "Exhibitor / sponsor teams": RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS,
  // "Overflow Housing" is an opportunityType — not a demand segment (do not alias)
  "Professional planners / DMCs": "professional_planners_dmc",
  "Professional Planners & DMCs": "professional_planners_dmc",
});

export function canonicalizeSegmentId(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  if (SEGMENT_ALIAS_TO_CANONICAL[s]) return SEGMENT_ALIAS_TO_CANONICAL[s];
  const upper = s.toUpperCase();
  if (SEGMENT_ALIAS_TO_CANONICAL[upper]) return SEGMENT_ALIAS_TO_CANONICAL[upper];
  // Fuzzy label contains
  const lower = s.toLowerCase();
  if (/scaling/.test(lower)) return RESEARCH_SEGMENT.SCALING_COMPANIES;
  if (/local meetings|catering|chamber of commerce|shrm|pmi /.test(lower))
    return RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING;
  // Advocacy / legislative before medical keyword so "Medical / Advocacy" classifies correctly
  if (/advocacy|legislative|hill day|gad institute|qui tam|ntca|ptab|real.?tor/.test(lower))
    return RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT;
  if (/association|government/.test(lower))
    return RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT;
  if (/sport|weekend group|soccer|lacrosse|tournament/.test(lower))
    return RESEARCH_SEGMENT.SPORTS;
  if (/corporate|project team|offsite|retreat/.test(lower)
    && !/training program/.test(lower))
    return RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING;
  if (/entertainment|production|crew|nutcracker|talmi/.test(lower))
    return RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION;
  if (/medical|academic|healthcare|scientific|nih|nci|pda|bio-?trac|renal|pharmacy|anesthes/.test(lower)
    && !/advocacy|legislative/.test(lower))
    return RESEARCH_SEGMENT.MEDICAL_ACADEMIC;
  if (/tour|educational|specialist|university|alumni|georgetown|umd/.test(lower)
    && !/association of governing|trusteeship/.test(lower))
    return RESEARCH_SEGMENT.TOURS_EDUCATIONAL_SPECIALIST;
  if (/wedding|social|private.?event|woman.?s club/.test(lower))
    return RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS;
  if (/planner|dmc|cohera|csi dmc/.test(lower)) return "professional_planners_dmc";
  // Exhibitor/sponsor only — never treat "Overflow Housing" opportunityType as a segment
  if (/exhibitor|sponsor team|hospitality suite/.test(lower))
    return RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS;
  if (/^overflow\s*housing$/i.test(s) || /overflow housing/.test(lower))
    return ""; // not a canonical segment — require demand-mechanism classification
  return s;
}

/**
 * Soft floors for sufficiency heuristics — NOT a hard "tasks >= 2" completion law.
 * Segment completeness uses assessSegmentResearchSufficiency() instead.
 */
export const DEFAULT_MINIMUM_DEPTH = Object.freeze({
  /** Advisory only — one shallow task is never enough; two shallow tasks also insufficient. */
  preferMinIndependentTasks: 2,
  minSourceFamilies: 2,
  preferMinFollowUpPasses: 1,
});

export const SUPERVISOR_DEFAULTS = Object.freeze({
  maxIterations: 8,
  maxAttemptsPerBlocker: 4,
  maxTotalAttempts: 18,
  minSourceFamiliesForExhaustion: 3,
  noNewEvidenceStopAfter: 3,
  minStrategiesForExhaustion: 3,
  minQueriesForExhaustion: 6,
  requireActionableContactRoute: true,
  requireFutureDemand: true,
  requireHotelFit: true,
  /** Soft buyer: missing named buyer does not block PUBLISHABLE if route usable — but marks exploratory debt. */
  requireNamedBuyerForPublishable: false,
  /** Priority B: keep researching named buyer for hotel-facing org-route prospects. */
  deepenNamedBuyer: true,
  maxBuyerDepthAttempts: 3,
  maxCostUsd: Infinity,
  allowPublicWhenPaidBlocked: true,
});
