/**
 * GDI V5 funnel — SIGNAL ≠ RESEARCH_LEAD ≠ CANDIDATE ≠ CUSTOMER_READY.
 * No SIGNAL → CANDIDATE shortcut.
 */

export const FUNNEL_STAGE = Object.freeze({
  SIGNAL: "SIGNAL",
  RESEARCH_LEAD: "RESEARCH_LEAD",
  CANDIDATE_OPPORTUNITY: "CANDIDATE_OPPORTUNITY",
  CUSTOMER_READY_OPPORTUNITY: "CUSTOMER_READY_OPPORTUNITY",
  VALID_FUTURE_WATCH: "VALID_FUTURE_WATCH",
  REJECTED: "REJECTED",
});

export const FUNNEL_DEFINITIONS = Object.freeze({
  SIGNAL:
    "Interesting market activity only (expansion, conference exists, project announced). Not research-worthy alone.",
  RESEARCH_LEAD:
    "Valid entity + market relevance + plausible future group motion + ≥2 concrete supporting signals. Justifies targeted page research.",
  CANDIDATE_OPPORTUNITY:
    "Organization + group motion + future decision/cycle + plausible lodging need + hotel fit + buyer/organizer path (or clear resolve path). Page-validated.",
  CUSTOMER_READY_OPPORTUNITY:
    "Passes canonical isGdiCustomerOpportunityReady() — no manual promotion.",
});
