/**
 * Dealality Decision Radar — GTM Airtable field maps (internal acquisition only).
 *
 * Base: AIRTABLE_GTM_BASE_ID (same as Owner Targets / Contacts / Properties).
 * Never sync to product bases, Target OS, Hotel Census, or Explorers.
 *
 * Identity rule: one hotel/project + one meaningful strategic decision window.
 * Not an owner account, portfolio, news article, or contact.
 *
 * SoT: docs/gtm-decision-radar.md
 */

import { GTM_OWNER_TARGET_TABLES } from "./field-map.js";
import { GTM_CONTACT_TABLE } from "./contact-field-map.js";

/** @type {string} */
export const GTM_DECISION_OPPORTUNITIES_TABLE =
  process.env.AIRTABLE_GTM_DECISION_OPPORTUNITIES_TABLE || "Decision Opportunities";

/** @type {string} */
export const GTM_DECISION_OPPORTUNITY_EVIDENCE_TABLE =
  process.env.AIRTABLE_GTM_DECISION_OPPORTUNITY_EVIDENCE_TABLE ||
  "Decision Opportunity Evidence";

/** Linked GTM tables (reuse existing — do not duplicate). */
export const GTM_DECISION_RADAR_LINKED_TABLES = {
  ownerTargets: GTM_OWNER_TARGET_TABLES.ownerTargets,
  properties: GTM_OWNER_TARGET_TABLES.properties,
  contacts: GTM_CONTACT_TABLE,
  decisionOpportunities: GTM_DECISION_OPPORTUNITIES_TABLE,
  evidence: GTM_DECISION_OPPORTUNITY_EVIDENCE_TABLE,
};

/**
 * Decision Opportunities — Airtable column names.
 * @type {Record<string, string>}
 */
export const MAP_DECISION_OPPORTUNITY = {
  // Identity
  opportunityId: "Opportunity ID",
  opportunityName: "Opportunity Name",
  projectHotelName: "Project / Hotel Name",
  country: "Country",
  cityMarket: "City / Market",
  projectType: "Project Type",
  leadProperty: "Lead Property",
  ownerTarget: "Owner Target",

  // Decision
  likelyDecisionType: "Likely Decision Type",
  decisionStage: "Decision Stage",
  decisionWindow: "Decision Window",
  decisionStillOpen: "Decision Still Open",
  decisionOpenConfidence: "Decision Open Confidence",
  brandStatus: "Brand Status",
  operatorStatus: "Operator Status",
  exclusivityStatus: "Exclusivity Status",
  trigger: "Trigger",
  whyNow: "Why Now",
  whyDealality: "Why Dealality",

  // Contact / relationship (links reuse Contacts; warm path is manual)
  decisionMakers: "Decision Makers",
  warmPathType: "Warm Path Type",
  warmPathContactSource: "Warm Path Contact / Source",
  warmPathNotes: "Warm Path Notes",

  // Workflow
  status: "Status",
  recommendedAction: "Recommended Action",
  founderNotes: "Founder Notes",
  founderReviewed: "Founder Reviewed",
  lastReviewedAt: "Last Reviewed At",

  // Score foundation (contract only in Stage 1 — do not populate speculative scores)
  opportunityScore: "Opportunity Score",
  scoreBand: "Score Band",
  scoreExplanation: "Score Explanation",
  scoredAt: "Scored At",

  // Dedupe / entity resolution support (no engine in Stage 1)
  canonicalOpportunityKey: "Canonical Opportunity Key",
  externalSourceProjectName: "External / Source Project Name",
  possibleDuplicate: "Possible Duplicate",
  duplicateOf: "Duplicate Of",

  // Evidence inverse (Airtable auto-creates from Evidence → Decision Opportunity link)
  evidence: "Decision Opportunity Evidence",

  // Guardrails
  visibility: "Visibility",
  internalNotes: "Internal Notes",
  dataSource: "Data Source",
};

/**
 * Decision Opportunity Evidence — Airtable column names.
 * @type {Record<string, string>}
 */
export const MAP_DECISION_OPPORTUNITY_EVIDENCE = {
  evidenceId: "Evidence ID",
  decisionOpportunity: "Decision Opportunity",
  sourceUrl: "Source URL",
  sourceName: "Source Name",
  sourceType: "Source Type",
  publicationDate: "Publication Date",
  retrievedDate: "Retrieved Date",
  evidenceExcerpt: "Evidence Excerpt",
  supportsField: "Supports Field",
  evidenceConfidence: "Evidence Confidence",
  evidenceDirection: "Evidence Direction",
  notes: "Notes",
};

/** Project type — controlled taxonomy (minimal). */
export const VAL_DECISION_PROJECT_TYPE = [
  "Existing Hotel",
  "New Development",
  "Conversion",
  "Repositioning",
  "Acquisition",
  "Mixed-Use",
  "Branded Residences",
  "Unknown",
];

/** Likely decision type. */
export const VAL_DECISION_LIKELY_TYPE = [
  "Brand Selection",
  "Reflag",
  "Operator Selection",
  "HMA vs Franchise",
  "Conversion",
  "Independent Strategy",
  "Repositioning",
  "Mixed-Use Hotel Strategy",
  "Acquisition Strategy",
  "Branded Residences Strategy",
  "Multiple / Strategic Path",
  "Unknown",
];

/**
 * Decision stage — distinguishes early signal vs exclusive/signed.
 * Mapping note: GTM branding-decision timing uses pre_decision / post_decision / uncertain
 * (lib/gtm-owner-target/branding-decision-signals.js). Radar stages are richer for founder review.
 */
export const VAL_DECISION_STAGE = [
  "Early Signal",
  "Exploring Options",
  "Active Evaluation",
  "Shortlisting",
  "Selected / Not Signed",
  "Exclusive / Signed",
  "Uncertain",
];

/** Timing window for founder attention. */
export const VAL_DECISION_WINDOW = [
  "0–6 Months",
  "6–18 Months",
  "18–36 Months",
  "> 36 Months",
  "Unknown",
];

/**
 * Brand status — CRITICAL:
 * "Not Publicly Identified" ≠ "brand has not been selected".
 * Absence of public brand info must not become Decision Still Open = Yes + Confirmed.
 */
export const VAL_DECISION_BRAND_STATUS = [
  "Not Publicly Identified",
  "Exploring",
  "Shortlist / Discussions",
  "Selected / Not Signed",
  "Signed / Exclusive",
  "Existing Brand",
  "Unknown",
];

/** Operator status — same semantic principle as brand status. */
export const VAL_DECISION_OPERATOR_STATUS = [
  "Not Publicly Identified",
  "Exploring",
  "Shortlist / Discussions",
  "Selected / Not Signed",
  "Signed / Exclusive",
  "Existing Operator",
  "Unknown",
];

export const VAL_DECISION_EXCLUSIVITY_STATUS = [
  "None Known",
  "Discussions (Non-Exclusive)",
  "Shortlist",
  "Exclusive",
  "Signed",
  "Unknown",
];

export const VAL_DECISION_STILL_OPEN = ["Yes", "No", "Uncertain"];

/**
 * Field-level confidence for Decision Still Open and other critical claims.
 * Separate from Opportunity Score. Reuses Dealality fact-vs-inference vocabulary.
 */
export const VAL_DECISION_OPEN_CONFIDENCE = [
  "Confirmed",
  "Probable",
  "Inferred",
  "Unknown",
];

/**
 * Trigger — Title Case for Radar UI.
 * Maps from GTM Owner Target Deal Trigger (VAL_GTM_DEAL_TRIGGER snake_case) where applicable:
 *   conversion → Conversion Candidate
 *   reflag → Reflag / Operator Mismatch
 *   operator_rfp → Operator Selection Proxy
 *   new_build → New Development
 *   development_pipeline → Development Pipeline
 *   independent_unbranded → Independent / Unbranded
 *   brand_renewal_window → Brand Renewal Window
 *   portfolio_standardization → Portfolio Standardization
 *   sale_process → Acquisition / Sale Process
 *   recent_open_branded → Recent Open Branded (Late)
 *   none_known → Unknown
 * Extra Radar triggers cover filings/news not present in CoStar Deal Trigger.
 */
export const VAL_DECISION_TRIGGER = [
  "New Development",
  "Development Pipeline",
  "Independent / Unbranded",
  "Conversion Candidate",
  "Reflag / Operator Mismatch",
  "Operator Selection Proxy",
  "Brand Renewal Window",
  "Portfolio Standardization",
  "Acquisition / Sale Process",
  "Ownership Change",
  "Renovation / Repositioning",
  "Deflag / Brand Departure",
  "Planning / Environmental Approval",
  "Construction Approval / Restart",
  "Financing / Recapitalization",
  "Branded Residences Strategy Open",
  "Recent Open Branded (Late)",
  "Other High Intent",
  "Monitor / Lower Intent",
  "Unknown",
];

export const VAL_DECISION_WARM_PATH_TYPE = [
  "Direct",
  "1st Degree",
  "2nd Degree",
  "Cold",
  "Unknown",
];

export const VAL_DECISION_STATUS = [
  "Discovered",
  "Researching",
  "Qualified",
  "Monitor",
  "Founder Review",
  "Outreach Ready",
  "Contacted",
  "Conversation",
  "Opportunity Opened",
  "Disqualified",
  "Archived",
];

export const VAL_DECISION_RECOMMENDED_ACTION = [
  "Founder Review",
  "Warm Introduction",
  "Direct Outreach",
  "Enrich",
  "Monitor",
  "Disqualify",
  "No Action",
];

/** Score band contract — Stage 1 stores empty; engine deferred. */
export const VAL_DECISION_SCORE_BAND = [
  "Founder Action",
  "Enrich",
  "Monitor",
  "Low Priority",
];

/** Reuse GTM visibility guardrail. */
export const VAL_DECISION_VISIBILITY = ["internal_only"];

export const VAL_DECISION_DATA_SOURCE = [
  "manual",
  "costar_derived",
  "webhound_candidate",
  "market_alert",
  "registry",
  "mixed",
];

/** Evidence confidence — aligns with Partner Source Library Source Quality (High/Medium/Low). */
export const VAL_EVIDENCE_CONFIDENCE = ["High", "Medium", "Low"];

/**
 * Evidence direction — too-late / closed evidence must be first-class.
 */
export const VAL_EVIDENCE_DIRECTION = [
  "Supports Open",
  "Supports Closed / Too Late",
  "Neutral / Context",
];

export const VAL_EVIDENCE_SUPPORTS_FIELD = [
  "Project Exists",
  "Ownership",
  "Development Stage",
  "Brand Status",
  "Operator Status",
  "Decision Timing",
  "Acquisition",
  "Renovation / Repositioning",
  "Financing",
  "Opening Date",
  "Decision Still Open",
  "Contact / Decision Maker",
  "Other",
];

export const VAL_EVIDENCE_SOURCE_TYPE = [
  "Press Release",
  "Trade Publication",
  "Local Business Press",
  "Government Filing",
  "Planning Record",
  "Developer Website",
  "Company Website",
  "LinkedIn",
  "CoStar Internal",
  "Registry",
  "Manual Note",
  "Other",
];

/** Statuses that require Qualified/Founder-grade fields + evidence. */
export const VAL_DECISION_STATUS_REQUIRES_QUALIFICATION = [
  "Qualified",
  "Founder Review",
];

/** Statuses that encode Outreach Ready contract (no auto-transition in Stage 1). */
export const VAL_DECISION_STATUS_OUTREACH_READY_CONTRACT = ["Outreach Ready"];

/**
 * Branding-decision timing → Radar Decision Stage (documentation / future seed mapping).
 * @type {Record<string, string>}
 */
export const MAP_BRANDING_TIMING_TO_DECISION_STAGE = {
  pre_decision: "Exploring Options",
  post_decision: "Exclusive / Signed",
  uncertain: "Uncertain",
};
