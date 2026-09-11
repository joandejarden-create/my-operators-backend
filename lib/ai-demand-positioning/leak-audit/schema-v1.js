/**
 * ADP Demand Leak Audit — schema / enums / field maps (Phase 1).
 * External product name: AI Demand Leak Audit.
 * Storage is filesystem with Airtable-shaped contracts; never writes production ADP/Census/Explorer.
 */

import {
  LEAK_AUDIT_LITE_COST_CONTROLS,
  RESEARCH_MODES,
  PROMPT_SET_VERSION_LITE,
} from "./research-mode-lite-v1.js";

export const LEAK_AUDIT_SCHEMA_VERSION = "adp_leak_audit_schema_v1";
export const LEAK_AUDIT_PRODUCT_NAME = "AI Demand Leak Audit";
export const LEAK_AUDIT_INTERNAL_NAME = "ADP Demand Leak Audit Runner";

export const PROMPT_SET_VERSION = "leak_audit_lite_v1";
export const PROVIDER_SET_VERSION = "leak_audit_provider_set_v1";

/** Free-audit caps — ADP Lite (leak_audit_lite). */
export const FREE_AUDIT_SCOPE = Object.freeze({
  maxHotels: LEAK_AUDIT_LITE_COST_CONTROLS.maxHotels,
  minDemandTerritories: LEAK_AUDIT_LITE_COST_CONTROLS.minDemandTerritories,
  maxDemandTerritories: LEAK_AUDIT_LITE_COST_CONTROLS.maxDemandTerritories,
  minProviders: LEAK_AUDIT_LITE_COST_CONTROLS.minProviders,
  maxProviders: LEAK_AUDIT_LITE_COST_CONTROLS.maxProviders,
  maxScenarios: LEAK_AUDIT_LITE_COST_CONTROLS.maxScenarios,
  maxObservations: LEAK_AUDIT_LITE_COST_CONTROLS.maxObservations,
  maxCompetitorsShown: LEAK_AUDIT_LITE_COST_CONTROLS.maxCompetitorsShown,
  maxEvidenceCards: LEAK_AUDIT_LITE_COST_CONTROLS.maxEvidenceCards,
  maxActionItems: LEAK_AUDIT_LITE_COST_CONTROLS.maxActionItems,
  maxSourceLinksPerEvidence: LEAK_AUDIT_LITE_COST_CONTROLS.maxSourceLinksPerEvidence,
  noFullSourceCrawl: true,
  noHistoricalTrend: true,
  noPaidAdpWrite: true,
  researchMode: RESEARCH_MODES.LEAK_AUDIT_LITE,
  promptSetVersion: PROMPT_SET_VERSION_LITE,
});

export { RESEARCH_MODES, LEAK_AUDIT_LITE_COST_CONTROLS, PROMPT_SET_VERSION_LITE };

export const REQUEST_STATUS = Object.freeze({
  REQUESTED: "requested",
  APPROVED: "approved",
  QUEUED: "queued",
  RUNNING: "running",
  COMPLETED: "completed",
  REPORT_READY: "report_ready",
  SENT: "sent",
  CONVERTED: "converted",
  REJECTED: "rejected",
  ERROR: "error",
});

export const REQUEST_SOURCES = Object.freeze([
  "LinkedIn",
  "referral",
  "direct",
  "manual",
  "sample",
  "other",
]);

export const AUDIENCE_TYPES = Object.freeze([
  "owner",
  "operator",
  "asset_manager",
  "brand_avp",
  "portfolio_company",
  "agency",
  "other",
]);

export const COMMERCIAL_PRIORITIES = Object.freeze([
  "grow_demand_segment",
  "reposition_hotel",
  "improve_direct_demand",
  "defend_against_competitor",
  "support_owner_reporting",
  "prepare_for_renovation_or_relaunch",
  "improve_group_or_event_visibility",
  "improve_brand_or_category_perception",
  "portfolio_benchmarking",
  "other",
]);

export const DEMAND_PRIORITIES = Object.freeze([
  "leisure",
  "couples_romantic",
  "wellness",
  "meetings_groups",
  "weddings_celebrations",
  "family",
  "business_travel",
  "luxury_transient",
  "extended_stay",
  "direct_booking",
  "other",
]);

export const REPORT_MODES = Object.freeze({
  SINGLE_PROPERTY: "single_property",
  PORTFOLIO_ROLLUP: "portfolio_rollup",
});

export const PRIORITY_SOURCES = Object.freeze({
  INFERRED_FROM_RESULTS: "inferred_from_results",
  CLIENT_CONFIRMED: "client_confirmed",
  ADMIN_SELECTED: "admin_selected",
  UNKNOWN: "unknown",
});

export const RUN_STATUS = Object.freeze({
  QUEUED: "queued",
  RUNNING: "running",
  COMPLETED: "completed",
  FAILED: "failed",
});

export const CONFIDENCE_FLAG = Object.freeze({
  HIGH: "high",
  MEDIUM: "medium",
  LOW: "low",
});

export const REPORT_STATUS = Object.freeze({
  DRAFT: "draft",
  APPROVED: "approved",
  SENT: "sent",
});

/**
 * Demand territories for free-audit selection (internal keys + customer labels).
 * Aligned with ADP INTENT_TERRITORY_LABELS where possible.
 */
export const LEAK_AUDIT_DEMAND_TERRITORIES = Object.freeze({
  leisure: "Leisure Travel",
  couples: "Couples / Romantic Stay",
  meetings_groups: "Meetings & Groups",
  wellness: "Wellness",
  family: "Family Travel",
  business: "Business Travel",
  celebration: "Celebrations & Events",
});

export const DEMAND_SEGMENT_ALIASES = Object.freeze({
  leisure: "leisure",
  "leisure travel": "leisure",
  "resort leisure": "leisure",
  couples: "couples",
  romantic: "couples",
  "couples / romantic": "couples",
  meetings: "meetings_groups",
  groups: "meetings_groups",
  "meetings / groups": "meetings_groups",
  "meetings & groups": "meetings_groups",
  wellness: "wellness",
  family: "family",
  "family travel": "family",
  business: "business",
  "business travel": "business",
  celebration: "celebration",
  weddings: "celebration",
  "celebration / weddings": "celebration",
  "celebrations & events": "celebration",
});

/** Central field maps — no scattered raw names in UI/API. */
export const map_leak_audit_request = Object.freeze({
  id: "id",
  hotelName: "hotelName",
  hotelWebsite: "hotelWebsite",
  city: "city",
  market: "market",
  country: "country",
  contactName: "contactName",
  contactEmail: "contactEmail",
  companyName: "companyName",
  role: "role",
  demandSegmentOfInterest: "demandSegmentOfInterest",
  commercialPriority: "commercialPriority",
  demandPriority: "demandPriority",
  secondaryDemandPriority: "secondaryDemandPriority",
  portfolioName: "portfolioName",
  portfolioGroupId: "portfolioGroupId",
  audienceType: "audienceType",
  reportMode: "reportMode",
  decisionContext: "decisionContext",
  urgencyReason: "urgencyReason",
  prioritySource: "prioritySource",
  status: "status",
  source: "source",
  notes: "notes",
  createdAt: "createdAt",
  updatedAt: "updatedAt",
});

export const map_leak_audit_run = Object.freeze({
  id: "id",
  auditRequestId: "auditRequestId",
  matchedHotelId: "matchedHotelId",
  runDate: "runDate",
  providerSetVersion: "providerSetVersion",
  promptSetVersion: "promptSetVersion",
  demandTerritoriesTested: "demandTerritoriesTested",
  providersUsed: "providersUsed",
  totalPromptsRun: "totalPromptsRun",
  totalObservations: "totalObservations",
  subjectMentionCount: "subjectMentionCount",
  competitorMentionCount: "competitorMentionCount",
  displacementCount: "displacementCount",
  confidenceFlag: "confidenceFlag",
  completenessFlag: "completenessFlag",
  status: "status",
  errorLog: "errorLog",
  createdAt: "createdAt",
  updatedAt: "updatedAt",
});

export const map_leak_audit_observation = Object.freeze({
  id: "id",
  auditRunId: "auditRunId",
  provider: "provider",
  demandTerritory: "demandTerritory",
  promptId: "promptId",
  promptLabel: "promptLabel",
  promptIntentSummary: "promptIntentSummary",
  subjectHotelMentioned: "subjectHotelMentioned",
  subjectMentionRank: "subjectMentionRank",
  competitorsMentioned: "competitorsMentioned",
  displacedByCompetitor: "displacedByCompetitor",
  displacedCompetitorName: "displacedCompetitorName",
  aiResponseExcerpt: "aiResponseExcerpt",
  citedSources: "citedSources",
  sourceUrls: "sourceUrls",
  notes: "notes",
  createdAt: "createdAt",
});

export const map_leak_audit_report = Object.freeze({
  id: "id",
  auditRunId: "auditRunId",
  reportStatus: "reportStatus",
  bottomLineSummary: "bottomLineSummary",
  biggestDemandLeak: "biggestDemandLeak",
  mainCompetitorShowingUpInstead: "mainCompetitorShowingUpInstead",
  likelyReason: "likelyReason",
  firstFix: "firstFix",
  secondFix: "secondFix",
  thirdFix: "thirdFix",
  recommendedNextStep: "recommendedNextStep",
  reportUrl: "reportUrl",
  pdfUrl: "pdfUrl",
  sentAt: "sentAt",
  createdAt: "createdAt",
  updatedAt: "updatedAt",
});

/** Paths that Phase 1 leak-audit code must never write to. */
export const FORBIDDEN_WRITE_PATH_FRAGMENTS = Object.freeze([
  "data/ai-demand-positioning/runtime",
  "data/ai-demand-positioning/published",
  "data/ai-demand-positioning/brand-portfolio-history",
  "data/ai-demand-positioning/core-history",
  "fixtures/brand-explorer",
  "fixtures/operator-",
  "Hotel Property Census",
]);
