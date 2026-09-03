/**
 * Brand & Portfolio historical schema extensions — reuse Core history tables.
 * MEASUREMENT_FAMILY_HISTORY_ISOLATION
 *
 * Does not enable production Airtable writes.
 */

import {
  HISTORY_TABLES,
  IDEMPOTENCY_KEYS,
} from "../longitudinal/airtable-history-schema-final-v1.js";

export const MEASUREMENT_FAMILY = Object.freeze({
  CORE: "CORE",
  BRAND_PORTFOLIO: "BRAND_PORTFOLIO",
});

export const BPP_HISTORY_SCHEMA_VERSION = "adp_bpp_history_schema_v1";
export const BPP_REPORT_EDITION = "CUSTOMER_PUBLISHED_BASELINE_V1_1";
export const BPP_PUBLICATION_VERSION = "bpp-customer-v1.1-20260821";
export const BPP_PERIOD_ID = "bpp_first_cycle_2026-08-21T2057";
export const BPP_CALENDAR_WEEK_ID = "adp_week_2026-08-21";
export const BPP_ASSET_TOKEN_FROZEN = "adp-v70-20260822-bpp-visual";

/** Shared monitoring period + family-scoped children */
export const BPP_IDEMPOTENCY_KEYS = Object.freeze({
  ...IDEMPOTENCY_KEYS,
  monitoringPeriod: "propertyId+calendarWeekId",
  reportSnapshot: "snapshotId+measurementFamily",
  periodMetrics: "propertyId+measurementPeriodId+measurementFamily+historicalVersion",
  territoryMetric:
    "propertyId+measurementPeriodId+measurementFamily+territoryId+historicalVersion",
  providerMetric:
    "propertyId+measurementPeriodId+measurementFamily+providerId+historicalVersion",
  competitiveRank:
    "propertyId+measurementPeriodId+measurementFamily+scope+canonicalEntityId+historicalVersion",
  evidenceIndex: "propertyId+measurementPeriodId+measurementFamily+observationId",
  reportCorrection: "correctionId",
  promptLedger: "propertyId+measurementPeriodId+measurementFamily+observationId",
  peerSetFreeze: "propertyId+measurementPeriodId+peerSetId+peerSetVersion",
});

/**
 * Proposed Airtable field additions (ensure-script only — not applied here).
 * Existing tables remain; Measurement Family isolates CORE vs BRAND_PORTFOLIO.
 */
export const BPP_HISTORY_FIELD_EXTENSIONS = Object.freeze({
  [HISTORY_TABLES.MONITORING_PERIODS]: [
    { name: "Calendar Week ID", type: "singleLineText" },
    { name: "Measurement Period IDs JSON", type: "multilineText" },
    { name: "Families Present JSON", type: "multilineText" },
  ],
  [HISTORY_TABLES.REPORT_SNAPSHOTS]: [
    { name: "Measurement Family", type: "singleSelect", choices: Object.values(MEASUREMENT_FAMILY) },
    { name: "Publication Version", type: "singleLineText" },
    { name: "Report Edition", type: "singleLineText" },
    { name: "Portfolio Lens ID", type: "singleLineText" },
    { name: "Peer Set ID", type: "singleLineText" },
    { name: "Peer Set Version", type: "singleLineText" },
    { name: "Peer Set Hash", type: "singleLineText" },
    { name: "Prompt Manifest Hash", type: "singleLineText" },
    { name: "Customer Visible Content Hash", type: "singleLineText" },
  ],
  [HISTORY_TABLES.PERIOD_METRICS]: [
    { name: "Measurement Family", type: "singleSelect", choices: Object.values(MEASUREMENT_FAMILY) },
    { name: "Portfolio Lens ID", type: "singleLineText" },
    { name: "Portfolio Lens Label", type: "singleLineText" },
    { name: "Peer Set ID", type: "singleLineText" },
    { name: "Peer Set Version", type: "singleLineText" },
    { name: "Peer Set Hash", type: "singleLineText" },
    { name: "Portfolio AI Presence", type: "number" },
    { name: "Portfolio AI Presence Numerator", type: "number" },
    { name: "Portfolio AI Presence Denominator", type: "number" },
    { name: "Portfolio Rank", type: "number" },
    { name: "Portfolio Rank Of", type: "number" },
    { name: "Portfolio Benchmark", type: "number" }, // null when suppressed
    { name: "Portfolio Presence Index", type: "number" }, // null when suppressed
    { name: "Number One Appearance Rate", type: "number" },
    { name: "Top Three Appearance Rate", type: "number" },
    { name: "Portfolio Scenario Presence", type: "number" }, // INTERNAL_DIAGNOSTIC only
    { name: "Portfolio Scenario Presence Role", type: "singleLineText" },
    { name: "Rank Movement State", type: "singleSelect", choices: ["INITIAL", "COMPARABLE", "NOT_COMPARABLE"] },
  ],
  [HISTORY_TABLES.TERRITORY_METRICS]: [
    { name: "Measurement Family", type: "singleSelect", choices: Object.values(MEASUREMENT_FAMILY) },
    { name: "Peer Set Version", type: "singleLineText" },
    { name: "Scenario Count", type: "number" },
    { name: "Number One Appearance Rate", type: "number" },
    { name: "Top Three Appearance Rate", type: "number" },
  ],
  [HISTORY_TABLES.PROVIDER_METRICS]: [
    { name: "Measurement Family", type: "singleSelect", choices: Object.values(MEASUREMENT_FAMILY) },
  ],
  [HISTORY_TABLES.COMPETITIVE_RANKINGS]: [
    { name: "Measurement Family", type: "singleSelect", choices: Object.values(MEASUREMENT_FAMILY) },
    { name: "Brand", type: "singleLineText" },
    { name: "Peer Set ID", type: "singleLineText" },
    { name: "Peer Set Hash", type: "singleLineText" },
    { name: "Is Subject", type: "checkbox" },
  ],
  [HISTORY_TABLES.EVIDENCE_INDEX]: [
    { name: "Measurement Family", type: "singleSelect", choices: Object.values(MEASUREMENT_FAMILY) },
  ],
  [HISTORY_TABLES.REPORT_CORRECTIONS]: [
    { name: "Measurement Family", type: "singleSelect", choices: Object.values(MEASUREMENT_FAMILY) },
    { name: "Lineage Stage", type: "singleLineText" },
  ],
  [HISTORY_TABLES.PROMPT_LEDGER]: [
    { name: "Measurement Family", type: "singleSelect", choices: Object.values(MEASUREMENT_FAMILY) },
    {
      name: "Scenario Class",
      type: "singleSelect",
      choices: ["NEUTRAL_DEMAND", "BRAND_PORTFOLIO_DEMAND", "PROPERTY_SPECIFIC", "OTHER_GOVERNED_SPECIAL"],
    },
  ],
});

export const PEER_SET_CHANGE_CLASS = Object.freeze({
  PEER_SET_UNCHANGED: "PEER_SET_UNCHANGED",
  PEER_SET_ROUTINE_MARKET_CHANGE: "PEER_SET_ROUTINE_MARKET_CHANGE",
  PEER_SET_MATERIAL_CHANGE: "PEER_SET_MATERIAL_CHANGE",
  LENS_CHANGE_NOT_COMPARABLE: "LENS_CHANGE_NOT_COMPARABLE",
  METHODOLOGY_CHANGE_NOT_COMPARABLE: "METHODOLOGY_CHANGE_NOT_COMPARABLE",
});

/**
 * PORTFOLIO_LONGITUDINAL_PEER_SET_INTEGRITY — comparability policy.
 */
export const BPP_COMPARABILITY_POLICY_V1 = Object.freeze({
  version: "BPP_COMPARABILITY_POLICY_V1",
  normallyComparableWhen: [
    "same propertyId",
    "same measurementFamily = BRAND_PORTFOLIO",
    "same primary lens semantics (lensId)",
    "compatible KPI contract major (V1.1 lineage)",
    "compatible prompt methodology (manifest contract)",
    "compatible market definition",
    "peer universe change not materially distortive",
  ],
  peerChangeClasses: PEER_SET_CHANGE_CLASS,
  routinePeerChangeMayRemainComparableWhen: [
    "lens unchanged",
    "market definition unchanged",
    "methodology / KPI contract compatible",
    "peer open/close is naturally occurring market evolution",
    "denominator logic remains observation-grain compatible",
    "change disclosed/flagged on comparison",
  ],
  materialNonComparableExamples: [
    "Hilton Honors → Curio (lens change)",
    "Marriott Bonvoy → other universe",
    "Times Square → all NYC market expansion",
    "independent methodology material change",
  ],
  firstPeriodRankMovement: "INITIAL",
  comparisonWindowsReuse: ["PRIOR_RUN", "LAST_30_DAYS", "MONTH_TO_DATE"],
  comparisonIdentityIncludes: ["propertyId", "measurementFamily", "lensId", "peerSetHash class"],
});

export const CORRECTION_LINEAGE_STAGES = Object.freeze({
  ORIGINAL_FIRST_CYCLE_CANDIDATE: "ORIGINAL_FIRST_CYCLE_CANDIDATE",
  CORRECTED_FIRST_CYCLE_CANDIDATE: "CORRECTED_FIRST_CYCLE_CANDIDATE",
  CUSTOMER_PUBLISHED_BASELINE: "CUSTOMER_PUBLISHED_BASELINE",
});

export const CORRECTION_REASON = "PORTFOLIO_METRIC_GRAIN_MISMATCH";
