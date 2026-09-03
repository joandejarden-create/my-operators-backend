/**
 * Production ADP historical Airtable schema — FINALIZED DESIGN (writes DISABLED).
 *
 * Gap vs today:
 *   EXISTS: "AI Demand Positioning - Published Reports" = current Live publish overlay
 *           (one Live row per property; Payload JSON; evidence stub when large)
 *   MISSING: append-only weekly history, snapshot index, rankings, territories,
 *            providers, evidence refs, corrections, persistence-state machine
 *
 * SoT policy (unchanged):
 *   FILESYSTEM / immutable object = exact report reproduction
 *   AIRTABLE = structured historical query/index (future)
 *   Published Reports Live row = optional current overlay — NOT longitudinal SoT
 */

import { ADP_PUBLISHED_REPORTS_TABLE, map_adp_published_report } from "../airtable-field-map.js";

export const ADP_HISTORY_SCHEMA_VERSION = "adp_airtable_history_schema_v1_final";
/** Default posture OFF. Runtime enable only via ADP_HISTORY_WRITES_ENABLED=true for controlled runs. */
export function isAdpHistoryWritesEnabled() {
  const v = process.env.ADP_HISTORY_WRITES_ENABLED;
  return v === "true" || v === "1";
}
/** @deprecated Prefer isAdpHistoryWritesEnabled() — kept false as static posture for readiness docs/tests. */
export const ADP_HISTORY_WRITES_ENABLED = false;

export const HISTORY_ENV = Object.freeze({
  /** Must remain unset/0 for production until founder activation */
  ADP_HISTORY_AIRTABLE_WRITE_APPLY: "ADP_HISTORY_AIRTABLE_WRITE_APPLY",
  ADP_HISTORY_AIRTABLE_BASE_ID: "ADP_HISTORY_AIRTABLE_BASE_ID",
  ADP_HISTORY_SANDBOX_BASE_ID: "ADP_HISTORY_SANDBOX_BASE_ID",
  ADP_AIRTABLE_READ_LIVE: "ADP_AIRTABLE_READ_LIVE",
});

/** Reuse existing Live table — do NOT use it as append-only history. */
export const EXISTING_LIVE_PUBLISH = Object.freeze({
  table: ADP_PUBLISHED_REPORTS_TABLE,
  fieldMap: map_adp_published_report,
  role: "CURRENT_LIVE_OVERLAY_ONLY",
  longitudinalSafe: false,
  note: "Upsert Live overwrites current payload — not historical SoT",
});

/**
 * Consolidated production history tables (9).
 * Trend queries derive from Monitoring Periods + Period Metrics (no separate Trend table).
 * Prompt Ledger added for METRIC_TO_PROMPT_TRACEABILITY (Prompt Integrity V1).
 */
export const HISTORY_TABLES = Object.freeze({
  MONITORING_PERIODS: "ADP Monitoring Periods",
  REPORT_SNAPSHOTS: "ADP Report Snapshots",
  PERIOD_METRICS: "ADP Period Metrics",
  TERRITORY_METRICS: "ADP Territory Metrics",
  PROVIDER_METRICS: "ADP Provider Metrics",
  COMPETITIVE_RANKINGS: "ADP Competitive Rankings",
  EVIDENCE_INDEX: "ADP Evidence Index",
  REPORT_CORRECTIONS: "ADP Report Corrections",
  PROMPT_LEDGER: "ADP Observation Prompt Ledger",
});

export const PERSISTENCE_STATES = Object.freeze({
  PENDING: "PENDING",
  WRITING: "WRITING",
  VERIFYING: "VERIFYING",
  COMPLETE: "COMPLETE",
  FAILED: "FAILED",
});

export const IDEMPOTENCY_KEYS = Object.freeze({
  monitoringPeriod: "propertyId+periodId",
  reportSnapshot: "snapshotId",
  periodMetrics: "propertyId+periodId+historicalVersion",
  territoryMetric: "propertyId+periodId+territoryId+historicalVersion",
  providerMetric: "propertyId+periodId+providerId+historicalVersion",
  competitiveRank: "propertyId+periodId+scope+canonicalEntityId+historicalVersion",
  evidenceIndex: "propertyId+periodId+observationId",
  reportCorrection: "correctionId",
  promptLedger: "propertyId+periodId+observationId",
});

/** Field specs for ensure-script proposal (not applied in this task). */
export const HISTORY_FIELD_SPECS = Object.freeze({
  [HISTORY_TABLES.MONITORING_PERIODS]: [
    { name: "Period Key", type: "singleLineText", primary: true }, // propertyId|periodId
    { name: "Property ID", type: "singleLineText" },
    { name: "Period ID", type: "singleLineText" },
    { name: "Snapshot ID", type: "singleLineText" },
    { name: "Monitoring Date", type: "date" },
    { name: "Certification Timestamp", type: "dateTime" },
    { name: "Publication Timestamp", type: "dateTime" },
    { name: "Certification Status", type: "singleLineText" },
    { name: "Report Schema Version", type: "singleLineText" },
    { name: "Persistence State", type: "singleSelect", choices: Object.values(PERSISTENCE_STATES) },
    { name: "Content Hash", type: "singleLineText" },
    { name: "Envelope Hash", type: "singleLineText" },
    { name: "Historical Version", type: "number" },
    { name: "Is Current Historical Version", type: "checkbox" },
    { name: "Corrects Period Key", type: "singleLineText" },
    { name: "Synthetic", type: "checkbox" },
  ],
  [HISTORY_TABLES.REPORT_SNAPSHOTS]: [
    { name: "Snapshot ID", type: "singleLineText", primary: true },
    { name: "Property ID", type: "singleLineText" },
    { name: "Period ID", type: "singleLineText" },
    { name: "Monitoring Date", type: "date" },
    { name: "Schema Version", type: "singleLineText" },
    { name: "Content Hash", type: "singleLineText" },
    { name: "Envelope Hash", type: "singleLineText" },
    { name: "Publication Commit", type: "singleLineText" },
    { name: "Renderer Version", type: "singleLineText" },
    { name: "Measurement Contract Version", type: "singleLineText" },
    { name: "Parser Version", type: "singleLineText" },
    { name: "Entity Resolver Version", type: "singleLineText" },
    { name: "Ranking Version", type: "singleLineText" },
    { name: "Evidence Version", type: "singleLineText" },
    { name: "Assurance Version", type: "singleLineText" },
    { name: "Certification Status", type: "singleLineText" },
    { name: "Correction Version", type: "number" },
    { name: "Is Current Historical Version", type: "checkbox" },
    { name: "Snapshot Store Ref", type: "singleLineText" }, // filesystem/object path
    { name: "Synthetic", type: "checkbox" },
  ],
  [HISTORY_TABLES.PERIOD_METRICS]: [
    { name: "Metrics Key", type: "singleLineText", primary: true },
    { name: "Property ID", type: "singleLineText" },
    { name: "Period ID", type: "singleLineText" },
    { name: "Historical Version", type: "number" },
    { name: "Reality Coverage", type: "number" },
    { name: "Scenario Presence", type: "number" },
    { name: "Scenario Presence Numerator", type: "number" },
    { name: "Scenario Presence Denominator", type: "number" },
    { name: "Demand Capture", type: "number" },
    { name: "Consideration Rate", type: "number" },
    { name: "Consideration Numerator", type: "number" },
    { name: "Consideration Denominator", type: "number" },
    { name: "Number One Appearance Rate", type: "number" },
    { name: "Top Three Appearance Rate", type: "number" },
    { name: "Presence Index", type: "number" },
    { name: "CORE Benchmark", type: "number" },
    { name: "CORE Count", type: "number" },
    { name: "Certification Status", type: "singleLineText" },
    { name: "Disclosures JSON", type: "multilineText" },
  ],
  [HISTORY_TABLES.TERRITORY_METRICS]: [
    { name: "Territory Key", type: "singleLineText", primary: true },
    { name: "Property ID", type: "singleLineText" },
    { name: "Period ID", type: "singleLineText" },
    { name: "Territory ID", type: "singleLineText" },
    { name: "Territory Name", type: "singleLineText" },
    { name: "Numerator", type: "number" },
    { name: "Denominator", type: "number" },
    { name: "AI Presence Pct", type: "number" },
    { name: "Benchmark", type: "number" },
    { name: "Presence Index", type: "number" },
    { name: "Rank", type: "number" },
    { name: "Missing Evidence Count", type: "number" },
    { name: "Positive Evidence Eligible Count", type: "number" },
    { name: "Historical Version", type: "number" },
  ],
  [HISTORY_TABLES.PROVIDER_METRICS]: [
    { name: "Provider Key", type: "singleLineText", primary: true },
    { name: "Property ID", type: "singleLineText" },
    { name: "Period ID", type: "singleLineText" },
    { name: "Provider ID", type: "singleLineText" },
    { name: "Provider Name", type: "singleLineText" },
    { name: "Numerator", type: "number" },
    { name: "Denominator", type: "number" },
    { name: "Presence Pct", type: "number" },
    { name: "Missing Count", type: "number" },
    { name: "Completeness State", type: "singleLineText" },
    { name: "Recovery State", type: "singleLineText" },
    { name: "Historical Version", type: "number" },
  ],
  [HISTORY_TABLES.COMPETITIVE_RANKINGS]: [
    { name: "Rank Key", type: "singleLineText", primary: true },
    { name: "Property ID", type: "singleLineText" },
    { name: "Period ID", type: "singleLineText" },
    { name: "Scope", type: "singleLineText" },
    { name: "Ranking Universe Type", type: "singleSelect", choices: ["MARKET_CORE", "BRAND_PORTFOLIO"] },
    { name: "Portfolio Lens", type: "singleLineText" },
    { name: "Portfolio Type", type: "singleLineText" },
    { name: "Peer Set Version", type: "singleLineText" },
    { name: "Canonical Entity ID", type: "singleLineText" },
    { name: "Display Name At That Time", type: "singleLineText" },
    { name: "Numerator", type: "number" },
    { name: "Denominator", type: "number" },
    { name: "AI Presence Pct", type: "number" },
    { name: "Rank", type: "number" },
    { name: "Tie State", type: "singleLineText" },
    { name: "Historical Version", type: "number" },
  ],
  [HISTORY_TABLES.EVIDENCE_INDEX]: [
    { name: "Evidence Key", type: "singleLineText", primary: true },
    { name: "Property ID", type: "singleLineText" },
    { name: "Period ID", type: "singleLineText" },
    { name: "Observation ID", type: "singleLineText" },
    { name: "Evidence Lane", type: "singleLineText" }, // positive|missing|displacement
    { name: "Provider", type: "singleLineText" },
    { name: "Territory", type: "singleLineText" },
    { name: "Subject Present", type: "checkbox" },
    { name: "Subject Rank", type: "number" },
    { name: "Response Store Ref", type: "singleLineText" }, // filesystem — NOT full text in Airtable
    { name: "Response Length", type: "number" },
    { name: "Response Content Hash", type: "singleLineText" },
    { name: "Mention Spans JSON", type: "multilineText" },
    { name: "Competitors JSON", type: "multilineText" },
    { name: "Citations JSON", type: "multilineText" },
  ],
  [HISTORY_TABLES.REPORT_CORRECTIONS]: [
    { name: "Correction ID", type: "singleLineText", primary: true },
    { name: "Property ID", type: "singleLineText" },
    { name: "Period ID", type: "singleLineText" },
    { name: "Original Snapshot ID", type: "singleLineText" },
    { name: "Corrected Snapshot ID", type: "singleLineText" },
    { name: "Reason", type: "multilineText" },
    { name: "Changed Fields JSON", type: "multilineText" },
    { name: "Authorized By", type: "singleLineText" },
    { name: "Corrected At", type: "dateTime" },
    { name: "Version Number", type: "number" },
  ],
  [HISTORY_TABLES.PROMPT_LEDGER]: [
    { name: "Prompt Ledger Key", type: "singleLineText", primary: true }, // propertyId|periodId|observationId
    { name: "Observation ID", type: "singleLineText" },
    { name: "Property ID", type: "singleLineText" },
    { name: "Period ID", type: "singleLineText" },
    { name: "Scenario ID", type: "singleLineText" },
    { name: "Provider", type: "singleLineText" },
    { name: "Territory", type: "singleLineText" },
    { name: "Owner Intent", type: "singleLineText" },
    { name: "Scenario Class", type: "singleSelect", choices: ["NEUTRAL_DEMAND", "PROPERTY_SPECIFIC", "BRAND_SPECIFIC", "COMPETITOR_SPECIFIC", "OTHER_GOVERNED_SPECIAL"] },
    { name: "Prompt Hash", type: "singleLineText" },
    { name: "Exact Prompt Preview", type: "multilineText" }, // ≤500 chars
    { name: "Template ID", type: "singleLineText" },
    { name: "Template Version", type: "singleLineText" },
    { name: "Generator Version", type: "singleLineText" },
    { name: "Measurement Eligibility", type: "checkbox" },
    { name: "Integrity Classification", type: "singleLineText" },
    { name: "Request Timestamp", type: "dateTime" },
    { name: "Response ID", type: "singleLineText" },
    { name: "Immutable Prompt Path", type: "singleLineText" },
    { name: "Immutable Observation Ref", type: "singleLineText" },
  ],
});

export const SNAPSHOT_STORAGE_STRATEGY = Object.freeze({
  airtable: "metadata + Snapshot Store Ref + hashes + version stamps",
  filesystemOrObject: "full ADP_REPORT_SNAPSHOT_V1 JSON + verbatim evidence responses",
  rationale:
    "Airtable multiline ~100k char limit; weekly evidence corpora + full ranking payloads exceed safe AT blob storage at scale. Index in Airtable; reproduce from immutable file/object layer.",
  livePublishedReportsTable: "remains CURRENT overlay only — not historical append log",
});

export const SCALE_ASSUMPTIONS = Object.freeze({
  periodsPerPropertyPerYear: 52,
  territoriesPerPeriod: 8,
  providersPerPeriod: 4,
  rankingRowsPerPeriod: 9 * 25, // scopes × avg entities in full universe
  evidenceIndexRowsPerPeriod: 40, // index only; text on disk
  promptLedgerRowsPerPeriod: 63 * 4, // scenarios × providers — index only; exact prompt on disk
  snapshotMetaPerPeriod: 1,
  periodMetricsPerPeriod: 1,
  monitoringPeriodPerPeriod: 1,
  correctionsPerYearSparse: 2,
});

export function estimateAnnualRecords(propertyCount) {
  const a = SCALE_ASSUMPTIONS;
  const perPeriod =
    a.monitoringPeriodPerPeriod +
    a.snapshotMetaPerPeriod +
    a.periodMetricsPerPeriod +
    a.territoriesPerPeriod +
    a.providersPerPeriod +
    a.rankingRowsPerPeriod +
    a.evidenceIndexRowsPerPeriod +
    a.promptLedgerRowsPerPeriod;
  const perPropertyYear = perPeriod * a.periodsPerPropertyPerYear + a.correctionsPerYearSparse;
  return {
    propertyCount,
    recordsPerPeriodApprox: perPeriod,
    recordsPerPropertyYearApprox: perPropertyYear,
    recordsTotalYearApprox: perPropertyYear * propertyCount,
    evidenceTextOnDiskNotAirtable: true,
    exactPromptTextOnDiskNotAirtable: true,
    fullSnapshotJsonOnDiskNotAirtable: true,
  };
}

export const ACTIVATION_REQUIREMENTS = Object.freeze([
  "founder_approval_for_history_schema_create",
  "docs/ai-demand-positioning-airtable-fields.md updated with history tables",
  "ensure script dry-run then ADP_SCHEMA_APPLY for history tables only",
  "ADP_HISTORY_AIRTABLE_WRITE_APPLY explicitly true",
  "sandbox or designated history base preferred over Live overlay table",
  "dry-run write manifest PASS",
  "idempotency PASS",
  "no-silent-overwrite PASS",
  "synthetic round-trip PASS",
  "REAL_SECOND_PERIOD_HISTORY_PERSISTENCE_READY PASS",
  "ADP_AIRTABLE_READ_LIVE remains off for historical SoT (filesystem remains report SoT)",
]);
