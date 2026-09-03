/**
 * Future Airtable historical schema for Existing Hotel ADP — DESIGN ONLY.
 * Production writes are NOT enabled. Controlled validation uses filesystem fixtures.
 *
 * AIRTABLE = structured historical / query layer
 * IMMUTABLE SNAPSHOT = exact report reproduction (JSON + hash)
 */

export const ADP_AIRTABLE_HISTORICAL_SCHEMA_VERSION = "adp_airtable_historical_schema_v1";

export const AIRTABLE_WRITE_STATUS = Object.freeze({
  PRODUCTION_WRITES: "DISABLED",
  ACTIVATION_REQUIRES: [
    "founder_approval",
    "schema_created_in_platform_or_product_base",
    "field_docs_in_docs/*-airtable-fields.md",
    "dry_run_pass",
    "REPORT_HISTORY_PERSISTENCE_COMPLETE gate green",
    "synthetic_leakage_zero",
    "second_real_comparable_certified_period_optional_for_schema_only",
  ],
});

/**
 * Logical tables (names are proposals — must be confirmed against schema docs before create).
 */
export const PROPOSED_TABLES = Object.freeze([
  {
    table: "ADP Monitoring Periods",
    purpose: "One row per certified monitoring period (append-only)",
    keyFields: [
      "propertyId",
      "periodId",
      "monitoringDate",
      "certificationStatus",
      "disclosureState",
      "reportVersion",
      "generatedAt",
      "certifiedAt",
      "publishedAt",
      "snapshotId",
      "contentHash",
      "isCurrentHistoricalVersion",
      "correctsPeriodId",
      "synthetic",
    ],
  },
  {
    table: "ADP Report Snapshots",
    purpose: "Immutable serialized customer payload + metadata",
    keyFields: [
      "snapshotId",
      "periodId",
      "propertyId",
      "schemaVersion",
      "contentHash",
      "envelopeHash",
      "publicationCommit",
      "payloadJson",
      "historicalVersion",
      "correctsSnapshotId",
      "correctionReason",
      "rendererVersion",
      "measurementContractVersion",
    ],
  },
  {
    table: "ADP Period Metrics",
    purpose: "Normalized KPI / executive / actions for querying",
    keyFields: [
      "periodId",
      "snapshotId",
      "considerationRate",
      "scenarioPresence",
      "demandCapture",
      "realityCoverage",
      "presenceIndex",
      "coreBenchmark",
      "executiveNarrative",
    ],
  },
  {
    table: "ADP Territory Metrics",
    purpose: "Per Demand Territory metrics",
    keyFields: ["periodId", "territoryId", "numerator", "denominator", "aiPresence", "coreBenchmark", "presenceIndex"],
  },
  {
    table: "ADP Provider Metrics",
    purpose: "Per provider presence",
    keyFields: ["periodId", "providerId", "numerator", "denominator", "presenceRate", "completeness"],
  },
  {
    table: "ADP Competitive Rankings",
    purpose: "FULL competitive universe per scope (not Top-10 only)",
    keyFields: [
      "periodId",
      "scope",
      "entityId",
      "displayName",
      "rank",
      "numerator",
      "denominator",
      "presencePct",
      "tieState",
      "entityResolverVersion",
      "rankingVersion",
    ],
  },
  {
    table: "ADP Evidence Observations",
    purpose: "Positive / Missing / Displacement evidence with verbatim LLM response",
    keyFields: [
      "observationId",
      "periodId",
      "evidenceLane",
      "provider",
      "territory",
      "subjectStatus",
      "rank",
      "aiResponse",
      "subjectMentionSpansJson",
      "competitorsJson",
      "citationsJson",
    ],
  },
  {
    table: "ADP Trend History",
    purpose: "Certified metric points known at report time",
    keyFields: ["periodId", "snapshotId", "pointPeriodId", "calendarDate", "metric", "value"],
  },
  {
    table: "ADP Report Corrections",
    purpose: "Correction provenance without erasing originals",
    keyFields: [
      "correctionId",
      "originalSnapshotId",
      "correctedSnapshotId",
      "periodId",
      "reason",
      "changedFields",
      "authorizedBy",
      "assuranceResult",
      "correctedAt",
    ],
  },
]);

/**
 * Future weekly write contract (not executed).
 */
export const WEEKLY_HISTORY_WRITE_PIPELINE = Object.freeze([
  "LLM_MONITORING",
  "GOVERNED_INTERPRETATION",
  "PROVIDER_RECOVERY",
  "ASSURANCE",
  "CERTIFICATION",
  "FINAL_REPORT_PAYLOAD",
  "FREEZE_IMMUTABLE_SNAPSHOT",
  "WRITE_STRUCTURED_HISTORY",
  "VERIFY_HISTORICAL_WRITE",
  "VERIFY_SNAPSHOT_HASH",
  "PUBLICATION",
]);

export const WRITE_FAILURE_POLICY = Object.freeze({
  rule: "A report must NOT publish successfully while silently failing to preserve its history.",
  gate: "REPORT_HISTORY_PERSISTENCE_COMPLETE",
  onSnapshotPersistFail: "BLOCK_PUBLICATION",
  onStructuredWriteFail: "BLOCK_PUBLICATION",
  onHashMismatch: "BLOCK_PUBLICATION",
  neverAutoPublishBecauseSevenDaysPassed: true,
});
