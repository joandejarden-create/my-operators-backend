/**
 * ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1
 *
 * Versioned permanent contract: every certified ADP Full E2E run must satisfy
 * these Airtable (+ FS blob) persistence rules before research-cycle finalization.
 *
 * Authority split:
 *   - Durable published registry / FS snapshots = customer-visible current SoT
 *   - Airtable history tables = structured index + reconstructable metadata
 *   - Published Reports = Live overlay only (NOT longitudinal SoT)
 *
 * Methodology: V1.1.1 Independent Unprompted — response-text mention adjudication
 * is authoritative; historical runtime `mentioned` flags are NOT.
 */

import { HISTORY_TABLES, IDEMPOTENCY_KEYS, PERSISTENCE_STATES } from "./airtable-history-schema-final-v1.js";
import { ADP_PUBLISHED_REPORTS_TABLE } from "../airtable-field-map.js";
import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "../certification/adp-unprompted-independent-discovery-standard-v1.js";

export const ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1 = "ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1";
export const ADP_E2E_PERSISTENCE_TABLE_CONTRACT_VERSION = "1.0.0";

/** Distinct from publication — never conflate. */
export const PERSISTENCE_LIFECYCLE = Object.freeze({
  NOT_ATTEMPTED: "NOT_ATTEMPTED",
  PLANNED_DRY_RUN: "PLANNED_DRY_RUN",
  AWAITING_WRITE_APPROVAL: "AWAITING_WRITE_APPROVAL",
  WRITING: "WRITING",
  VERIFYING: "VERIFYING",
  COMPLETE: "COMPLETE",
  FAILED: "FAILED",
  UNAVAILABLE: "UNAVAILABLE",
});

export const PUBLICATION_LIFECYCLE = Object.freeze({
  NOT_PUBLISHED: "NOT_PUBLISHED",
  STAGED: "STAGED",
  FS_PUBLISHED: "FS_PUBLISHED",
  LIVE_OVERLAY_SYNCED: "LIVE_OVERLAY_SYNCED",
  LIVE_OVERLAY_STALE: "LIVE_OVERLAY_STALE",
  LIVE_OVERLAY_MISSING: "LIVE_OVERLAY_MISSING",
  FAILED: "FAILED",
});

export const CURRENT_VERSION_RULE = Object.freeze({
  id: "ADP_CURRENT_CERTIFIED_MEASUREMENT_V1",
  rule: "Exactly one designated current certified measurement per hotel per reporting scope (Core Independent Unprompted V1.1.1).",
  designationField: "Is Current Historical Version",
  designationTables: [
    HISTORY_TABLES.MONITORING_PERIODS,
    HISTORY_TABLES.REPORT_SNAPSHOTS,
  ],
  currentMustMatch: [
    "Property ID",
    "Period ID",
    "methodology fingerprint / Certification Status lineage",
  ],
  historicalQueryable: true,
  historicalMayOverrideCurrent: false,
  olderResultMayReplaceCurrent: false,
  demotePriorsOnNewCurrent: true,
  airtableUnavailableImpliesSuccess: false,
  airtableFailureMustNotRollbackFsPublication: true,
  runtimeMentionedFlagsAuthoritative: false,
  responseTextAdjudicationAuthoritative: true,
  methodologyId: CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
});

/**
 * Per-table contract rows for the 10 named ADP tables (+ notes).
 */
export const TABLE_PERSISTENCE_CONTRACT = Object.freeze([
  {
    table: ADP_PUBLISHED_REPORTS_TABLE,
    role: "LIVE_OVERLAY",
    requiredForHistoryFinalization: false,
    requiredForCustomerParity: true,
    requiredFields: [
      "ADP Property ID",
      "Period ID",
      "Publish Status",
      "Published At",
      "Demand Capture Rate",
      "Payload Store Ref",
    ],
    sources: {
      "ADP Property ID": "durable registry / publish package propertyId",
      "Period ID": "certified measurement periodId",
      "Publish Status": "publication transaction (Live)",
      "Published At": "publication timestamp (NOT measurement date)",
      "Demand Capture Rate": "certified KPIs (Demand Capture; do not confuse with Consideration)",
      "Payload Store Ref": "FS published snapshot ref",
    },
    writtenWhen: "Separate PUBLICATION_HISTORY authorization — NOT Full E2E S9B",
    idempotencyKey: "ADP Property ID (Live upsert)",
    historicalVersioning: "NONE — overlay overwrite; history lives in ADP Monitoring Periods+",
    certificationRequirements: "Only certified packages may go Live",
    currentRecordDesignation: "Publish Status=Live (one Live per property)",
    validationFailure: "Refuse Live upsert; FS publication remains valid if already sealed",
  },
  {
    table: HISTORY_TABLES.MONITORING_PERIODS,
    role: "MEASUREMENT_HISTORY",
    requiredForHistoryFinalization: true,
    requiredFields: [
      "Period Key",
      "Property ID",
      "Period ID",
      "Monitoring Date",
      "Certification Status",
      "Persistence State",
      "Historical Version",
      "Is Current Historical Version",
      "Content Hash",
    ],
    sources: {
      "Period Key": "propertyId|periodId|vN",
      "Property ID": "orchestrator identity",
      "Period ID": "monitoring period id",
      "Monitoring Date": "period.monitoringDate / certifiedAt date",
      "Certification Status": "cert gate output",
      "Persistence State": "persist adapter state machine",
      "Is Current Historical Version": "CURRENT_VERSION_RULE + demotion",
      "Content Hash": "report snapshot contentHash",
    },
    writtenWhen: "S9B after certification, under MEASUREMENT_HISTORY auth + dual env gates",
    idempotencyKey: IDEMPOTENCY_KEYS.monitoringPeriod + "+historicalVersion",
    historicalVersioning: "Append new Period Key version; demote prior Is Current",
    certificationRequirements: "CERTIFIED / Independent Unprompted V1.1.1",
    currentRecordDesignation: "Is Current Historical Version=true for exactly one Core scope period",
    validationFailure: "Persistence State=FAILED; refuse REPORT_HISTORY_PERSISTENCE_COMPLETE",
  },
  {
    table: HISTORY_TABLES.REPORT_SNAPSHOTS,
    role: "MEASUREMENT_HISTORY",
    requiredForHistoryFinalization: true,
    requiredFields: [
      "Snapshot ID",
      "Property ID",
      "Period ID",
      "Content Hash",
      "Envelope Hash",
      "Certification Status",
      "Is Current Historical Version",
      "Snapshot Store Ref",
    ],
    sources: {
      "Snapshot ID": "reportSnapshot.snapshotId",
      "Snapshot Store Ref": "FS snapshot-blobs path (full JSON off-Airtable)",
      versions: "renderer/parser/ranking/evidence/assurance from snapshot.versions",
    },
    writtenWhen: "Same S9B persist as Monitoring Periods",
    idempotencyKey: IDEMPOTENCY_KEYS.reportSnapshot,
    historicalVersioning: "New Snapshot ID per correction; demote prior current",
    certificationRequirements: "Snapshot must pass auditReportSnapshotCompleteness",
    currentRecordDesignation: "Is Current Historical Version",
    validationFailure: "Refuse COMPLETE; keep blobs if already written",
  },
  {
    table: HISTORY_TABLES.PERIOD_METRICS,
    role: "MEASUREMENT_HISTORY",
    requiredForHistoryFinalization: true,
    requiredFields: [
      "Metrics Key",
      "Property ID",
      "Period ID",
      "Consideration Rate",
      "Consideration Numerator",
      "Consideration Denominator",
      "Certification Status",
    ],
    sources: {
      "Consideration Rate": "customerPayload.kpis.considerationRate (V1.1.1 response-text adjudication)",
      numerators: "kpis.*Numerator",
      denominators: "kpis.*Denominator",
    },
    writtenWhen: "S9B persist",
    idempotencyKey: IDEMPOTENCY_KEYS.periodMetrics,
    historicalVersioning: "Versioned Metrics Key; never overwrite prior version rows",
    certificationRequirements: "Metrics must match certified package within tolerance",
    currentRecordDesignation: "Joined via Period ID to current Monitoring Period",
    validationFailure: "Incomplete metrics → refuse certification finalization",
  },
  {
    table: HISTORY_TABLES.TERRITORY_METRICS,
    role: "MEASUREMENT_HISTORY",
    requiredForHistoryFinalization: true,
    requiredFields: [
      "Territory Key",
      "Property ID",
      "Period ID",
      "Territory ID",
      "Numerator",
      "Denominator",
    ],
    sources: { rows: "customerPayload.territories[]" },
    writtenWhen: "S9B persist",
    idempotencyKey: IDEMPOTENCY_KEYS.territoryMetric,
    historicalVersioning: "Append-only per territory/version",
    certificationRequirements: "Territory set from certified payload",
    currentRecordDesignation: "Via Period ID",
    validationFailure: "Missing territories when payload has territories → PARTIAL",
  },
  {
    table: HISTORY_TABLES.PROVIDER_METRICS,
    role: "MEASUREMENT_HISTORY",
    requiredForHistoryFinalization: true,
    requiredFields: [
      "Provider Key",
      "Property ID",
      "Period ID",
      "Provider ID",
      "Numerator",
      "Denominator",
    ],
    sources: { rows: "customerPayload.providers[]" },
    writtenWhen: "S9B persist",
    idempotencyKey: IDEMPOTENCY_KEYS.providerMetric,
    historicalVersioning: "Append-only",
    certificationRequirements: "Provider presence from certified payload",
    currentRecordDesignation: "Via Period ID",
    validationFailure: "Missing providers → PARTIAL",
  },
  {
    table: HISTORY_TABLES.COMPETITIVE_RANKINGS,
    role: "MEASUREMENT_HISTORY",
    requiredForHistoryFinalization: true,
    requiredFields: [
      "Rank Key",
      "Property ID",
      "Period ID",
      "Scope",
      "Canonical Entity ID",
      "Rank",
      "Numerator",
      "Denominator",
      "Ranking Universe Type",
    ],
    sources: {
      entities: "rankingPack.byScope or payload.competitive.overview",
      "Ranking Universe Type": "MARKET_CORE for Core Independent Unprompted",
    },
    writtenWhen: "S9B persist",
    idempotencyKey: IDEMPOTENCY_KEYS.competitiveRank,
    historicalVersioning: "Append-only",
    certificationRequirements: "MARKET_CORE universe consistency",
    currentRecordDesignation: "Via Period ID",
    validationFailure: "Universe inconsistency → refuse COMPLETE",
  },
  {
    table: HISTORY_TABLES.EVIDENCE_INDEX,
    role: "MEASUREMENT_HISTORY",
    requiredForHistoryFinalization: true,
    requiredFields: [
      "Evidence Key",
      "Property ID",
      "Period ID",
      "Observation ID",
      "Evidence Lane",
      "Response Store Ref",
      "Response Content Hash",
      "Subject Present",
    ],
    sources: {
      lanes: "payload.evidence.{positive,missing,displacement}",
      "Subject Present": "response-text adjudication — NOT runtime mentioned flag",
      "Response Store Ref": "FS evidence-blobs (verbatim off-Airtable)",
    },
    writtenWhen: "S9B persist (requires evidence cards or period observations)",
    idempotencyKey: IDEMPOTENCY_KEYS.evidenceIndex,
    historicalVersioning: "Append-only; corrections add new keys",
    certificationRequirements: "Missing evidence for certified mentions → refuse cert",
    currentRecordDesignation: "Via Period ID",
    validationFailure: "Empty evidence for certified period → INCOMPLETE",
  },
  {
    table: HISTORY_TABLES.PROMPT_LEDGER,
    role: "MEASUREMENT_HISTORY",
    requiredForHistoryFinalization: true,
    requiredFields: [
      "Prompt Ledger Key",
      "Property ID",
      "Period ID",
      "Observation ID",
      "Provider",
      "Prompt Text",
      "Eligibility",
    ],
    sources: {
      rows: "period.observations via buildCorePromptLedgerRecordsFromPeriod",
      note: "S9B must pass `period` — UI evidence cards alone are insufficient",
    },
    writtenWhen: "S9B persist when period observations provided",
    idempotencyKey: IDEMPOTENCY_KEYS.promptLedger,
    historicalVersioning: "Append-only",
    certificationRequirements: "Eligible prompts must match measurement denominator inputs",
    currentRecordDesignation: "Via Period ID",
    validationFailure: "NO_PRIMARY_FIELD_MAPPING if apply map omits Prompt Ledger Key",
  },
  {
    table: HISTORY_TABLES.REPORT_CORRECTIONS,
    role: "MEASUREMENT_HISTORY_AUDIT",
    requiredForHistoryFinalization: false,
    requiredWhen: "correctionMode=true",
    requiredFields: [
      "Correction ID",
      "Property ID",
      "Period ID",
      "Original Snapshot ID",
      "Corrected Snapshot ID",
      "Reason",
      "Version Number",
    ],
    sources: { correction: "correctionMode branch of buildHistoricalWriteManifest" },
    writtenWhen: "Explicit correctionMode persist — never destroy prior rows",
    idempotencyKey: IDEMPOTENCY_KEYS.reportCorrection,
    historicalVersioning: "Append-only supersession lineage",
    certificationRequirements: "Corrected package must re-certify V1.1.1",
    currentRecordDesignation: "Points Monitoring/Snapshot current flags to corrected version",
    validationFailure: "Refuse correction without Original Snapshot ID",
  },
]);

export const FULL_E2E_MUST_PRESERVE = Object.freeze([
  "original research evidence (FS observation archive + Evidence Index refs)",
  "all required measurement inputs and outputs (Period/Territory/Provider/Competitive)",
  "stable hotel and period identities",
  "complete idempotent records",
  "historical measurements (no silent overwrite)",
  "certification and methodology metadata",
  "metrics/evidence linked to exact period",
  "corrections without destroying history",
  "prevent older result from replacing current status",
  "verify persistence completeness before research-cycle finalization",
]);

export function requiredHistoryTablesForFinalization() {
  return TABLE_PERSISTENCE_CONTRACT.filter((t) => t.requiredForHistoryFinalization).map(
    (t) => t.table
  );
}

export function assertPersistenceDistinctFromPublication({
  persistenceState,
  publicationState,
  claimedComplete,
} = {}) {
  const defects = [];
  if (claimedComplete && persistenceState === PERSISTENCE_LIFECYCLE.UNAVAILABLE) {
    defects.push({
      code: "PERSISTENCE_CLAIMED_COMPLETE_WHILE_UNAVAILABLE",
      detail: "Airtable unavailable — must not claim persistence succeeded",
    });
  }
  if (
    publicationState === PUBLICATION_LIFECYCLE.FS_PUBLISHED &&
    persistenceState === PERSISTENCE_LIFECYCLE.FAILED
  ) {
    // Allowed: FS publication must survive Airtable failure
    defects.push({
      code: "PERSISTENCE_FAILED_PUBLICATION_INTACT",
      severity: "INFO",
      detail: "Airtable failure must not roll back valid immutable FS publication",
    });
  }
  if (
    claimedComplete &&
    persistenceState !== PERSISTENCE_LIFECYCLE.COMPLETE &&
    persistenceState !== PERSISTENCE_STATES.COMPLETE
  ) {
    defects.push({
      code: "PERSISTENCE_CLAIMED_COMPLETE_INVALID_STATE",
      detail: String(persistenceState),
    });
  }
  return {
    ok: defects.every((d) => d.severity === "INFO"),
    defects,
    contract: ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1,
    version: ADP_E2E_PERSISTENCE_TABLE_CONTRACT_VERSION,
  };
}
