/**
 * ADP_HISTORY_FIELD_SEMANTICS_CONTRACT_V1
 *
 * Business/measurement-derived field requirements for ADP Airtable history tables.
 * Do NOT derive REQUIRED from current serializer omissions — serializer gaps are defects.
 *
 * Measurement families:
 *   CORE  — independent-unprompted market competitive ranking (MARKET_CORE)
 *   BPP   — brand portfolio positioning (BRAND_PORTFOLIO + peer-set identity)
 */

export const ADP_HISTORY_FIELD_SEMANTICS_CONTRACT_V1 =
  "ADP_HISTORY_FIELD_SEMANTICS_CONTRACT_V1";

export const FIELD_REQUIREMENT_CLASS = Object.freeze({
  REQUIRED_ALWAYS: "REQUIRED_ALWAYS",
  REQUIRED_WHEN_APPLICABLE: "REQUIRED_WHEN_APPLICABLE",
  OPTIONAL: "OPTIONAL",
  COMPUTED: "COMPUTED",
  LEGACY: "LEGACY",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

export const MEASUREMENT_FAMILY = Object.freeze({
  CORE: "CORE",
  BPP: "BPP",
  UNKNOWN: "UNKNOWN",
});

export const RANKING_UNIVERSE_TYPE = Object.freeze({
  MARKET_CORE: "MARKET_CORE",
  BRAND_PORTFOLIO: "BRAND_PORTFOLIO",
});

/**
 * Hash field semantics — do not substitute one for another.
 */
export const HASH_FIELD_SEMANTICS = Object.freeze({
  CONTENT_HASH: {
    airtableFields: ["Content Hash"],
    meaning: "ADP_REPORT_SNAPSHOT_V1 contentHash = sha256(stableStringify(customerPayload))",
    algorithmModule: "lib/ai-demand-positioning/longitudinal/report-snapshot-v1.js#sha256OfPayload",
    notEqualTo: ["staged artifactHash", "_adpArtifactHash", "snapshotHash", "file SHA of staged JSON"],
  },
  ENVELOPE_HASH: {
    airtableFields: ["Envelope Hash"],
    meaning:
      "ADP_REPORT_SNAPSHOT_V1 envelopeHash = sha256(stableStringify({...snapshotBody, contentHash, envelopeHash:null}))",
    algorithmModule: "lib/ai-demand-positioning/longitudinal/report-snapshot-v1.js#buildReportSnapshotV1",
    requires: "Full snapshot envelope fields (versions, disclosures, schema, identity)",
    notEqualTo: ["Content Hash alone", "staged artifactHash", "KPI-subset hash"],
  },
  STAGED_ARTIFACT_HASH: {
    proposedFieldName: "Staged Artifact Hash",
    meaning: "SHA-256 of the immutable staged report artifact file / _adpArtifactHash",
    status: "PROPOSED_SEPARATE_FIELD — do not write into Content Hash or Envelope Hash",
  },
  BACKFILL_VERIFICATION_HASH: {
    proposedFieldName: "Backfill Verification Hash",
    meaning: "SHA-256 of staged report file used only to prove backfill source identity",
    status: "PROPOSED_SEPARATE_FIELD — never substitute for original immutable artifact hashes",
  },
  ATOMIC_SNAPSHOT_HASH: {
    sourcePath: "payload._adpSnapshotMeta.assessment.snapshotHash",
    meaning: "Atomic report snapshot contract assessment hash — distinct from Content/Envelope",
    status: "INTERNAL_ASSESSMENT — not the Airtable Content Hash field",
  },
});

/**
 * Per-table field semantics. `applicability` lists measurement families where the class applies.
 * When family is CORE, BPP-only fields are NOT_APPLICABLE (and vice versa).
 */
export const HISTORY_FIELD_SEMANTICS = Object.freeze({
  "ADP Monitoring Periods": {
    "Period Key": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Idempotent period identity", source: "propertyId|periodId|version", pipeline: "buildHistoricalWriteManifest" },
    "Property ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Hotel identity", source: "propertyId", pipeline: "buildHistoricalWriteManifest" },
    "Period ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Monitoring period identity", source: "periodId", pipeline: "buildHistoricalWriteManifest" },
    "Snapshot ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Link to report snapshot", source: "reportSnapshot.snapshotId", pipeline: "buildHistoricalWriteManifest" },
    "Monitoring Date": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Calendar monitoring date", source: "period.executionDate / monitoringDate", pipeline: "buildHistoricalWriteManifest" },
    "Certification Timestamp": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Authentic certification time", source: "period.completedAt|executedAt|executionDate", pipeline: "enrichHistoricalWriteManifestFromStaged" },
    "Publication Timestamp": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Customer publication time", source: "published manifest only", pipeline: "publication path", note: "Null until published — NOT staged" },
    "Certification Status": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Certification state", source: "certificationStatus", pipeline: "buildHistoricalWriteManifest" },
    "Report Schema Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Snapshot schema stamp", source: "snap.schema", pipeline: "buildHistoricalWriteManifest" },
    "Persistence State": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Write state machine", source: "PERSISTENCE_STATES", pipeline: "persist + enricher" },
    "Content Hash": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: HASH_FIELD_SEMANTICS.CONTENT_HASH.meaning, source: "sha256OfPayload(customerPayload)", pipeline: "buildReportSnapshotV1 / core writer" },
    "Envelope Hash": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: HASH_FIELD_SEMANTICS.ENVELOPE_HASH.meaning, source: "buildReportSnapshotV1 envelope", pipeline: "buildReportSnapshotV1" },
    "Historical Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Correction lineage version", source: "historicalVersion", pipeline: "buildHistoricalWriteManifest" },
    "Is Current Historical Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Current version flag", source: "boolean", pipeline: "buildHistoricalWriteManifest" },
    "Corrects Period Key": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Correction pointer", source: "correction only", pipeline: "correction mode" },
    Synthetic: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Synthetic vs production", source: "synthetic flag", pipeline: "buildHistoricalWriteManifest", note: "Empty checkbox = false" },
  },
  "ADP Report Snapshots": {
    "Snapshot ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Snapshot primary key", source: "snapshotId", pipeline: "buildHistoricalWriteManifest" },
    "Property ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Hotel identity", source: "propertyId", pipeline: "buildHistoricalWriteManifest" },
    "Period ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Period identity", source: "periodId", pipeline: "buildHistoricalWriteManifest" },
    "Monitoring Date": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Monitoring date", source: "monitoringDate", pipeline: "buildHistoricalWriteManifest" },
    "Schema Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "ADP_REPORT_SNAPSHOT_V1", source: "snap.schema", pipeline: "buildHistoricalWriteManifest" },
    "Content Hash": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: HASH_FIELD_SEMANTICS.CONTENT_HASH.meaning, source: "sha256OfPayload(customerPayload)", pipeline: "buildReportSnapshotV1" },
    "Envelope Hash": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: HASH_FIELD_SEMANTICS.ENVELOPE_HASH.meaning, source: "buildReportSnapshotV1", pipeline: "buildReportSnapshotV1" },
    "Publication Commit": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Real publication identity", source: "published commit only", pipeline: "publication path", note: "Never staged_* — null for staged/unpublished" },
    "Renderer Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Owner report renderer stamp", source: "payload.version / versions.rendererVersion", pipeline: "enricher" },
    "Measurement Contract Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Methodology / measurement contract", source: "methodologyId", pipeline: "enricher" },
    "Parser Version": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Parser stamp", source: "versions.parserVersion", pipeline: "buildReportSnapshotV1" },
    "Entity Resolver Version": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Entity resolver stamp", source: "versions.entityResolverVersion", pipeline: "buildReportSnapshotV1" },
    "Ranking Version": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Ranking ledger version", source: "versions.rankingVersion", pipeline: "buildReportSnapshotV1" },
    "Evidence Version": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Evidence contract version", source: "versions.evidenceVersion", pipeline: "buildReportSnapshotV1" },
    "Assurance Version": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Assurance version", source: "versions.assuranceVersion", pipeline: "buildReportSnapshotV1" },
    "Certification Status": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Certification state", source: "certificationStatus", pipeline: "buildHistoricalWriteManifest" },
    "Correction Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Historical version on snapshot", source: "historicalVersion", pipeline: "buildHistoricalWriteManifest" },
    "Is Current Historical Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Current flag", source: "boolean", pipeline: "buildHistoricalWriteManifest" },
    "Snapshot Store Ref": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Filesystem blob path", source: "snapshot-blobs/…", pipeline: "buildHistoricalWriteManifest" },
    Synthetic: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Synthetic flag", source: "synthetic", pipeline: "buildHistoricalWriteManifest" },
  },
  "ADP Period Metrics": {
    "Metrics Key": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Primary key", source: "period|metrics|v", pipeline: "buildHistoricalWriteManifest" },
    "Property ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Hotel", source: "propertyId", pipeline: "buildHistoricalWriteManifest" },
    "Period ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Period", source: "periodId", pipeline: "buildHistoricalWriteManifest" },
    "Historical Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Version", source: "historicalVersion", pipeline: "buildHistoricalWriteManifest" },
    "Consideration Rate": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "AI Consideration %", source: "executiveMetrics.considerationRate", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Consideration Numerator": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Present observations", source: "presentObservations", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Consideration Denominator": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Eligible observations", source: "comparableObservations", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Scenario Presence": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Scenario presence %", source: "executiveMetrics.scenarioPresence", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Scenario Presence Numerator": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Captured scenarios", source: "capturedScenarios", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Scenario Presence Denominator": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Eligible scenarios", source: "eligibleScenarios", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Certification Status": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Status", source: "certificationStatus", pipeline: "buildHistoricalWriteManifest" },
    "Disclosures JSON": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Methodology disclosure", source: "methodology + KPI disclosure", pipeline: "enricher" },
    "Reality Coverage": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Reality gap coverage", source: "realityGap", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Demand Capture": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Demand capture", source: "demandCapture", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Number One Appearance Rate": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Rank-1 rate", source: "rankMetrics", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Top Three Appearance Rate": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Top-3 rate", source: "rankMetrics", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Presence Index": { class: FIELD_REQUIREMENT_CLASS.NOT_APPLICABLE, applicability: [MEASUREMENT_FAMILY.CORE], purpose: "Brand AI presence index — not Core ADP KPI", source: "n/a", pipeline: "n/a" },
    "CORE Benchmark": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Optional core benchmark", source: "kpis.coreBenchmark", pipeline: "buildHistoricalWriteManifest" },
    "CORE Count": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Core composition size", source: "coreComposition.length", pipeline: "buildHistoricalWriteManifest" },
  },
  "ADP Territory Metrics": {
    "Territory Key": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "PK", source: "period|territory|v", pipeline: "buildHistoricalWriteManifest" },
    "Property ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Hotel", source: "propertyId", pipeline: "buildHistoricalWriteManifest" },
    "Period ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Period", source: "periodId", pipeline: "buildHistoricalWriteManifest" },
    "Territory ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Demand territory", source: "territoryId", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Territory Name": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Label", source: "territory name", pipeline: "stagedOwnerPayloadToHistoryShape" },
    Numerator: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Subject appearances", source: "subject.appearances", pipeline: "stagedOwnerPayloadToHistoryShape" },
    Denominator: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Comparable N", source: "comparableN", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "AI Presence Pct": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Territory presence %", source: "aiPresencePct", pipeline: "stagedOwnerPayloadToHistoryShape" },
    Rank: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Subject territory rank", source: "displayRank", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Historical Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Version", source: "historicalVersion", pipeline: "buildHistoricalWriteManifest" },
    Benchmark: { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Territory benchmark", source: "optional", pipeline: "buildHistoricalWriteManifest" },
    "Presence Index": { class: FIELD_REQUIREMENT_CLASS.NOT_APPLICABLE, applicability: [MEASUREMENT_FAMILY.CORE], purpose: "Not Core territory KPI", source: "n/a", pipeline: "n/a" },
    "Missing Evidence Count": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Missing evidence count", source: "optional", pipeline: "buildHistoricalWriteManifest" },
    "Positive Evidence Eligible Count": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Positive evidence eligible", source: "optional", pipeline: "buildHistoricalWriteManifest" },
  },
  "ADP Provider Metrics": {
    "Provider Key": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "PK", source: "period|provider|v", pipeline: "buildHistoricalWriteManifest" },
    "Property ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Hotel", source: "propertyId", pipeline: "buildHistoricalWriteManifest" },
    "Period ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Period", source: "periodId", pipeline: "buildHistoricalWriteManifest" },
    "Provider ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Provider id", source: "providerId", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Provider Name": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Display name", source: "name", pipeline: "stagedOwnerPayloadToHistoryShape" },
    Numerator: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Present count", source: "evidence.providerPresence", pipeline: "stagedOwnerPayloadToHistoryShape" },
    Denominator: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Scheduled count", source: "evidence.providerPresence", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Presence Pct": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Provider presence", source: "presenceRate", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Missing Count": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Missing responses", source: "missingCount", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Completeness State": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Coverage completeness", source: "completeness", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Recovery State": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Recovery state", source: "recoveryState", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Historical Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Version", source: "historicalVersion", pipeline: "buildHistoricalWriteManifest" },
  },
  "ADP Competitive Rankings": {
    "Rank Key": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "PK", source: "period|scope|entity|v", pipeline: "buildHistoricalWriteManifest" },
    "Property ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Subject hotel property id", source: "propertyId", pipeline: "buildHistoricalWriteManifest" },
    "Period ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Monitoring period", source: "periodId", pipeline: "buildHistoricalWriteManifest" },
    Scope: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "overall | demand territory", source: "scope key", pipeline: "buildHistoricalWriteManifest" },
    "Ranking Universe Type": {
      class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE,
      purpose: "MARKET_CORE vs BRAND_PORTFOLIO ranking methodology",
      source: "measurement family — CORE→MARKET_CORE; BPP→BRAND_PORTFOLIO",
      pipeline: "persist-adp-historical-period-v1 (Core) / persist-bpp-historical-period-v1 (BPP)",
      note: "Blank on Core rows is a SERIALIZER OMISSION — not intentional null",
    },
    "Portfolio Lens": {
      class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE,
      applicability: [MEASUREMENT_FAMILY.BPP],
      purpose: "BPP lens label",
      source: "structured.lensLabel",
      pipeline: "persist-bpp-historical-period-v1",
      coreClass: FIELD_REQUIREMENT_CLASS.NOT_APPLICABLE,
      note: "Do not invent for Core",
    },
    "Portfolio Type": {
      class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE,
      applicability: [MEASUREMENT_FAMILY.BPP],
      purpose: "BPP portfolio type",
      source: "lens.portfolioType",
      pipeline: "persist-bpp-historical-period-v1",
      coreClass: FIELD_REQUIREMENT_CLASS.NOT_APPLICABLE,
      note: "Do not invent for Core",
    },
    "Peer Set Version": {
      class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE,
      applicability: [MEASUREMENT_FAMILY.BPP],
      purpose: "Frozen peer-set identity",
      source: "peerSetVersion",
      pipeline: "persist-bpp-historical-period-v1",
      coreClass: FIELD_REQUIREMENT_CLASS.NOT_APPLICABLE,
      note: "Do not invent for Core",
    },
    "Canonical Entity ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Competitor/subject entity", source: "entityId", pipeline: "stagedOwnerPayloadToHistoryShape" },
    "Display Name At That Time": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Display name freeze", source: "name", pipeline: "buildHistoricalWriteManifest" },
    Numerator: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Appearances in ranking scope", source: "appearances", pipeline: "competitive ranking" },
    Denominator: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Eligible observation population", source: "comparableN", pipeline: "competitive ranking" },
    "AI Presence Pct": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Presence %", source: "aiPresencePct", pipeline: "competitive ranking" },
    Rank: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Calculated position", source: "displayRank/observedRank", pipeline: "competitive ranking" },
    "Tie State": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Tie annotation", source: "tieState", pipeline: "rank-history-ledger" },
    "Historical Version": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Version", source: "historicalVersion", pipeline: "buildHistoricalWriteManifest" },
  },
  "ADP Evidence Index": {
    "Evidence Key": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "PK", source: "period|observation", pipeline: "buildHistoricalWriteManifest" },
    "Property ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Hotel", source: "propertyId", pipeline: "buildHistoricalWriteManifest" },
    "Period ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Period", source: "periodId", pipeline: "buildHistoricalWriteManifest" },
    "Observation ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Observation id", source: "observationId", pipeline: "buildHistoricalWriteManifest" },
    "Evidence Lane": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "positive|missing|displacement", source: "lane", pipeline: "buildHistoricalWriteManifest" },
    Provider: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Provider", source: "provider", pipeline: "buildHistoricalWriteManifest" },
    Territory: { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Territory", source: "territory", pipeline: "buildHistoricalWriteManifest" },
    "Subject Present": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Subject appeared", source: "subjectPresent", pipeline: "buildHistoricalWriteManifest" },
    "Subject Rank": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Subject rank in response", source: "rank", pipeline: "buildHistoricalWriteManifest" },
    "Response Store Ref": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Filesystem evidence ref", source: "evidence-blobs/…", pipeline: "buildHistoricalWriteManifest" },
    "Response Length": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Response length", source: "response.length", pipeline: "buildHistoricalWriteManifest" },
    "Response Content Hash": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Response text hash", source: "hash of response", pipeline: "buildHistoricalWriteManifest" },
    "Mention Spans JSON": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Mention spans", source: "spans", pipeline: "buildHistoricalWriteManifest" },
    "Competitors JSON": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Competitors in response", source: "competitors", pipeline: "buildHistoricalWriteManifest" },
    "Citations JSON": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Citations", source: "citations", pipeline: "buildHistoricalWriteManifest" },
  },
  "ADP Report Corrections": {
    "Correction ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "PK", source: "corr|…", pipeline: "correction mode" },
    "Property ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Hotel", source: "propertyId", pipeline: "correction mode" },
    "Period ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Period", source: "periodId", pipeline: "correction mode" },
    "Original Snapshot ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Prior snapshot", source: "correctsSnapshotId", pipeline: "correction mode" },
    "Corrected Snapshot ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "New snapshot", source: "snap.snapshotId", pipeline: "correction mode" },
    Reason: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Correction reason", source: "correctionReason", pipeline: "correction mode" },
    "Changed Fields JSON": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Changed fields", source: "changedFields", pipeline: "correction mode" },
    "Authorized By": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Authorizer", source: "correctionAuthorizedBy", pipeline: "correction mode" },
    "Corrected At": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Correction time", source: "correctionAt", pipeline: "correction mode" },
    "Version Number": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Version", source: "historicalVersion", pipeline: "correction mode" },
  },
  "ADP Observation Prompt Ledger": {
    "Prompt Ledger Key": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "PK", source: "period|observation", pipeline: "prompt integrity persistence" },
    "Observation ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Observation", source: "observationId", pipeline: "prompt integrity persistence" },
    "Property ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Hotel", source: "propertyId", pipeline: "prompt integrity persistence" },
    "Period ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_ALWAYS, purpose: "Period", source: "periodId", pipeline: "prompt integrity persistence" },
    "Scenario ID": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Scenario", source: "scenarioId", pipeline: "prompt integrity persistence" },
    Provider: { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Provider", source: "provider", pipeline: "prompt integrity persistence" },
    Territory: { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Territory", source: "territory", pipeline: "prompt integrity persistence" },
    "Owner Intent": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Owner intent", source: "intent", pipeline: "prompt integrity persistence" },
    "Scenario Class": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Scenario class", source: "scenario class", pipeline: "prompt integrity persistence" },
    "Prompt Hash": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Prompt hash", source: "prompt hash", pipeline: "prompt integrity persistence" },
    "Exact Prompt Preview": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "≤500 char preview", source: "prompt preview", pipeline: "prompt integrity persistence" },
    "Template ID": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Template id", source: "templateId", pipeline: "prompt integrity persistence" },
    "Template Version": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Template version", source: "templateVersion", pipeline: "prompt integrity persistence" },
    "Generator Version": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Generator version", source: "generatorVersion", pipeline: "prompt integrity persistence" },
    "Measurement Eligibility": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Eligible for KPIs", source: "eligibility", pipeline: "prompt integrity persistence" },
    "Integrity Classification": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Integrity class", source: "classification", pipeline: "prompt integrity persistence" },
    "Request Timestamp": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Request time", source: "timestamp", pipeline: "prompt integrity persistence" },
    "Response ID": { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, purpose: "Response id", source: "responseId", pipeline: "prompt integrity persistence" },
    "Immutable Prompt Path": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Disk prompt path", source: "prompt path", pipeline: "prompt integrity persistence" },
    "Immutable Observation Ref": { class: FIELD_REQUIREMENT_CLASS.REQUIRED_WHEN_APPLICABLE, purpose: "Observation ref", source: "observation ref", pipeline: "prompt integrity persistence" },
  },
});

/** Fields previously misclassified as intentional nulls for Core measurement history. */
export const FALSE_INTENTIONAL_NULL_FIELDS_CORE = Object.freeze([
  "Ranking Universe Type",
  "Content Hash",
  "Envelope Hash",
]);

/** BPP-only fields that are truly NOT_APPLICABLE on Core rows (blank is correct). */
export const CORE_NOT_APPLICABLE_RANKING_FIELDS = Object.freeze([
  "Portfolio Lens",
  "Portfolio Type",
  "Peer Set Version",
]);

export function resolveFieldClass(tableName, fieldName, measurementFamily = MEASUREMENT_FAMILY.CORE) {
  const spec = HISTORY_FIELD_SEMANTICS[tableName]?.[fieldName];
  if (!spec) return { class: FIELD_REQUIREMENT_CLASS.OPTIONAL, note: "LIVE_OR_UNSPECIFIED" };
  if (
    measurementFamily === MEASUREMENT_FAMILY.CORE &&
    spec.coreClass === FIELD_REQUIREMENT_CLASS.NOT_APPLICABLE
  ) {
    return { ...spec, class: FIELD_REQUIREMENT_CLASS.NOT_APPLICABLE };
  }
  if (
    Array.isArray(spec.applicability) &&
    spec.applicability.length &&
    !spec.applicability.includes(measurementFamily) &&
    measurementFamily === MEASUREMENT_FAMILY.CORE
  ) {
    return { ...spec, class: FIELD_REQUIREMENT_CLASS.NOT_APPLICABLE };
  }
  return spec;
}

export function isBlankValue(v) {
  return v == null || v === "" || (Array.isArray(v) && v.length === 0);
}
