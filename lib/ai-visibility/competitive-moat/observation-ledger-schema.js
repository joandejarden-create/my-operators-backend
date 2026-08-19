/**
 * Dealality observation ledger schema — append-only measurement history.
 * Extends brand longitudinal ledger; shared infrastructure for Brand + Operator.
 */

export const OBSERVATION_LEDGER_VERSION = "dealality_observation_ledger_v1";
export const APPEND_ONLY = true;

/** Required fields for a production observation record. */
export const OBSERVATION_RECORD_FIELDS = Object.freeze([
  "observationId",
  "timestamp",
  "measurementPeriodId",
  "waveId",
  "runId",
  "entityType",
  "subjectEntityIds",
  "provider",
  "providerReportedModel",
  "modelIdentifier",
  "canonicalIntentId",
  "scenarioId",
  "promptFamily",
  "promptId",
  "promptVersion",
  "promptTextHash",
  "mutationId",
  "geography",
  "language",
  "assetContext",
  "entitiesSurfaced",
  "subjectPresenceResult",
  "competitorPresence",
  "citationMetadata",
  "sourceDomains",
  "associationSpans",
  "narrativeSpans",
  "truthSpans",
  "stabilityMetadata",
  "rawResponseRef",
  "parsedResponseRef",
  "classifierVersions",
  "methodologyVersion",
  "datasetNamespace",
  "qualityState",
  "costMetadata",
]);

export const DATASET_CLASSES = Object.freeze([
  "DEMO_VALIDATION",
  "PILOT",
  "PRODUCTION_CLIENT",
]);

export const ENTITY_TYPES = Object.freeze(["BRAND", "OPERATOR"]);

/**
 * Validate observation record shape (does not validate business logic).
 */
export function validateObservationRecord(record = {}) {
  const errors = [];
  if (!record.observationId) errors.push("missing_observationId");
  if (!record.timestamp) errors.push("missing_timestamp");
  if (!record.entityType || !ENTITY_TYPES.includes(record.entityType)) {
    errors.push("invalid_entityType");
  }
  if (!record.promptId) errors.push("missing_promptId");
  if (!record.provider) errors.push("missing_provider");
  if (record.datasetNamespace && !DATASET_CLASSES.includes(record.datasetNamespace)) {
    errors.push("invalid_datasetNamespace");
  }
  for (const blocked of ["Recommended", "Winner", "Loser", "Preferred", "Displaced"]) {
    if (record[blocked] != null) errors.push(`unvalidated_inference_field:${blocked}`);
  }
  if (record.researchOnly !== true && record.qualityState === "RESEARCH_ONLY") {
    // research outputs tagged explicitly
  }
  return { ok: errors.length === 0, errors };
}

/**
 * Build observation ID from grain components.
 */
export function buildObservationId(parts = {}) {
  return [
    parts.waveId || parts.measurementPeriodId || "obs",
    parts.promptId || "",
    parts.provider || "",
    parts.entityType || "",
    parts.timestamp || Date.now(),
  ]
    .join("_")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 120);
}

/**
 * Map brand longitudinal ledger entry to observation record shape.
 */
export function mapLongitudinalEntryToObservation(entry = {}, defaults = {}) {
  return {
    observationId: entry.observationId || buildObservationId(entry),
    timestamp: entry.timestamp || entry.completedAt || null,
    measurementPeriodId: entry.measurementPeriodId || defaults.measurementPeriodId || null,
    waveId: entry.waveId || defaults.waveId || null,
    runId: entry.runId || entry.batchId || null,
    entityType: entry.entityType || "BRAND",
    subjectEntityIds: entry.subjectEntityIds || entry.brandIds || [],
    provider: entry.provider || null,
    providerReportedModel: entry.providerReportedModel || entry.model || null,
    modelIdentifier: entry.modelIdentifier || null,
    canonicalIntentId: entry.canonicalIntentId || null,
    scenarioId: entry.scenarioId || null,
    promptFamily: entry.promptFamily || null,
    promptId: entry.promptId || null,
    promptVersion: entry.promptVersion || null,
    promptTextHash: entry.promptTextHash || null,
    mutationId: entry.mutationId || null,
    geography: entry.geography || entry.commercialRegion || null,
    language: entry.language || null,
    assetContext: entry.assetContext || null,
    entitiesSurfaced: entry.entitiesSurfaced || [],
    subjectPresenceResult: entry.subjectPresenceResult || entry.presence || null,
    competitorPresence: entry.competitorPresence || [],
    citationMetadata: entry.citationMetadata || null,
    sourceDomains: entry.sourceDomains || [],
    associationSpans: entry.associationSpans || [],
    narrativeSpans: entry.narrativeSpans || [],
    truthSpans: entry.truthSpans || [],
    stabilityMetadata: entry.stabilityMetadata || null,
    rawResponseRef: entry.rawResponseRef || null,
    parsedResponseRef: entry.parsedResponseRef || null,
    classifierVersions: entry.classifierVersions || defaults.classifierVersions || {},
    methodologyVersion: entry.methodologyVersion || OBSERVATION_LEDGER_VERSION,
    datasetNamespace: entry.datasetNamespace || defaults.datasetNamespace || "DEMO_VALIDATION",
    qualityState: entry.qualityState || "VALID",
    costMetadata: entry.costMetadata || null,
    unvalidatedRecommendationFields: 0,
  };
}

export const BRAND_HISTORY_PRESERVED = true;
export const OPERATOR_LEDGER_READY = "PARTIAL";
