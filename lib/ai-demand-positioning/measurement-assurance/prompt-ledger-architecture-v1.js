/**
 * Prompt ledger + period manifest + Airtable / immutable storage design.
 * Writes DISABLED — design only.
 */

export const PROMPT_LEDGER_FIELDS_V1 = Object.freeze([
  "observationId",
  "propertyId",
  "periodId",
  "scenarioId",
  "canonicalScenarioText",
  "ownerIntent",
  "territoryId",
  "scenarioClass",
  "provider",
  "exactRenderedPrompt",
  "promptHash",
  "promptTemplateId",
  "promptTemplateVersion",
  "promptGeneratorVersion",
  "measurementEligibility",
  "integrityClassification",
  "requestTimestamp",
  "responseId",
  "exactCapturedResponseRef",
  "parserVersion",
  "entityResolverVersion",
  "immutableSnapshotRef",
]);

/**
 * Design choice: B — dedicated ADP Observation Prompt Ledger in Airtable
 * (index) + immutable filesystem snapshot (authoritative forensic body).
 *
 * Rationale: ~weekly × scenarios × providers × hotels → long-text prompts
 * and responses exceed comfortable Airtable cell scale; index + ref is safer.
 */
export const PROMPT_HISTORY_DESIGN_V1 = Object.freeze({
  airtablePattern: "B_DEDICATED_PROMPT_LEDGER_INDEX",
  tableNameProposed: "ADP Observation Prompt Ledger",
  airtableStores: [
    "observationId",
    "propertyId",
    "periodId",
    "scenarioId",
    "provider",
    "territory",
    "scenarioClass",
    "promptHash",
    "exactPromptPreview_500",
    "templateId",
    "templateVersion",
    "generatorVersion",
    "measurementEligibility",
    "integrityClassification",
    "immutableObservationRef",
    "immutablePromptPath",
  ],
  immutableStores: [
    "exactRenderedPrompt",
    "exactCapturedResponse",
    "full lineage metadata",
  ],
  reconciliationGate: "PROMPT_LEDGER_SNAPSHOT_RECONCILIATION",
  rule: "Airtable promptHash === immutable observation promptHash === request-time hash",
});

export function estimatePromptLedgerRowsPerYear({
  hotels,
  scenariosPerHotel = 63,
  providers = 4,
  periodsPerYear = 52,
}) {
  const rowsPerPeriod = scenariosPerHotel * providers;
  const rowsPerHotelYear = rowsPerPeriod * periodsPerYear;
  return {
    hotels,
    scenariosPerHotel,
    providers,
    periodsPerYear,
    rowsPerPeriodPerHotel: rowsPerPeriod,
    rowsPerYear: hotels * rowsPerHotelYear,
  };
}

export const SCALE_ESTIMATES_V1 = Object.freeze({
  assumptions: {
    scenariosPerHotel: 63,
    providers: 4,
    periodsPerYear: 52,
    note: "Matches current ~63 scenarios × 4 providers pattern; adjust if universe expands.",
  },
  byHotelCount: Object.freeze({
    1: estimatePromptLedgerRowsPerYear({ hotels: 1 }),
    10: estimatePromptLedgerRowsPerYear({ hotels: 10 }),
    100: estimatePromptLedgerRowsPerYear({ hotels: 100 }),
    500: estimatePromptLedgerRowsPerYear({ hotels: 500 }),
    1000: estimatePromptLedgerRowsPerYear({ hotels: 1000 }),
  }),
  storageRecommendation:
    "Airtable = queryable index + hash + short preview; immutable object/filesystem = full exact prompt + verbatim response. Do not force full forensic payloads into Airtable at 100+ hotels.",
});

export function buildPeriodPromptManifestV1({ periodId, propertyId, entries }) {
  const rows = (entries || []).map((e) => ({
    propertyId,
    periodId,
    scenarioId: e.scenarioId,
    provider: e.provider,
    territoryId: e.territoryId || e.intent || null,
    scenarioClass: e.scenarioClass,
    exactPrompt: e.exactPrompt,
    promptHash: e.promptHash,
    measurementEligibility: e.measurementEligibility,
  }));
  return {
    manifestVersion: "ADP_PERIOD_PROMPT_MANIFEST_V1",
    periodId,
    propertyId,
    rowCount: rows.length,
    rows,
    status: "DESIGN_ONLY_NOT_EXECUTED",
  };
}

export const SAME_PERIOD_RECOVERY_POLICY_V1 = Object.freeze({
  execute: false,
  steps: [
    "Preserve original observation (prompt + response) immutable",
    "Mark original measurementEligible=false reason=PROMPT_INTEGRITY_CORRECTION",
    "Create replacement observation with corrected governed prompt (new observationId, recoveryVersion)",
    "Re-run assurance on affected period",
    "Recalculate affected metrics",
    "If published values change: ORIGINAL REPORT + CORRECTED REPORT with PROMPT_INTEGRITY_CORRECTION",
  ],
  never: ["Erase defective original", "Silent overwrite of customer-visible report"],
});

export const ASSURANCE_LAYER_ORDER_V1 = Object.freeze([
  "Prompt Integrity",
  "Observation Integrity",
  "Measurement Integrity",
  "Comparability Integrity",
  "Analytical Coherence",
  "Publication Integrity",
]);
