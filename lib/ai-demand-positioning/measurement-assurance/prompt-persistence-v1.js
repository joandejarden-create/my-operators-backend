/**
 * Prompt persistence + period prompt manifest (pre-execution freeze).
 */

import crypto from "crypto";
import { SCENARIO_CLASSES } from "./prompt-integrity-contract-v1.js";

export const PROMPT_PERSISTENCE_VERSION = "ADP_PROMPT_PERSISTENCE_V1";
export const PERIOD_PROMPT_MANIFEST_VERSION = "ADP_PERIOD_PROMPT_MANIFEST_V1";

export function hashPrompt(exactRenderedPrompt) {
  return crypto.createHash("sha256").update(String(exactRenderedPrompt || ""), "utf8").digest("hex");
}

export function hashResponse(rawResponse) {
  return crypto.createHash("sha256").update(String(rawResponse || ""), "utf8").digest("hex");
}

/**
 * Fields that MUST be written onto every future observation at request time.
 */
export function buildPromptProvenanceFields({
  observationId,
  propertyId,
  periodId,
  provider,
  scenarioId,
  scenarioClass = SCENARIO_CLASSES.NEUTRAL_DEMAND,
  territoryId,
  ownerIntent,
  exactRenderedPrompt,
  promptTemplateId,
  promptTemplateVersion,
  promptGeneratorVersion,
  requestTimestamp = new Date().toISOString(),
  measurementEligibility = true,
  profileHash = null,
  profileToPromptProvenance = null,
  replacesObservationId = null,
  replacesScenarioId = null,
  correctionReason = null,
  correctionVersion = null,
  originalPromptHash = null,
}) {
  const promptHash = hashPrompt(exactRenderedPrompt);
  return {
    observationId,
    propertyId,
    periodId,
    provider,
    scenarioId,
    scenarioClass,
    territoryId: territoryId || ownerIntent || null,
    ownerIntent: ownerIntent || null,
    exactRenderedPrompt: String(exactRenderedPrompt || ""),
    promptHash,
    promptTemplateId: promptTemplateId || null,
    promptTemplateVersion: promptTemplateVersion || null,
    promptGeneratorVersion: promptGeneratorVersion || PROMPT_PERSISTENCE_VERSION,
    requestTimestamp,
    measurementEligibility: !!measurementEligibility,
    profileHash,
    profileToPromptProvenance,
    replacesObservationId,
    replacesScenarioId,
    correctionReason,
    correctionVersion,
    originalPromptHash,
    promptPersistenceVersion: PROMPT_PERSISTENCE_VERSION,
  };
}

/** Attach response forensic fields after provider return (no LLM here — helper only). */
export function attachResponseProvenance(obs, { rawResponse, responseId = null, parserVersion, resolverVersion }) {
  return {
    ...obs,
    rawResponse: rawResponse ?? obs.rawResponse,
    responseId,
    responseHash: hashResponse(rawResponse ?? obs.rawResponse),
    parserVersion: parserVersion || null,
    entityResolverVersion: resolverVersion || null,
  };
}

export function buildPeriodPromptManifestV1({
  periodId,
  propertyId,
  entries,
  measurementContractVersion = "ADP_MEASUREMENT_CONTRACT_V1_1",
  correctionRun = false,
}) {
  const rows = (entries || []).map((e) => ({
    propertyId: e.propertyId || propertyId,
    periodId: e.periodId || periodId,
    scenarioId: e.scenarioId,
    provider: e.provider,
    territoryId: e.territoryId || e.ownerIntent || null,
    scenarioClass: e.scenarioClass || SCENARIO_CLASSES.NEUTRAL_DEMAND,
    exactRenderedPrompt: e.exactRenderedPrompt,
    promptHash: e.promptHash || hashPrompt(e.exactRenderedPrompt),
    measurementEligibility: e.measurementEligibility !== false,
    replacesObservationId: e.replacesObservationId || null,
    replacesScenarioId: e.replacesScenarioId || null,
    correctionReason: e.correctionReason || null,
  }));
  const body = {
    manifestVersion: PERIOD_PROMPT_MANIFEST_VERSION,
    periodId,
    propertyId,
    measurementContractVersion,
    correctionRun,
    rowCount: rows.length,
    rows,
  };
  const manifestHash = crypto.createHash("sha256").update(JSON.stringify(body), "utf8").digest("hex");
  return {
    ...body,
    manifestHash,
    status: "DESIGNED_OR_FROZEN_PENDING_EXECUTION",
    gates: {
      PROMPT_MANIFEST_CERTIFIED_BEFORE_EXECUTION: false,
      EXECUTED_PROMPT_MATCHES_CERTIFIED_MANIFEST: false,
    },
  };
}

export function certifyPromptManifest(manifest, { preflightAllPass }) {
  if (!preflightAllPass) {
    return { ...manifest, gates: { ...manifest.gates, PROMPT_MANIFEST_CERTIFIED_BEFORE_EXECUTION: false } };
  }
  return {
    ...manifest,
    certifiedAt: new Date().toISOString(),
    gates: { ...manifest.gates, PROMPT_MANIFEST_CERTIFIED_BEFORE_EXECUTION: true },
    status: "CERTIFIED_PENDING_EXECUTION",
  };
}
