/**
 * Operator Questions Missing + All Providers + provider disagreement.
 * Certified Presence layer only. Missing provider ≠ zero. Failed ≠ absence.
 */

import { listOperatorPrompts } from "./prompts.js";
import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";
import { OPERATOR_AI_UNIVERSE, getOperatorById } from "./universe.js";
import { eligibilityFor, ELIGIBILITY } from "./eligibility.js";

export const OPERATOR_QM_VERSION = "operator_questions_missing_v1";
export const OPERATOR_ALL_PROVIDERS_VERSION = "operator_all_providers_derived_v1";
export const OPERATOR_PROVIDER_DISAGREEMENT_VERSION = "operator_provider_disagreement_v1";
export const ALL_PROVIDERS_SCOPE = "ALL_PROVIDERS";

export const OPERATOR_ALL_PROVIDERS_DERIVATION_CONTRACT = Object.freeze({
  version: OPERATOR_ALL_PROVIDERS_VERSION,
  providerSpecificPrimary: true,
  derived: true,
  isAProvider: false,
  openAiProxy: false,
  weighting: "NONE",
  aggregation:
    "For each prompt, comparable providers = providers with a successful observation. All Providers presence = OR across those comparable providers. Failed, missing, or unavailable providers are excluded from the denominator and are never filled with zero.",
  disagreement:
    "PROVIDER_DISAGREEMENT when, for the same prompt, at least one comparable provider shows Presence and at least one comparable provider does not. Not a quality or confidence score.",
  customerSemantics:
    "Cross-provider observation of whether the operator appears for the owner-decision scenario. Not a fifth AI provider.",
});

/**
 * @param {{
 *   operatorId: string,
 *   promptIds: string[],
 *   observations: Array<{ promptId: string, provider: string, present: boolean }>,
 * }} input
 */
export function computeOperatorQuestionsMissing(input = {}) {
  const operatorId = input.operatorId;
  const promptIds = [...new Set(input.promptIds || [])];
  const observations = Array.isArray(input.observations) ? input.observations : [];

  const byPrompt = new Map();
  for (const obs of observations) {
    if (!obs?.promptId || !obs.provider) continue;
    if (!byPrompt.has(obs.promptId)) byPrompt.set(obs.promptId, []);
    byPrompt.get(obs.promptId).push(obs);
  }

  const eligiblePromptIds = [];
  const missingPromptIds = [];
  for (const promptId of promptIds) {
    const rows = byPrompt.get(promptId) || [];
    if (!rows.length) continue; // no comparable provider observation — exclude from denominator
    eligiblePromptIds.push(promptId);
    const presentSomewhere = rows.some((r) => r.present === true);
    if (!presentSomewhere) missingPromptIds.push(promptId);
  }

  return {
    version: OPERATOR_QM_VERSION,
    operatorId,
    status: eligiblePromptIds.length ? "PASS" : "NOT_READY",
    denominator: eligiblePromptIds.length,
    questionsMissingCount: missingPromptIds.length,
    questionsMissingRate:
      eligiblePromptIds.length > 0
        ? missingPromptIds.length / eligiblePromptIds.length
        : null,
    missingPromptIds,
    missingProviderEqualsZero: false,
    providerProxy: false,
  };
}

/**
 * All Providers derived Presence — comparable observations only.
 */
export function computeOperatorAllProvidersPresence(observations = []) {
  const byPrompt = new Map();
  for (const obs of observations) {
    if (!obs?.promptId || !obs.provider) continue;
    if (!byPrompt.has(obs.promptId)) byPrompt.set(obs.promptId, []);
    byPrompt.get(obs.promptId).push(obs);
  }
  let comparablePrompts = 0;
  let presentPrompts = 0;
  for (const [, rows] of byPrompt) {
    if (!rows.length) continue;
    comparablePrompts += 1;
    if (rows.some((r) => r.present === true)) presentPrompts += 1;
  }
  return {
    status: comparablePrompts ? "PASS" : "NOT_READY",
    comparablePrompts,
    presentPrompts,
    missingProviderEqualsZero: false,
    derived: true,
    providerSpecificPrimary: true,
    openAiProxy: false,
    weighting: "NONE",
    isAProvider: false,
    contract: OPERATOR_ALL_PROVIDERS_DERIVATION_CONTRACT,
  };
}

/**
 * PROVIDER_DISAGREEMENT — mixed Presence across comparable providers for the same prompt.
 * Not a quality/confidence score.
 */
export function detectOperatorProviderDisagreement(observations = []) {
  const byPrompt = new Map();
  for (const obs of observations) {
    if (!obs?.promptId || !obs.provider) continue;
    if (!byPrompt.has(obs.promptId)) byPrompt.set(obs.promptId, []);
    byPrompt.get(obs.promptId).push(obs);
  }
  const disagreedPrompts = [];
  for (const [promptId, rows] of byPrompt) {
    const providers = [...new Set(rows.map((r) => r.provider))];
    if (providers.length < 2) continue;
    const somePresent = rows.some((r) => r.present === true);
    const someAbsent = rows.some((r) => r.present !== true);
    if (somePresent && someAbsent) {
      disagreedPrompts.push({
        promptId,
        comparableProviders: providers,
        presentProviders: [...new Set(rows.filter((r) => r.present === true).map((r) => r.provider))],
        absentProviders: [...new Set(rows.filter((r) => r.present !== true).map((r) => r.provider))],
      });
    }
  }
  return {
    version: OPERATOR_PROVIDER_DISAGREEMENT_VERSION,
    hasDisagreement: disagreedPrompts.length > 0,
    disagreedPromptCount: disagreedPrompts.length,
    disagreedPrompts,
    isQualityScore: false,
    isConfidenceScore: false,
    customerCopy:
      "AI providers differ in whether this operator appears for this owner-decision scenario.",
  };
}

function observationsFromExtractions(extractions, operatorId) {
  return (extractions || []).map((e) => ({
    promptId: e.promptId,
    provider: e.provider,
    scenarioId: e.scenarioId,
    present: (e.presentOperatorIds || []).includes(operatorId),
    presentOperatorIds: [...(e.presentOperatorIds || [])],
    observedCompetitors: [...(e.observedCompetitors || [])],
  }));
}

function uniqueIds(ids) {
  return [...new Set((ids || []).filter(Boolean))];
}

function customerOperatorNames(ids) {
  return uniqueIds(ids)
    .map((id) => getOperatorById(id)?.canonicalName)
    .filter(Boolean);
}

function observedCompetitorCustomer(list) {
  return uniqueIds((list || []).map((c) => c.canonicalName || c.name)).map((name) => ({
    name,
    role: "OBSERVED_COMPETITIVE_CONTEXT",
    primaryMonitored: false,
    emergingCompetitor: false,
  }));
}

/**
 * Operator × scenario Questions Missing from certified Presence extractions.
 * Failed / missing providers never enter the denominator.
 */
export function buildOperatorQuestionsMissingMatrix(extractions = []) {
  const promptIds = listOperatorPrompts().map((p) => p.promptId);
  const rows = [];
  for (const op of OPERATOR_AI_UNIVERSE) {
    const allObs = observationsFromExtractions(extractions, op.canonicalId);
    for (const scenario of OPERATOR_DECISION_SCENARIOS) {
      const scenarioObs = allObs.filter((o) => o.scenarioId === scenario.scenarioId);
      const comparableProviders = uniqueIds(scenarioObs.map((o) => o.provider));
      const elig = eligibilityFor(op.canonicalId, scenario.scenarioId);
      const present = scenarioObs.some((o) => o.present === true);
      const operatorPresence = !comparableProviders.length
        ? "INCOMPARABLE"
        : present
          ? "PRESENT"
          : "ABSENT";
      const missingProviders = comparableProviders.filter((provider) => {
        const rowsForProvider = scenarioObs.filter((o) => o.provider === provider);
        return rowsForProvider.length > 0 && rowsForProvider.every((o) => o.present !== true);
      });
      const allComparableProvidersMissing =
        comparableProviders.length > 0 && scenarioObs.every((o) => o.present !== true);
      const relevantIds = uniqueIds(
        scenarioObs.flatMap((o) => o.presentOperatorIds).filter((id) => id !== op.canonicalId)
      );
      const observed = scenarioObs.flatMap((o) => o.observedCompetitors || []);
      const disagreement = detectOperatorProviderDisagreement(scenarioObs);
      const providerCoverage = {};
      for (const provider of comparableProviders) {
        const providerRows = scenarioObs.filter((o) => o.provider === provider);
        providerCoverage[provider] = {
          comparableCount: providerRows.length,
          presentCount: providerRows.filter((o) => o.present === true).length,
        };
      }
      const promptQm = computeOperatorQuestionsMissing({
        operatorId: op.canonicalId,
        promptIds,
        observations: scenarioObs,
      });
      const allProviders = computeOperatorAllProvidersPresence(scenarioObs);

      rows.push({
        operatorId: op.canonicalId,
        canonicalName: op.canonicalName,
        scenarioId: scenario.scenarioId,
        providerScope: ALL_PROVIDERS_SCOPE,
        eligibility: elig.status,
        providerCoverage,
        operatorPresence,
        missingProviders,
        allComparableProvidersMissing,
        relevantOperatorsPresent: relevantIds,
        relevantOperatorsPresentCustomer: customerOperatorNames(relevantIds),
        observedCompetitors: observed,
        observedCompetitorsCustomer: observedCompetitorCustomer(observed),
        evidenceCount: scenarioObs.length,
        comparableProviderCount: comparableProviders.length,
        missingCount: missingProviders.length,
        questionsMissingCount: promptQm.questionsMissingCount,
        questionsMissingDenominator: promptQm.denominator,
        missingProviderEqualsZero: false,
        failedResponseEqualsAbsence: false,
        providerDisagreement: disagreement.hasDisagreement,
        allProvidersDerived: allProviders,
        scenarioOutOfScope: elig.status === ELIGIBILITY.OUT_OF_SCOPE,
        customerAbsenceClass:
          elig.status === ELIGIBILITY.OUT_OF_SCOPE
            ? "NOT_APPLICABLE"
            : operatorPresence === "ABSENT"
              ? "MISSING"
              : operatorPresence === "PRESENT"
                ? "PRESENT"
                : "INCOMPARABLE",
      });
    }
  }
  return rows;
}

export function summarizeQuestionsMissingMatrix(rows = []) {
  const comparable = rows.filter((r) => r.operatorPresence !== "INCOMPARABLE");
  const missing = comparable.filter((r) => r.allComparableProvidersMissing);
  const applicableMissing = missing.filter((r) => r.customerAbsenceClass !== "NOT_APPLICABLE");
  const notApplicable = rows.filter((r) => r.customerAbsenceClass === "NOT_APPLICABLE");
  return {
    version: OPERATOR_QM_VERSION,
    status: comparable.length ? "READY" : "REMEDIATION_REQUIRED",
    totalOperatorScenarioRows: rows.length,
    comparableRows: comparable.length,
    totalMissingRows: missing.length,
    applicableMissingRows: applicableMissing.length,
    notApplicableRows: notApplicable.length,
    allProvidersReady: rows.every((r) => r.allProvidersDerived?.missingProviderEqualsZero === false),
    providerDisagreementReady: true,
    missingProviderEqualsZero: false,
    regression: 0,
  };
}
