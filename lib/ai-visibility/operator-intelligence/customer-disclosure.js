/**
 * Operator customer-safe disclosure — SHOW WHAT WE MEASURE / PROTECT HOW WE MEASURE IT.
 * Operator-only labels. Do not reuse Brand scenario copy.
 * Presentation contract only — no UI wiring in this phase.
 */

import { OPERATOR_DECISION_SCENARIOS } from "./scenarios.js";

export const OPERATOR_CUSTOMER_DISCLOSURE_VERSION = "operator_customer_disclosure_v1";

export const OPERATOR_INTERNAL_PROMPT_FIELD_NAMES = Object.freeze([
  "QUESTION",
  "question",
  "questionText",
  "promptText",
  "promptId",
  "missingPromptIds",
  "canonicalPrompt",
  "rawPrompt",
  "promptTemplate",
  "promptVariants",
  "promptGenerationRules",
  "systemPrompt",
  "observedQuery",
]);

/** Customer-safe Owner Intent labels for all 12 operator scenarios. */
export const OPERATOR_CUSTOMER_OWNER_INTENT = Object.freeze({
  op_scenario_full_service_uu_operator_selection_v1:
    "Upper-upscale full-service operator selection",
  op_scenario_luxury_operator_selection_v1: "Luxury hotel operator selection",
  op_scenario_lifestyle_boutique_operator_selection_v1:
    "Lifestyle or boutique operator selection",
  op_scenario_owner_control_flexibility_v1: "Owner-control operator selection",
  op_scenario_third_party_management_v1: "Third-party management selection",
  op_scenario_brand_agnostic_operation_v1: "Brand-agnostic operator selection",
  op_scenario_independent_hotel_operation_v1: "Independent-hotel operator selection",
  op_scenario_conversion_repositioning_v1: "Conversion or repositioning operator selection",
  op_scenario_commercial_revenue_capability_v1:
    "Commercial and revenue-capability operator selection",
  op_scenario_resort_operation_v1: "Resort operator selection",
  op_scenario_cala_latam_regional_capability_v1: "CALA / Latin America operator selection",
  op_scenario_institutional_platform_alignment_v1:
    "Institutional-platform operator selection",
});

/** Customer-safe Decision Context — operator-selection decisions, not hotel-brand decisions. */
export const OPERATOR_CUSTOMER_DECISION_CONTEXT = Object.freeze({
  op_scenario_full_service_uu_operator_selection_v1:
    "Owners deciding who should operate an upper-upscale full-service hotel.",
  op_scenario_luxury_operator_selection_v1:
    "Owners deciding who should operate a luxury hotel.",
  op_scenario_lifestyle_boutique_operator_selection_v1:
    "Owners deciding which operator to engage for an independent lifestyle or boutique hotel.",
  op_scenario_owner_control_flexibility_v1:
    "Owners deciding which management companies fit greater strategic or operational control.",
  op_scenario_third_party_management_v1:
    "Owners deciding which third-party hotel operators to consider.",
  op_scenario_brand_agnostic_operation_v1:
    "Owners deciding which operators can manage hotels across more than one brand system.",
  op_scenario_independent_hotel_operation_v1:
    "Owners deciding which management companies to consider for an independent hotel.",
  op_scenario_conversion_repositioning_v1:
    "Owners deciding which operators to consider when converting or repositioning a hotel.",
  op_scenario_commercial_revenue_capability_v1:
    "Owners deciding which operators are associated with commercial, revenue-management, and distribution capability.",
  op_scenario_resort_operation_v1:
    "Owners deciding which management companies to consider for a resort hotel.",
  op_scenario_cala_latam_regional_capability_v1:
    "Owners deciding which operators to consider for hotels in the Caribbean and Latin America.",
  op_scenario_institutional_platform_alignment_v1:
    "Institutional owners deciding which operators to consider as a scalable management platform.",
});

export const OPERATOR_PROVIDER_DISAGREEMENT_CUSTOMER_COPY =
  "AI providers differ in whether this operator appears for this owner-decision scenario.";

export function listOperatorCustomerScenarioLabels() {
  return OPERATOR_DECISION_SCENARIOS.map((s) => ({
    scenarioId: s.scenarioId,
    ownerIntent: OPERATOR_CUSTOMER_OWNER_INTENT[s.scenarioId],
    decisionContext: OPERATOR_CUSTOMER_DECISION_CONTEXT[s.scenarioId],
  }));
}

export function getOperatorCustomerOwnerIntent(scenarioId) {
  return OPERATOR_CUSTOMER_OWNER_INTENT[scenarioId] || null;
}

export function getOperatorCustomerDecisionContext(scenarioId) {
  return OPERATOR_CUSTOMER_DECISION_CONTEXT[scenarioId] || null;
}

function stripInternalPromptFields(obj = {}) {
  const out = { ...obj };
  for (const key of OPERATOR_INTERNAL_PROMPT_FIELD_NAMES) delete out[key];
  return out;
}

function payloadContainsRawPrompt(value) {
  const text = typeof value === "string" ? value : JSON.stringify(value || {});
  return /op_p_(core|ext)_/i.test(text) || /"promptText"\s*:/.test(text);
}

/**
 * Customer-safe Questions Missing row. No raw prompts. No prompt IDs.
 */
export function toCustomerSafeQuestionsMissingRow(row = {}, opts = {}) {
  const scenarioId = row.scenarioId;
  const safe = {
    scenarioId,
    ownerIntent: getOperatorCustomerOwnerIntent(scenarioId),
    decisionContext: getOperatorCustomerDecisionContext(scenarioId),
    providerScope: row.providerScope || "ALL_PROVIDERS",
    subjectPresence:
      row.customerAbsenceClass === "NOT_APPLICABLE"
        ? "NOT_APPLICABLE"
        : row.operatorPresence || row.subjectPresence || null,
    absenceClass: row.customerAbsenceClass || null,
    missingCount: row.missingCount ?? (row.missingProviders || []).length,
    comparableProviderCount: row.comparableProviderCount ?? null,
    relevantOperatorsPresent: [...(row.relevantOperatorsPresentCustomer || row.relevantOperatorsPresent || [])],
    observedCompetitors: [...(row.observedCompetitorsCustomer || [])],
    evidenceCount: row.evidenceCount ?? 0,
    providerDisagreement: row.providerDisagreement === true,
    providerDisagreementNote: row.providerDisagreement
      ? OPERATOR_PROVIDER_DISAGREEMENT_CUSTOMER_COPY
      : null,
  };
  if (opts.includeInternal) return { ...row, customer: safe };
  if (payloadContainsRawPrompt(safe)) {
    throw new Error("operator_customer_qm_prompt_leak");
  }
  return stripInternalPromptFields(safe);
}

/**
 * Customer-safe Competitive Gap row. Diagnostic candidates must not leak when unpromoted.
 */
export function toCustomerSafeCompetitiveGapRow(row = {}, opts = {}) {
  if (!opts.clientPromoted) return null;
  const scenarioId = row.scenarioId;
  const safe = {
    scenarioId,
    ownerIntent: getOperatorCustomerOwnerIntent(scenarioId),
    decisionContext: getOperatorCustomerDecisionContext(scenarioId),
    providerScope: row.providerScope || null,
    subjectPresence: "ABSENT",
    missingCount: row.missingCount ?? null,
    comparableProviderCount: row.comparableProviderCount ?? null,
    relevantOperatorsPresent: [...(row.relevantOperatorsPresentCustomer || [])],
    gapInterpretation: row.gapInterpretation || null,
    evidenceCount: row.evidenceCount ?? 0,
  };
  if (safe.comparabilityMatrix || safe.pairs || safe.truthDiagnostics) {
    throw new Error("operator_customer_comparability_matrix_leak");
  }
  if (payloadContainsRawPrompt(safe)) {
    throw new Error("operator_customer_gap_prompt_leak");
  }
  return stripInternalPromptFields(safe);
}

export function assertNoOperatorPromptLeak(payload) {
  if (payloadContainsRawPrompt(payload)) {
    throw new Error("operator_prompt_moat_leak");
  }
  return true;
}
