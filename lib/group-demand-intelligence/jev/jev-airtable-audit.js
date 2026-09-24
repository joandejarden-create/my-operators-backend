/**
 * Optional Jev audit fields on GDI Research Target Runs.
 * Probe schema before write; never invent fields; never store raw payloads.
 */

import { MAP_RESEARCH_TARGET_RUN } from "../research-coverage/airtable-field-map.js";

/** Canonical names — only written when Airtable schema exposes them. */
export const MAP_JEV_TARGET_RUN_AUDIT = Object.freeze({
  jevDecisionId: "jevDecisionId",
  jevDecisionType: "jevDecisionType",
  jevDecision: "jevDecision",
  jevConfidence: "jevConfidence",
  jevModel: "jevModel",
  jevEvaluatedAt: "jevEvaluatedAt",
  jevShadow: "jevShadow",
  jevAdjudication: "jevAdjudication",
  jevPolicyOutcome: "jevPolicyOutcome",
});

/**
 * Compact audit object safe for Target Run Payload JSON (no prompts/raw).
 */
export function buildJevAuditCompact(decision = {}, adjudication = null) {
  return {
    jevDecisionId: decision.decisionId || null,
    jevDecisionType: decision.decisionType || null,
    jevDecision: decision.selected || null,
    jevConfidence:
      typeof decision.confidence === "number" ? decision.confidence : null,
    jevModel: decision.model || null,
    jevEvaluatedAt: new Date().toISOString(),
    jevShadow: decision.shadow !== false,
    jevAdjudication: adjudication || null,
    jevPolicyOutcome: decision.policy?.policyOutcome || decision.finalPolicyDecision || null,
    jevMatchExisting: decision.matchExisting,
    jevFallbackCause: decision.fallbackCause || null,
    jevTechnicalFallback: Boolean(decision.technicalFallback),
    inputHash: decision.inputHash || null,
  };
}

/**
 * Merge compact Jev audit into target-run field payload.
 * Uses dedicated columns when present in `availableFields` Set; else embeds under payloadJson key only if caller merges.
 */
export function jevAuditToAirtableFields(audit, { availableFields = null } = {}) {
  const fields = {};
  const allow = availableFields instanceof Set ? availableFields : null;
  for (const [key, airtableName] of Object.entries(MAP_JEV_TARGET_RUN_AUDIT)) {
    if (audit[key] == null || audit[key] === "") continue;
    if (allow && !allow.has(airtableName)) continue;
    let val = audit[key];
    if (key === "jevConfidence" && typeof val === "number") {
      // Airtable percent or number — store 0–1 number
      val = Math.round(val * 1000) / 1000;
    }
    if (key === "jevShadow") val = Boolean(val);
    fields[airtableName] = val;
  }
  return fields;
}

/** Extend field map export for documentation / ensure scripts. */
export function extendTargetRunFieldMap() {
  return {
    ...MAP_RESEARCH_TARGET_RUN,
    ...MAP_JEV_TARGET_RUN_AUDIT,
  };
}
