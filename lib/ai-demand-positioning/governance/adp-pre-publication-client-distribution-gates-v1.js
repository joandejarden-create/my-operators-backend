/**
 * Future pre-publication rule: every NEW ADP period must pass these gates
 * before CLIENT_DISTRIBUTION_READY = YES.
 *
 * The audit becomes the final independent check — not the first place defects
 * are discovered. Methodology remains governed (not redesigned here).
 */

import { METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN } from "./adp-methodology-governance-v1.js";

export const ADP_PRE_PUBLICATION_CLIENT_DISTRIBUTION_GATES_V1 =
  "ADP_PRE_PUBLICATION_CLIENT_DISTRIBUTION_GATES_V1";

export const PRE_PUBLICATION_REQUIRED_GATES = Object.freeze([
  "CANONICAL_SUBJECT_PATH",
  "ENTITY_INTEGRITY",
  "PROMPT_INTENT",
  "PROVIDER_FAILURE_POLICY",
  "INDEPENDENT_METRIC_RECALCULATION",
  "CROSS_REPORT_METRIC_RECONCILIATION",
  "REALITY_GAP_RECONCILIATION",
  "EVIDENCE_TRACEABILITY",
  "NARRATIVE_FACT_RECONCILIATION",
  "MEASUREMENT_ASSURANCE",
  "CLIENT_CHALLENGE_REPRODUCIBILITY",
  "BPP_MAXIMALLY_RESOLVED",
]);

/**
 * Evaluate whether a property period may be marked CLIENT_DISTRIBUTION_READY.
 * @param {Record<string, boolean|{pass?:boolean}>} gateResults
 */
export function evaluatePrePublicationClientDistributionReady(gateResults = {}) {
  const missing = [];
  const failed = [];
  for (const gate of PRE_PUBLICATION_REQUIRED_GATES) {
    const raw = gateResults[gate];
    if (raw == null) {
      missing.push(gate);
      continue;
    }
    const pass = typeof raw === "boolean" ? raw : raw.pass === true;
    if (!pass) failed.push(gate);
  }
  const ready = missing.length === 0 && failed.length === 0;
  return {
    gate: ADP_PRE_PUBLICATION_CLIENT_DISTRIBUTION_GATES_V1,
    doctrine: METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
    CLIENT_DISTRIBUTION_READY: ready ? "YES" : "NO",
    requiredGates: [...PRE_PUBLICATION_REQUIRED_GATES],
    missing,
    failed,
    pass: ready,
  };
}
