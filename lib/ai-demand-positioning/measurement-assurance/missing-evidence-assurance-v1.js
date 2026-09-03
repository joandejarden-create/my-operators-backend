/**
 * MISSING_EVIDENCE_CONTEXT_INTEGRITY — permanent assurance gate.
 */

import {
  buildMissingEvidenceResponse,
  DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH,
  DEFECT_PROPRIETARY_PROMPT_LEAKAGE,
  MISSING_EVIDENCE_VERSION,
  filterMissingEvidencePool,
} from "../customer/missing-evidence-v1.js";
import { isComparableObservation } from "../metrics/grain-governance.js";
import { getGovernedSubjectMentioned } from "../subject-presence/canonical-subject-presence-v1.js";

export const MISSING_EVIDENCE_CONTEXT_INTEGRITY = "MISSING_EVIDENCE_CONTEXT_INTEGRITY";

export function runMissingEvidenceContextIntegrity({
  period,
  scenarios,
  propertyProfile,
  intent = null,
  provider = null,
}) {
  const defects = [];
  const { pool } = filterMissingEvidencePool({ period, scenarios, intent, provider });
  const response = buildMissingEvidenceResponse({
    period,
    scenarios,
    propertyProfile,
    intent,
    provider,
    limit: Math.max(pool.length, 1),
    offset: 0,
  });

  if (response.periodId !== period.periodId) {
    defects.push({ code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH, detail: "periodId mismatch" });
  }
  if (response.propertyId !== period.propertyId) {
    defects.push({ code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH, detail: "propertyId mismatch" });
  }
  if (response.totalQualifying !== pool.length) {
    defects.push({
      code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH,
      detail: `totalQualifying ${response.totalQualifying} !== pool ${pool.length}`,
    });
  }
  if (response.capped === true) {
    defects.push({
      code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH,
      detail: "missing evidence must not be silently capped",
    });
  }

  const returnedIds = new Set(
    (response._assuranceCards || []).map((c) => c._observationId).filter(Boolean)
  );
  const poolIds = new Set(pool.map((o) => o.observationId).filter(Boolean));
  if (returnedIds.size !== poolIds.size) {
    defects.push({
      code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH,
      detail: `returned set size ${returnedIds.size} !== pool ${poolIds.size}`,
    });
  }
  for (const id of poolIds) {
    if (!returnedIds.has(id)) {
      defects.push({
        code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH,
        detail: `pool observation missing from response: ${id}`,
      });
      break;
    }
  }

  const scenarioMap = Object.fromEntries((scenarios || []).map((s) => [s.scenarioId, s]));
  for (const card of response._assuranceCards || []) {
    const obs = (period.observations || []).find((o) => o.observationId === card._observationId);
    if (!obs) {
      defects.push({
        code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH,
        detail: `missing observation ${card._observationId}`,
      });
      continue;
    }
    if (!isComparableObservation(obs)) {
      defects.push({
        code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH,
        detail: "example not comparable",
      });
    }
    if (getGovernedSubjectMentioned(obs)) {
      defects.push({
        code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH,
        detail: "subject presence must be FALSE for missing evidence",
      });
    }
    if (intent && intent !== "overall") {
      const sc = scenarioMap[obs.scenarioId];
      if (sc?.intent !== intent) {
        defects.push({
          code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH,
          detail: `cross-territory leakage: expected ${intent} got ${sc?.intent}`,
        });
      }
    }
    if (provider && obs.provider !== provider) {
      defects.push({
        code: DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH,
        detail: `provider mismatch: expected ${provider}`,
      });
    }
  }

  for (const d of response._leakageDefects || []) {
    defects.push(d);
  }

  return {
    gate: MISSING_EVIDENCE_CONTEXT_INTEGRITY,
    version: MISSING_EVIDENCE_VERSION,
    status: defects.length ? "FAIL" : "PASS",
    defects,
    totalQualifying: pool.length,
    returned: (response._assuranceCards || []).length,
  };
}
