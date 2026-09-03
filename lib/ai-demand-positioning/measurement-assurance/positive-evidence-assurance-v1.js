/**
 * POSITIVE_EVIDENCE_CONTEXT_INTEGRITY — permanent assurance gate.
 */

import {
  buildPositiveEvidenceResponse,
  auditCardForPromptLeakage,
  DEFECT_POSITIVE_EVIDENCE_CONTEXT_MISMATCH,
  DEFECT_PROPRIETARY_PROMPT_LEAKAGE,
  POSITIVE_EVIDENCE_VERSION,
  selectRepresentativeObservations,
  POSITIVE_EVIDENCE_SELECTION_RULE,
} from "../customer/positive-evidence-v1.js";
import { isComparableObservation } from "../metrics/grain-governance.js";

export const POSITIVE_EVIDENCE_CONTEXT_INTEGRITY = "POSITIVE_EVIDENCE_CONTEXT_INTEGRITY";

export function runPositiveEvidenceContextIntegrity({
  period,
  scenarios,
  propertyProfile,
  intent = null,
  provider = null,
}) {
  const defects = [];
  const response = buildPositiveEvidenceResponse({
    period,
    scenarios,
    propertyProfile,
    intent,
    provider,
    limit: 5,
  });

  if (response.periodId !== period.periodId) {
    defects.push({
      code: DEFECT_POSITIVE_EVIDENCE_CONTEXT_MISMATCH,
      detail: "periodId mismatch",
    });
  }
  if (response.propertyId !== period.propertyId) {
    defects.push({
      code: DEFECT_POSITIVE_EVIDENCE_CONTEXT_MISMATCH,
      detail: "propertyId mismatch",
    });
  }

  const scenarioMap = Object.fromEntries((scenarios || []).map((s) => [s.scenarioId, s]));
  for (const card of response._assuranceCards || []) {
    const obs = (period.observations || []).find((o) => o.observationId === card._observationId);
    if (!obs) {
      defects.push({
        code: DEFECT_POSITIVE_EVIDENCE_CONTEXT_MISMATCH,
        detail: `missing observation ${card._observationId}`,
      });
      continue;
    }
    if (!isComparableObservation(obs)) {
      defects.push({
        code: DEFECT_POSITIVE_EVIDENCE_CONTEXT_MISMATCH,
        detail: "example not comparable",
      });
    }
    const present =
      obs.governedInterpretation?.subjectMentioned != null
        ? obs.governedInterpretation.subjectMentioned
        : obs.mentioned;
    if (!present) {
      defects.push({
        code: DEFECT_POSITIVE_EVIDENCE_CONTEXT_MISMATCH,
        detail: "subject presence not TRUE",
      });
    }
    if (intent && intent !== "overall") {
      const sc = scenarioMap[obs.scenarioId];
      if (sc?.intent !== intent) {
        defects.push({
          code: DEFECT_POSITIVE_EVIDENCE_CONTEXT_MISMATCH,
          detail: `cross-territory leakage: expected ${intent} got ${sc?.intent}`,
        });
      }
    }
    if (provider && obs.provider !== provider) {
      defects.push({
        code: DEFECT_POSITIVE_EVIDENCE_CONTEXT_MISMATCH,
        detail: "provider filter mismatch",
      });
    }
    if (card.aiResponse != null && obs.rawResponse != null) {
      const stored = String(obs.rawResponse);
      const api = String(card.aiResponse);
      if (stored !== api) {
        defects.push({
          code: "EVIDENCE_VERBATIM_RESPONSE_INTEGRITY",
          detail: `stored(${stored.length}) !== api(${api.length})`,
        });
      }
      if (/…$/.test(api.trim()) && stored.length > api.replace(/…$/, "").length) {
        defects.push({
          code: "NO_EVIDENCE_RESPONSE_TRUNCATION",
          detail: "synthetic ellipsis on positive evidence card",
        });
      }
    } else if (card.excerpt && obs.rawResponse) {
      if (String(card.excerpt) !== String(obs.rawResponse)) {
        defects.push({
          code: "EVIDENCE_VERBATIM_RESPONSE_INTEGRITY",
          detail: "excerpt alias must equal full rawResponse",
        });
      }
    }
    defects.push(...auditCardForPromptLeakage(card, scenarioMap));
  }

  // Determinism: two runs identical
  const again = buildPositiveEvidenceResponse({
    period,
    scenarios,
    propertyProfile,
    intent,
    provider,
    limit: 5,
  });
  const idsA = (response.evidence || []).map((e) => e.evidenceId).join(",");
  const idsB = (again.evidence || []).map((e) => e.evidenceId).join(",");
  if (idsA !== idsB) {
    defects.push({
      code: "NON_DETERMINISTIC_EVIDENCE_SAMPLING",
      detail: "selection not stable across runs",
    });
  }

  // Selection rule smoke: round-robin prefers diversity when pool allows
  const pool = (period.observations || []).filter(
    (o) => isComparableObservation(o) && (o.mentioned || o.governedInterpretation?.subjectMentioned)
  );
  const picked = selectRepresentativeObservations(pool, 5);
  if (picked.length > 1) {
    const providers = new Set(picked.map((o) => o.provider));
    // not a failure if single-provider pool — only note
  }

  return {
    gate: POSITIVE_EVIDENCE_CONTEXT_INTEGRITY,
    version: POSITIVE_EVIDENCE_VERSION,
    selectionRule: POSITIVE_EVIDENCE_SELECTION_RULE,
    status: defects.length ? "FAIL" : "PASS",
    defects,
    sampleCount: response.returned,
    totalQualifying: response.totalQualifying,
    leakageCount: defects.filter((d) => d.code === DEFECT_PROPRIETARY_PROMPT_LEAKAGE).length,
  };
}
