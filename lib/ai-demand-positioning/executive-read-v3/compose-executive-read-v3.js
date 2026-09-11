/**
 * Generic Executive Read V3 composer orchestrator.
 * CERTIFIED STATE → candidates → rank → primary → anchors → sections → gates → versioned payload
 *
 * activated remains false. No methodology change.
 */

import { createHash } from "crypto";
import {
  ADP_EXECUTIVE_READ_COMPOSITION_V3,
  COMPOSITION_V3_STATUS,
  EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED,
} from "../governance/adp-executive-read-composition-v3.js";
import { METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN } from "../governance/adp-methodology-governance-v1.js";
import { buildExecutiveReadInputV3, ADP_EXECUTIVE_READ_INPUT_V3 } from "./input-contract-v3.js";
import {
  generateExecutiveInsightCandidatesV3,
  ADP_EXECUTIVE_INSIGHT_CANDIDATE_GENERATION_V3,
} from "./insight-candidates-v3.js";
import {
  selectPrimaryInsightV3,
  ADP_EXECUTIVE_INSIGHT_PRIORITY_V3,
  ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY,
} from "./insight-priority-v3.js";
import { selectNumericAnchorsV3 } from "./numeric-anchors-v3.js";
import { composeExecutiveReadSectionsV3 } from "./section-composer-v3.js";
import { evaluateExecutiveReadV3QualityGates } from "./quality-gates-v3.js";
import {
  resolveExecutiveReadCompositionWritePolicyV3,
  HISTORICAL_IMMUTABILITY_POLICY_V3,
} from "./historical-immutability-v3.js";
import { buildExecutiveReadRendererViewModelV3 } from "./renderer-contract-v3.js";

export const ADP_EXECUTIVE_READ_V3 = "ADP_EXECUTIVE_READ_V3";

function hashObj(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex").slice(0, 16);
}

function buildEvidenceTrace(input, selection, anchors) {
  const trace = [];
  for (const m of selection.primary?.supportingMetrics || []) {
    trace.push({ kind: "metric", ref: m.metricId, value: m.value });
  }
  for (const t of selection.primary?.supportingTerritories || []) {
    trace.push({ kind: "territory", ref: t });
  }
  for (const c of selection.primary?.supportingCompetitors || []) {
    trace.push({ kind: "competitor", ref: c });
  }
  for (const g of selection.primary?.supportingRealityGaps || []) {
    trace.push({ kind: "realityGap", ref: g });
  }
  for (const a of anchors || []) {
    trace.push({ kind: "numericAnchor", ref: a.metricId, displayValue: a.displayValue });
  }
  if (input.evidenceRefs?.periodId) {
    trace.push({ kind: "period", ref: input.evidenceRefs.periodId });
  }
  return trace;
}

/**
 * Compose Executive Read V3 from certified analytical payload.
 * @param {object} publishedOrPayload
 * @param {{ market?: string, propertyId?: string, distributionStatus?: string, zeroCodePath?: boolean }=} opts
 */
export function composeExecutiveReadV3(publishedOrPayload, opts = {}) {
  const input = buildExecutiveReadInputV3(publishedOrPayload, opts);
  if (!input.ok) {
    return {
      ok: false,
      contract: ADP_EXECUTIVE_READ_V3,
      compositionVersion: ADP_EXECUTIVE_READ_COMPOSITION_V3,
      activated: false,
      reason: "EXECUTIVE_READ_REVIEW_REQUIRED",
      detail: input.reason || "MISSING_INPUT",
      methodologyChanged: false,
    };
  }

  const candidates = generateExecutiveInsightCandidatesV3(input);
  const selection = selectPrimaryInsightV3(candidates, input);
  if (!selection.ok) {
    return {
      ok: false,
      contract: ADP_EXECUTIVE_READ_V3,
      compositionVersion: ADP_EXECUTIVE_READ_COMPOSITION_V3,
      activated: false,
      reason: "EXECUTIVE_READ_REVIEW_REQUIRED",
      detail: selection.detail,
      candidates,
      methodologyChanged: false,
      failClosed: true,
    };
  }

  const anchorResult = selectNumericAnchorsV3(input, selection);
  const composedSections = composeExecutiveReadSectionsV3(input, selection, anchorResult);
  const evidenceTrace = buildEvidenceTrace(input, selection, anchorResult.anchors);
  const sourceSnapshotHash = hashObj(input.sourceSnapshotSeed);
  const compositionBody = {
    primaryIssueId: selection.primaryIssueId,
    insightArchetype: selection.insightArchetype,
    constraintClass: selection.constraintClass,
    sections: composedSections.sections,
    numericAnchors: anchorResult.anchors,
  };
  const compositionHash = hashObj(compositionBody);

  const writePolicy = resolveExecutiveReadCompositionWritePolicyV3({
    distributionStatus: opts.distributionStatus || "INTERNAL_ONLY",
    v3Activated: COMPOSITION_V3_STATUS.activated === true,
    allowNewEdition: opts.allowNewEdition === true,
  });

  const payload = {
    ok: true,
    contract: ADP_EXECUTIVE_READ_V3,
    compositionVersion: ADP_EXECUTIVE_READ_COMPOSITION_V3,
    compositionContract: ADP_EXECUTIVE_READ_COMPOSITION_V3,
    generatedAt: new Date().toISOString(),
    propertyId: input.property.propertyId,
    propertyName: input.property.name,
    periodId: input.period.periodId,
    primaryIssueId: selection.primaryIssueId,
    insightArchetype: selection.insightArchetype,
    constraintClass: selection.constraintClass,
    sections: composedSections.sections,
    numericAnchors: anchorResult.anchors,
    evidenceTrace,
    qualityGates: null, // filled below
    sourceSnapshotHash,
    compositionHash,
    activated: COMPOSITION_V3_STATUS.activated === true,
    customerFacingDefault: COMPOSITION_V3_STATUS.activated === true,
    zeroCodePath: opts.zeroCodePath !== false,
    usedHardcodedMap: false,
    historicalImmutability: {
      enforced: true,
      policy: HISTORICAL_IMMUTABILITY_POLICY_V3,
      writePolicy,
    },
    wordCount: composedSections.wordCount,
    watchOmitted: composedSections.watchOmitted,
    candidates: candidates.map((c) => ({
      candidateId: c.candidateId,
      archetype: c.archetype,
      priorityScore: selection.ranked.find((r) => r.candidateId === c.candidateId)?.priorityScore,
      summary: c.summary,
    })),
    selectionMeta: {
      rules: [
        ADP_EXECUTIVE_READ_INPUT_V3,
        ADP_EXECUTIVE_INSIGHT_CANDIDATE_GENERATION_V3,
        ADP_EXECUTIVE_INSIGHT_PRIORITY_V3,
        ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY,
      ],
      supportingCandidateIds: selection.supporting.map((s) => s.candidateId),
    },
    doctrine: [
      METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
      EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION_MEASUREMENT_REMAINS_GOVERNED,
    ],
    methodologyChanged: false,
  };

  payload.qualityGates = evaluateExecutiveReadV3QualityGates(payload, {
    usedHardcodedMap: false,
  });
  payload.rendererViewModel = buildExecutiveReadRendererViewModelV3(payload);

  if (!payload.qualityGates.claimDiscipline.pass) {
    return {
      ...payload,
      ok: false,
      reason: "EXECUTIVE_READ_REVIEW_REQUIRED",
      detail: "CLAIM_DISCIPLINE_FAILED",
      failClosed: true,
    };
  }

  return payload;
}

export {
  buildExecutiveReadInputV3,
  generateExecutiveInsightCandidatesV3,
  selectPrimaryInsightV3,
  selectNumericAnchorsV3,
  composeExecutiveReadSectionsV3,
  evaluateExecutiveReadV3QualityGates,
  resolveExecutiveReadCompositionWritePolicyV3,
  attachInactiveExecutiveReadV3,
} from "./reexports-v3.js";
