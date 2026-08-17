/**
 * Hierarchical recommendation classifier v5.
 * Deterministic evidence → Q1–Q6 node decisions → governed role.
 */

import {
  extractEntityLocalEvidence,
  buildTypedSections,
} from "../recommendation-evidence-v4_1.js";
import { questionStatusFromRecommendationRole } from "../recommendation-classifier-v4_1.js";
import {
  HIERARCHICAL_CLASSIFIER_VERSION,
  composeRoleFromNodePath,
} from "./tree.js";
import { decideNodeDeterministic } from "./node-decide.js";
import { adjudicateHierarchicalNode } from "./node-adjudicator.js";

function cueFacts(evidence) {
  const ev = evidence?.recommendationEvidence || {};
  return {
    directNegativeCue: Boolean(ev.directNegativeCue),
    directPositiveCue: Boolean(ev.directPositiveCue),
    sectionPositiveCue: Boolean(ev.sectionPositiveCue),
    leadCue: Boolean(ev.leadCue),
    rankCue: Boolean(ev.rankCue),
    considerationSetCue: Boolean(ev.considerationSetCue),
    comparatorCue: Boolean(ev.comparatorCue),
    descriptiveCue: Boolean(ev.descriptiveCue),
    incidentalCue: Boolean(ev.incidentalCue),
    sourceOnlyCue: Boolean(ev.sourceOnlyCue),
    sectionType: evidence?.sectionType || null,
    confirmedRankStructure: Boolean(
      evidence?.confirmedRankStructure || evidence?.structure?.confirmedRankStructure
    ),
  };
}

/**
 * Run hierarchical classification for one entity mention.
 * @param {object} args
 * @param {boolean} [args.callAdjudicator=true]
 * @param {number} [args.remainingCallBudget] - stop semantic calls when 0
 */
export async function classifyHierarchicalRecommendation(args = {}) {
  const {
    text,
    start,
    end,
    rawMention,
    canonicalEntityName,
    canonicalEntityId,
    entityPresent = true,
    typedSections,
    callAdjudicator = true,
    remainingCallBudget = Infinity,
    adjudicatorOptions = {},
  } = args;

  const sections = typedSections || buildTypedSections(String(text || ""));
  const evidence = entityPresent
    ? extractEntityLocalEvidence({
        text,
        start,
        end,
        rawMention,
        canonicalEntityName,
        canonicalEntityId,
        typedSections: sections,
      })
    : null;

  const path = {};
  const nodeTrace = [];
  let calls = 0;
  let cost = 0;
  let budget = remainingCallBudget;

  async function resolveNode(nodeId) {
    const det = decideNodeDeterministic(nodeId, evidence, { entityPresent });
    const rec = {
      CASE_ID: args.caseId || null,
      NODE: nodeId,
      INPUT_EVIDENCE: cueFacts(evidence),
      DETERMINISTIC_RESULT: det.result,
      ADJUDICATOR_NEEDED: det.needsAdjudicator,
      ADJUDICATOR_RESULT: null,
      FINAL_NODE_RESULT: det.result,
      reason: det.reason,
      ambiguityReasons: det.ambiguityReasons,
    };

    if (!det.needsAdjudicator && det.result) {
      nodeTrace.push(rec);
      path[nodeId] = det.result;
      return det.result;
    }

    if (!callAdjudicator || budget <= 0) {
      rec.FINAL_NODE_RESULT = null;
      rec.abstained = true;
      nodeTrace.push(rec);
      path[nodeId] = null;
      return null;
    }

    const adj = await adjudicateHierarchicalNode({
      nodeId,
      entityName: canonicalEntityName,
      entityLocalEvidence: String(evidence?.localListItem || evidence?.localSentence || "").slice(
        0,
        1000
      ),
      sectionHeading: evidence?.sectionHeading || evidence?.parentHeading,
      cueFacts: cueFacts(evidence),
      structuralEvidence: evidence?.structure || {},
      ...adjudicatorOptions,
    });

    calls += adj.LIVE_PROVIDER_CALL ? 1 : 0;
    cost += Number(adj.actualCostUsd || 0);
    if (adj.LIVE_PROVIDER_CALL) budget -= 1;

    if (adj.ok) {
      rec.ADJUDICATOR_RESULT = adj.selected;
      rec.FINAL_NODE_RESULT = adj.selected;
      path[nodeId] = adj.selected;
    } else {
      rec.ADJUDICATOR_RESULT = null;
      rec.FINAL_NODE_RESULT = null;
      rec.abstained = true;
      rec.adjudicationErrors = adj.errors || [adj.error || adj.code];
      path[nodeId] = null;
    }
    nodeTrace.push(rec);
    return path[nodeId];
  }

  // Q1
  const q1 = await resolveNode("Q1");
  if (q1 === "ABSENT" || q1 === "SOURCE_ONLY" || q1 === "INCIDENTAL") {
    const role = composeRoleFromNodePath(path);
    return finish(role, path, nodeTrace, calls, cost, evidence);
  }
  if (!q1) return finish(null, path, nodeTrace, calls, cost, evidence, true);

  // Q2
  const q2 = await resolveNode("Q2");
  if (q2 === "YES_NEGATIVE") {
    return finish(composeRoleFromNodePath(path), path, nodeTrace, calls, cost, evidence);
  }
  if (!q2) return finish(null, path, nodeTrace, calls, cost, evidence, true);

  // Q3
  const q3 = await resolveNode("Q3");
  if (q3 === "COMPARATOR" || q3 === "NEUTRAL_DISCUSSION") {
    return finish(composeRoleFromNodePath(path), path, nodeTrace, calls, cost, evidence);
  }
  if (!q3) return finish(null, path, nodeTrace, calls, cost, evidence, true);

  // Q4
  const q4 = await resolveNode("Q4");
  if (q4 === "CONSIDERATION_SET") {
    return finish(composeRoleFromNodePath(path), path, nodeTrace, calls, cost, evidence);
  }
  if (!q4) return finish(null, path, nodeTrace, calls, cost, evidence, true);

  // Q5
  const q5 = await resolveNode("Q5");
  if (q5 === "NO_MEANINGFUL_ORDER") {
    return finish(composeRoleFromNodePath(path), path, nodeTrace, calls, cost, evidence);
  }
  if (!q5) return finish(null, path, nodeTrace, calls, cost, evidence, true);

  // Q6
  const q6 = await resolveNode("Q6");
  if (!q6) return finish(null, path, nodeTrace, calls, cost, evidence, true);
  return finish(composeRoleFromNodePath(path), path, nodeTrace, calls, cost, evidence);
}

function finish(role, path, nodeTrace, calls, cost, evidence, abstained = false) {
  // Validate composition
  let compositionError = null;
  if (role == null && !abstained) compositionError = "null_role";
  if (role && composeRoleFromNodePath(path) !== role) compositionError = "path_role_mismatch";

  return {
    role: abstained ? null : role,
    finalRole: abstained ? null : role,
    abstained: Boolean(abstained || role == null),
    path,
    nodeTrace,
    semanticCalls: calls,
    actualCostUsd: cost,
    evidence,
    questionStatus:
      role != null ? questionStatusFromRecommendationRole(role, true) : null,
    classifierVersion: HIERARCHICAL_CLASSIFIER_VERSION,
    compositionError,
  };
}

export { cueFacts };
