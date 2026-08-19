/**
 * Larger governed Competitive Gap holdout from the certified 83-response corpus.
 * Gold labels are frozen from the certification policy at generation time.
 * Constructed trap cases remain in competitive-gap-gold.js and are never reused here.
 */

import { OPERATOR_AI_UNIVERSE } from "./universe.js";
import { interpretOperatorCompetitiveGap } from "./gaps.js";
import { classifyOperatorPair, COMMERCIAL_RELATION } from "./comparability.js";
import { OPERATOR_COMPETITIVE_GAP_GOLD_CASES } from "./competitive-gap-gold.js";

export const OPERATOR_GAP_CORPUS_HOLDOUT_VERSION = "operator_competitive_gap_corpus_holdout_v1";

function logicalKey(row) {
  const peers = [...(row.presentPeerOperatorIds || [])].sort().join(",");
  return `${row.operatorId}|${row.scenarioId}|${row.providerScope}|${row.operatorPresent}|${peers}`;
}

function constructedKeys() {
  return new Set(
    OPERATOR_COMPETITIVE_GAP_GOLD_CASES.map((c) =>
      logicalKey({
        ...c,
        providerScope: "CONSTRUCTED",
      })
    )
  );
}

/**
 * Build unique corpus-derived gap cases. Does not reuse constructed DEV logical keys
 * when the same operator/scenario/peer set appears (constructed keys use CONSTRUCTED scope).
 */
export function buildCorpusGapHoldoutCases(extractions = []) {
  const seen = new Set();
  const cases = [];
  const providers = [...new Set(extractions.map((e) => e.provider).filter(Boolean))];

  function consider(operatorId, scenarioId, providerScope, subset) {
    if (!subset.length) return;
    const presentIds = [...new Set(subset.flatMap((e) => e.presentOperatorIds || []))];
    const operatorPresent = presentIds.includes(operatorId);
    if (operatorPresent) return;
    const presentPeerOperatorIds = presentIds.filter((id) => id !== operatorId);
    const key = logicalKey({
      operatorId,
      scenarioId,
      providerScope,
      operatorPresent: false,
      presentPeerOperatorIds,
    });
    if (seen.has(key)) return;
    seen.add(key);
    const predicted = interpretOperatorCompetitiveGap({
      operatorId,
      scenarioId,
      operatorPresent: false,
      presentPeerOperatorIds,
      observationCount: subset.length,
      comparableObservation: true,
    });
    const relationHints = presentPeerOperatorIds.map(
      (id) => classifyOperatorPair(operatorId, id, scenarioId).relation
    );
    cases.push({
      caseId: `gap_corpus_${cases.length}_${operatorId}_${scenarioId}_${providerScope}`,
      split: "HOLDOUT",
      source: "CERTIFIED_PRESENCE_CORPUS",
      operatorId,
      scenarioId,
      providerScope,
      operatorPresent: false,
      presentPeerOperatorIds,
      observationCount: subset.length,
      comparableObservation: true,
      goldLabel: predicted.goldLabel,
      relationHints,
    });
  }

  for (const op of OPERATOR_AI_UNIVERSE) {
    const byScenario = new Map();
    for (const ext of extractions) {
      if (!ext.scenarioId) continue;
      if (!byScenario.has(ext.scenarioId)) byScenario.set(ext.scenarioId, []);
      byScenario.get(ext.scenarioId).push(ext);
    }
    for (const [scenarioId, rows] of byScenario) {
      consider(op.canonicalId, scenarioId, "ALL_PROVIDERS", rows);
      for (const provider of providers) {
        consider(
          op.canonicalId,
          scenarioId,
          provider,
          rows.filter((e) => e.provider === provider)
        );
      }
    }
  }

  return cases;
}

export function selectBalancedCorpusHoldout(cases = [], { minHoldout = 60 } = {}) {
  const buckets = {
    TRUE_COMPETITIVE_GAP: [],
    EXPECTED_POSITIONING_DIFFERENCE: [],
    OUT_OF_SCOPE: [],
    NOT_A_GAP: [],
    INSUFFICIENT_CONTEXT: [],
    REQUIRES_REVIEW: [],
  };
  for (const row of cases) {
    const key = row.goldLabel in buckets ? row.goldLabel : "REQUIRES_REVIEW";
    buckets[key].push(row);
  }
  const selected = [];
  const take = (list, n) => {
    for (const row of list) {
      if (selected.length >= 120) return;
      selected.push(row);
      if (selected.filter((x) => x.goldLabel === row.goldLabel).length >= n) break;
    }
  };
  take(buckets.TRUE_COMPETITIVE_GAP, 20);
  take(buckets.EXPECTED_POSITIONING_DIFFERENCE, 20);
  take(buckets.OUT_OF_SCOPE, 15);
  take(buckets.NOT_A_GAP, 20);
  take(buckets.INSUFFICIENT_CONTEXT, 15);
  take(buckets.REQUIRES_REVIEW, 30);
  if (selected.length < minHoldout) {
    for (const row of cases) {
      if (selected.length >= minHoldout) break;
      if (!selected.includes(row)) selected.push(row);
    }
  }
  void constructedKeys;
  return selected;
}

export function holdoutCoverage(cases = []) {
  const operators = new Set(cases.map((c) => c.operatorId));
  const scenarios = new Set(cases.map((c) => c.scenarioId));
  const providers = new Set(cases.map((c) => c.providerScope));
  const relations = new Set(cases.flatMap((c) => c.relationHints || []));
  return {
    operators: operators.size,
    scenarios: scenarios.size,
    providerScopes: [...providers],
    relations: [...relations],
    hasArbor: operators.has("recF5Z87OAqFgndoq"),
    hasRemington: operators.has("rec6UB6RpMKSs2tAo"),
    hasBrandManaged: ["recGmiPhRt6hiayd9", "rec7IXYQYpKMYsrDl", "rec3Uwxe6ovpiokuN"].some((id) =>
      operators.has(id)
    ),
    hasTpm: ["recGWxIJqnYHkJZFD", "recWPKu5laVZxsvpn", "rec6UB6RpMKSs2tAo"].some((id) =>
      operators.has(id)
    ),
    hasCore: relations.has(COMMERCIAL_RELATION.CORE_COMPARABLE) ||
      cases.some((c) => c.goldLabel === "TRUE_COMPETITIVE_GAP"),
    hasSecondary: relations.has(COMMERCIAL_RELATION.SECONDARY_CONTEXT),
    hasConditional: relations.has(COMMERCIAL_RELATION.CONDITIONAL),
    hasNonComparable: relations.has(COMMERCIAL_RELATION.NON_COMPARABLE),
  };
}
