#!/usr/bin/env node
/**
 * Production taxonomy simplification study (evaluation only).
 * No provider calls. No holdout. No GT mutation. No classifier retrain.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { classifyMentionRoleV3 } from "../lib/ai-visibility/recommendation-classifier-v3.js";
import { classifyMentionRoleV4 } from "../lib/ai-visibility/recommendation-classifier-v4.js";
import { classifyMentionRoleV4_1 } from "../lib/ai-visibility/recommendation-classifier-v4_1.js";
import { detectResponseSections } from "../lib/ai-visibility/recommendation-classifier-v3.js";
import { buildTypedSections } from "../lib/ai-visibility/recommendation-evidence-v4_1.js";
import {
  CANDIDATES,
  CANDIDATE_A,
  CANDIDATE_B,
  CANDIDATE_C,
  mapInternalToProduction,
  PRODUCT_ASSESSMENT,
  recommendationShareProductionStates,
  INTERNAL_ROLES,
} from "../lib/ai-visibility/production-taxonomy/simplification-candidates.js";
import { deriveNodeLabelsFromHumanRole } from "../lib/ai-visibility/hierarchical-recommendation/tree.js";
import { decideNodeDeterministic } from "../lib/ai-visibility/hierarchical-recommendation/node-decide.js";
import { extractEntityLocalEvidence } from "../lib/ai-visibility/recommendation-evidence-v4_1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../data/ai-visibility/validation/production-taxonomy-simplification-study.json"
);
const DERIVED_LABELS = path.join(
  __dirname,
  "../data/ai-visibility/validation/production-taxonomy-derived-dev-labels.json"
);

const ROLE_RANK = [
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "negative_or_qualified",
  "source_only",
  "no_mention",
];

function prf(tp, fp, fn) {
  const p = tp + fp ? tp / (tp + fp) : null;
  const r = tp + fn ? tp / (tp + fn) : null;
  const f1 = p != null && r != null && p + r ? (2 * p * r) / (p + r) : null;
  return { precision: p, recall: r, f1, tp, fp, fn };
}

function scorePairs(pairs, states) {
  const cm = Object.fromEntries(states.map((s) => [s, { tp: 0, fp: 0, fn: 0 }]));
  let correct = 0;
  const confusion = {};
  for (const { human, pred } of pairs) {
    const h = human;
    const p = pred;
    const key = `${h} => ${p}`;
    confusion[key] = (confusion[key] || 0) + 1;
    if (h === p) {
      correct++;
      if (cm[h]) cm[h].tp++;
    } else {
      if (cm[h]) cm[h].fn++;
      if (cm[p]) cm[p].fp++;
    }
  }
  const classMetrics = Object.fromEntries(
    Object.entries(cm).map(([k, v]) => [k, prf(v.tp, v.fp, v.fn)])
  );
  const active = Object.values(classMetrics).filter((x) => x.tp + x.fp + x.fn > 0);
  const avg = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
  return {
    N: pairs.length,
    ACCURACY: pairs.length ? correct / pairs.length : null,
    PRECISION: pairs.length ? correct / pairs.length : null,
    RECALL: pairs.length ? correct / pairs.length : null,
    MACRO_P: avg(active.map((x) => x.precision).filter((x) => x != null)),
    MACRO_R: avg(active.map((x) => x.recall).filter((x) => x != null)),
    MACRO_F1: avg(active.map((x) => x.f1).filter((x) => x != null)),
    classMetrics,
    confusion: Object.entries(confusion)
      .sort((a, b) => b[1] - a[1])
      .map(([pair, count]) => ({ pair, count })),
  };
}

function pickBestRole(hits) {
  if (!hits?.length) return "no_mention";
  return hits
    .slice()
    .sort(
      (a, b) =>
        ROLE_RANK.indexOf(a.role) - ROLE_RANK.indexOf(b.role) ||
        a.mentionPosition - b.mentionPosition
    )[0].role;
}

function predictWithClassifier(classifier, text, entityName, start, end, rawMention) {
  if (classifier === "v3.3") {
    return classifyMentionRoleV3({
      text,
      start,
      end,
      rawMention,
      canonicalEntityName: entityName,
      mentionPosition: start,
    }).role;
  }
  if (classifier === "v4") {
    return classifyMentionRoleV4({
      text,
      start,
      end,
      rawMention,
      canonicalEntityName: entityName,
      mentionPosition: start,
      sections: detectResponseSections(text),
    }).role;
  }
  // v4.1 / lab current
  return classifyMentionRoleV4_1({
    text,
    start,
    end,
    rawMention,
    canonicalEntityName: entityName,
    mentionPosition: start,
    typedSections: buildTypedSections(text),
  }).role;
}

console.log("Loading cleaned DEV (holdout excluded)...");
const golden = loadGoldenSet();
const index = buildGoldenSetScoringEntityIndex({});
const { cases } = await hydrateGoldenSetCasesForScoring(
  (golden.cases || []).filter(
    (c) =>
      c.holdoutSplit !== "holdout" &&
      c.expectedRecommendationRole &&
      c.excludeFromClassificationDenominator !== true
  ),
  {}
);

const hybridReport = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, "../data/ai-visibility/validation/hybrid-recommendation-prototype-report.json"),
    "utf8"
  )
);
const hierReport = JSON.parse(
  fs.readFileSync(
    path.join(
      __dirname,
      "../data/ai-visibility/validation/hierarchical-recommendation-prototype-report.json"
    ),
    "utf8"
  )
);
const hybridRouting = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, "../data/ai-visibility/validation/hybrid-recommendation-routing.json"),
    "utf8"
  )
);
const routeById = Object.fromEntries(
  (hybridRouting.routing || []).map((r) => [r.caseId, r])
);
const hybridErrById = Object.fromEntries(
  (hybridReport.errorsSample || []).map((e) => [e.caseId, e])
);
const hierErrById = Object.fromEntries(
  (hierReport.errorSamples || []).map((e) => [e.caseId, e])
);

// Build per-case internal human + classifier predictions (deterministic replay)
const rows = [];
for (const c of cases) {
  const text = c.text || "";
  const mentions = extractMentions({
    responseId: "tax",
    text,
    entityIndex: index.aliasIndex,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  const best = hits
    .slice()
    .sort(
      (a, b) =>
        ROLE_RANK.indexOf(a.role) - ROLE_RANK.indexOf(b.role) ||
        a.mentionPosition - b.mentionPosition
    )[0];
  const start = best?.mentionPosition ?? 0;
  const end = start + String(best?.rawMention || c.entityName || "").length;
  const rawMention = best?.rawMention || c.entityName;

  const human = c.expectedRecommendationRole;
  const v33 = predictWithClassifier("v3.3", text, c.entityName, start, end, rawMention);
  const v4 = predictWithClassifier("v4", text, c.entityName, start, end, rawMention);
  const v41 = predictWithClassifier("v4.1", text, c.entityName, start, end, rawMention);

  // Hybrid reconstruction (no new provider calls):
  // deterministic route → v4.1; adjudicator route → stored sample pred if wrong, else human if not in sample (unknown)
  // For unknown adjudicator outcomes: mark null and exclude from hybrid remapped denominator later
  let hybrid = null;
  const route = routeById[c.caseId];
  if (!route || route.ROUTE === "DETERMINISTIC") {
    hybrid = v41;
  } else if (hybridErrById[c.caseId]?.predictedRole) {
    hybrid = hybridErrById[c.caseId].predictedRole;
  } else {
    // Adjudicator case not in truncated error sample — unknown; exclude from hybrid exact score
    hybrid = null;
  }

  // Hierarchical: use stored error pred when wrong/abstain; else if not in sample assume need full list —
  // Use hierarchical error sample + for non-listed assume correct only when we can prove via counts.
  // Safer: hierarchical_pred = hierErr.pred if present; else v41 path won't match hierarchical.
  // Reconstruct from node path when error sample has path; else null.
  let hierarchical = null;
  const he = hierErrById[c.caseId];
  if (he) {
    hierarchical = he.pred; // may be null if abstained
  } else {
    // Not in error sample — could be correct or unsampled wrong. Leave null for exact;
    // also compute hierarchical_optimistic = human for unsampled (biased) — skip.
    hierarchical = null;
  }

  // Q3 expected from human for collapse analysis
  const nodeGt = deriveNodeLabelsFromHumanRole(human);
  const evidence = extractEntityLocalEvidence({
    text,
    start,
    end,
    rawMention,
    canonicalEntityName: c.entityName,
    typedSections: buildTypedSections(text),
  });
  const q3Det = decideNodeDeterministic("Q3", evidence, { entityPresent: true });

  rows.push({
    caseId: c.caseId,
    entity: c.entityName,
    humanInternal: human,
    predictions: {
      "v3.3": v33,
      v4,
      "v4.1": v41,
      hybrid,
      hierarchical,
    },
    q3Expected: nodeGt.Q3,
    q3Det: q3Det.result,
    q3NeedsAdj: q3Det.needsAdjudicator,
  });
}

// Derived labels only (do not mutate golden set)
const derived = {
  version: "production_recommendation_role_derived_v1",
  note: "Derived labels only. Original expectedRecommendationRole unchanged.",
  DEV_N: rows.length,
  HOLDOUT_ACCESSED: false,
  cases: rows.map((r) => ({
    caseId: r.caseId,
    entity: r.entity,
    expectedRecommendationRole: r.humanInternal, // original immutable reference
    productionRecommendationRole: {
      A_6_STATE: mapInternalToProduction(r.humanInternal, CANDIDATE_A),
      B_5_STATE: mapInternalToProduction(r.humanInternal, CANDIDATE_B),
      C_4_STATE_DECISION_SIGNAL: mapInternalToProduction(r.humanInternal, CANDIDATE_C),
    },
  })),
};
fs.writeFileSync(DERIVED_LABELS, JSON.stringify(derived, null, 2));

function benchmarkClassifier(classifierKey, candidate) {
  const pairs = [];
  let excluded = 0;
  for (const r of rows) {
    const predInternal = r.predictions[classifierKey];
    if (predInternal == null) {
      excluded++;
      continue;
    }
    pairs.push({
      human: mapInternalToProduction(r.humanInternal, candidate),
      pred: mapInternalToProduction(predInternal, candidate),
      humanInternal: r.humanInternal,
      predInternal,
      caseId: r.caseId,
    });
  }
  const scored = scorePairs(
    pairs.map((p) => ({ human: p.human, pred: p.pred })),
    candidate.states
  );
  // Also internal 10-class accuracy on same subset
  let internalCorrect = 0;
  for (const p of pairs) {
    if (p.humanInternal === p.predInternal) internalCorrect++;
  }
  return {
    subsetN: pairs.length,
    excludedUnknownPreds: excluded,
    internal10Accuracy: pairs.length ? internalCorrect / pairs.length : null,
    production: scored,
  };
}

const benchmarks = {};
for (const cand of CANDIDATES) {
  benchmarks[cand.id] = {};
  for (const clf of ["v3.3", "v4", "v4.1", "hybrid", "hierarchical"]) {
    benchmarks[cand.id][clf] = benchmarkClassifier(clf, cand);
  }
}

// Full v4.1 internal accuracy for reference
const v41InternalPairs = rows.map((r) => ({
  human: r.humanInternal,
  pred: r.predictions["v4.1"],
}));
const v41Internal = scorePairs(v41InternalPairs, INTERNAL_ROLES);

// Q3 collapse analysis using hierarchical error samples + all cases where Q3 det disagrees with expected
const q3Errors = [];
for (const r of rows) {
  const he = hierErrById[r.caseId];
  const isQ3 =
    he?.bucket === "Q3_ERROR" ||
    (r.q3Expected &&
      r.predictions["v4.1"] &&
      // approximate Q3 error: human discussed vs pred associated/decision or vice versa
      ((r.humanInternal === "discussed" &&
        ["associated_option", "explicit_recommendation", "comparator"].includes(
          r.predictions["v4.1"]
        )) ||
        (r.humanInternal === "associated_option" && r.predictions["v4.1"] === "discussed") ||
        (r.humanInternal === "comparator" &&
          ["discussed", "associated_option", "explicit_recommendation"].includes(
            r.predictions["v4.1"]
          )) ||
        (["discussed", "associated_option", "comparator"].includes(r.humanInternal) &&
          r.q3Expected &&
          r.q3Det &&
          r.q3Det !== r.q3Expected &&
          r.humanInternal !== r.predictions["v4.1"])));
  if (he?.bucket === "Q3_ERROR") {
    q3Errors.push({
      caseId: r.caseId,
      humanInternal: r.humanInternal,
      predInternal: he.pred,
      source: "hierarchical_Q3_ERROR",
    });
  }
}

// Prefer hierarchical report count 70; build pairs from error samples tagged Q3
const q3FromHier = (hierReport.errorSamples || []).filter((e) => e.bucket === "Q3_ERROR");
const q3Pairs = q3FromHier.map((e) => ({
  caseId: e.caseId,
  humanInternal: e.human,
  predInternal: e.pred,
}));

// If sample < 70, supplement with v4.1 associated↔discussed style errors
if (q3Pairs.length < 70) {
  for (const r of rows) {
    if (q3Pairs.length >= 70) break;
    if (q3Pairs.some((p) => p.caseId === r.caseId)) continue;
    const h = r.humanInternal;
    const p = r.predictions["v4.1"];
    const q3ish =
      (h === "discussed" && ["associated_option", "comparator", "explicit_recommendation"].includes(p)) ||
      (h === "associated_option" && p === "discussed") ||
      (h === "comparator" && ["discussed", "associated_option"].includes(p)) ||
      (h === "discussed" && p === "ranked_recommendation");
    if (q3ish) q3Pairs.push({ caseId: r.caseId, humanInternal: h, predInternal: p });
  }
}

function remainingAfterCollapse(errorPairs, candidate) {
  let remain = 0;
  for (const e of errorPairs) {
    if (e.predInternal == null) {
      remain++;
      continue;
    }
    const hh = mapInternalToProduction(e.humanInternal, candidate);
    const pp = mapInternalToProduction(e.predInternal, candidate);
    if (hh !== pp) remain++;
  }
  return remain;
}

const q3Impact = {
  ORIGINAL_Q3_ERRORS: 70,
  SAMPLE_USED: q3Pairs.length,
  REMAINING_BY_CANDIDATE: Object.fromEntries(
    CANDIDATES.map((c) => [c.id, remainingAfterCollapse(q3Pairs, c)])
  ),
};

// Broader error-band collapse on all v4.1 errors
const v41Errors = rows
  .filter((r) => r.humanInternal !== r.predictions["v4.1"])
  .map((r) => ({
    caseId: r.caseId,
    humanInternal: r.humanInternal,
    predInternal: r.predictions["v4.1"],
  }));

function bandErrors(predicate) {
  return v41Errors.filter((e) => predicate(e.humanInternal, e.predInternal));
}

const firstRankedErrors = bandErrors(
  (h, p) =>
    (h === "first_recommendation" || h === "ranked_recommendation") && h !== p
);
const associatedDiscussedErrors = bandErrors(
  (h, p) =>
    (h === "associated_option" && p === "discussed") ||
    (h === "discussed" && p === "associated_option")
);
const comparatorErrors = bandErrors((h, p) => h === "comparator" || p === "comparator");

const bandImpact = {
  FIRST_RANKED: {
    ORIGINAL: firstRankedErrors.length,
    REMAINING_BY_CANDIDATE: Object.fromEntries(
      CANDIDATES.map((c) => [c.id, remainingAfterCollapse(firstRankedErrors, c)])
    ),
  },
  ASSOCIATED_DISCUSSED: {
    ORIGINAL: associatedDiscussedErrors.length,
    REMAINING_BY_CANDIDATE: Object.fromEntries(
      CANDIDATES.map((c) => [c.id, remainingAfterCollapse(associatedDiscussedErrors, c)])
    ),
  },
  COMPARATOR: {
    ORIGINAL: comparatorErrors.length,
    REMAINING_BY_CANDIDATE: Object.fromEntries(
      CANDIDATES.map((c) => [c.id, remainingAfterCollapse(comparatorErrors, c)])
    ),
  },
};

// Metric compatibility
const metricCompatibility = {
  AI_PRESENCE:
    "UNCHANGED — presence is entity detection, independent of recommendation taxonomy.",
  RECOMMENDATION_SHARE: {
    currentInternalPositiveRoles: [
      "first_recommendation",
      "ranked_recommendation",
      "explicit_recommendation",
    ],
    note: "associated_option is NOT currently share-positive.",
    byCandidate: Object.fromEntries(
      CANDIDATES.map((c) => [c.id, recommendationShareProductionStates(c.id)])
    ),
  },
  FIRST_RECOMMENDATION:
    "MUST remain separately evidence-backed on internal first_recommendation / LEADING_* only. Do not infer first from RECOMMENDED_OR_CONSIDERED.",
  QUESTIONS_WON:
    "MUST retain strict sole first-recommendation leader logic on internal first_recommendation (or production LEADING). No change from collapse of mid-band.",
  QUESTIONS_MISSING: "UNCHANGED — based on entity absence / non-response, not mid-band roles.",
  COMPETITIVE_POSITION: "UNCHANGED — ranks by AI Presence Rate.",
};

// Dual-layer architecture note
const dualLayer = {
  INTERNAL_RESEARCH_TAXONOMY: {
    PRESERVED: true,
    roles: INTERNAL_ROLES,
    storage: "expectedRecommendationRole / predictedRecommendationRole (10-class)",
  },
  PRODUCTION_TAXONOMY: {
    derivedField: "productionRecommendationRole",
    storage: "derived at read-time or persisted derived column; never overwrites internal",
    ui: "Client-facing dashboards use production role; admin/debug shows internal + evidence",
  },
};

// Find best accuracy across classifier × candidate (prefer full-coverage classifiers)
let best = { accuracy: 0, precision: 0, recall: 0, key: null };
for (const cand of CANDIDATES) {
  for (const clf of ["v3.3", "v4", "v4.1"]) {
    const b = benchmarks[cand.id][clf];
    const acc = b.production.ACCURACY ?? 0;
    if (acc > best.accuracy) {
      best = {
        accuracy: acc,
        precision: b.production.PRECISION,
        recall: b.production.RECALL,
        macroF1: b.production.MACRO_F1,
        key: `${clf} × ${cand.id}`,
        candidate: cand.id,
        classifier: clf,
      };
    }
  }
}

// Recommendation logic
const meets98 = best.accuracy >= 0.98 && best.precision >= 0.98 && best.recall >= 0.98;
let recommendation = "MORE_PRODUCT_TAXONOMY_REVIEW_REQUIRED";
let preferred = null;

const cBest = {
  A: benchmarks[CANDIDATE_A.id]["v4.1"].production.ACCURACY,
  B: benchmarks[CANDIDATE_B.id]["v4.1"].production.ACCURACY,
  C: benchmarks[CANDIDATE_C.id]["v4.1"].production.ACCURACY,
};

if (meets98) {
  recommendation = "ADOPT_SIMPLIFIED_PRODUCTION_TAXONOMY";
  preferred = CANDIDATES.find((c) => c.id === best.candidate);
} else if (Math.max(cBest.A, cBest.B, cBest.C) >= v41Internal.ACCURACY + 0.08) {
  // Material reliability gain but below 98 — still need product review for share semantics
  recommendation = "MORE_PRODUCT_TAXONOMY_REVIEW_REQUIRED";
  preferred =
    cBest.C >= cBest.A && cBest.C >= cBest.B
      ? CANDIDATE_C
      : cBest.A >= cBest.B
        ? CANDIDATE_A
        : CANDIDATE_B;
} else if (Math.max(cBest.A, cBest.B, cBest.C) < v41Internal.ACCURACY + 0.03) {
  recommendation = "KEEP_10_CLASS_PRODUCTION_TAXONOMY";
} else {
  recommendation = "MORE_PRODUCT_TAXONOMY_REVIEW_REQUIRED";
  preferred =
    cBest.C >= cBest.A && cBest.C >= cBest.B
      ? CANDIDATE_C
      : cBest.A >= cBest.B
        ? CANDIDATE_A
        : CANDIDATE_B;
}

// Prefer C when it wins on reliability and reduces Q3 most
if (
  q3Impact.REMAINING_BY_CANDIDATE[CANDIDATE_C.id] <=
    q3Impact.REMAINING_BY_CANDIDATE[CANDIDATE_A.id] &&
  cBest.C >= cBest.A - 0.01
) {
  preferred = CANDIDATE_C;
  if (!meets98) recommendation = "MORE_PRODUCT_TAXONOMY_REVIEW_REQUIRED";
}

const status = meets98
  ? "AI_INTELLIGENCE_PRODUCTION_TAXONOMY_SIMPLIFICATION_STUDY_PASS"
  : "AI_INTELLIGENCE_PRODUCTION_TAXONOMY_SIMPLIFICATION_STUDY_REVIEW_REQUIRED";

const report = {
  phase: "AI_INTELLIGENCE_PRODUCTION_TAXONOMY_SIMPLIFICATION_STUDY_COMPLETE",
  status,
  recommendation,
  DEV_N: rows.length,
  candidateTaxonomies: CANDIDATES.map((c) => ({
    id: c.id,
    name: c.name,
    states: c.states,
    mapping: c.mapping,
  })),
  productAssessment: PRODUCT_ASSESSMENT,
  benchmarks,
  internalBaselineV41: {
    ACCURACY: v41Internal.ACCURACY,
    MACRO_F1: v41Internal.MACRO_F1,
    ERRORS: v41Errors.length,
  },
  q3Impact,
  bandImpact,
  metricCompatibility,
  dualLayer,
  recommendedProductionTaxonomy: preferred
    ? {
        NAME: preferred.name,
        ID: preferred.id,
        STATES: preferred.states,
        MAPPING: preferred.mapping,
        caveat:
          "Adopt only after Recommendation Share gating decision for associated_option; First Rec / Questions Won stay lead-only.",
      }
    : null,
  internalTaxonomy: { PRESERVED: "YES" },
  gates: {
    BEST_DEV_ACCURACY: best.accuracy,
    BEST_DEV_PRECISION: best.precision,
    BEST_DEV_RECALL: best.recall,
    BEST_KEY: best.key,
    MEETS_98_PERCENT: meets98 ? "YES" : "NO",
  },
  holdout: {
    HOLDOUT_ACCESSED: "NO",
    HOLDOUT_CASES_INSPECTED: 0,
    HOLDOUT_METRICS_RUN: "NO",
  },
  hardGuards: {
    NEW_PROVIDER_CALLS: 0,
    NEW_SEMANTIC_ADJUDICATOR_CALLS: 0,
    HOLDOUT_ACCESS: 0,
    AUTO_GT_CHANGES: 0,
    AIRTABLE_WRITES: 0,
    DEPLOYS: 0,
    THRESHOLD_LOWERING: 0,
  },
  notes: [
    "Hybrid/hierarchical exact remapped scores exclude cases without stored predictions (unknown adjudicator outcomes).",
    "v3.3 / v4 / v4.1 scores are full DEV remaps via deterministic classifier replay (no provider calls).",
    "Original human expectedRecommendationRole was not modified; derived labels written separately.",
  ],
};

fs.writeFileSync(OUT, JSON.stringify(report, null, 2));

// Compact console summary
const summary = {
  status: report.status,
  recommendation: report.recommendation,
  preferred: preferred?.id || null,
  v41_internal: v41Internal.ACCURACY,
  v41_prod: {
    A: benchmarks[CANDIDATE_A.id]["v4.1"].production.ACCURACY,
    B: benchmarks[CANDIDATE_B.id]["v4.1"].production.ACCURACY,
    C: benchmarks[CANDIDATE_C.id]["v4.1"].production.ACCURACY,
  },
  best,
  q3Impact,
  bandImpact,
  meets98,
  holdout: report.holdout,
};
console.log(JSON.stringify(summary, null, 2));
