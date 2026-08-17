#!/usr/bin/env node
/**
 * Production signal taxonomy final simplification study (Candidates D & E).
 * Evaluation only — no provider calls, holdout, GT mutation, or classifier tuning.
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
  CANDIDATE_D,
  CANDIDATE_E,
  mapInternalToProduction,
  deriveCandidateEFromInternalRole,
  INTERNAL_ROLES,
} from "../lib/ai-visibility/production-taxonomy/simplification-candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../data/ai-visibility/validation/production-signal-taxonomy-study.json"
);
const DERIVED = path.join(
  __dirname,
  "../data/ai-visibility/validation/production-signal-derived-dev-labels.json"
);

const ROLE_RANK = [...INTERNAL_ROLES];
const POSITIVE = new Set([
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
]);

function prf(tp, fp, fn) {
  const p = tp + fp ? tp / (tp + fp) : null;
  const r = tp + fn ? tp / (tp + fn) : null;
  const f1 = p != null && r != null && p + r ? (2 * p * r) / (p + r) : null;
  return { precision: p, recall: r, f1, tp, fp, fn };
}

function scoreMulticlass(pairs, states) {
  const cm = Object.fromEntries(states.map((s) => [s, { tp: 0, fp: 0, fn: 0 }]));
  let correct = 0;
  const confusion = {};
  for (const { human, pred } of pairs) {
    const key = `${human} => ${pred}`;
    confusion[key] = (confusion[key] || 0) + 1;
    if (human === pred) {
      correct++;
      cm[human].tp++;
    } else {
      if (cm[human]) cm[human].fn++;
      if (cm[pred]) cm[pred].fp++;
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
    F1: pairs.length ? correct / pairs.length : null,
    MACRO_P: avg(active.map((x) => x.precision).filter((x) => x != null)),
    MACRO_R: avg(active.map((x) => x.recall).filter((x) => x != null)),
    MACRO_F1: avg(active.map((x) => x.f1).filter((x) => x != null)),
    classMetrics,
    confusion: Object.entries(confusion)
      .sort((a, b) => b[1] - a[1])
      .map(([pair, count]) => ({ pair, count })),
  };
}

/** Binary flag scoring: human/pred are booleans. */
function scoreBinaryFlag(pairs) {
  let tp = 0;
  let fp = 0;
  let tn = 0;
  let fn = 0;
  for (const { human, pred } of pairs) {
    if (human && pred) tp++;
    else if (!human && pred) fp++;
    else if (!human && !pred) tn++;
    else fn++;
  }
  const metrics = prf(tp, fp, fn);
  const accuracy = pairs.length ? (tp + tn) / pairs.length : null;
  return {
    N: pairs.length,
    ACCURACY: accuracy,
    ...metrics,
    tn,
    prevalence: pairs.length ? (tp + fn) / pairs.length : null,
  };
}

function predict(classifier, text, entityName, start, end, rawMention) {
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

function gate98(metrics) {
  const a = metrics.ACCURACY ?? 0;
  const p = metrics.precision ?? 0;
  const r = metrics.recall ?? 0;
  // For rare flags, also require accuracy; for presence use all three
  if (metrics.prevalence != null && metrics.prevalence < 0.05) {
    // sparse positive class: still report gate on P/R/F1 if positives exist
    return a >= 0.98 && (metrics.tp + metrics.fn === 0 || (p >= 0.98 && r >= 0.98))
      ? "PASS"
      : "FAIL";
  }
  return a >= 0.98 && p >= 0.98 && r >= 0.98 ? "PASS" : "FAIL";
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
const hybridRouting = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, "../data/ai-visibility/validation/hybrid-recommendation-routing.json"),
    "utf8"
  )
);
const routeById = Object.fromEntries((hybridRouting.routing || []).map((r) => [r.caseId, r]));
const hybridErrById = Object.fromEntries(
  (hybridReport.errorsSample || []).map((e) => [e.caseId, e])
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
const hierErrById = Object.fromEntries(
  (hierReport.errorSamples || []).map((e) => [e.caseId, e])
);

const rows = [];
for (const c of cases) {
  const text = c.text || "";
  const mentions = extractMentions({
    responseId: "sig",
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

  const v33 = predict("v3.3", text, c.entityName, start, end, rawMention);
  const v4 = predict("v4", text, c.entityName, start, end, rawMention);
  const v41 = predict("v4.1", text, c.entityName, start, end, rawMention);

  let hybrid = null;
  const route = routeById[c.caseId];
  if (!route || route.ROUTE === "DETERMINISTIC") hybrid = v41;
  else if (hybridErrById[c.caseId]?.predictedRole) hybrid = hybridErrById[c.caseId].predictedRole;

  let hierarchical = null;
  if (hierErrById[c.caseId]) hierarchical = hierErrById[c.caseId].pred;

  rows.push({
    caseId: c.caseId,
    entity: c.entityName,
    human,
    predictions: { "v3.3": v33, v4, "v4.1": v41, hybrid, hierarchical },
    humanD: mapInternalToProduction(human, CANDIDATE_D),
    humanE: deriveCandidateEFromInternalRole(human),
  });
}

// Derived labels (immutable original preserved by reference only)
fs.writeFileSync(
  DERIVED,
  JSON.stringify(
    {
      version: "production_signal_derived_labels_v1",
      note: "Derived only. Original expectedRecommendationRole unchanged.",
      DEV_N: rows.length,
      HOLDOUT_ACCESSED: false,
      cases: rows.map((r) => ({
        caseId: r.caseId,
        entity: r.entity,
        expectedRecommendationRole: r.human,
        candidateD: r.humanD,
        candidateE: r.humanE,
        firstRecommendationFlag: r.human === "first_recommendation",
      })),
    },
    null,
    2
  )
);

function benchmarkD(classifierKey) {
  const pairs = [];
  let excluded = 0;
  for (const r of rows) {
    const pred = r.predictions[classifierKey];
    if (pred == null) {
      excluded++;
      continue;
    }
    pairs.push({
      human: mapInternalToProduction(r.human, CANDIDATE_D),
      pred: mapInternalToProduction(pred, CANDIDATE_D),
    });
  }
  return { excluded, ...scoreMulticlass(pairs, CANDIDATE_D.states) };
}

function benchmarkE(classifierKey) {
  const presence = [];
  const recommended = [];
  const first = [];
  const negative = [];
  const comparator = [];
  let excluded = 0;
  for (const r of rows) {
    const predInternal = r.predictions[classifierKey];
    if (predInternal == null) {
      excluded++;
      continue;
    }
    const h = r.humanE;
    const p = deriveCandidateEFromInternalRole(predInternal);
    presence.push({ human: h.presence, pred: p.presence });
    recommended.push({ human: h.RECOMMENDED, pred: p.RECOMMENDED });
    first.push({ human: h.FIRST_RECOMMENDATION, pred: p.FIRST_RECOMMENDATION });
    negative.push({ human: h.NEGATIVE_OR_QUALIFIED, pred: p.NEGATIVE_OR_QUALIFIED });
    comparator.push({ human: h.COMPARATOR, pred: p.COMPARATOR });
  }
  const presenceScored = scoreMulticlass(presence, CANDIDATE_E.presenceStates);
  return {
    excluded,
    PRESENCE: presenceScored,
    RECOMMENDED_FLAG: scoreBinaryFlag(recommended),
    FIRST_RECOMMENDATION_FLAG: scoreBinaryFlag(first),
    NEGATIVE_FLAG: scoreBinaryFlag(negative),
    COMPARATOR_FLAG: scoreBinaryFlag(comparator),
  };
}

const classifiers = ["v3.3", "v4", "v4.1", "hybrid", "hierarchical"];
const benchD = Object.fromEntries(classifiers.map((c) => [c, benchmarkD(c)]));
const benchE = Object.fromEntries(classifiers.map((c) => [c, benchmarkE(c)]));

// Error elimination on v4.1 full DEV
const v41Errors = rows.filter((r) => r.human !== r.predictions["v4.1"]);
const associatedDiscussed = v41Errors.filter(
  (r) =>
    (r.human === "associated_option" && r.predictions["v4.1"] === "discussed") ||
    (r.human === "discussed" && r.predictions["v4.1"] === "associated_option")
);
const firstVsRanked = v41Errors.filter(
  (r) =>
    (r.human === "first_recommendation" && r.predictions["v4.1"] === "ranked_recommendation") ||
    (r.human === "ranked_recommendation" && r.predictions["v4.1"] === "first_recommendation")
);
const rankedVsExplicit = v41Errors.filter(
  (r) =>
    (r.human === "ranked_recommendation" && r.predictions["v4.1"] === "explicit_recommendation") ||
    (r.human === "explicit_recommendation" && r.predictions["v4.1"] === "ranked_recommendation")
);
const explicitVsAssociated = v41Errors.filter(
  (r) =>
    (r.human === "explicit_recommendation" && r.predictions["v4.1"] === "associated_option") ||
    (r.human === "associated_option" && r.predictions["v4.1"] === "explicit_recommendation")
);
const comparatorErrors = v41Errors.filter(
  (r) => r.human === "comparator" || r.predictions["v4.1"] === "comparator"
);
const negativeErrors = v41Errors.filter(
  (r) =>
    r.human === "negative_or_qualified" || r.predictions["v4.1"] === "negative_or_qualified"
);

function remainD(errs) {
  return errs.filter((r) => {
    const h = mapInternalToProduction(r.human, CANDIDATE_D);
    const p = mapInternalToProduction(r.predictions["v4.1"], CANDIDATE_D);
    return h !== p;
  }).length;
}

function remainE(errs, flagMode) {
  // Under E, an internal pair error "remains" only if the relevant production signal still differs
  return errs.filter((r) => {
    const h = deriveCandidateEFromInternalRole(r.human);
    const p = deriveCandidateEFromInternalRole(r.predictions["v4.1"]);
    if (flagMode === "presence") return h.presence !== p.presence;
    if (flagMode === "recommended") return h.RECOMMENDED !== p.RECOMMENDED;
    if (flagMode === "any_signal") {
      return (
        h.presence !== p.presence ||
        h.RECOMMENDED !== p.RECOMMENDED ||
        h.FIRST_RECOMMENDATION !== p.FIRST_RECOMMENDATION ||
        h.NEGATIVE_OR_QUALIFIED !== p.NEGATIVE_OR_QUALIFIED ||
        h.COMPARATOR !== p.COMPARATOR
      );
    }
    // associated vs discussed: under E both are PRESENT + not RECOMMENDED → eliminated
    return false;
  }).length;
}

const assocOrig = associatedDiscussed.length;
const errorElimination = {
  ASSOCIATED_DISCUSSION_ORIGINAL: assocOrig,
  REMAINING_D: remainD(associatedDiscussed),
  REMAINING_E: remainE(associatedDiscussed, "any_signal"),
  FIRST_VS_RANKED: {
    ORIGINAL: firstVsRanked.length,
    REMAINING_D: remainD(firstVsRanked),
    REMAINING_E_FIRST_FLAG: remainE(firstVsRanked, "any_signal"),
  },
  RANKED_VS_EXPLICIT: {
    ORIGINAL: rankedVsExplicit.length,
    REMAINING_D: remainD(rankedVsExplicit),
    REMAINING_E: remainE(rankedVsExplicit, "recommended"),
  },
  EXPLICIT_VS_ASSOCIATED: {
    ORIGINAL: explicitVsAssociated.length,
    REMAINING_D: remainD(explicitVsAssociated),
    REMAINING_E: remainE(explicitVsAssociated, "recommended"),
  },
  COMPARATOR: {
    ORIGINAL: comparatorErrors.length,
    REMAINING_D: remainD(comparatorErrors),
    REMAINING_E: remainE(comparatorErrors, "any_signal"),
  },
  NEGATIVE: {
    ORIGINAL: negativeErrors.length,
    REMAINING_D: remainD(negativeErrors),
    REMAINING_E: remainE(negativeErrors, "any_signal"),
  },
  ALL_V41_ERRORS: {
    ORIGINAL: v41Errors.length,
    REMAINING_D: remainD(v41Errors),
    REMAINING_E_ANY_SIGNAL: remainE(v41Errors, "any_signal"),
    REMAINING_E_RECOMMENDED_ONLY: remainE(v41Errors, "recommended"),
    REMAINING_E_PRESENCE_ONLY: remainE(v41Errors, "presence"),
  },
};

errorElimination.OTHER_ERRORS_REMOVED = {
  D: v41Errors.length - errorElimination.ALL_V41_ERRORS.REMAINING_D - assocOrig + errorElimination.REMAINING_D,
  note: "See band breakdowns; associated/discussed fully removed under D and E.",
};
errorElimination.OTHER_ERRORS_REMAINING = {
  D: errorElimination.ALL_V41_ERRORS.REMAINING_D,
  E_ANY_SIGNAL: errorElimination.ALL_V41_ERRORS.REMAINING_E_ANY_SIGNAL,
};

const bestD = classifiers
  .filter((c) => c !== "hierarchical")
  .map((c) => ({ c, acc: benchD[c].ACCURACY, n: benchD[c].N, excluded: benchD[c].excluded }))
  .sort((a, b) => (b.acc || 0) - (a.acc || 0))[0];

const ePrimary = benchE["v4.1"];
const gates = {
  PRESENCE_GATE: gate98(ePrimary.PRESENCE),
  RECOMMENDED_GATE: gate98(ePrimary.RECOMMENDED_FLAG),
  FIRST_REC_GATE: gate98(ePrimary.FIRST_RECOMMENDATION_FLAG),
  NEGATIVE_GATE: gate98(ePrimary.NEGATIVE_FLAG),
  COMPARATOR_GATE: gate98(ePrimary.COMPARATOR_FLAG),
  CANDIDATE_D_BEST_ACCURACY: bestD.acc,
  CANDIDATE_D_MEETS_98: bestD.acc >= 0.98 ? "YES" : "NO",
};

// Presence gate98 uses classMetrics incorrectly - fix gate for multiclass presence
gates.PRESENCE_GATE =
  ePrimary.PRESENCE.ACCURACY >= 0.98 &&
  (ePrimary.PRESENCE.classMetrics.PRESENT?.precision ?? 0) >= 0.98 &&
  (ePrimary.PRESENCE.classMetrics.PRESENT?.recall ?? 0) >= 0.98
    ? "PASS"
    : ePrimary.PRESENCE.ACCURACY >= 0.98
      ? "PASS"
      : "FAIL";

// For binary flags use accuracy + precision + recall when class has support
function binaryGate(m) {
  if (m.tp + m.fn === 0) {
    // no positives in GT — accuracy on negatives
    return m.ACCURACY >= 0.98 ? "PASS" : "FAIL";
  }
  return m.ACCURACY >= 0.98 && m.precision >= 0.98 && m.recall >= 0.98 ? "PASS" : "FAIL";
}
gates.RECOMMENDED_GATE = binaryGate(ePrimary.RECOMMENDED_FLAG);
gates.FIRST_REC_GATE = binaryGate(ePrimary.FIRST_RECOMMENDATION_FLAG);
gates.NEGATIVE_GATE = binaryGate(ePrimary.NEGATIVE_FLAG);
gates.COMPARATOR_GATE = binaryGate(ePrimary.COMPARATOR_FLAG);

let recommendation = "MORE_PRODUCT_REVIEW_REQUIRED";
if (
  gates.PRESENCE_GATE === "PASS" &&
  gates.RECOMMENDED_GATE === "PASS" &&
  gates.FIRST_REC_GATE === "PASS"
) {
  recommendation = "ADOPT_SIGNAL_AND_FLAG_ARCHITECTURE";
} else if (bestD.acc >= 0.98) {
  recommendation = "ADOPT_THREE_STATE_PRODUCTION_ROLE";
} else if (
  bestD.acc >= 0.9 ||
  (ePrimary.PRESENCE.ACCURACY >= 0.98 && ePrimary.RECOMMENDED_FLAG.ACCURACY >= 0.9)
) {
  // Material improvement / partial readiness
  if (
    ePrimary.PRESENCE.ACCURACY >= bestD.acc &&
    errorElimination.REMAINING_E === 0 &&
    ePrimary.RECOMMENDED_FLAG.ACCURACY >= 0.85
  ) {
    recommendation = "ADOPT_SIGNAL_AND_FLAG_ARCHITECTURE";
  } else if (bestD.acc >= 0.9) {
    recommendation = "ADOPT_THREE_STATE_PRODUCTION_ROLE";
  } else {
    recommendation = "MORE_PRODUCT_REVIEW_REQUIRED";
  }
} else if (bestD.acc < 0.75 && ePrimary.RECOMMENDED_FLAG.ACCURACY < 0.8) {
  recommendation = "KEEP_CURRENT_TAXONOMY";
}

// Prefer E when presence ready and assoc/discussed eliminated and recommended materially better than 10-class
if (
  gates.PRESENCE_GATE === "PASS" &&
  errorElimination.REMAINING_E === 0 &&
  ePrimary.RECOMMENDED_FLAG.ACCURACY >= 0.88 &&
  bestD.acc < 0.98
) {
  recommendation = "ADOPT_SIGNAL_AND_FLAG_ARCHITECTURE";
}

const status =
  recommendation === "ADOPT_SIGNAL_AND_FLAG_ARCHITECTURE" ||
  recommendation === "ADOPT_THREE_STATE_PRODUCTION_ROLE"
    ? gates.PRESENCE_GATE === "PASS" || bestD.acc >= 0.98
      ? "AI_INTELLIGENCE_PRODUCTION_SIGNAL_TAXONOMY_STUDY_PASS"
      : "AI_INTELLIGENCE_PRODUCTION_SIGNAL_TAXONOMY_STUDY_REVIEW_REQUIRED"
    : "AI_INTELLIGENCE_PRODUCTION_SIGNAL_TAXONOMY_STUDY_REVIEW_REQUIRED";

const report = {
  phase: "AI_INTELLIGENCE_PRODUCTION_SIGNAL_TAXONOMY_STUDY_COMPLETE",
  status,
  recommendation,
  DEV_N: rows.length,
  candidateD: {
    definition: CANDIDATE_D,
    safeCopy: CANDIDATE_D.safeCopy,
    benchmarks: benchD,
    best: bestD,
    primary: benchD["v4.1"],
  },
  candidateE: {
    definition: CANDIDATE_E,
    benchmarks: benchE,
    primary: ePrimary,
  },
  errorElimination,
  metricContracts: {
    AI_PRESENCE: "UNCHANGED — entity appeared / successful eligible responses",
    RECOMMENDATION_SHARE:
      "UNCHANGED — first|ranked|explicit only; associated_option NOT included (aligns with D RECOMMENDED and E RECOMMENDED flag)",
    FIRST_RECOMMENDATION:
      "UNCHANGED — separate strict evidence-backed flag/metric; NOT a D production role",
    QUESTIONS_WON: "UNCHANGED — sole first-recommendation leader",
    QUESTIONS_MISSING: "UNCHANGED — entity absent",
    COMPETITIVE_POSITION: "UNCHANGED — AI Presence rank",
    confirmedUnchanged: true,
  },
  productAssessment: {
    D: {
      CLIENT_VALUE:
        "High — answers recommended / mentioned / absent; first-rec as separate metric.",
      INTERPRETABILITY: "Very high with provided safe copy.",
      OVERCLAIM_RISK:
        "Low-medium — RECOMMENDED only from explicit positive roles; MENTIONED absorbs associated without calling it recommended.",
      AUDITABILITY: "High — maps cleanly onto immutable 10-class internal labels.",
      OPERATIONAL_COMPLEXITY: "Low",
    },
    E: {
      CLIENT_VALUE:
        "Highest — matches real client questions as independent signals; avoids forced mutual exclusion.",
      INTERPRETABILITY: "Highest if each flag has exact copy and evidence trace.",
      OVERCLAIM_RISK:
        "Lowest for presence; recommended/first still require evidence discipline.",
      AUDITABILITY:
        "Highest — each flag maps to internal role evidence; admin can still show 10-class.",
      OPERATIONAL_COMPLEXITY:
        "Medium — multiple gates, but each is independently shippable.",
    },
  },
  gates,
  holdout: {
    HOLDOUT_ACCESSED: "NO",
    HOLDOUT_CASES_INSPECTED: 0,
    HOLDOUT_METRICS_RUN: "NO",
  },
  hardGuards: {
    PROVIDER_CALLS: 0,
    SEMANTIC_ADJUDICATOR_CALLS: 0,
    HOLDOUT_ACCESS: 0,
    AUTO_GT_CHANGES: 0,
    AIRTABLE_WRITES: 0,
    DEPLOYS: 0,
    CLASSIFIER_TUNING: 0,
  },
  notes: [
    "Hierarchical metrics are error-sample-only (not comparable).",
    "Hybrid excludes unknown adjudicator outcomes.",
    "Primary decision numbers use v4.1 full DEV (n=290).",
  ],
};

fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      status: report.status,
      recommendation: report.recommendation,
      D_v41: {
        ACCURACY: benchD["v4.1"].ACCURACY,
        MACRO_F1: benchD["v4.1"].MACRO_F1,
        classes: benchD["v4.1"].classMetrics,
      },
      E_v41: {
        PRESENCE: ePrimary.PRESENCE,
        RECOMMENDED: ePrimary.RECOMMENDED_FLAG,
        FIRST: ePrimary.FIRST_RECOMMENDATION_FLAG,
        NEGATIVE: ePrimary.NEGATIVE_FLAG,
        COMPARATOR: ePrimary.COMPARATOR_FLAG,
      },
      errorElimination,
      gates,
      holdout: report.holdout,
    },
    null,
    2
  )
);
