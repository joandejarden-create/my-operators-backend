/**
 * AI Intelligence Classifier Lab — DEV scoring + error inventory.
 * Holdout never accessed.
 */
import { loadGoldenSet, scoreGoldenSetHydrated } from "../validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../validation/golden-set-entity-index.js";

export const ROLES = [
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "negative_or_qualified",
  "source_only",
];

const roleRank = new Map(ROLES.map((r, i) => [r, i]));

export function prf(tp, fp, fn) {
  const p = tp + fp ? tp / (tp + fp) : null;
  const r = tp + fn ? tp / (tp + fn) : null;
  const f1 = p != null && r != null && p + r ? (2 * p * r) / (p + r) : null;
  return { precision: p, recall: r, f1, tp, fp, fn };
}

export function macroFromClassMetrics(classMetrics) {
  const vals = Object.values(classMetrics).filter((x) => x.tp + x.fp + x.fn > 0);
  const avg = (a) => (a.length ? a.reduce((s, x) => s + x, 0) / a.length : null);
  return {
    MACRO_P: avg(vals.map((v) => v.precision).filter((x) => x != null)),
    MACRO_R: avg(vals.map((v) => v.recall).filter((x) => x != null)),
    MACRO_F1: avg(vals.map((v) => v.f1).filter((x) => x != null)),
  };
}

/**
 * Infer generalized root-cause cluster for an error pair + text snippet.
 */
export function inferRootCause(human, predicted, text = "") {
  const t = String(text || "").slice(0, 600);
  const pair = `${human} => ${predicted}`;
  if (pair === "discussed => associated_option") {
    if (/brand\s+profiles|soft\s+brand|market\s+overview|portfolio/i.test(t)) {
      return "FALSE_ASSOCIATED_FROM_NEUTRAL_OR_PROFILE_CATALOG";
    }
    if (/to\s+consider|options?\s+include|commonly\s+considered|a\s+considerar/i.test(t)) {
      return "FALSE_ASSOCIATED_FROM_BROAD_CONSIDERATION_PROPAGATION";
    }
    return "FALSE_ASSOCIATED_OVER_PROMOTION";
  }
  if (pair === "associated_option => discussed") {
    if (/commonly\s+(?:associated|considered)|names?\s+that\s+appear|marcas.*considerad|opciones\s+incluyen/i.test(t)) {
      return "MISSING_ASSOCIATED_FROM_DECISION_SET_MEMBERSHIP";
    }
    return "MISSING_ASSOCIATED_RECALL";
  }
  if (pair === "first_recommendation => discussed") {
    if (/shortlist|solicit|orden\s+de\s+prioridad|top\s+\d+|1\.\s/i.test(t)) {
      return "MISSING_FIRST_FROM_CONFIRMED_RANK_OR_SHORTLIST";
    }
    if (/first\s+(?:call|choice|option)|primary\s+recommendation|primera\s+opci/i.test(t)) {
      return "MISSING_FIRST_FROM_LEAD_CUE";
    }
    return "MISSING_FIRST_RECALL";
  }
  if (pair === "first_recommendation => explicit_recommendation") {
    if (/strong\s+(?:candidate|alternative|option|fit)|particularly\s+suitable/i.test(t)) {
      return "FIRST_LABELED_BUT_ONLY_EXPLICIT_POSITIVE_CUE";
    }
    return "FIRST_UNDERPROMOTED_TO_EXPLICIT";
  }
  if (pair === "first_recommendation => associated_option") {
    if (/brands?\s+to\s+consider|operators?\s+to\s+consider|a\s+considerar/i.test(t)) {
      return "FIRST_VS_ASSOCIATED_CONSIDERATION_LIST_TAXONOMY";
    }
    return "FIRST_UNDERPROMOTED_TO_ASSOCIATED";
  }
  if (pair === "ranked_recommendation => associated_option") {
    if (/brands?\s+to\s+consider|operators?\s+to\s+consider/i.test(t)) {
      return "RANKED_VS_ASSOCIATED_CONSIDERATION_LIST_TAXONOMY";
    }
    return "MISSING_RANKED_FROM_CONFIRMED_STRUCTURE";
  }
  if (pair === "ranked_recommendation => discussed") {
    return "MISSING_RANKED_RECALL";
  }
  if (pair === "explicit_recommendation => discussed") {
    return "MISSING_EXPLICIT_POSITIVE_OR_SECTION";
  }
  if (pair === "discussed => explicit_recommendation") {
    return "FALSE_EXPLICIT_OVER_PROMOTION";
  }
  if (pair === "comparator => discussed" || pair === "discussed => comparator") {
    return "COMPARATOR_BOUNDARY";
  }
  return `PAIR_${human}_TO_${predicted}`;
}

export async function scoreDevRecommendationLab(opts = {}) {
  const golden = loadGoldenSet();
  const live = await scoreGoldenSetHydrated(golden, { holdoutPolicy: "exclude" });
  if (live.HOLDOUT_ACCESSED || live.HOLDOUT_METRICS_RUN) {
    throw new Error("BLOCKED: holdout accessed during classifier lab");
  }

  const index = buildGoldenSetScoringEntityIndex({});
  const { cases } = await hydrateGoldenSetCasesForScoring(
    (golden.cases || []).filter((c) => c.holdoutSplit !== "holdout"),
    {}
  );

  const cm = Object.fromEntries(ROLES.map((r) => [r, { tp: 0, fp: 0, fn: 0 }]));
  const pairs = {};
  const errors = [];
  const clusterCounts = {};

  for (const c of cases) {
    if (!c.expectedRecommendationRole) continue;
    const text = c.text || "";
    const mentions = extractMentions({
      responseId: "lab",
      text,
      entityIndex: index.aliasIndex,
    });
    const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
    const predicted = hits.length
      ? hits
          .slice()
          .sort(
            (a, b) =>
              (roleRank.get(a.role) ?? 99) - (roleRank.get(b.role) ?? 99) ||
              a.mentionPosition - b.mentionPosition
          )[0].role
      : null;
    const human = c.expectedRecommendationRole;
    if (human === predicted) {
      cm[human].tp++;
    } else {
      cm[human].fn++;
      if (predicted && cm[predicted]) cm[predicted].fp++;
      const pair = `${human} => ${predicted}`;
      pairs[pair] = (pairs[pair] || 0) + 1;
      const rootCause = inferRootCause(human, predicted, text);
      clusterCounts[rootCause] = (clusterCounts[rootCause] || 0) + 1;
      errors.push({
        caseId: c.caseId,
        entity: c.entityName,
        humanRole: human,
        predictedRole: predicted,
        provider: c.provider || c.engine || null,
        language: c.language || c.responseLanguage || null,
        geography: c.geography || c.market || c.country || null,
        rootCause,
        pair,
        snippet: text.slice(0, 220).replace(/\s+/g, " "),
      });
    }
  }

  const classMetrics = Object.fromEntries(
    Object.entries(cm).map(([k, v]) => [k, prf(v.tp, v.fp, v.fn)])
  );
  const macro = macroFromClassMetrics(classMetrics);

  return {
    metrics: {
      ACCURACY: live.RECOMMENDATION_CLASSIFICATION_ACCURACY,
      PRECISION: live.RECOMMENDATION_PRECISION,
      RECALL: live.RECOMMENDATION_RECALL,
      F1: live.RECOMMENDATION_F1,
      ...macro,
      FIRST_REC: live.FIRST_RECOMMENDATION_ACCURACY,
      QUESTION_STATUS: live.QUESTION_STATUS_ACCURACY,
      ENTITY_P: live.ENTITY_RESOLUTION_PRECISION,
      ENTITY_R: live.ENTITY_RESOLUTION_RECALL,
      ENTITY_F1: live.ENTITY_RESOLUTION_F1,
    },
    classMetrics,
    errors,
    pairs: Object.entries(pairs)
      .sort((a, b) => b[1] - a[1])
      .map(([pair, count]) => ({ pair, count })),
    clusters: Object.entries(clusterCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([rootCause, count]) => ({ rootCause, count })),
    errorCount: errors.length,
    holdout: {
      HOLDOUT_ACCESSED: false,
      HOLDOUT_CASES_INSPECTED: 0,
      HOLDOUT_METRICS_RUN: false,
    },
    classifierVersion: opts.classifierVersion || null,
  };
}

export function acceptanceDecision(before, after, opts = {}) {
  const materialClassDrop = opts.materialClassDrop ?? 0.05;
  const materialMacroDrop = opts.materialMacroDrop ?? 0.015;
  const reasons = [];

  if ((after.metrics.ENTITY_P ?? 1) < 0.98 || (after.metrics.ENTITY_R ?? 1) < 0.98) {
    reasons.push("entity_gate_failed");
  }
  if (!(after.metrics.ACCURACY > before.metrics.ACCURACY)) {
    reasons.push("accuracy_did_not_improve");
  }
  if (
    after.metrics.MACRO_F1 != null &&
    before.metrics.MACRO_F1 != null &&
    after.metrics.MACRO_F1 < before.metrics.MACRO_F1 - materialMacroDrop
  ) {
    reasons.push("macro_f1_material_regression");
  }
  const materialFirstDrop = opts.materialFirstDrop ?? 0.01;
  if (
    after.metrics.FIRST_REC != null &&
    before.metrics.FIRST_REC != null &&
    after.metrics.FIRST_REC < before.metrics.FIRST_REC - materialFirstDrop
  ) {
    reasons.push("first_rec_material_regression");
  }

  const watch = [
    "first_recommendation",
    "ranked_recommendation",
    "explicit_recommendation",
    "associated_option",
    "discussed",
    "negative_or_qualified",
  ];
  for (const role of watch) {
    const b = before.classMetrics[role]?.f1;
    const a = after.classMetrics[role]?.f1;
    if (b != null && a != null && a < b - materialClassDrop) {
      reasons.push(`class_f1_regression:${role}`);
    }
  }

  // Unsafe over-promotion: discussed → associated/explicit/first/ranked increase
  const beforePairs = Object.fromEntries((before.pairs || []).map((p) => [p.pair, p.count]));
  const afterPairs = Object.fromEntries((after.pairs || []).map((p) => [p.pair, p.count]));
  for (const unsafe of [
    "discussed => associated_option",
    "discussed => explicit_recommendation",
    "discussed => first_recommendation",
    "discussed => ranked_recommendation",
  ]) {
    const b = beforePairs[unsafe] || 0;
    const a = afterPairs[unsafe] || 0;
    if (a > b + 2) reasons.push(`unsafe_overpromotion:${unsafe}`);
  }

  return {
    accepted: reasons.length === 0,
    rejectionReason: reasons.length ? reasons.join("|") : null,
    reasons,
  };
}

export function diffErrors(beforeErrors, afterErrors) {
  const key = (e) => `${e.caseId}||${e.entity}||${e.humanRole}`;
  const b = new Set(beforeErrors.map(key));
  const a = new Set(afterErrors.map(key));
  return {
    fixedErrors: [...b].filter((k) => !a.has(k)),
    newErrors: [...a].filter((k) => !b.has(k)),
  };
}
