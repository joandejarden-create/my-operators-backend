/**
 * Narrative Intelligence V1 validation — sealed set + precision metrics.
 * Reuses association golden set; no provider calls.
 */

import {
  loadAssociationGoldenSet,
  scoreAssociationClassifier,
  DEFAULT_GOLDEN_SET_PATH,
} from "./associations/golden-set.js";
import {
  mapAttributeToNarrativeFamily,
  classifyNarrativeFamilyProductionState,
  summarizeTaxonomyGating,
} from "./narrative-taxonomy.js";
import { createBrandAiVisibilityReadStore } from "./storage/index.js";

export const NARRATIVE_VALIDATION_VERSION = "ai_visibility_narrative_validation_v1";

function splitCases(cases = [], holdoutRatio = 0.25) {
  const sorted = [...cases].sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));
  const holdoutCount = Math.max(1, Math.floor(sorted.length * holdoutRatio));
  return {
    dev: sorted.slice(holdoutCount),
    holdout: sorted.slice(0, holdoutCount),
  };
}

function labelBucket(label) {
  if (["POSITIVE", "NEGATIVE", "MIXED", "NEUTRAL_DESCRIPTIVE"].includes(label)) return "POSITIVE";
  if (label === "NO_ASSOCIATION") return "NEGATIVE";
  return "AMBIGUOUS";
}

/**
 * Build narrative validation cases from association golden set.
 */
export function buildNarrativeValidationSet(options = {}) {
  const golden = loadAssociationGoldenSet(options.goldenSetPath || DEFAULT_GOLDEN_SET_PATH);
  const cases = golden?.cases || [];
  const { dev, holdout } = splitCases(cases, options.holdoutRatio ?? 0.25);

  let positive = 0;
  let negative = 0;
  let ambiguous = 0;
  for (const c of cases) {
    const bucket = labelBucket(c.humanLabel);
    if (bucket === "POSITIVE") positive += 1;
    else if (bucket === "NEGATIVE") negative += 1;
    else ambiguous += 1;
  }

  return {
    TOTAL_CASES: cases.length,
    DEV: dev.length,
    HOLDOUT: holdout.length,
    POSITIVE: positive,
    NEGATIVE: negative,
    AMBIGUOUS: ambiguous,
    devCases: dev,
    holdoutCases: holdout,
    source: options.goldenSetPath || DEFAULT_GOLDEN_SET_PATH,
  };
}

/**
 * Score validation using association golden-set scorer (deterministic extractor).
 */
export async function scoreNarrativeValidation(options = {}) {
  const store = options.store || createBrandAiVisibilityReadStore();
  const evidence = await store.listEvidence({});

  const golden = loadAssociationGoldenSet(options.goldenSetPath || DEFAULT_GOLDEN_SET_PATH);
  const cases = golden?.cases || [];
  const scored = scoreAssociationClassifier(cases, evidence, options);

  const familyPrecision = {};
  for (const row of scored.attributeResults || []) {
    const family = mapAttributeToNarrativeFamily(row.attributeId);
    if (familyPrecision[family] == null || (row.precision ?? 0) > familyPrecision[family]) {
      familyPrecision[family] = row.precision;
    }
  }
  familyPrecision.ALL = scored.overall?.precision ?? null;

  let executiveFalsePositives = 0;
  let brandAttributionFalsePositives = 0;
  let sourceLinkFalsePositives = 0;

  for (const row of scored.attributeResults || []) {
    if (row.precision != null && row.precision < 0.9 && row.cases >= 3) {
      brandAttributionFalsePositives += Math.round((1 - row.precision) * row.cases);
    }
    if (
      ["OWNER_FLEXIBILITY", "OWNER_CONTROL", "CONVERSION_SUITABILITY", "MARKET_FIT"].includes(
        row.attributeId
      ) &&
      row.precision != null &&
      row.precision < 0.95
    ) {
      executiveFalsePositives += Math.max(1, Math.round((1 - row.precision) * (row.cases || 1)));
    }
  }
  if (scored.overall?.spanValidity != null && scored.overall.spanValidity < 0.95) {
    sourceLinkFalsePositives = Math.round((1 - scored.overall.spanValidity) * (scored.scoredCount || 0));
  }

  return {
    PRECISION: scored.overall?.precision ?? null,
    RECALL: scored.overall?.recall ?? null,
    F1: scored.overall?.f1 ?? null,
    EXECUTIVE_FALSE_POSITIVES: executiveFalsePositives,
    BRAND_ATTRIBUTION_FALSE_POSITIVES: brandAttributionFalsePositives,
    SOURCE_LINK_FALSE_POSITIVES: sourceLinkFalsePositives,
    SOURCE_LINK_PRECISION:
      scored.overall?.spanValidity != null ? scored.overall.spanValidity : null,
    familyPrecision,
    attributeResults: scored.attributeResults,
    spanValidity: scored.overall?.spanValidity ?? null,
    entityBindingErrorRate: scored.overall?.entityBindingErrorRate ?? null,
  };
}

export function buildExecutiveCandidates(args = {}) {
  const {
    narrativesByBrand = {},
    sourceRelationships = [],
    tensions = [],
    providerVariations = [],
    truthComparisons = [],
    maxCandidates = 5,
  } = args;

  const candidates = [];

  for (const [brandName, narratives] of Object.entries(narrativesByBrand)) {
    for (const n of narratives) {
      if (n.productionState === "RESEARCH_ONLY" || n.productionState === "BLOCKED") continue;
      if (n.relationshipToBrand !== "EXPLICIT_BRAND_ASSOCIATION") continue;
      if (n.observationCount < 3 && n.materiality !== "MATERIAL") continue;

      candidates.push({
        TYPE: "NARRATIVE_PATTERN",
        HEADLINE: `${brandName}: recurring ${n.narrativeLabel.toLowerCase()}`,
        EVIDENCE: `${n.providers.length} provider(s) · ${n.comparableResponseCount} comparable response(s) · ${n.stabilityContext?.timeWindow || "short-term"}`,
        INTERPRETATION: n.recurrence.label,
        SUGGESTED_REVIEW: `Review whether owned development materials clearly communicate ${n.narrativeLabel.toLowerCase()} in owner-decision contexts.`,
        DISPOSITION: n.reviewDisposition,
        EXECUTIVE_SAFE: n.productionState === "PRODUCTION_ELIGIBLE" && n.observationCount >= 3,
        brandName,
        narrativeFamily: n.narrativeFamily,
        score: n.observationCount * 10 + n.providers.length,
      });
    }
  }

  for (const rel of sourceRelationships.slice(0, 20)) {
    if (rel.relationship !== "DIRECTLY_CITED_WITH_NARRATIVE") continue;
    if (rel.responses < 2) continue;
    candidates.push({
      TYPE: "SOURCE_PATTERN",
      HEADLINE: `${rel.brandName}: ${rel.domain} cited alongside ${rel.narrativeLabel?.toLowerCase() || rel.narrativeFamily}`,
      EVIDENCE: `${rel.responses} response(s) · ${rel.providers.length} provider(s) · ${rel.ownedExternal}`,
      INTERPRETATION: rel.recurrence.label,
      SUGGESTED_REVIEW:
        rel.ownedExternal === "OWNED"
          ? "Owned sources are directly cited alongside this narrative in the sample."
          : "External industry sources recur alongside this narrative — review whether owned materials address the same theme.",
      DISPOSITION: "REVIEW_REQUIRED",
      EXECUTIVE_SAFE: rel.responses >= 2 && rel.productionState === "DETAIL_ONLY",
      score: rel.responses * 5,
    });
  }

  for (const t of tensions) {
    candidates.push({
      TYPE: "NARRATIVE_TENSION",
      HEADLINE: `${t.brand}: ${t.narrativeA.label} vs ${t.narrativeB.label}`,
      EVIDENCE: `Positive ${t.evidence.positiveResponses} · Negative ${t.evidence.negativeResponses} comparable responses`,
      INTERPRETATION: "Contradictory positioning themes both recur in stored responses.",
      SUGGESTED_REVIEW: "Review whether commercial messaging resolves the tension for owner decision-makers.",
      DISPOSITION: t.disposition,
      EXECUTIVE_SAFE: false,
      score: 15,
    });
  }

  for (const v of providerVariations) {
    candidates.push({
      TYPE: "PROVIDER_NARRATIVE_VARIATION",
      HEADLINE: `${v.brand}: provider narrative emphasis differs`,
      EVIDENCE: `ChatGPT ${v.openai || "—"} · Perplexity ${v.perplexity || "—"} · Claude ${v.claude || "—"} · Gemini ${v.gemini || "—"}`,
      INTERPRETATION: "Providers emphasize different narrative families in comparable owner-decision responses.",
      SUGGESTED_REVIEW: "Review cross-provider consistency of owned positioning materials.",
      DISPOSITION: "MONITOR_ONLY",
      EXECUTIVE_SAFE: false,
      score: 8,
    });
  }

  // Westin truth link — narrative does not duplicate truth engine
  const westinTruth = truthComparisons.filter(
    (c) => c.subjectBrandId === "recIPuBC50fv13zRR" && c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP"
  );
  if (westinTruth.length) {
    const westinNarratives = narrativesByBrand.Westin || [];
    const linked = westinNarratives.some((n) =>
      ["BRAND_POSITIONING", "CHAIN_SCALE_POSITIONING"].includes(n.narrativeFamily)
    );
    if (linked) {
      candidates.push({
        TYPE: "NARRATIVE_PATTERN",
        HEADLINE: "Westin: positioning narrative co-occurs with existing perception-gap signal",
        EVIDENCE: `${westinTruth.length} governed truth comparison(s) · narrative evidence in baseline`,
        INTERPRETATION:
          "Recurring AI positioning language appears alongside an existing Potential Perception Gap — narrative does not replace truth findings.",
        SUGGESTED_REVIEW: "Review upper-upscale positioning claims against Brand Basics; truth finding remains independently valid.",
        DISPOSITION: "REVIEW_REQUIRED",
        EXECUTIVE_SAFE: false,
        score: 20,
      });
    }
  }

  return candidates
    .sort((a, b) => (b.score || 0) - (a.score || 0))
    .slice(0, maxCandidates)
    .map(({ score, ...rest }) => rest);
}

export { summarizeTaxonomyGating, classifyNarrativeFamilyProductionState };
