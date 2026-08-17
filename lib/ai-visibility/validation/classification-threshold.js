/**
 * Governed classification release threshold for AI Intelligence Golden Set.
 * Provisional until sample size / representativeness meet enterprise bar.
 * Do not lower thresholds to force PASS.
 *
 * Production client release is now per-signal (PRESENCE / RECOMMENDED / FIRST_REC / …).
 * Multiclass 10-class recommendation thresholds below are INTERNAL_RESEARCH_VALIDATION only —
 * they must not control client publication.
 */

export const CLASSIFICATION_THRESHOLD_VERSION =
  "ai_intelligence_classification_threshold_v2_subgroup_aware";

/** Recommended enterprise sample size before non-provisional governance. */
export const GOLDEN_SET_EXPANSION_TARGET = Object.freeze({
  minCases: 150,
  recommendedCases: 250,
  note:
    "Expand human-labelled cases across EN/ES, four providers, regions, aliases, parent refs, negatives, multi-brand, citations before external enterprise use. Do not create synthetic labels. n≥150 alone is not sufficient.",
});

/**
 * Research / audit thresholds for the internal 10-class recommendation taxonomy.
 * NOT the production client release gate (see SIGNAL_PRODUCTION_GATES).
 */
export const CLASSIFICATION_RELEASE_THRESHOLDS = Object.freeze({
  ENTITY_RESOLUTION_PRECISION: 0.98,
  ENTITY_RESOLUTION_RECALL: 0.98,
  RECOMMENDATION_CLASSIFICATION_ACCURACY: 0.98,
  RECOMMENDATION_PRECISION: 0.98,
  RECOMMENDATION_RECALL: 0.98,
  FIRST_RECOMMENDATION_ACCURACY: 0.98,
  // QUESTION_STATUS_ACCURACY: adopt only when labelled coverage supports it
});

export const CLASSIFICATION_THRESHOLD_ROLE = Object.freeze({
  ENTITY_RESOLUTION: "PRODUCTION_GATED",
  RECOMMENDATION_10_CLASS: "INTERNAL_RESEARCH_VALIDATION",
  note:
    "Old multiclass recommendation classifier production gate retired. Preserve historical metrics; production gates are per-signal.",
});

/** Production gates under signal/flag architecture (no composite). */
export const SIGNAL_PRODUCTION_GATES = Object.freeze({
  PRESENCE_GATE: Object.freeze({ precision: 0.98, recall: 0.98 }),
  RECOMMENDED_GATE: Object.freeze({ precision: 0.98, recall: 0.98 }),
  FIRST_REC_GATE: Object.freeze({ precision: 0.98, recall: 0.98 }),
  NEGATIVE_GATE: Object.freeze({ precision: 0.98, recall: 0.98 }),
  COMPARATOR_GATE: Object.freeze({ precision: 0.98, recall: 0.98 }),
  COMPOSITE: "NONE",
});
/** Minimum labelled cases before a subgroup can fail the aggregate gate. */
export const MANDATORY_SUBGROUP_MIN_N = 10;

export const QUESTION_STATUS_THRESHOLD_STATUS = Object.freeze({
  NOT_YET_GOVERNED: "NOT_YET_GOVERNED",
  PARTIAL: "PARTIAL",
});

export const CITATION_THRESHOLD_STATUS = Object.freeze({
  PARTIAL: "PARTIAL",
  NOT_YET_GOVERNED: "NOT_YET_GOVERNED",
});

/**
 * @param {object} goldenScore
 * @param {{ coverage?: object, subgroupMetrics?: object }} [context]
 */
export function evaluateClassificationThreshold(goldenScore, context = {}) {
  const n = Number(goldenScore?.CASE_COUNT) || 0;
  const coverage = context.coverage || goldenScore?.coverage || {};
  const subgroupMetrics = context.subgroupMetrics || goldenScore?.subgroupMetrics || null;

  const sampleSizeSufficient = n >= GOLDEN_SET_EXPANSION_TARGET.minCases;
  const subgroupCoverage = assessSubgroupCoverage(coverage, n);
  const subgroupFailures = assessMandatorySubgroupFailures(
    subgroupMetrics,
    CLASSIFICATION_RELEASE_THRESHOLDS
  );

  const failures = [];
  const checks = {};

  for (const [key, min] of Object.entries(CLASSIFICATION_RELEASE_THRESHOLDS)) {
    const val = goldenScore?.[key];
    checks[key] = { value: val ?? null, required: min, pass: val != null && val >= min };
    if (val == null) {
      failures.push(`${key}: not measured`);
    } else if (val < min) {
      failures.push(`${key}: ${(val * 100).toFixed(1)}% < ${(min * 100).toFixed(0)}%`);
    }
  }

  if (subgroupFailures.length) {
    for (const f of subgroupFailures) failures.push(f);
  }

  const metricsPass = failures.length === 0;
  const questionLabelCount = Number(coverage.QUESTION_STATUS_LABEL_COUNT || 0);
  const citationLabelCount = Number(coverage.CITATION_ASSOCIATION_LABEL_COUNT || 0);

  const questionStatusThreshold =
    questionLabelCount >= MANDATORY_SUBGROUP_MIN_N
      ? {
          status: QUESTION_STATUS_THRESHOLD_STATUS.PARTIAL,
          proposed: 0.98,
          note: "Propose ≥98% when labelled n is adequate; not yet adopted as governed release gate.",
          measured: goldenScore?.QUESTION_STATUS_ACCURACY ?? null,
          labelledN: questionLabelCount,
        }
      : {
          status: QUESTION_STATUS_THRESHOLD_STATUS.NOT_YET_GOVERNED,
          proposed: null,
          note: "Insufficient human-labelled question-status cases to adopt a threshold.",
          measured: goldenScore?.QUESTION_STATUS_ACCURACY ?? null,
          labelledN: questionLabelCount,
        };

  const citationThresholdStatus =
    citationLabelCount >= MANDATORY_SUBGROUP_MIN_N
      ? CITATION_THRESHOLD_STATUS.PARTIAL
      : CITATION_THRESHOLD_STATUS.NOT_YET_GOVERNED;

  let status = "FAIL";
  let thresholdGovernance = "PROVISIONAL";

  if (n === 0) {
    status = "THRESHOLD_NOT_YET_GOVERNED";
    thresholdGovernance = "PROVISIONAL";
  } else if (!metricsPass) {
    status = subgroupFailures.length && metricsPass === false && failures.length === subgroupFailures.length
      ? "REVIEW"
      : "FAIL";
    // If only subgroup issues (aggregate metrics pass), REVIEW; if aggregate fails → FAIL
    const aggregateFail = Object.values(checks).some((c) => !c.pass);
    status = aggregateFail ? "FAIL" : subgroupFailures.length ? "REVIEW" : "FAIL";
    thresholdGovernance = "PROVISIONAL";
  } else if (
    sampleSizeSufficient &&
    subgroupCoverage.sufficient &&
    subgroupFailures.length === 0
  ) {
    status = "PASS";
    thresholdGovernance = "GOVERNED";
  } else if (metricsPass) {
    status = "PROVISIONAL_PASS";
    thresholdGovernance = "PROVISIONAL";
  }

  return {
    THRESHOLD_STATUS: status,
    THRESHOLD_GOVERNANCE: thresholdGovernance,
    THRESHOLD_VERSION: CLASSIFICATION_THRESHOLD_VERSION,
    PROVISIONAL: thresholdGovernance === "PROVISIONAL",
    CASE_COUNT: n,
    checks,
    failures,
    SAMPLE_SIZE_SUFFICIENT: sampleSizeSufficient,
    SUBGROUP_COVERAGE_SUFFICIENT: subgroupCoverage.sufficient,
    subgroupCoverage,
    NO_MAJOR_SUBGROUP_HIDDEN_BY_AGGREGATE: subgroupFailures.length === 0,
    mandatorySubgroupFailures: subgroupFailures,
    QUESTION_STATUS_THRESHOLD: questionStatusThreshold,
    CITATION_THRESHOLD_STATUS: citationThresholdStatus,
    GOVERNED_THRESHOLDS: { ...CLASSIFICATION_RELEASE_THRESHOLDS },
    expansionRecommendation: GOLDEN_SET_EXPANSION_TARGET,
    note: !sampleSizeSufficient
      ? `n=${n} below ${GOLDEN_SET_EXPANSION_TARGET.minCases} — thresholds adopted as PROVISIONAL only.`
      : !subgroupCoverage.sufficient
        ? `n=${n} meets size floor but subgroup coverage insufficient for GOVERNED status: ${subgroupCoverage.gaps.join("; ")}`
        : null,
  };
}

function assessSubgroupCoverage(coverage, n) {
  const gaps = [];
  const providers = coverage.PROVIDER || {};
  const languages = coverage.LANGUAGE || {};
  const geos = coverage.GEOGRAPHY || {};

  const providerKeys = ["openai", "gemini", "perplexity", "claude"];
  const providersPresent = providerKeys.filter((p) => (providers[p] || 0) > 0);
  if (providersPresent.length < 4 && n >= GOLDEN_SET_EXPANSION_TARGET.minCases) {
    gaps.push(`Provider coverage incomplete (${providersPresent.join(",") || "none"})`);
  }

  const en = languages.en || languages.English || languages.english || 0;
  const es = languages.es || languages.Spanish || languages.spanish || 0;
  if (n >= GOLDEN_SET_EXPANSION_TARGET.minCases) {
    if (en < MANDATORY_SUBGROUP_MIN_N) gaps.push(`English n=${en} < ${MANDATORY_SUBGROUP_MIN_N}`);
    if (es < MANDATORY_SUBGROUP_MIN_N) gaps.push(`Spanish n=${es} < ${MANDATORY_SUBGROUP_MIN_N}`);
  }

  const geoKeys = Object.keys(geos).filter((k) => k !== "unspecified" && (geos[k] || 0) > 0);
  if (n >= GOLDEN_SET_EXPANSION_TARGET.minCases && geoKeys.length < 2) {
    gaps.push("Geography diversity insufficient");
  }

  const hard = coverage.HARD_CASE_COUNT || 0;
  if (n >= GOLDEN_SET_EXPANSION_TARGET.minCases && hard < MANDATORY_SUBGROUP_MIN_N) {
    gaps.push(`Hard cases ${hard} < ${MANDATORY_SUBGROUP_MIN_N}`);
  }

  // v1 all-unspecified is never sufficient for GOVERNED
  if ((providers.unspecified || 0) === n && n > 0) {
    gaps.push("All cases lack provider stamps");
  }
  if ((languages.unspecified || 0) === n && n > 0) {
    gaps.push("All cases lack language stamps");
  }

  return {
    sufficient: gaps.length === 0 && n >= GOLDEN_SET_EXPANSION_TARGET.minCases,
    gaps,
    providersPresent,
    englishN: en,
    spanishN: es,
    hardCaseCount: hard,
  };
}

function assessMandatorySubgroupFailures(subgroupMetrics, thresholds) {
  if (!subgroupMetrics || typeof subgroupMetrics !== "object") return [];
  const failures = [];
  for (const [dim, rows] of Object.entries(subgroupMetrics)) {
    if (!rows || typeof rows !== "object") continue;
    for (const [key, metrics] of Object.entries(rows)) {
      const caseN = Number(metrics?.CASE_COUNT) || 0;
      if (caseN < MANDATORY_SUBGROUP_MIN_N) continue;
      for (const [metricKey, min] of Object.entries(thresholds)) {
        const val = metrics?.[metricKey];
        if (val != null && val < min) {
          failures.push(
            `NO_MAJOR_SUBGROUP_HIDDEN_BY_AGGREGATE: ${dim}=${key} ${metricKey}=${(val * 100).toFixed(1)}% < ${(min * 100).toFixed(0)}% (n=${caseN})`
          );
        }
      }
    }
  }
  return failures;
}
