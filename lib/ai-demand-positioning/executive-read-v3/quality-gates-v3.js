/**
 * V3 quality gates + claim discipline + fail-closed helpers.
 */

import {
  COMPOSITION_V3_GATES,
  COMPOSITION_V3_RULES,
} from "../governance/adp-executive-read-composition-v3.js";

export const ADP_EXECUTIVE_NO_PROPERTY_HARDCODED_NARRATIVE =
  "ADP_EXECUTIVE_NO_PROPERTY_HARDCODED_NARRATIVE";
export const ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ZERO_CODE_PATH =
  "ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ZERO_CODE_PATH";
export const ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY =
  "ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY";
export const ADP_EXECUTIVE_READ_SECTION_TRACEABILITY =
  "ADP_EXECUTIVE_READ_SECTION_TRACEABILITY";
export const ADP_EXECUTIVE_READ_VS_MONTHLY_ACTION_LAYER_SEPARATION =
  "ADP_EXECUTIVE_READ_VS_MONTHLY_ACTION_LAYER_SEPARATION";
export const ADP_EXECUTIVE_CUSTOMER_LANGUAGE_LOCK = "ADP_EXECUTIVE_CUSTOMER_LANGUAGE_LOCK";
export const ADP_EXECUTIVE_SUMMARY_MUST_ADD_ANALYTICAL_VALUE =
  "ADP_EXECUTIVE_SUMMARY_MUST_ADD_ANALYTICAL_VALUE";
export const ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY =
  "ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY";
export const ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY = "ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY";
export const ADP_EXECUTIVE_WHAT_TO_REVIEW_ACTIONABLE =
  "ADP_EXECUTIVE_WHAT_TO_REVIEW_ACTIONABLE";
export const ADP_EXECUTIVE_NUMERIC_CONTRAST_VALUE = "ADP_EXECUTIVE_NUMERIC_CONTRAST_VALUE";
export const ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY =
  "ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY";

const CAUSAL =
  /\bshould (support|reinforce|travel|cause|drive|improve|carry)\b|\bis (driving|causing|holding back)\b|\bwill improve\b|\bbrand (advantage|lever)\b/i;
const RHETORIC =
  /\bprotect-and-polish\b|\bowns the answer set\b|\bcompetitor war\b|\bwrong fight\b|\bweak-brand\b|\belite\b|\bfranchise\b|\brival takeover\b/i;
const METHOD_DEFENSE =
  /\bno causal relationship should be inferred\b|\bcannot establish\b|\bdoes not,? by itself,? prove\b|\bmethodology\b/i;

const CLAIM_TYPES = Object.freeze({
  OBSERVED_FACT: "OBSERVED_FACT",
  GOVERNED_METRIC: "GOVERNED_METRIC",
  SUPPORTED_INTERPRETATION: "SUPPORTED_INTERPRETATION",
  REASONABLE_REVIEW_HYPOTHESIS: "REASONABLE_REVIEW_HYPOTHESIS",
  UNSUPPORTED_CAUSAL_IMPLICATION: "UNSUPPORTED_CAUSAL_IMPLICATION",
  OVERSTATED: "OVERSTATED",
});

export function classifyClaimsInText(text) {
  const claims = [];
  if (CAUSAL.test(text)) {
    claims.push({ claimType: CLAIM_TYPES.UNSUPPORTED_CAUSAL_IMPLICATION, excerpt: text.match(CAUSAL)?.[0] });
  }
  if (RHETORIC.test(text)) {
    claims.push({ claimType: CLAIM_TYPES.OVERSTATED, excerpt: text.match(RHETORIC)?.[0] });
  }
  return claims;
}

function wordCount(sections) {
  return Object.values(sections || {})
    .filter(Boolean)
    .join(" ")
    .split(/\s+/)
    .filter(Boolean).length;
}

/**
 * Evaluate quality gates for a composed V3 payload (inactive preview/foundation).
 */
export function evaluateExecutiveReadV3QualityGates(composed, { usedHardcodedMap = false } = {}) {
  const sections = composed?.sections || {};
  const text = [sections.headline, sections.keyInsight, sections.whyItMatters, sections.focusNow, sections.watch, sections.whatToReview]
    .filter(Boolean)
    .join("\n");
  const claims = classifyClaimsInText(text);
  const unsupported = claims.filter((c) => c.claimType === CLAIM_TYPES.UNSUPPORTED_CAUSAL_IMPLICATION).length;
  const overstated = claims.filter((c) => c.claimType === CLAIM_TYPES.OVERSTATED).length;
  const wc = wordCount(sections);
  const anchors = composed?.numericAnchors || [];
  const hotelName = composed?.propertyName || "";
  const results = {};

  const pass = (id, ok, detail) => {
    results[id] = { pass: !!ok, detail: detail || null };
  };

  pass(ADP_EXECUTIVE_SUMMARY_MUST_ADD_ANALYTICAL_VALUE, Boolean(sections.keyInsight && sections.keyInsight.length > 80), "keyInsight length");
  pass(
    ADP_EXECUTIVE_SUMMARY_PROPERTY_SPECIFICITY,
    !hotelName || text.toLowerCase().includes(String(hotelName).split(",")[0].toLowerCase().slice(0, 12)),
    "property name referenced"
  );
  pass("ADP_EXECUTIVE_KEY_INSIGHT_SUBSTANTIVE", (sections.keyInsight || "").split(/\s+/).length >= 40);
  pass("ADP_EXECUTIVE_WHY_IT_MATTERS_SUBSTANTIVE", (sections.whyItMatters || "").split(/\s+/).length >= 25);
  pass(ADP_EXECUTIVE_ONE_PRIMARY_PRIORITY, Boolean(composed?.primaryIssueId));
  pass("ADP_EXECUTIVE_FOCUS_NOW_FINAL_QUALITY", /focus first|priority|review whether|confirm/i.test(sections.focusNow || ""));
  pass(ADP_EXECUTIVE_WHAT_TO_REVIEW_ACTIONABLE, /open|compare|review|confirm|evidence/i.test(sections.whatToReview || ""));
  pass(
    "ADP_EXECUTIVE_WHAT_TO_REVIEW_INCREMENTAL_VALUE",
    Boolean(sections.whatToReview) &&
      sections.whatToReview !== sections.focusNow &&
      (sections.whatToReview || "").length > 40
  );
  pass(
    "ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS",
    anchors.length >= 1 && anchors.length <= 4
  );
  pass(
    "ADP_EXECUTIVE_NO_KPI_ENUMERATION",
    !/consideration.*scenario presence.*top-?3.*reality gap/i.test(text)
  );
  pass(
    "ADP_EXECUTIVE_NUMERIC_REFERENCE_PARITY",
    anchors.every((a) => a.metricId && a.displayValue && a.paritySource)
  );
  pass(ADP_EXECUTIVE_NUMERIC_CONTRAST_VALUE, anchors.every((a) => a.removingWeakensInsight !== false));
  pass("ADP_EXECUTIVE_NO_IMPLIED_CAUSATION", unsupported === 0);
  pass("ADP_EXECUTIVE_CLEAR_NOT_CLEVER", !RHETORIC.test(text));
  pass(ADP_EXECUTIVE_CUSTOMER_LANGUAGE_LOCK, !RHETORIC.test(text) && !METHOD_DEFENSE.test(text));
  pass("ADP_EXECUTIVE_NO_CUSTOMER_FACING_METHODOLOGY_DEFENSE", !METHOD_DEFENSE.test(text));
  pass(
    "ADP_EXECUTIVE_WATCH_EXECUTIVE_MATERIALITY",
    sections.watch == null || (typeof sections.watch === "string" && sections.watch.length > 20)
  );
  pass(
    ADP_EXECUTIVE_READ_SECTION_TRACEABILITY,
    Array.isArray(composed?.evidenceTrace) && composed.evidenceTrace.length > 0
  );
  pass(
    ADP_EXECUTIVE_READ_VS_MONTHLY_ACTION_LAYER_SEPARATION,
    !/this month'?s action plan|implement the following tactics/i.test(text)
  );
  pass(ADP_EXECUTIVE_NO_PROPERTY_HARDCODED_NARRATIVE, usedHardcodedMap === false);
  pass(ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ZERO_CODE_PATH, composed?.zeroCodePath === true || usedHardcodedMap === false);
  pass(ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY, composed?.historicalImmutability?.enforced === true);
  pass(ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY, composed?.compositionVersion != null);
  pass("ADP_EXECUTIVE_V3_FOUNDER_READ_TEST", wc >= COMPOSITION_V3_RULES.targetWordCount.min);

  const failed = Object.entries(results)
    .filter(([, v]) => !v.pass)
    .map(([k]) => k);

  return {
    gates: results,
    gateIds: COMPOSITION_V3_GATES,
    claimDiscipline: {
      unsupportedCausalImplication: unsupported,
      overstated,
      pass: unsupported === 0 && overstated === 0,
    },
    wordCount: wc,
    allRequiredPass: failed.length === 0,
    failed,
  };
}
