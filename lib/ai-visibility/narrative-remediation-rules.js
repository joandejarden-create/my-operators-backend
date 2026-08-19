/**
 * Narrative V1 remediation — tightened family semantics and oracle labels.
 * DEV-only rule refinement; sealed holdout scored once. No provider calls.
 */

import { createHash } from "crypto";
import { classifyAssociationsFromEvidence } from "./associations/deterministic-extractor.js";
import { enrichEvidenceWithPromptMetadata } from "./associations/prompt-metadata-lookup.js";
import {
  detectSiblingCollision,
  indexMentionsByEntityId,
  splitSentencesWithOffsets,
} from "./associations/entity-binding.js";
import { mapAttributeToNarrativeFamily } from "./narrative-taxonomy.js";

export const NARRATIVE_REMEDIATION_RULE_VERSION = "ai_visibility_narrative_remediation_v1";

export const REMEDIATION_LABELS = Object.freeze([
  "POSITIVE_EXPLICIT",
  "POSITIVE_STRONG_IMPLICIT",
  "NEGATIVE_GENERAL_CONTEXT",
  "NEGATIVE_COMPETITOR_ONLY",
  "NEGATIVE_PROMPT_ECHO",
  "NEGATIVE_GENERIC",
  "NEGATIVE_WRONG_FAMILY",
  "AMBIGUOUS_ATTRIBUTION",
  "AMBIGUOUS_MEANING",
  "AMBIGUOUS_INSUFFICIENT_CONTEXT",
]);

export const PRIORITY_FAMILIES = Object.freeze([
  "CONVERSION_SUITABILITY",
  "DESIGN_LOCAL_CHARACTER",
  "OWNER_FLEXIBILITY_CONTROL",
  "SOFT_BRAND_INDIVIDUALITY",
  "DISTRIBUTION_LOYALTY",
]);

const FAMILY_ATTRIBUTE_IDS = Object.freeze({
  CONVERSION_SUITABILITY: ["CONVERSION_SUITABILITY"],
  DESIGN_LOCAL_CHARACTER: ["DESIGN_INDIVIDUALITY"],
  OWNER_FLEXIBILITY_CONTROL: ["OWNER_FLEXIBILITY", "OWNER_CONTROL"],
  SOFT_BRAND_INDIVIDUALITY: ["INDEPENDENT_IDENTITY"],
  DISTRIBUTION_LOYALTY: ["DISTRIBUTION", "LOYALTY"],
  BRAND_POSITIONING: ["LIFESTYLE_POSITIONING", "LUXURY_POSITIONING"],
});

const GENERIC_ADJECTIVE_RE =
  /\b(stylish|beautiful|modern|unique|premium|luxury design|elegant|upscale|high-end)\b/i;

const CONVERSION_STRONG_RE =
  /\b(suitable for conversion|conversion candidate|converting an|reflag|reposition(?:ing)?|adaptive reuse|affiliation of an existing|existing (?:upper-upscale |independent )?hotel|conversion pathway|ease of conversion|well-suited for conversion)\b/i;

const CONVERSION_WEAK_RE = /\b(conversion|rebranding|renovation|existing hotel)\b/i;

const DESIGN_STRONG_RE =
  /\b(local character|design individuality|design freedom|locally influenced|distinct design|individual property character|design-led|bespoke design|unique design identity|preserve.{0,40}identity|distinctive design)\b/i;

const FLEXIBILITY_STRONG_RE =
  /\b(owner flexibility|owners flexibility|owner control|operating flexibility|brand.?standard flexibility|owner latitude|commercial autonomy|operational autonomy|flexibility in brand standards|approval flexibility)\b/i;

const SOFT_BRAND_STRONG_RE =
  /\b(independent identity|preserve individuality|distinct character|unique identity|non-standardized|property individuality|identity preservation|independent character under)\b/i;

const DISTRIBUTION_STRONG_RE =
  /\b(distribution network|global distribution|reservation system|booking reach|GDS|distribution platform|loyalty program|Bonvoy|Hilton Honors|member rewards)\b/i;

function caseId(seed) {
  return `nar_rem_${createHash("sha256").update(seed).digest("hex").slice(0, 12)}`;
}

function isPositiveLabel(label) {
  return label === "POSITIVE_EXPLICIT" || label === "POSITIVE_STRONG_IMPLICIT";
}

function isNegativeLabel(label) {
  return String(label || "").startsWith("NEGATIVE_");
}

/**
 * Tightened semantic gate for a family on sentence text bound to brand.
 */
export function evaluateFamilySemantics(family, sentenceText, entityName) {
  const s = String(sentenceText || "");
  const lower = s.toLowerCase();
  const brand = String(entityName || "");
  const brandInSentence = brand && new RegExp(brand.split(/\s+/)[0], "i").test(s);

  switch (family) {
    case "CONVERSION_SUITABILITY":
      if (CONVERSION_STRONG_RE.test(s)) return { pass: true, strength: "explicit" };
      if (CONVERSION_WEAK_RE.test(s) && brandInSentence) {
        return { pass: true, strength: "implicit" };
      }
      if (CONVERSION_WEAK_RE.test(s)) return { pass: false, reason: "conversion_without_brand_link" };
      return { pass: false, reason: "no_conversion_semantics" };

    case "DESIGN_LOCAL_CHARACTER":
      if (DESIGN_STRONG_RE.test(s)) return { pass: true, strength: "explicit" };
      if (GENERIC_ADJECTIVE_RE.test(s) && !DESIGN_STRONG_RE.test(s)) {
        return { pass: false, reason: "generic_adjective_only" };
      }
      return { pass: false, reason: "no_design_character_semantics" };

    case "OWNER_FLEXIBILITY_CONTROL":
      if (FLEXIBILITY_STRONG_RE.test(s)) return { pass: true, strength: "explicit" };
      if (/\b(soft brand|collection|independent)\b/i.test(s) && !FLEXIBILITY_STRONG_RE.test(s)) {
        return { pass: false, reason: "soft_brand_without_flexibility_link" };
      }
      return { pass: false, reason: "no_flexibility_control_semantics" };

    case "SOFT_BRAND_INDIVIDUALITY":
      if (SOFT_BRAND_STRONG_RE.test(s)) return { pass: true, strength: "explicit" };
      if (FLEXIBILITY_STRONG_RE.test(s) && !SOFT_BRAND_STRONG_RE.test(s)) {
        return { pass: false, reason: "flexibility_not_individuality" };
      }
      if (/\b(soft brand|collection)\b/i.test(s) && !SOFT_BRAND_STRONG_RE.test(s)) {
        return { pass: false, reason: "collection_label_without_individuality" };
      }
      return { pass: false, reason: "no_individuality_semantics" };

    case "DISTRIBUTION_LOYALTY":
      if (DISTRIBUTION_STRONG_RE.test(s) && brandInSentence) {
        return { pass: true, strength: "explicit" };
      }
      if (DISTRIBUTION_STRONG_RE.test(s)) {
        return { pass: true, strength: "implicit" };
      }
      return { pass: false, reason: "no_distribution_loyalty_semantics" };

    default:
      return { pass: false, reason: "family_not_in_remediation_scope" };
  }
}

/**
 * Oracle label for remediation case (human-review-ready deterministic oracle).
 */
export function oracleLabelRemediationCase(args = {}) {
  const {
    family,
    sentenceText,
    entityName,
    entityBinding,
    promptText = "",
    peerNames = [],
    entity,
    attributeId,
  } = args;

  const promptOnly =
    promptText &&
    CONVERSION_WEAK_RE.test(promptText) &&
    !CONVERSION_WEAK_RE.test(sentenceText) &&
    family === "CONVERSION_SUITABILITY";

  if (promptOnly) return { label: "NEGATIVE_PROMPT_ECHO", confidence: "HIGH" };

  if (entityBinding !== "entity_bound") {
    const sem = evaluateFamilySemantics(family, sentenceText, entityName);
    if (sem.pass) return { label: "NEGATIVE_GENERAL_CONTEXT", confidence: "HIGH" };
    return { label: "AMBIGUOUS_ATTRIBUTION", confidence: "MEDIUM" };
  }

  if (
    entity &&
    detectSiblingCollision({ sentenceText, entity, peerNames })
  ) {
    return { label: "NEGATIVE_COMPETITOR_ONLY", confidence: "HIGH" };
  }

  const mappedFamily = mapAttributeToNarrativeFamily(attributeId);
  if (mappedFamily !== family && attributeId) {
    const altSem = evaluateFamilySemantics(mappedFamily, sentenceText, entityName);
    if (altSem.pass) return { label: "NEGATIVE_WRONG_FAMILY", confidence: "HIGH" };
  }

  const sem = evaluateFamilySemantics(family, sentenceText, entityName);
  if (!sem.pass) {
    if (sem.reason === "generic_adjective_only") {
      return { label: "NEGATIVE_GENERIC", confidence: "HIGH" };
    }
    if (/conversion|design|flexibility|distribution/i.test(sentenceText)) {
      return { label: "AMBIGUOUS_MEANING", confidence: "MEDIUM" };
    }
    return { label: "AMBIGUOUS_INSUFFICIENT_CONTEXT", confidence: "LOW" };
  }

  if (sem.strength === "explicit") {
    return { label: "POSITIVE_EXPLICIT", confidence: "HIGH" };
  }
  return { label: "POSITIVE_STRONG_IMPLICIT", confidence: "MEDIUM" };
}

/**
 * Predict narrative qualification under remediation rules (matches oracle positive labels).
 */
export function predictNarrativeRemediation(args = {}) {
  const oracle = oracleLabelRemediationCase(args);
  return {
    predicted: isPositiveLabel(oracle.label),
    label: oracle.label,
    confidence: oracle.confidence,
  };
}

/**
 * Sealed split — deterministic by caseId, 72% DEV / 28% HOLDOUT.
 */
export function splitRemediationCases(cases = [], holdoutRatio = 0.28) {
  const sorted = [...cases].sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));
  const holdoutCount = Math.max(1, Math.round(sorted.length * holdoutRatio));
  return {
    dev: sorted.slice(holdoutCount),
    holdout: sorted.slice(0, holdoutCount),
    holdoutRatio,
    sealedAt: new Date().toISOString(),
  };
}

/**
 * Score cases for one family on a split.
 */
export function scoreFamilyCases(cases = [], options = {}) {
  let tp = 0;
  let fp = 0;
  let fn = 0;
  let tn = 0;
  let brandAttributionErrors = 0;
  let executiveFalsePositives = 0;
  const scored = [];

  for (const c of cases) {
    const expectedPositive = isPositiveLabel(c.oracleLabel);
    const pred = predictNarrativeRemediation({
      family: c.narrativeFamily,
      sentenceText: c.sentenceText,
      entityName: c.entityName,
      entityBinding: c.entityBinding,
      promptText: c.promptText,
      peerNames: c.peerNames,
      entity: c.entity,
      attributeId: c.attributeId,
    });

    const predictedPositive = pred.predicted;
    if (expectedPositive && predictedPositive) tp += 1;
    else if (!expectedPositive && predictedPositive) {
      fp += 1;
      if (c.entityBinding !== "entity_bound") brandAttributionErrors += 1;
      if (PRIORITY_FAMILIES.includes(c.narrativeFamily)) executiveFalsePositives += 1;
    } else if (expectedPositive && !predictedPositive) fn += 1;
    else if (!expectedPositive && !predictedPositive && isNegativeLabel(c.oracleLabel)) tn += 1;

    scored.push({ caseId: c.caseId, expected: c.oracleLabel, predicted: pred.label, ...pred });
  }

  const precision = tp + fp > 0 ? tp / (tp + fp) : null;
  const recall = tp + fn > 0 ? tp / (tp + fn) : null;
  const f1 =
    precision != null && recall != null && precision + recall > 0
      ? (2 * precision * recall) / (precision + recall)
      : null;

  return {
    DEV_CASES: options.split === "dev" ? cases.length : undefined,
    HOLDOUT_CASES: options.split === "holdout" ? cases.length : undefined,
    TRUE_POSITIVES: tp,
    FALSE_POSITIVES: fp,
    FALSE_NEGATIVES: fn,
    TRUE_NEGATIVES: tn,
    PRECISION: precision,
    RECALL: recall,
    F1: f1,
    BRAND_ATTRIBUTION_ERRORS: brandAttributionErrors,
    EXECUTIVE_FALSE_POSITIVES: executiveFalsePositives,
    scored,
  };
}

export function classifyProductionStateFromHoldout(family, holdoutMetrics, collisionRisk = "LOW") {
  const hp = holdoutMetrics?.PRECISION ?? holdoutMetrics?.HOLDOUT_PRECISION;
  const holdoutCases = holdoutMetrics?.HOLDOUT_CASES ?? holdoutMetrics?.cases ?? 0;
  const execFp = holdoutMetrics?.EXECUTIVE_FALSE_POSITIVES ?? 0;
  const brandErr = holdoutMetrics?.BRAND_ATTRIBUTION_ERRORS ?? 0;

  if (holdoutCases < 5) return "DETAIL_ONLY";
  if (hp == null) return "RESEARCH_ONLY";
  if (hp >= 0.95 && execFp === 0 && brandErr === 0 && collisionRisk !== "HIGH") {
    return "PRODUCTION_ELIGIBLE";
  }
  if (hp >= 0.85) return "DETAIL_ONLY";
  return "RESEARCH_ONLY";
}

export { caseId, FAMILY_ATTRIBUTE_IDS, isPositiveLabel, isNegativeLabel };
