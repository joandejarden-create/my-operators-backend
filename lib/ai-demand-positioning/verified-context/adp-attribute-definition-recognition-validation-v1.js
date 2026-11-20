/**
 * Validate selected attributes against ATTRIBUTE_DEFINITIONS + recognition keywords.
 */

import { ATTRIBUTE_DEFINITIONS } from "../intelligence/reality-gap.js";
import {
  hasAttributeRecognitionKeywords,
  listAttributeRecognitionKeys,
  ATTRIBUTE_RECOGNITION_KEYWORDS,
} from "../execution/attribute-recognition-keywords-v1.js";
import { classifySelectedAttributeEligibility } from "./adp-attribute-eligibility-classifier-v1.js";
import { analyzeSelectedSemanticOverlaps } from "./adp-semantic-equivalence-v1.js";

export const ADP_ATTRIBUTE_DEFINITION_RECOGNITION_VALIDATION_V1 =
  "ADP_ATTRIBUTE_DEFINITION_RECOGNITION_VALIDATION_V1";

function isAmbiguousKeywordSet(keywords = []) {
  const weak = new Set(["pool", "spa", "design", "urban", "luxury", "meeting", "resort"]);
  const list = Array.isArray(keywords) ? keywords : [];
  if (list.length === 0) return true;
  if (list.length === 1 && weak.has(String(list[0]).toLowerCase())) return true;
  return false;
}

/**
 * @param {string[]} selectedKeys
 * @param {object} propertyProfile
 */
export function validateAttributeDefinitionAndRecognition(
  selectedKeys,
  propertyProfile,
  opts = {}
) {
  const selected = Array.isArray(selectedKeys) ? selectedKeys : [];
  const eligibility = classifySelectedAttributeEligibility(selected, propertyProfile, opts);
  const semantic = analyzeSelectedSemanticOverlaps(selected);

  const rows = selected.map((key) => {
    const def = ATTRIBUTE_DEFINITIONS[key];
    const hasRecognition = hasAttributeRecognitionKeywords(key);
    const keywords = hasRecognition ? ATTRIBUTE_RECOGNITION_KEYWORDS[key] : [];
    const eligRow = eligibility.rows.find((r) => r.attribute === key);
    return {
      attribute: key,
      label: def?.label || key,
      definitionPresent: Boolean(def),
      recognitionRulePresent: hasRecognition,
      recognitionKeywords: keywords,
      ambiguousRecognitionKeywords: hasRecognition ? isAmbiguousKeywordSet(keywords) : null,
      eligibilityStatus: eligRow?.eligibilityStatus || null,
      verificationStatus: eligRow?.verificationStatus || null,
      evidenceUrl: eligRow?.evidenceUrl || null,
      issues: [
        !def ? "MISSING_DEFINITION" : null,
        def && !hasRecognition ? "MISSING_RECOGNITION_RULE" : null,
        hasRecognition && isAmbiguousKeywordSet(keywords) ? "AMBIGUOUS_RECOGNITION_KEYWORDS" : null,
        eligRow?.eligibilityStatus === "SELECTED_NEEDS_VERIFICATION"
          ? "INSUFFICIENT_VERIFICATION"
          : null,
        eligRow?.eligibilityStatus === "SELECTED_CONFLICTING" ? "CONFLICTING_EVIDENCE" : null,
        eligRow?.eligibilityStatus === "SELECTED_UNDEFINED" ? "UNDEFINED_SELECTED_KEY" : null,
      ].filter(Boolean),
    };
  });

  const recognitionKeys = new Set(listAttributeRecognitionKeys());
  const definitionKeys = new Set(Object.keys(ATTRIBUTE_DEFINITIONS));
  const defsMissingRecognition = [...definitionKeys].filter((k) => !recognitionKeys.has(k));
  const recognitionWithoutDefs = [...recognitionKeys].filter((k) => !definitionKeys.has(k));

  return {
    contractVersion: ADP_ATTRIBUTE_DEFINITION_RECOGNITION_VALIDATION_V1,
    propertyId: propertyProfile?.propertyId || null,
    rows,
    missingDefinitions: rows.filter((r) => r.issues.includes("MISSING_DEFINITION")).map((r) => r.attribute),
    missingRecognitionRules: rows
      .filter((r) => r.issues.includes("MISSING_RECOGNITION_RULE"))
      .map((r) => r.attribute),
    ambiguousRecognition: rows
      .filter((r) => r.issues.includes("AMBIGUOUS_RECOGNITION_KEYWORDS"))
      .map((r) => r.attribute),
    insufficientVerification: rows
      .filter((r) => r.issues.includes("INSUFFICIENT_VERIFICATION"))
      .map((r) => r.attribute),
    conflictingEvidence: rows
      .filter((r) => r.issues.includes("CONFLICTING_EVIDENCE"))
      .map((r) => r.attribute),
    semanticOverlaps: semantic.pairs,
    registryDrift: {
      definitionsMissingRecognition: defsMissingRecognition,
      recognitionWithoutDefinitions: recognitionWithoutDefs,
    },
    eligibility,
  };
}
