/**
 * Research Engine V2 — research mode constants.
 */

export const RESEARCH_MODES = Object.freeze({
  FRESHNESS: "freshness",
  SHADOW: "shadow_monitoring",
  BRAND_ACTIVATION: "brand_activation",
  VALIDATION: "validation",
  RECONCILIATION: "reconciliation",
  DISCOVERY: "discovery",
  DEEP_RESOLUTION: "deep_resolution",
  IMAGE_INTEGRITY: "image_integrity",
});

export const ACTIVATION_STATUSES = Object.freeze([
  "Ready for Activation Review",
  "Targeted Remediation Required",
  "Deep Research Required",
  "Hold — Conflicting Evidence",
  "Hold — Insufficient Current Evidence",
  "Brand Appears Inactive / Discontinued",
]);

export const IMAGE_CLASSIFICATIONS = Object.freeze([
  "Current",
  "Missing",
  "Stale",
  "Wrong Property",
  "Wrong Brand",
  "Rendering Only",
  "Duplicate",
  "Low Confidence",
  "Needs Review",
]);

export const IMAGE_ACTIONS = Object.freeze([
  "Keep",
  "Review",
  "Replace Candidate",
  "Add Candidate",
  "Remove Candidate",
  "Needs Manual Verification",
]);
