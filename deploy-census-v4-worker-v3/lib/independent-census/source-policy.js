/**
 * Conservative source policy helpers for independent hotel census.
 * Unknown sources default to restrictive flags (no product use, manual review, high risk).
 */

import { getSourceProfile, normalizeSourceTypeKey } from "./source-registry.js";

const UNKNOWN_POLICY = {
  sourceType: "unknown",
  sourceName: "Unknown",
  sourceLicense: "unknown",
  initialUse: "discovery",
  canUseInProduct: "no",
  requiresAttribution: true,
  requiresManualReview: true,
  requiresRefresh: false,
  canShowToUsers: false,
  canUseForScoring: false,
  riskLevel: "high",
  notes: "Unregistered source type — block product use until profile is defined.",
  allowedStorageNotes: ["Do not ingest without source-registry entry."],
};

/**
 * @param {string} sourceType
 */
export function getSourcePolicy(sourceType) {
  return getSourceProfile(sourceType) || { ...UNKNOWN_POLICY, sourceType: normalizeSourceTypeKey(sourceType) || "unknown" };
}

function productUseAllows(use) {
  return use === "yes" || use === "conditional";
}

function productUseRestricted(use) {
  return use === "restricted_refresh_required" || use === "review_required";
}

export function canUseInProduct(sourceType) {
  const p = getSourcePolicy(sourceType);
  if (p.canUseInProduct === "yes" || p.canUseInProduct === "conditional") return true;
  if (p.canUseInProduct === "review_required") return false;
  return false;
}

export function canShowToUsers(sourceType) {
  const p = getSourcePolicy(sourceType);
  if (!productUseAllows(p.canUseInProduct) && p.canUseInProduct !== "review_required") {
    return false;
  }
  return !!p.canShowToUsers;
}

export function canUseForScoring(sourceType) {
  const p = getSourcePolicy(sourceType);
  return !!p.canUseForScoring;
}

export function requiresRefresh(sourceType) {
  return !!getSourcePolicy(sourceType).requiresRefresh;
}

export function requiresAttribution(sourceType) {
  return !!getSourcePolicy(sourceType).requiresAttribution;
}

export function requiresManualReview(sourceType) {
  return getSourcePolicy(sourceType).requiresManualReview !== false;
}

export function getAllowedStorageNotes(sourceType) {
  const p = getSourcePolicy(sourceType);
  return p.allowedStorageNotes ? [...p.allowedStorageNotes] : [];
}

export function getSourceRiskLevel(sourceType) {
  return getSourcePolicy(sourceType).riskLevel || "high";
}

/**
 * Attach policy flags for local dry-run reports (not Airtable columns yet).
 * @param {object} candidate
 */
export function attachSourcePolicyFlags(candidate) {
  const p = getSourcePolicy(candidate.sourceType);
  return {
    ...candidate,
    sourcePolicyFlags: {
      canUseInProduct: canUseInProduct(candidate.sourceType),
      canShowToUsers: canShowToUsers(candidate.sourceType),
      canUseForScoring: canUseForScoring(candidate.sourceType),
      requiresRefresh: requiresRefresh(candidate.sourceType),
      requiresAttribution: requiresAttribution(candidate.sourceType),
      requiresManualReview: requiresManualReview(candidate.sourceType),
      riskLevel: getSourceRiskLevel(candidate.sourceType),
      productUseClass: p.canUseInProduct,
    },
  };
}
