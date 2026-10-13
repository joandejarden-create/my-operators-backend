/**
 * Ownership confidence — source authority ≠ claim strength.
 *
 * Score = f(
 *   source_authority,
 *   relationship_explicitness,
 *   entity_plausibility,
 *   property_identity_match,
 *   temporal_relevance,
 *   corroboration,
 *   page_backed
 * )
 *
 * Maps onto Hotel Intelligence CONFIDENCE_TIERS — no parallel product system.
 */

import {
  CONFIDENCE_TIERS,
  labelForConfidence,
} from "../confidence.js";
import { VERIFICATION_STATUSES } from "./ontology.js";
import { entityPlausibilityScore } from "./research/entity-name-guard.js";

export const OWNERSHIP_CONFIDENCE_MAP_VERSION = "ownership-confidence-map-v2";

export const PRODUCT_LABEL_TO_VERIFICATION = Object.freeze({
  VERIFIED: "verified",
  HIGH: "high",
  MEDIUM: "probable",
  LOW: "needs_review",
  CONFLICT: "conflict",
  UNKNOWN: "unknown",
  HISTORICAL: "historical",
});

/** Source trust alone — never sufficient for HIGH/VERIFIED. */
export const OWNERSHIP_SOURCE_AUTHORITY = Object.freeze({
  government: 0.95,
  securities_filing: 0.94,
  corporate_registry: 0.93,
  first_party: 0.88,
  trade_press: 0.72,
  dealality_census: 0.58,
  dealality_existing: 0.7,
  web_search: 0.45,
  manual: 0.7,
  other: 0.4,
});

/** Max confidence for unverified flat Census ownership fields. */
export const CENSUS_FLAT_FIELD_CONFIDENCE_CAP = 0.69;

/** Snippet-only OWNED_BY never exceeds this (prefer page fetch). */
export const SNIPPET_ONLY_OWNED_BY_CAP = 0.64;

/**
 * @param {number} score
 */
export function verificationFromConfidence(score) {
  const tier = labelForConfidence(score);
  if (tier === CONFIDENCE_TIERS.VERIFIED.label) return "verified";
  if (tier === CONFIDENCE_TIERS.HIGH.label) return "high";
  if (tier === CONFIDENCE_TIERS.PROBABLE.label) return "probable";
  if (tier === CONFIDENCE_TIERS.NEEDS_REVIEW.label) return "needs_review";
  if (tier === "unknown") return "unknown";
  return "needs_review";
}

/**
 * Multi-dimension ownership evidence score.
 *
 * @param {string} sourceType
 * @param {{
 *   relationshipType?: string,
 *   relationshipExplicitness?: number,
 *   entityName?: string,
 *   entityPlausibility?: number,
 *   propertyIdentityMatch?: number,
 *   temporalRelevance?: number,
 *   corroborationCount?: number,
 *   pageBacked?: boolean,
 *   snippetOnly?: boolean,
 *   completeness?: number,
 *   agreementBonus?: number,
 * }} [opts]
 */
export function scoreOwnershipEvidence(sourceType, opts = {}) {
  const key = String(sourceType || "other").trim().toLowerCase();
  const sourceAuthority =
    OWNERSHIP_SOURCE_AUTHORITY[key] ?? OWNERSHIP_SOURCE_AUTHORITY.other;

  // When relationshipType omitted (legacy Stage A lead scoring), do not assume OWNED_BY gates.
  const relationshipType = opts.relationshipType
    ? String(opts.relationshipType)
    : "UNSPECIFIED";
  const explicitness = clamp01(
    opts.relationshipExplicitness ??
      (relationshipType === "OWNED_BY"
        ? 0.5
        : relationshipType === "UNSPECIFIED"
          ? 0.6
          : 0.7)
  );
  const entityPlaus =
    Number.isFinite(opts.entityPlausibility)
      ? clamp01(opts.entityPlausibility)
      : entityPlausibilityScore(opts.entityName || "");
  const identity = clamp01(opts.propertyIdentityMatch ?? 0.55);
  const temporal = clamp01(opts.temporalRelevance ?? 0.7);
  const corroboration = Math.min(
    1,
    Math.max(0, Number(opts.corroborationCount) || 0) * 0.12
  );
  const pageBacked = opts.pageBacked !== false && opts.snippetOnly !== true;

  // Weak claim semantics → collapse regardless of source authority
  if (relationshipType === "OWNED_BY" && explicitness < 0.5) {
    return finalizeScore({
      source_type: key,
      source_authority: sourceAuthority,
      relationship_explicitness: explicitness,
      entity_plausibility: entityPlaus,
      property_identity_match: identity,
      temporal_relevance: temporal,
      corroboration,
      page_backed: pageBacked,
      confidence: Math.min(0.45, sourceAuthority * 0.4),
      explanation: "weak_ownership_semantics",
    });
  }

  if (entityPlaus < 0.4) {
    return finalizeScore({
      source_type: key,
      source_authority: sourceAuthority,
      relationship_explicitness: explicitness,
      entity_plausibility: entityPlaus,
      property_identity_match: identity,
      temporal_relevance: temporal,
      corroboration,
      page_backed: pageBacked,
      confidence: 0.35,
      explanation: "implausible_entity",
    });
  }

  // Weighted blend — all dimensions must be decent for HIGH+
  let confidence =
    sourceAuthority * 0.25 +
    explicitness * 0.28 +
    entityPlaus * 0.22 +
    identity * 0.12 +
    temporal * 0.05 +
    corroboration * 0.08;

  // Strong government / registry ownership claims get a small authority boost
  if (
    relationshipType === "OWNED_BY" &&
    (key === "government" ||
      key === "corporate_registry" ||
      key === "securities_filing") &&
    explicitness >= 0.85 &&
    entityPlaus >= 0.7 &&
    pageBacked
  ) {
    confidence = Math.min(1, confidence + 0.08);
  }

  if (Number.isFinite(opts.completeness)) {
    confidence *= 0.85 + 0.15 * clamp01(opts.completeness);
  }
  if (Number.isFinite(opts.agreementBonus)) {
    confidence = Math.min(1, confidence + Number(opts.agreementBonus));
  }

  // Snippet-only OWNED_BY: hard cap below HIGH
  if (
    relationshipType === "OWNED_BY" &&
    (opts.snippetOnly === true || pageBacked === false)
  ) {
    confidence = Math.min(confidence, SNIPPET_ONLY_OWNED_BY_CAP);
  }

  // First-party alone without high explicitness cannot reach HIGH
  if (
    key === "first_party" &&
    relationshipType === "OWNED_BY" &&
    explicitness < 0.75
  ) {
    confidence = Math.min(confidence, CONFIDENCE_TIERS.PROBABLE.max - 0.01);
  }

  // VERIFIED / HIGH gates — deliberately hard during remediation
  let verification = verificationFromConfidence(confidence);
  const gate = applyVerificationGates({
    confidence,
    verification,
    sourceType: key,
    relationshipType,
    explicitness,
    entityPlaus,
    identity,
    corroborationCount: Number(opts.corroborationCount) || 0,
    pageBacked,
  });

  return finalizeScore({
    source_type: key,
    source_authority: sourceAuthority,
    relationship_explicitness: explicitness,
    entity_plausibility: entityPlaus,
    property_identity_match: identity,
    temporal_relevance: temporal,
    corroboration,
    page_backed: pageBacked,
    confidence: gate.confidence,
    verification_status: gate.verification_status,
    explanation: gate.explanation,
  });
}

/**
 * Hard gates so source-category weighting alone cannot create HIGH/VERIFIED.
 */
export function applyVerificationGates(input) {
  let { confidence, verification } = input;
  const {
    sourceType,
    relationshipType,
    explicitness,
    entityPlaus,
    identity,
    corroborationCount,
    pageBacked,
  } = input;

  let explanation = "multi_dimension_score";

  const canHigh =
    relationshipType !== "OWNED_BY" ||
    (explicitness >= 0.75 &&
      entityPlaus >= 0.7 &&
      identity >= 0.55 &&
      pageBacked &&
      (sourceType === "government" ||
        sourceType === "corporate_registry" ||
        sourceType === "securities_filing" ||
        sourceType === "first_party" ||
        (sourceType === "trade_press" && corroborationCount >= 1)));

  const canVerified =
    relationshipType === "OWNED_BY"
      ? (sourceType === "government" ||
          sourceType === "corporate_registry" ||
          sourceType === "securities_filing") &&
        explicitness >= 0.85 &&
        entityPlaus >= 0.75 &&
        pageBacked
        ? true
        : sourceType === "first_party" &&
          explicitness >= 0.9 &&
          entityPlaus >= 0.8 &&
          corroborationCount >= 1 &&
          pageBacked
      : sourceType === "government" ||
        sourceType === "first_party" ||
        sourceType === "corporate_registry";

  if (verification === "verified" && !canVerified) {
    confidence = Math.min(confidence, CONFIDENCE_TIERS.HIGH.max - 0.01);
    verification = verificationFromConfidence(confidence);
    explanation = "verified_gate_failed";
  }
  if (
    (verification === "verified" || verification === "high") &&
    !canHigh
  ) {
    confidence = Math.min(confidence, CONFIDENCE_TIERS.PROBABLE.max - 0.01);
    verification = verificationFromConfidence(confidence);
    explanation = "high_gate_failed_weak_claim_or_source";
  }

  // Absolute ceiling without page-backed OWNED_BY
  if (relationshipType === "OWNED_BY" && !pageBacked) {
    confidence = Math.min(confidence, SNIPPET_ONLY_OWNED_BY_CAP);
    verification = verificationFromConfidence(confidence);
    explanation = "snippet_only_owned_by_cap";
  }

  return {
    confidence: round2(confidence),
    verification_status: verification,
    explanation,
  };
}

function finalizeScore(dims) {
  const confidence = round2(dims.confidence);
  const verification_status =
    dims.verification_status || verificationFromConfidence(confidence);
  return {
    source_type: dims.source_type,
    source_authority: round2(dims.source_authority),
    relationship_explicitness: round2(dims.relationship_explicitness),
    entity_plausibility: round2(dims.entity_plausibility),
    property_identity_match: round2(dims.property_identity_match),
    temporal_relevance: round2(dims.temporal_relevance),
    corroboration: round2(dims.corroboration),
    page_backed: Boolean(dims.page_backed),
    confidence,
    verification_status,
    auto_accept:
      verification_status === "verified" || verification_status === "high",
    tier_label: labelForConfidence(confidence),
    explanation: dims.explanation || null,
  };
}

function clamp01(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.min(1, Math.max(0, x));
}

function round2(n) {
  return Math.round(clamp01(n) * 100) / 100;
}

/**
 * Prefer higher verification; conflict stays conflict.
 * @param {string[]} statuses
 */
export function rollupVerificationStatus(statuses) {
  const set = new Set(
    (statuses || []).map((s) => String(s || "").trim()).filter(Boolean)
  );
  if (!set.size) return "unknown";
  if (set.has("conflict")) return "conflict";
  for (const s of [
    "verified",
    "high",
    "probable",
    "needs_review",
    "historical",
    "unknown",
  ]) {
    if (set.has(s)) return s;
  }
  return "unknown";
}

export function isVerificationStatus(value) {
  return VERIFICATION_STATUSES.includes(String(value || "").trim());
}

export { CONFIDENCE_TIERS, labelForConfidence };
