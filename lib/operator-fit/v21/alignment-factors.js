/**
 * Operator Fit v2.1 — Operator–Project factors.
 * Same top-level weights as v2; geography + asset/development refined.
 */

import { OPERATOR_PROJECT_FACTORS } from "../config.js";
import {
  scoreSegmentFactor,
  scoreProjectComplexityFactor,
  scoreBrandExperienceFactor,
  scoreOwnershipGovernanceFactor,
  scoreRegionalResourcesFactor,
  scoreCommercialDifferentiatorFactor,
  aggregateOperatorProjectAlignment,
} from "../alignment-factors.js";
import { listValue, scalarValue, isKnownPositive } from "../adapters/field-state.js";
import { calculateGeographyRelevanceScore } from "./geography.js";
import {
  calculateComparableRelevanceIndex,
  scoreDevelopmentModeDepth,
  blendAssetDevelopmentScore,
} from "./comparable-relevance.js";
import { V21_DEFAULT_CRI_FORMULATION } from "./config.js";

function knownScore(key, score, rationale, extras = {}) {
  const def = OPERATOR_PROJECT_FACTORS[key];
  return {
    key,
    label: def.label,
    weight: def.weight,
    applicable: true,
    state: "known",
    score: Math.max(0, Math.min(100, score)),
    evidenceClassHint: extras.evidenceClassHint || null,
    rationale,
    positiveEvidence: extras.positiveEvidence || [],
    negativeEvidence: extras.negativeEvidence || [],
    unknownNotes: extras.unknownNotes || [],
    v21: extras.v21 || null,
  };
}

function unknownFactor(key, note) {
  const def = OPERATOR_PROJECT_FACTORS[key];
  return {
    key,
    label: def.label,
    weight: def.weight,
    applicable: true,
    state: "unknown",
    score: 0,
    evidenceClassHint: null,
    rationale: note,
    positiveEvidence: [],
    negativeEvidence: [],
    unknownNotes: [note],
  };
}

export function scoreGeographyFactorV21(project, operator) {
  const key = "geographyMarket";
  const g = calculateGeographyRelevanceScore(project, operator);
  if (g.state === "unknown") {
    return unknownFactor(key, g.unknownNotes[0] || "Geography unknown.");
  }
  return knownScore(key, g.score, "Geographic relevance assessed with presence depth (v2.1).", {
    positiveEvidence: g.positiveEvidence,
    negativeEvidence: g.negativeEvidence,
    v21: { geography: g.breakdown },
  });
}

/**
 * Segment: keep v2 buckets; optional light depth when comps show repeated exact scale.
 */
export function scoreSegmentFactorV21(project, operator) {
  const base = scoreSegmentFactor(project, operator);
  if (base.state !== "known" || !base.applicable) return base;
  const scale = scalarValue(project.hotelSegment);
  if (!scale) return base;
  const comps = isKnownPositive(operator.comparables) ? operator.comparables.value || [] : [];
  const hits = comps.filter((c) => {
    const hay = String([c.segment, c.chainScale, c.hotelType].filter(Boolean).join(" ")).toLowerCase();
    return hay && hay.includes(String(scale).toLowerCase());
  }).length;
  if (hits >= 2 && base.score >= 100) {
    return {
      ...base,
      rationale: `${base.rationale} Repeated segment-tagged comparables noted (informational; score unchanged at exact match).`,
      v21: { segmentCompHits: hits, depthApplied: false },
    };
  }
  if (hits === 0) return { ...base, v21: { segmentCompHits: 0 } };
  // Partial lift only when base was partial (62)
  if (base.score === 62 && hits >= 1) {
    return knownScore(base.key, 70, "Partial scale proximity with at least one segment-tagged comparable.", {
      positiveEvidence: base.positiveEvidence,
      negativeEvidence: base.negativeEvidence,
      v21: { segmentCompHits: hits, depthApplied: true },
    });
  }
  return { ...base, v21: { segmentCompHits: hits } };
}

export function scoreAssetDevelopmentFactorV21(
  project,
  operator,
  formulationId = V21_DEFAULT_CRI_FORMULATION
) {
  const key = "assetDevelopmentExperience";
  const asset = scalarValue(project.assetType);
  const dev = scalarValue(project.developmentType);
  const opAssets = listValue(operator.assetExperience);
  const opSit = listValue(operator.developmentExperience);
  const comps = isKnownPositive(operator.comparables) ? operator.comparables.value : [];

  if (!asset && !dev && !(comps || []).length) {
    return unknownFactor(key, "Insufficient project or comparable experience inputs.");
  }
  if (!opAssets.length && !opSit.length && !(comps || []).length) {
    return unknownFactor(key, "Operator asset/development experience is unknown.");
  }

  const mode = scoreDevelopmentModeDepth(project, operator);
  const cri = calculateComparableRelevanceIndex(project, operator, formulationId);
  const blended = blendAssetDevelopmentScore(mode, cri, formulationId);

  let score = blended.score;
  const positive = [];
  const negative = [];

  if (cri.topComparables.length) {
    positive.push(
      `Comparable relevance (${cri.label}): ${cri.topComparables
        .map((t) => t.property)
        .slice(0, 2)
        .join(", ")}`
    );
  }
  if (mode.label && mode.score != null) {
    positive.push(`Development-mode depth: ${mode.label}`);
  }

  // Fallback if blend null — structural ratios like v2
  if (score == null) {
    let fallback = 20;
    const assetHit = asset && opAssets.some((a) => String(a).toLowerCase().includes(String(asset).toLowerCase()));
    if (assetHit) fallback = 55;
    if (mode.score) fallback = Math.max(fallback, mode.score);
    score = fallback;
    if (!assetHit && asset) negative.push(`Limited asset-type overlap with ${asset}`);
  }

  // Cap broad portfolio without CRI signal
  if (!cri.topComparables.length && opAssets.length >= 5 && (mode.count || 0) === 0) {
    score = Math.min(score, 40);
    negative.push("Broad portfolio lists without project-similar verified comparables");
  }

  return knownScore(key, score, "Asset/development with Comparable Relevance Index (v2.1).", {
    positiveEvidence: positive,
    negativeEvidence: negative,
    v21: {
      cri,
      developmentMode: mode,
      blend: blended.blend,
    },
  });
}

export function scoreAllOperatorProjectFactorsV21(project, operator, opts = {}) {
  const formulationId = opts.criFormulation || V21_DEFAULT_CRI_FORMULATION;
  return [
    scoreGeographyFactorV21(project, operator),
    scoreSegmentFactorV21(project, operator),
    scoreAssetDevelopmentFactorV21(project, operator, formulationId),
    scoreProjectComplexityFactor(project, operator),
    scoreBrandExperienceFactor(project, operator),
    scoreOwnershipGovernanceFactor(project, operator),
    scoreRegionalResourcesFactor(project, operator),
    scoreCommercialDifferentiatorFactor(project, operator),
  ];
}

export { aggregateOperatorProjectAlignment };
