/**
 * Shared acceptance rubric across all providers (0–3 per criterion).
 */

import { CLAIM_ACCEPTANCE, EVIDENCE_LAYER } from "./contract.js";
import { coverageReport } from "./claim-schema.js";

export const RUBRIC_CRITERIA = Object.freeze([
  { id: "ownership_correctness", weight: 1 },
  { id: "owner_operator_distinction", weight: 1 },
  { id: "relationship_graph_usefulness", weight: 1 },
  { id: "decision_maker_relevance", weight: 1 },
  { id: "public_contact_usefulness", weight: 1 },
  { id: "source_quality_freshness", weight: 1 },
  { id: "hallucination_penalty", weight: 1, subtract: true },
  { id: "completeness", weight: 1 },
]);

export const PASS_THRESHOLD_NORMALIZED = 0.7;

function clamp03(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(3, x));
}

/**
 * Score a normalized claim set. Blind-safe: does not require armId.
 */
export function scoreClaims(claims = [], opts = {}) {
  const accepted = claims.filter(
    (c) => c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] === CLAIM_ACCEPTANCE.ACCEPTED
  );
  const unsupported = claims.filter(
    (c) =>
      c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] === CLAIM_ACCEPTANCE.REJECTED ||
      c[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.claimType === "UNSUPPORTED" ||
      (!c[EVIDENCE_LAYER.SUPPORTING_SOURCE]?.url &&
        c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] === CLAIM_ACCEPTANCE.ACCEPTED)
  );
  const unknownForced = claims.filter(
    (c) => c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] === CLAIM_ACCEPTANCE.UNKNOWN
  );

  const types = new Set(
    accepted.map((c) => c[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.claimType)
  );

  const scores = {
    ownership_correctness: types.has("OWNER_ENTITY") || types.has("ECONOMIC_SPONSOR") ? 3 : 0,
    owner_operator_distinction: types.has("OWNER_VS_OPERATOR") ? 3 : 0,
    relationship_graph_usefulness: types.has("ORG_RELATIONSHIP") ? 3 : types.size ? 1 : 0,
    decision_maker_relevance: types.has("DECISION_MAKER") ? 3 : 0,
    public_contact_usefulness:
      types.has("PUBLIC_EMAIL") || types.has("PUBLIC_PHONE") ? 3 : 0,
    source_quality_freshness: accepted.every((c) => c[EVIDENCE_LAYER.SUPPORTING_SOURCE]?.url)
      ? accepted.length
        ? 3
        : 0
      : accepted.length
        ? 1
        : 0,
    hallucination_penalty: Math.min(3, unsupported.length),
    completeness: 0,
  };

  const coverage = coverageReport(claims);
  scores.completeness = clamp03(
    Math.round((coverage.coveredCount / Math.max(1, coverage.totalRequired - 1)) * 3)
  );

  if (opts.scores) {
    for (const [k, v] of Object.entries(opts.scores)) {
      if (k in scores) scores[k] = clamp03(v);
    }
  }

  let positive = 0;
  let positiveMax = 0;
  let penalty = 0;
  for (const c of RUBRIC_CRITERIA) {
    const v = clamp03(scores[c.id]);
    if (c.subtract) penalty += v;
    else {
      positive += v;
      positiveMax += 3;
    }
  }
  const raw = Math.max(0, positive - penalty);
  const normalized = positiveMax ? Number((raw / positiveMax).toFixed(4)) : 0;
  const criticalUnsupportedOwnership = claims.filter((c) => {
    const t = c[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.claimType;
    return (
      (t === "OWNER_ENTITY" || t === "ECONOMIC_SPONSOR") &&
      c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] !== CLAIM_ACCEPTANCE.ACCEPTED &&
      c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] !== CLAIM_ACCEPTANCE.UNKNOWN &&
      !c[EVIDENCE_LAYER.SUPPORTING_SOURCE]?.url
    );
  }).length;

  const pass =
    normalized >= PASS_THRESHOLD_NORMALIZED && criticalUnsupportedOwnership === 0;

  return {
    scores,
    positive,
    penalty,
    raw,
    normalized,
    pass,
    passThreshold: PASS_THRESHOLD_NORMALIZED,
    unsupportedCount: unsupported.length,
    unknownCount: unknownForced.length,
    acceptedCount: accepted.length,
    criticalUnsupportedOwnership,
    coverage,
  };
}

/**
 * Unit economics helpers — never coerce UNKNOWN USD to 0.
 */
export function computeCostEfficiency({ costUsd, costUsdStatus, verifiedUsefulFacts, verifiedDecisionMakers, hotelCount = 1 } = {}) {
  const unknown = costUsdStatus === "UNKNOWN" || costUsd == null || !Number.isFinite(Number(costUsd));
  if (unknown) {
    return {
      cost_per_verified_useful_fact: null,
      cost_per_verified_decision_maker: null,
      cost_per_canonical_hotel: null,
      cost_usd_status: "UNKNOWN",
    };
  }
  const usd = Number(costUsd);
  const facts = Math.max(0, Number(verifiedUsefulFacts || 0));
  const dms = Math.max(0, Number(verifiedDecisionMakers || 0));
  const hotels = Math.max(1, Number(hotelCount || 1));
  return {
    cost_per_verified_useful_fact: facts ? Number((usd / facts).toFixed(6)) : null,
    cost_per_verified_decision_maker: dms ? Number((usd / dms).toFixed(6)) : null,
    cost_per_canonical_hotel: Number((usd / hotels).toFixed(6)),
    cost_usd_status: costUsdStatus || "REPORTED",
  };
}
