/**
 * Deterministic Top-5 Operator Alignment selector.
 */

import { TOP5_MAX, ELIGIBILITY_STATUS, EVIDENCE_CONFIDENCE } from "./config.js";
import { isOwnerFacingEligible } from "./eligibility.js";
import { evaluateCandidate } from "./evaluate-candidate.js";

const CONF_RANK = {
  [EVIDENCE_CONFIDENCE.STRONG.label]: 3,
  [EVIDENCE_CONFIDENCE.MODERATE.label]: 2,
  [EVIDENCE_CONFIDENCE.LIMITED.label]: 1,
};

function compareCandidates(a, b) {
  // 1 Eligibility preference: Preferred > Eligible > With Conditions
  const eligRank = (s) => {
    if (s === ELIGIBILITY_STATUS.PREFERRED) return 3;
    if (s === ELIGIBILITY_STATUS.ELIGIBLE) return 2;
    if (s === ELIGIBILITY_STATUS.WITH_CONDITIONS) return 1;
    return 0;
  };
  const er = eligRank(b.eligibilityStatus) - eligRank(a.eligibilityStatus);
  if (er) return er;

  // 2 Displayed alignment
  const ds = (b.displayedOperatorAlignment || 0) - (a.displayedOperatorAlignment || 0);
  if (ds) return ds;

  // 3 Evidence confidence
  const cr =
    (CONF_RANK[b.evidenceConfidence] || 0) - (CONF_RANK[a.evidenceConfidence] || 0);
  if (cr) return cr;

  // 4 Data coverage
  const cov = (b.dataCoveragePct || 0) - (a.dataCoveragePct || 0);
  if (cov) return cov;

  // 5 Lower execution risk better
  const risk = (a.executionRiskPenalty || 0) - (b.executionRiskPenalty || 0);
  if (risk) return risk;

  // 6 Stable tie-breaker
  return String(a.candidateId).localeCompare(String(b.candidateId));
}

/**
 * @param {object} project
 * @param {object[]} operators - adapted operator domain models
 * @returns {{ top5: object[], diagnostics: object }}
 */
export function selectTop5OperatorAlignment(project, operators, opts = {}) {
  const evaluated = (operators || []).map((op) => evaluateCandidate(project, op, opts));

  const excluded = evaluated.filter((e) => !isOwnerFacingEligible(e.eligibilityStatus));
  const eligible = evaluated.filter((e) => isOwnerFacingEligible(e.eligibilityStatus));

  // Do not fill with weak unsupported candidates — exclude Not Eligible only;
  // With Conditions allowed but sorted below Eligible/Preferred
  const sorted = [...eligible].sort(compareCandidates);

  const top5 = sorted.slice(0, TOP5_MAX).map((e, i) => ({
    ...e,
    rank: i + 1,
  }));

  const diagnostics = {
    candidateUniverseCount: evaluated.length,
    eligibleCount: eligible.length,
    excludedCount: excluded.length,
    returnedCount: top5.length,
    fewerThanFiveReason:
      top5.length < TOP5_MAX
        ? `Only ${top5.length} owner-facing-eligible candidates after eligibility filters (do not pad to five).`
        : null,
    excludedCandidates: excluded.map((e) => ({
      candidateId: e.candidateId,
      operatorName: e.operatorName,
      eligibilityStatus: e.eligibilityStatus,
      hardConflicts: e.eligibilityHardConflicts,
    })),
    rankingOrder: top5.map((e) => ({
      rank: e.rank,
      candidateId: e.candidateId,
      displayed: e.displayedOperatorAlignment,
      raw: e.rawOperatorAlignment,
      confidence: e.evidenceConfidence,
      coverage: e.dataCoveragePct,
      eligibility: e.eligibilityStatus,
      ceilingApplied: e.confidenceCeilingApplied,
    })),
    ceilingsApplied: top5
      .filter((e) => e.confidenceCeilingApplied != null)
      .map((e) => ({
        candidateId: e.candidateId,
        ceiling: e.confidenceCeilingApplied,
        raw: e.rawOperatorAlignment,
        displayed: e.displayedOperatorAlignment,
      })),
  };

  return { top5, diagnostics, allEvaluated: evaluated };
}
