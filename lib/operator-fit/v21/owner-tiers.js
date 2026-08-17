/**
 * Operator Fit v2.1 — owner candidate tiers + tie-aware presentation helpers.
 */

import { ELIGIBILITY_STATUS } from "../config.js";
import { READINESS_STATUS } from "../readiness.js";
import { mapAlignmentBand, OWNER_ALIGNMENT_BANDS } from "../owner-presentation.js";
import { V21_OWNER_TIERS, V21_TIE_MATERIALITY_POINTS } from "./config.js";

function bandId(displayed, eligibilityStatus) {
  return mapAlignmentBand(displayed, eligibilityStatus).id;
}

/**
 * Assign owner-facing tier for a single evaluated candidate.
 */
export function assignOwnerCandidateTier(candidate = {}, opts = {}) {
  const research =
    opts.researchStage === true ||
    candidate.researchStage === true ||
    /research/i.test(String(candidate.lifecycle || candidate.candidateLane || ""));

  if (research) {
    return {
      tier: V21_OWNER_TIERS.UNDER_EVALUATION,
      tierId: "under_evaluation",
      showOrdinal: false,
    };
  }

  const elig = String(candidate.eligibilityStatus || candidate.eligibility || "");
  const hardFail = /not currently eligible/i.test(elig);
  const displayed =
    Number(candidate.displayedOperatorAlignment ?? candidate.displayed ?? candidate.alignment) || 0;
  const band = bandId(displayed, elig);
  const readiness = String(candidate.readiness || candidate.readinessStatus || "");
  const isRR = readiness === READINESS_STATUS.RANKING_READY || /ranking ready/i.test(readiness);
  const preferredOrEligible =
    elig === ELIGIBILITY_STATUS.PREFERRED || elig === ELIGIBILITY_STATUS.ELIGIBLE;
  const withConditions = /eligible with conditions/i.test(elig);

  if (
    isRR &&
    !hardFail &&
    preferredOrEligible &&
    (band === "strong" || band === "good")
  ) {
    return {
      tier: V21_OWNER_TIERS.LEADING,
      tierId: "leading",
      showOrdinal: true,
    };
  }

  if (
    !hardFail &&
    (band === "potential" ||
      (withConditions && (band === "good" || band === "strong" || band === "potential")) ||
      (isRR && withConditions && displayed >= 40))
  ) {
    return {
      tier: V21_OWNER_TIERS.POTENTIAL,
      tierId: "potential",
      showOrdinal: true,
    };
  }

  return {
    tier: V21_OWNER_TIERS.ADDITIONAL,
    tierId: "additional",
    showOrdinal: true,
  };
}

/**
 * Group candidates into tiers; suppress ordinals within material ties.
 */
export function buildOwnerTierPresentation(candidates = [], opts = {}) {
  const materiality = opts.tieMateriality ?? V21_TIE_MATERIALITY_POINTS;
  const decorated = (candidates || []).map((c, idx) => {
    const tierInfo = assignOwnerCandidateTier(c, opts);
    return {
      ...c,
      ...tierInfo,
      displayed: Number(c.displayedOperatorAlignment ?? c.displayed ?? 0),
      _sortIndex: idx,
    };
  });

  const buckets = {
    leading: [],
    potential: [],
    additional: [],
    under_evaluation: [],
  };
  for (const c of decorated) {
    if (c.tierId === "leading") buckets.leading.push(c);
    else if (c.tierId === "potential") buckets.potential.push(c);
    else if (c.tierId === "under_evaluation") buckets.under_evaluation.push(c);
    else buckets.additional.push(c);
  }

  function applyTieAwareOrdinals(list) {
    const sorted = [...list].sort((a, b) => {
      const ds = (b.displayed || 0) - (a.displayed || 0);
      if (Math.abs(ds) >= materiality) return ds;
      return String(a.candidateId || "").localeCompare(String(b.candidateId || ""));
    });
    let groupRank = 1;
    const out = [];
    for (let i = 0; i < sorted.length; i++) {
      const cur = sorted[i];
      const prev = sorted[i - 1];
      if (prev && Math.abs((prev.displayed || 0) - (cur.displayed || 0)) < materiality) {
        // same ordinal group — no distinct owner rank
        out.push({
          ...cur,
          ownerOrdinal: null,
          ownerOrdinalLabel: null,
          tiedWithPrevious: true,
          internalSortRank: i + 1,
          tieGroup: groupRank,
        });
      } else {
        groupRank = i + 1;
        const next = sorted[i + 1];
        const tiedNext =
          next && Math.abs((next.displayed || 0) - (cur.displayed || 0)) < materiality;
        out.push({
          ...cur,
          ownerOrdinal: tiedNext ? null : groupRank,
          ownerOrdinalLabel: tiedNext ? null : String(groupRank),
          tiedWithPrevious: false,
          internalSortRank: i + 1,
          tieGroup: groupRank,
          tieNote: tiedNext
            ? "These candidates are not meaningfully separated by the current Operator Alignment calculation."
            : null,
        });
      }
    }
    return out;
  }

  return {
    materialityPoints: materiality,
    tiers: {
      [V21_OWNER_TIERS.LEADING]: applyTieAwareOrdinals(buckets.leading),
      [V21_OWNER_TIERS.POTENTIAL]: applyTieAwareOrdinals(buckets.potential),
      [V21_OWNER_TIERS.ADDITIONAL]: applyTieAwareOrdinals(buckets.additional),
      [V21_OWNER_TIERS.UNDER_EVALUATION]: applyTieAwareOrdinals(buckets.under_evaluation),
    },
    counts: {
      leading: buckets.leading.length,
      potential: buckets.potential.length,
      additional: buckets.additional.length,
      underEvaluation: buckets.under_evaluation.length,
    },
  };
}

export { OWNER_ALIGNMENT_BANDS, V21_OWNER_TIERS, V21_TIE_MATERIALITY_POINTS };
