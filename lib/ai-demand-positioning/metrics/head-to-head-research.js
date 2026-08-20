/**
 * Head-to-head positioning — RESEARCH ONLY.
 * No win/loss customer language.
 */

import { roundAdpPercent } from "../format-percent.js";
import { filterComparableObservations } from "./grain-governance.js";
import { matchesDeclaredComp, canonicalizeCompetitorName } from "../intelligence/competitor-name-resolution.js";
import { isRankEligibleObservation } from "./position-metrics.js";

export const HEAD_TO_HEAD_OUTCOMES = Object.freeze({
  SUBJECT_RANKED_ABOVE: "SUBJECT_RANKED_ABOVE",
  COMPETITOR_RANKED_ABOVE: "COMPETITOR_RANKED_ABOVE",
  TIED_SAME_TIER: "TIED_SAME_TIER",
  BOTH_PRESENT_UNRANKED: "BOTH_PRESENT_UNRANKED",
  NOT_COMPARABLE: "NOT_COMPARABLE",
});

export function computeHeadToHeadResearch(observations, propertyProfile) {
  const market = propertyProfile?.market || "";
  const declared = propertyProfile.declaredCompSet || [];
  const comparable = filterComparableObservations(observations);
  const outcomes = {
    SUBJECT_RANKED_ABOVE: 0,
    COMPETITOR_RANKED_ABOVE: 0,
    TIED_SAME_TIER: 0,
    BOTH_PRESENT_UNRANKED: 0,
    NOT_COMPARABLE: 0,
  };
  const byCompetitor = {};

  for (const obs of comparable) {
    if (!obs.mentioned) continue;
    const comps = (obs.competitorsMentioned || [])
      .map((c) => canonicalizeCompetitorName(c, { market }) || c)
      .filter((c) => declared.some((d) => matchesDeclaredComp(c, d)));

    for (const comp of comps) {
      if (!byCompetitor[comp]) {
        byCompetitor[comp] = { ...outcomes };
      }
      let outcome;
      if (!isRankEligibleObservation(obs)) {
        outcome = HEAD_TO_HEAD_OUTCOMES.BOTH_PRESENT_UNRANKED;
      } else {
        const compMentioned = (obs.competitorsMentioned || []).some((c) =>
          matchesDeclaredComp(c, comp)
        );
        if (!compMentioned) {
          outcome = HEAD_TO_HEAD_OUTCOMES.NOT_COMPARABLE;
        } else {
          // Without per-competitor rank parsing, research marks as NOT_COMPARABLE when both ranked
          outcome = HEAD_TO_HEAD_OUTCOMES.NOT_COMPARABLE;
        }
      }
      outcomes[outcome] += 1;
      byCompetitor[comp][outcome] += 1;
    }
  }

  const candidateCases = Object.values(outcomes).reduce((a, b) => a + b, 0);

  return {
    candidateCases,
    outcomes,
    byCompetitor,
    validationStatus: "INSUFFICIENT_PER_COMPETITOR_RANK",
    recommendedCustomerLabel: "Head-to-Head Position (research)",
    winLossLanguageAllowed: false,
    note: "Per-competitor rank extraction required before Head-to-Head Position Rate can be validated.",
  };
}

export function computeHeadToHeadPositionRate(observations, propertyProfile) {
  const research = computeHeadToHeadResearch(observations, propertyProfile);
  const ranked = research.outcomes.SUBJECT_RANKED_ABOVE + research.outcomes.COMPETITOR_RANKED_ABOVE;
  const total = ranked + research.outcomes.TIED_SAME_TIER;
  return {
    rate: total > 0 ? roundAdpPercent((research.outcomes.SUBJECT_RANKED_ABOVE / total) * 100) : null,
    ...research,
  };
}
