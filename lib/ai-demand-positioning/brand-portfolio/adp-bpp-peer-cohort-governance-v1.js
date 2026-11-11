/**
 * ADP_BPP_PEER_COHORT_GOVERNANCE_V1
 *
 * Universal peer-cohort governance for Brand & Portfolio Position.
 * Lenses are mutually exclusive unless an explicit dual-lens protocol is certified.
 */

export const ADP_BPP_PEER_COHORT_GOVERNANCE_V1 = "ADP_BPP_PEER_COHORT_GOVERNANCE_V1";

export const BPP_COMPARISON_LENSES_V1 = Object.freeze({
  BRAND: {
    id: "BRAND",
    description: "Same verified hard brand (e.g. Westin, YOTEL, Nobu Hotels).",
  },
  COLLECTION: {
    id: "COLLECTION",
    description: "Same verified soft-brand / collection (e.g. Autograph, Curio, SLH).",
  },
  PORTFOLIO: {
    id: "PORTFOLIO",
    description: "Defined operating/ownership portfolio with certified membership.",
  },
  LOYALTY: {
    id: "LOYALTY",
    description: "Same verified loyalty ecosystem (e.g. Marriott Bonvoy, Wyndham Rewards, Hilton Honors).",
  },
  INDEPENDENT_POSITIONING: {
    id: "INDEPENDENT_POSITIONING",
    description: "True independents / lifestyle independents under Independent Positioning V1.",
  },
  NOT_APPLICABLE: {
    id: "NOT_APPLICABLE",
    description: "No governed BPP lens applies for this property under current doctrine.",
  },
});

/** Eligibility / exclusion doctrine (selection rules — not arbitrary size padding). */
export const PEER_ELIGIBILITY_RULES_V1 = Object.freeze({
  requireVerifiedAffiliation: true,
  requireMarketOrDemandRelevance: true,
  requireComparablePositioning: true,
  forbidPadToMinimum: true,
  forbidCrossLensMixingWithoutProtocol: true,
  forbidGlobalLoyaltyDump: true,
  subjectNeverCountsAsPeer: true,
  rebrandsRequireFreshAffiliationCheck: true,
  unknownAppearanceIsNotZero: true,
  eligibleAbsentIsVerifiedZeroOnlyWhenUniverseComplete: true,
  rankUsablePeerMinimum: 3,
  preferredPeerCount: 5,
  tooThinPeerMaximumInclusive: 2,
  founderCertificationRequiredBeforeReadyRank: true,
  observedMentionNotRequiredForEligibility: true,
  observedMentionRequiredForPositiveAppearanceRate: true,
});

export const PEER_APPEARANCE_CLASS_V1 = Object.freeze({
  A_OBSERVED_IN_COMPLETE_UNIVERSE: "A_OBSERVED_IN_COMPLETE_UNIVERSE",
  B_ABSENT_VERIFIED_ZERO_COMPLETE_UNIVERSE: "B_ABSENT_VERIFIED_ZERO_COMPLETE_UNIVERSE",
  C_INCOMPLETE_OR_INCOMPATIBLE_COVERAGE: "C_INCOMPLETE_OR_INCOMPATIBLE_COVERAGE",
  D_NOT_SCENARIO_ELIGIBLE: "D_NOT_SCENARIO_ELIGIBLE",
});

export const COHORT_AUDIT_CLASS_V1 = Object.freeze({
  VALID_CERTIFIED: "VALID_CERTIFIED",
  VALID_WITH_DOCUMENTED_LIMITATIONS: "VALID_WITH_DOCUMENTED_LIMITATIONS",
  REQUIRES_COHORT_CORRECTION: "REQUIRES_COHORT_CORRECTION",
  REQUIRES_RANK_RECALCULATION: "REQUIRES_RANK_RECALCULATION",
  INSUFFICIENT_COMPARABLE_EVIDENCE: "INSUFFICIENT_COMPARABLE_EVIDENCE",
  INVALID: "INVALID",
  PROPOSED_AWAITING_FOUNDER: "PROPOSED_AWAITING_FOUNDER",
  NOT_APPLICABLE_CONFIRMED: "NOT_APPLICABLE_CONFIRMED",
  NOT_APPLICABLE_REASSESS: "NOT_APPLICABLE_REASSESS",
});

export function mapLensIdToGovernanceFamily(lensId) {
  const id = String(lensId || "").toLowerCase();
  if (!id) return null;
  if (id.includes("independent")) return BPP_COMPARISON_LENSES_V1.INDEPENDENT_POSITIONING.id;
  if (
    id.includes("bonvoy") ||
    id.includes("honors") ||
    id.includes("wyndham_rewards") ||
    id.includes("one_rewards") ||
    id.includes("choice_privileges") ||
    id.includes("rewards")
  ) {
    return BPP_COMPARISON_LENSES_V1.LOYALTY.id;
  }
  if (
    id.includes("autograph") ||
    id.includes("curio") ||
    id.includes("collection") ||
    id.includes("slh") ||
    id.includes("vignette") ||
    id.includes("tapestry")
  ) {
    return BPP_COMPARISON_LENSES_V1.COLLECTION.id;
  }
  if (id.includes("governed_affiliation") || id.includes("hotels") || id.includes("brand")) {
    return BPP_COMPARISON_LENSES_V1.BRAND.id;
  }
  if (id.includes("not_applicable") || id === "n/a") {
    return BPP_COMPARISON_LENSES_V1.NOT_APPLICABLE.id;
  }
  return BPP_COMPARISON_LENSES_V1.BRAND.id;
}

export function governanceContractMarkdown() {
  return `# ADP_BPP_PEER_COHORT_GOVERNANCE_V1

Contract id: \`${ADP_BPP_PEER_COHORT_GOVERNANCE_V1}\`

## Purpose
One defensible peer-cohort governance system for the entire ADP portfolio. Selection and measurement rules are universal; peer membership and cohort size may legitimately differ by hotel.

## Comparison lenses (do not mix without an explicit dual-lens protocol)

| Lens | Meaning |
|------|---------|
| BRAND | Same verified hard brand |
| COLLECTION | Same verified soft-brand / collection |
| PORTFOLIO | Certified operating/ownership portfolio |
| LOYALTY | Same verified loyalty ecosystem |
| INDEPENDENT_POSITIONING | True independent / lifestyle independent (Independent Positioning V1) |
| NOT_APPLICABLE | No governed BPP lens applies |

**Marriott Bonvoy ≠ Marriott Hotels brand.** A Bonvoy loyalty cohort is not automatically every Bonvoy hotel worldwide — geography and positioning filters still apply.

## Eligibility rules
${Object.entries(PEER_ELIGIBILITY_RULES_V1)
  .map(([k, v]) => `- **${k}:** \`${v}\``)
  .join("\n")}

## Appearance classes (eligibility ≠ measured appearance)
| Class | Meaning |
|-------|---------|
| A | Explicitly observed in a complete shared response universe |
| B | Absent from a complete shared response universe → **verified zero** |
| C | Incomplete / incompatible coverage → **unknown, not zero** |
| D | Not meaningfully eligible for the measured scenarios |

## Rank usability
- Rank requires a **founder-certified** peer set with \`canRank\` and peer count ≥ ${PEER_ELIGIBILITY_RULES_V1.rankUsablePeerMinimum} (preferred ≥ ${PEER_ELIGIBILITY_RULES_V1.preferredPeerCount}).
- Do **not** pad cohorts solely to hit the minimum.
- A genuinely narrow brand/collection may remain rank-blocked if local same-lens peers do not exist.
- Subject hotel is included in the ranking universe but never as a peer row.

## Approval
- Proposed cohorts stay \`PROPOSED\` until founder approval.
- Do not record \`FOUNDER_CERTIFIED\` without explicit approval.
- READY_RANK requires certified cohort + complete comparable Loyalty STANDARD-28 (or governed measurement) archive + independent recalculation match.

## Future Full E2E gate
No paid BPP measurement launches without a versioned governed peer-cohort definition under this contract.
`;
}
