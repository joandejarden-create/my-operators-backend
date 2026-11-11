/**
 * ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1
 *
 * Makes governed BPP research (subject + required peers) a first-class Full ADP E2E
 * completion criterion when BPP is applicable and a measurable peer cohort exists.
 *
 * Distinguishes:
 *   CORE_ADP_CERTIFIED · BPP_CERTIFIED · FULL_ADP_E2E_CERTIFIED
 *   CUSTOMER_READY · CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS
 *
 * Zero paid. Does not invent ranks.
 */

import {
  planFullE2eBppMeasurement,
  assessFullE2eBppMeasurementCompleteness,
  BPP_MEASUREMENT_STATES,
  ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1,
  estimateBppPeerResearchCost,
} from "./adp-full-e2e-bpp-measurement-completeness-v1.js";
import { PROVIDER_CONFIGS } from "../execution/multi-provider-runner.js";
import { listCanonicalAdp31PropertyIds } from "../governance/adp-operational-registry-hygiene-v1.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";
import {
  CHAPULTEPEC_PROPERTY_ID,
  CHAPULTEPEC_APPROVED_INCLUDED_PEERS,
} from "../greenfield/adp-chapultepec-bpp-peer-cohort-v1.js";
import {
  CONDESA_PROPERTY_ID,
  getCondesaPeerSetIfApproved,
} from "../greenfield/adp-condesa-bpp-peer-cohort-v1.js";
import { createHash } from "crypto";

export const ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1 =
  "ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1";

export const BPP_REQUIRED_STATES = Object.freeze({
  BPP_NOT_APPLICABLE: "BPP_NOT_APPLICABLE",
  BPP_LENS_PENDING: "BPP_LENS_PENDING",
  BPP_PEER_GOVERNANCE_PENDING: "BPP_PEER_GOVERNANCE_PENDING",
  BPP_SUBJECT_MEASUREMENT_PENDING: "BPP_SUBJECT_MEASUREMENT_PENDING",
  BPP_PEER_MEASUREMENT_PENDING: "BPP_PEER_MEASUREMENT_PENDING",
  BPP_COMPARISON_INCOMPLETE: "BPP_COMPARISON_INCOMPLETE",
  BPP_COMPARISON_CERTIFIED: "BPP_COMPARISON_CERTIFIED",
  BPP_RANK_UNAVAILABLE_WITH_GOVERNED_REASON: "BPP_RANK_UNAVAILABLE_WITH_GOVERNED_REASON",
});

export const COMPLETION_TIERS = Object.freeze({
  CORE_ADP_CERTIFIED: "CORE_ADP_CERTIFIED",
  BPP_CERTIFIED: "BPP_CERTIFIED",
  FULL_ADP_E2E_CERTIFIED: "FULL_ADP_E2E_CERTIFIED",
  CUSTOMER_READY: "CUSTOMER_READY",
  CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS: "CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS",
  NOT_CUSTOMER_READY: "NOT_CUSTOMER_READY",
});

const LOYALTY_SCENARIOS = 28;
const PROVIDERS = ["openai", "gemini", "perplexity", "claude"];

function peerRosterHash(peers) {
  const ids = (peers || [])
    .map((p) => p.canonicalEntityId || p.propertyId || p.peerHotel)
    .filter(Boolean)
    .sort();
  return createHash("sha256").update(JSON.stringify(ids)).digest("hex").slice(0, 16);
}

/**
 * Pre-monitoring: complete subject + peer research budget that MUST appear
 * in the founder authorization request before any paid monitoring.
 */
export function buildMandatoryBppPremonitoringCostContract(propertyId) {
  const plan = planFullE2eBppMeasurement({ propertyId });
  if (!plan.ok && plan.error) {
    return { ok: false, contract: ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1, ...plan };
  }

  if (plan.bppApplies === false) {
    return {
      ok: true,
      contract: ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1,
      propertyId,
      bppRequired: false,
      state: BPP_REQUIRED_STATES.BPP_NOT_APPLICABLE,
      plan,
      researchBudget: {
        subjectLoyaltyUsd: 0,
        peerLoyaltyUsd: 0,
        totalBppUsd: 0,
        hardMaxSuggestedUsd: 0,
        note: "BPP not applicable — peer cost must not be invented",
      },
      blocksUnqualifiedFullE2eComplete: false,
    };
  }

  const missingPeers = (plan.peers?.included || []).filter((p) => !p.hasLoyaltyCorpus);
  const peerCost = estimateBppPeerResearchCost(missingPeers.length);
  const subjectUsd = plan.cost?.estimatedSubjectLoyaltyUsd || 0;
  const peerUsd = peerCost.estimatedPeerLoyaltyUsd || 0;
  const total = Number((subjectUsd + peerUsd).toFixed(2));
  // Bounded retry allowance (+$2.00) — matches Full E2E hardMaxSuggestedUsd pattern
  const hardMax = Number((total + 2).toFixed(2));

  let state = BPP_REQUIRED_STATES.BPP_COMPARISON_INCOMPLETE;
  if (plan.state === BPP_MEASUREMENT_STATES.BPP_SUBJECT_RESEARCH_PENDING) {
    state = BPP_REQUIRED_STATES.BPP_SUBJECT_MEASUREMENT_PENDING;
  } else if (!plan.peers?.membershipApproved && (plan.peers?.included || []).length === 0) {
    state = BPP_REQUIRED_STATES.BPP_PEER_GOVERNANCE_PENDING;
  } else if (missingPeers.length) {
    state = BPP_REQUIRED_STATES.BPP_PEER_MEASUREMENT_PENDING;
  } else if (plan.peers?.measuredRankEligible) {
    state = BPP_REQUIRED_STATES.BPP_COMPARISON_CERTIFIED;
  } else if ((plan.peers?.included || []).length === 0) {
    state = BPP_REQUIRED_STATES.BPP_RANK_UNAVAILABLE_WITH_GOVERNED_REASON;
  }

  const bppRequired =
    plan.bppApplies === true &&
    plan.peers?.membershipApproved === true &&
    (plan.peers?.included || []).length >= 3;

  return {
    ok: true,
    contract: ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1,
    completenessGate: ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1,
    propertyId,
    bppRequired,
    state,
    plan,
    peerRosterHash: peerRosterHash(plan.peers?.included),
    researchBudget: {
      lensId: plan.lens?.lensId || null,
      scenariosPerPeer: LOYALTY_SCENARIOS,
      providers: PROVIDERS,
      providerUnitCosts: Object.fromEntries(
        PROVIDERS.map((p) => [p, PROVIDER_CONFIGS[p]?.costPerCall || 0])
      ),
      peersMissing: missingPeers.map((p) => ({
        peerHotel: p.peerHotel,
        canonicalEntityId: p.canonicalEntityId,
        reusableObservations: p.reusableObservations || 0,
        missingObservations: Math.max(
          0,
          LOYALTY_SCENARIOS * PROVIDERS.length - (p.reusableObservations || 0)
        ),
      })),
      expectedPeerResponses: missingPeers.length * LOYALTY_SCENARIOS * PROVIDERS.length,
      subjectLoyaltyUsd: subjectUsd,
      peerLoyaltyUsd: peerUsd,
      estimatedTotalBppUsd: total,
      hardMaxSuggestedUsd: hardMax,
      retryAllowanceUsd: 2,
      note:
        "Hard max includes bounded retry allowance only. Do not reuse prior subject Core/Loyalty ceilings for peer research.",
    },
    blocksUnqualifiedFullE2eComplete: bppRequired && state !== BPP_REQUIRED_STATES.BPP_COMPARISON_CERTIFIED,
  };
}

/**
 * Gate: may this run claim FULL_ADP_E2E_CERTIFIED / unqualified CUSTOMER_READY?
 */
export function assessFullE2eCompleteBppRequired({
  propertyId,
  coreAdpCertified = false,
  disclosedLimitationsAccepted = false,
} = {}) {
  const prem = buildMandatoryBppPremonitoringCostContract(propertyId);
  const completeness = assessFullE2eBppMeasurementCompleteness({ propertyId });
  const failures = [];

  if (!prem.ok) {
    failures.push(prem.error || "BPP_PLAN_FAILED");
  }

  if (prem.bppRequired) {
    if (prem.state === BPP_REQUIRED_STATES.BPP_PEER_MEASUREMENT_PENDING) {
      failures.push("REQUIRED_PEER_RESEARCH_MISSING");
    }
    if (prem.state === BPP_REQUIRED_STATES.BPP_SUBJECT_MEASUREMENT_PENDING) {
      failures.push("REQUIRED_SUBJECT_LOYALTY_MISSING");
    }
    if (prem.state === BPP_REQUIRED_STATES.BPP_PEER_GOVERNANCE_PENDING) {
      failures.push("PEER_COHORT_NOT_APPROVED");
    }
    if (
      completeness.plan?.peers?.membershipApproved &&
      completeness.plan?.peers?.measuredRankEligible !== true &&
      /COMPARISON_CERTIFIED|FULL.*COMPLETE|CUSTOMER_READY/i.test(
        String(completeness.state || "")
      ) === false
    ) {
      // expected — peer pending
    }
  }

  // Fabricated rank when peers incomplete
  if ((completeness.failures || []).includes("PEER_DEPENDENT_RANK_FABRICATED")) {
    failures.push("PEER_DEPENDENT_RANK_FABRICATED");
  }
  if ((completeness.failures || []).includes("MARKED_FULLY_BPP_CERTIFIED_WITH_INCOMPLETE_PEERS")) {
    failures.push("MARKED_FULLY_BPP_CERTIFIED_WITH_INCOMPLETE_PEERS");
  }

  let completionTier = COMPLETION_TIERS.NOT_CUSTOMER_READY;
  const bppCertified = prem.state === BPP_REQUIRED_STATES.BPP_COMPARISON_CERTIFIED;
  const bppNa = prem.state === BPP_REQUIRED_STATES.BPP_NOT_APPLICABLE;
  const rankBlockedGoverned =
    prem.state === BPP_REQUIRED_STATES.BPP_RANK_UNAVAILABLE_WITH_GOVERNED_REASON;

  if (coreAdpCertified && bppCertified) {
    completionTier = COMPLETION_TIERS.FULL_ADP_E2E_CERTIFIED;
  } else if (bppCertified) {
    completionTier = COMPLETION_TIERS.BPP_CERTIFIED;
  } else if (coreAdpCertified && (bppNa || rankBlockedGoverned)) {
    completionTier = disclosedLimitationsAccepted
      ? COMPLETION_TIERS.CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS
      : COMPLETION_TIERS.CORE_ADP_CERTIFIED;
  } else if (coreAdpCertified && prem.blocksUnqualifiedFullE2eComplete) {
    completionTier = COMPLETION_TIERS.CORE_ADP_CERTIFIED;
    // Explicit: not FULL_ADP_E2E_CERTIFIED while required peers pending
  }

  const unqualifiedFullE2ePass =
    completionTier === COMPLETION_TIERS.FULL_ADP_E2E_CERTIFIED ||
    (bppNa && coreAdpCertified) ||
    (rankBlockedGoverned && coreAdpCertified && disclosedLimitationsAccepted);

  return {
    ok: failures.length === 0,
    contract: ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1,
    propertyId,
    prem,
    completeness,
    failures,
    completionTier,
    unqualifiedFullE2ePass,
    blocksUnqualifiedFullE2eComplete: Boolean(prem.blocksUnqualifiedFullE2eComplete),
    customerReadyAllowed:
      unqualifiedFullE2ePass ||
      (coreAdpCertified &&
        disclosedLimitationsAccepted &&
        !prem.blocksUnqualifiedFullE2eComplete),
  };
}

/**
 * Build peer-only authorization request (not yet approved).
 */
export function buildChapultepecPeerOnlyAuthorizationRequest({
  runId = "adp_e2e_adp_grand_fiesta_americana_chapultepec_2026-10-21T22-26-56-306Z_8965bc",
  propertyId = CHAPULTEPEC_PROPERTY_ID,
} = {}) {
  const prem = buildMandatoryBppPremonitoringCostContract(propertyId);
  const peers = CHAPULTEPEC_APPROVED_INCLUDED_PEERS;
  const budget = prem.researchBudget;

  return {
    ok: true,
    status: "AWAITING_FOUNDER_PAID_PEER_AUTHORIZATION",
    contract: ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1,
    handoffContract: "ADP_PAID_SUPERVISOR_HANDOFF_V1",
    runId,
    propertyId,
    hotelIdentity: "Grand Fiesta Americana Chapultepec",
    verifiedHpcRecordId: "recterKmNPdntCm0v",
    scope: {
      family: "PEER_LOYALTY_BPP_ONLY",
      coreExecute: false,
      coreReason: "ALREADY_COMPLETE_252_OBSERVATIONS — do not re-run",
      subjectLoyaltyExecute: false,
      subjectLoyaltyReason: "ALREADY_COMPLETE_112_OBSERVATIONS — do not re-run",
      peerLoyaltyAuthorized: true,
      lensId: "fiesta_rewards",
      scenariosPerPeer: LOYALTY_SCENARIOS,
      providers: PROVIDERS,
      approvedPeerCanonicalIds: peers.map((p) => p.canonicalEntityId),
      peerRosterHash: peerRosterHash(peers),
      peerRosterRef: "bps_adp_grand_fiesta_americana_chapultepec_fiesta_rewards_founder_v1",
    },
    cost: {
      estimatedUsd: budget.peerLoyaltyUsd,
      hardMaxUsd: budget.hardMaxSuggestedUsd,
      retryAllowanceUsd: budget.retryAllowanceUsd,
      expectedResponses: budget.expectedPeerResponses,
      byProvider: estimateBppPeerResearchCost(peers.length).byProvider,
      pricingScheduleVersion: "PROVIDER_CONFIGS_v1",
      priorSubjectAuthorizationMaxSpendUsd: 13.83,
      priorSubjectAuthorizationReusable: false,
      note: "Do NOT reuse the original $13.83 Core+subject ceiling for peer research.",
    },
    peers: peers.map((p) => ({
      peerHotel: p.peerHotel,
      canonicalEntityId: p.canonicalEntityId,
      market: p.market,
      evidenceSource: p.evidenceSource,
      reusableObservations: 0,
      requiredObservations: LOYALTY_SCENARIOS * PROVIDERS.length,
    })),
    publicationAuthorized: false,
    simulation: false,
    resumeInstruction:
      "After founder records exact authorization (approverIdentity + maxSpendUsd + peerLoyaltyAuthorized), resume the same durable run — no new Cursor prompt required for routine stages.",
  };
}

/**
 * Zero-paid audit of 31-hotel BPP applicability / completeness.
 */
export function auditAll31BppApplicabilityAndCompleteness() {
  const rows = [];
  for (const propertyId of listCanonicalAdp31PropertyIds()) {
    const prem = buildMandatoryBppPremonitoringCostContract(propertyId);
    const staged = loadStagedOwnerPayload(propertyId);
    const bpp = staged?.payload?.brandPortfolioPosition || null;
    const rank = (bpp?.kpis || []).find((k) => k.id === "portfolioRank")?.value || null;
    const peerSet = getBrandPortfolioPeerSet(propertyId);
    let classification = "UNCERTAIN";
    // Prefer staged customer evidence for the frozen 31-hotel baseline (do not
    // re-open certified ranks solely because draft peer reuse heuristics differ).
    if (rank && /^#\d+\s+of\s+\d+/i.test(String(rank))) {
      classification = "FULLY_BPP_CERTIFIED";
    } else if (bpp && (rank === "—" || rank == null) && bpp.status === "READY") {
      classification = "GOVERNED_RANK_BLOCKED";
    } else if (prem.state === BPP_REQUIRED_STATES.BPP_NOT_APPLICABLE) {
      classification = "BPP_N_A";
    } else if (prem.state === BPP_REQUIRED_STATES.BPP_COMPARISON_CERTIFIED) {
      classification = "FULLY_BPP_CERTIFIED";
    } else if (prem.state === BPP_REQUIRED_STATES.BPP_RANK_UNAVAILABLE_WITH_GOVERNED_REASON) {
      classification = "GOVERNED_RANK_BLOCKED";
    } else if (
      prem.bppRequired &&
      prem.state === BPP_REQUIRED_STATES.BPP_PEER_MEASUREMENT_PENDING
    ) {
      classification = "MISSING_REQUIRED_PEER_RESEARCH";
    } else if (prem.state === BPP_REQUIRED_STATES.BPP_PEER_GOVERNANCE_PENDING) {
      classification = "PEER_GOVERNANCE_PENDING";
    } else if (prem.state === BPP_REQUIRED_STATES.BPP_SUBJECT_MEASUREMENT_PENDING) {
      classification = "SUBJECT_LOYALTY_PENDING";
    } else if (prem.state === BPP_REQUIRED_STATES.BPP_PEER_MEASUREMENT_PENDING) {
      classification = "PEER_CORPORA_GAP_INVESTIGATE";
    }

    rows.push({
      propertyId,
      classification,
      bppRequiredState: prem.state,
      bppApplies: prem.bppApplies !== false && prem.plan?.bppApplies !== false,
      peerCount: (prem.plan?.peers?.included || peerSet?.included || []).length,
      peersMissing: (prem.plan?.peers?.missingCorpora || []).length,
      portfolioRank: rank,
      stagedVersionId: staged?.versionId || null,
      blocksUnqualifiedFullE2e: Boolean(prem.blocksUnqualifiedFullE2eComplete),
    });
  }
  return {
    contract: ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1,
    baselinePreserved: "ADP_CERTIFIED_31_HOTEL_BASELINE_20261021_V1",
    count: rows.length,
    rows,
    summary: {
      FULLY_BPP_CERTIFIED: rows.filter((r) => r.classification === "FULLY_BPP_CERTIFIED").length,
      BPP_N_A: rows.filter((r) => r.classification === "BPP_N_A").length,
      GOVERNED_RANK_BLOCKED: rows.filter((r) => r.classification === "GOVERNED_RANK_BLOCKED")
        .length,
      MISSING_REQUIRED_PEER_RESEARCH: rows.filter(
        (r) => r.classification === "MISSING_REQUIRED_PEER_RESEARCH"
      ).length,
      OTHER: rows.filter(
        (r) =>
          ![
            "FULLY_BPP_CERTIFIED",
            "BPP_N_A",
            "GOVERNED_RANK_BLOCKED",
            "MISSING_REQUIRED_PEER_RESEARCH",
          ].includes(r.classification)
      ).length,
    },
  };
}

export function condesaBppNonRegressionCheck() {
  const propertyId = CONDESA_PROPERTY_ID;
  const staged = loadStagedOwnerPayload(propertyId);
  const bpp = staged?.payload?.brandPortfolioPosition || null;
  const byId = Object.fromEntries((bpp?.kpis || []).map((k) => [k.id, k.value]));
  const peerSet = getCondesaPeerSetIfApproved();
  const failures = [];
  if (byId.portfolioAiPresence !== "63.4%") failures.push("PRESENCE_REGRESSION");
  if (byId.portfolioRank !== "#1 of 4") failures.push("RANK_REGRESSION");
  if (byId.numberOneAppearance !== "0.0%") failures.push("N1_REGRESSION");
  if (byId.top3Appearance !== "75.0%") failures.push("TOP3_REGRESSION");
  if ((peerSet?.included || []).length !== 3) failures.push("PEER_COHORT_SIZE");
  return {
    ok: failures.length === 0,
    propertyId,
    stagedVersionId: staged?.versionId || null,
    kpis: byId,
    peerCount: (peerSet?.included || []).length,
    failures,
    note: "Positive control — do not re-monitor; do not alter certified resort cohort",
  };
}

export function permanentCompleteBppRequiredNegativeFixtures() {
  const chap = assessFullE2eCompleteBppRequired({
    propertyId: CHAPULTEPEC_PROPERTY_ID,
    coreAdpCertified: true,
  });
  const fixtures = [
    {
      id: "BUDGET_OMITS_REQUIRED_PEERS",
      pass: chap.blocksUnqualifiedFullE2eComplete === true,
      detail: "Chapultepec with approved peers missing corpora must block unqualified FULL E2E",
    },
    {
      id: "APPROVED_COHORT_ABSENT_CORPORA_NO_UNRESTRICTED_PASS",
      pass: chap.unqualifiedFullE2ePass === false,
      detail: chap.completionTier,
    },
    {
      id: "REQUIRED_PEER_FAILURE_CODE",
      pass: (chap.failures || []).includes("REQUIRED_PEER_RESEARCH_MISSING"),
      detail: chap.failures,
    },
  ];
  const condesa = condesaBppNonRegressionCheck();
  fixtures.push({
    id: "CONDESA_POSITIVE_CONTROL",
    pass: condesa.ok === true,
    detail: condesa.kpis,
  });
  return {
    contract: ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1,
    pass: fixtures.every((f) => f.pass),
    fixtures,
  };
}
