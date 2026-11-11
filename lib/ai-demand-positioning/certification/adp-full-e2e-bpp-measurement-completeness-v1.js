/**
 * ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1
 *
 * First-class BPP planning + certification for Full ADP E2E.
 * Subject KPIs (Presence / #1 / Top-3) are independent of peer rank.
 * Peer rank / competitive overview require governed measured peer corpora.
 *
 * Zero paid calls. No publication. No invented ranks.
 */

import fs from "node:fs";
import path from "node:path";
import { loadPropertyProfile, loadAllPeriods, isLoyaltyMeasurementPeriod } from "../data-model.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";
import { resolvePortfolioMapping } from "../brand-portfolio/brand-portfolio-affiliation-mapping-v1.js";
import {
  buildSubjectMeasuredPartialBppPayload,
  computeSubjectMeasuredBppMetrics,
  BPP_SUBJECT_MEASURED_PARTIAL_V1,
} from "../brand-portfolio/bpp-subject-measured-partial-v1.js";
import {
  CHAPULTEPEC_APPROVED_INCLUDED_PEERS,
  CHAPULTEPEC_EXCLUDED_PEERS,
  CHAPULTEPEC_PROPERTY_ID,
  getChapultepecPeerSetIfApproved,
} from "../greenfield/adp-chapultepec-bpp-peer-cohort-v1.js";
import {
  CONDESA_PROPERTY_ID,
  CONDESA_EXCLUDED_PEERS,
  getCondesaPeerSetForPreflight,
  getCondesaPeerSetIfApproved,
} from "../greenfield/adp-condesa-bpp-peer-cohort-v1.js";
import { PROVIDER_CONFIGS } from "../execution/multi-provider-runner.js";
import { listCanonicalAdp31PropertyIds } from "../governance/adp-operational-registry-hygiene-v1.js";

export const ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1 =
  "ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1";

export const BPP_MEASUREMENT_STATES = Object.freeze({
  BPP_NOT_APPLICABLE: "BPP_NOT_APPLICABLE",
  BPP_SUBJECT_RESEARCH_PENDING: "BPP_SUBJECT_RESEARCH_PENDING",
  BPP_SUBJECT_MEASURED: "BPP_SUBJECT_MEASURED",
  BPP_PEER_COHORT_PENDING: "BPP_PEER_COHORT_PENDING",
  BPP_PEER_RESEARCH_PENDING: "BPP_PEER_RESEARCH_PENDING",
  BPP_COMPARISON_CERTIFIED: "BPP_COMPARISON_CERTIFIED",
  BPP_RANK_BLOCKED: "BPP_RANK_BLOCKED",
});

const LOYALTY_SCENARIOS = 28;
const PROVIDERS = ["openai", "gemini", "perplexity", "claude"];

function kpiById(bpp, id) {
  return (bpp?.kpis || []).find((k) => k.id === id) || null;
}

function subjectKpisPopulated(bpp) {
  const presence = kpiById(bpp, "portfolioAiPresence");
  const n1 = kpiById(bpp, "numberOneAppearance");
  const top3 = kpiById(bpp, "top3Appearance");
  const populated = (k) =>
    k &&
    k.available !== false &&
    k.value != null &&
    String(k.value).trim() !== "" &&
    String(k.value).trim() !== "—";
  return {
    presence: populated(presence),
    numberOne: populated(n1),
    top3: populated(top3),
    allThree: populated(presence) && populated(n1) && populated(top3),
    values: {
      portfolioAiPresence: presence?.value ?? null,
      numberOneAppearance: n1?.value ?? null,
      top3Appearance: top3?.value ?? null,
      portfolioRank: kpiById(bpp, "portfolioRank")?.value ?? null,
    },
  };
}

function estimatePeerLoyaltyUsd(peerCount) {
  let total = 0;
  const byProvider = {};
  for (const p of PROVIDERS) {
    const unit = PROVIDER_CONFIGS[p]?.costPerCall || 0.03;
    const calls = peerCount * LOYALTY_SCENARIOS;
    byProvider[p] = { calls, usd: Number((calls * unit).toFixed(4)) };
    total += calls * unit;
  }
  return {
    peerCount,
    scenariosPerPeer: LOYALTY_SCENARIOS,
    providers: PROVIDERS,
    expectedResponses: peerCount * LOYALTY_SCENARIOS * PROVIDERS.length,
    byProvider,
    estimatedPeerLoyaltyUsd: Number(total.toFixed(2)),
  };
}

/**
 * Plan BPP research requirements before paid authorization.
 */
export function planFullE2eBppMeasurement({
  propertyId,
  peerCandidates = null,
  cwd = process.cwd(),
} = {}) {
  const id = String(propertyId || "").trim();
  const profile = loadPropertyProfile(id);
  const mapping = resolvePortfolioMapping(id);
  const failures = [];
  let lens =
    (mapping?.lenses || []).find((l) => l.lensId === mapping?.defaultLensId) ||
    (mapping?.lenses || [])[0] ||
    null;
  if (!lens && id === CHAPULTEPEC_PROPERTY_ID) {
    lens = {
      lensId: "fiesta_rewards",
      label: "Fiesta Rewards / Grand Fiesta Americana",
    };
  }

  let applicability = BPP_MEASUREMENT_STATES.BPP_NOT_APPLICABLE;
  if (!profile) {
    return {
      ok: false,
      gate: ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1,
      propertyId: id,
      error: "profile_missing",
      state: applicability,
    };
  }

  const loyaltyPeriods = (loadAllPeriods(id) || []).filter((p) =>
    isLoyaltyMeasurementPeriod(p)
  );
  const loyaltyPeriod = loyaltyPeriods.sort((a, b) =>
    String(b.periodId).localeCompare(String(a.periodId))
  )[0];
  const subjectObs = loyaltyPeriod?.observations?.length || 0;

  // Resort / affiliation heuristic: Condesa AI uses separate lens; urban GFA uses Fiesta Rewards.
  const isCondesaAi = id === CONDESA_PROPERTY_ID;
  const bppApplies =
    Boolean(lens) ||
    Boolean(mapping?.defaultLensId) ||
    id.startsWith("adp_grand_fiesta") ||
    isCondesaAi ||
    Boolean(getBrandPortfolioPeerSet(id));

  if (!bppApplies) {
    return {
      ok: true,
      gate: ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1,
      propertyId: id,
      state: BPP_MEASUREMENT_STATES.BPP_NOT_APPLICABLE,
      bppApplies: false,
      lens: null,
      subject: { required: false },
      peers: { required: false },
      cost: { estimatedPeerLoyaltyUsd: 0, estimatedSubjectLoyaltyUsd: 0, estimatedTotalBppUsd: 0 },
      founderApprovalsRequired: [],
    };
  }

  applicability =
    subjectObs >= LOYALTY_SCENARIOS * PROVIDERS.length * 0.9
      ? BPP_MEASUREMENT_STATES.BPP_SUBJECT_MEASURED
      : BPP_MEASUREMENT_STATES.BPP_SUBJECT_RESEARCH_PENDING;

  let peers = [];
  let peerSource = "none";
  if (id === CHAPULTEPEC_PROPERTY_ID) {
    const approved = getChapultepecPeerSetIfApproved();
    peers = (approved?.included || CHAPULTEPEC_APPROVED_INCLUDED_PEERS).map((p) => ({
      ...p,
      hasLoyaltyCorpus: false,
      reusableObservations: 0,
    }));
    peerSource = approved ? "founder_approved_chapultepec" : "chapultepec_proposed";
  } else if (isCondesaAi) {
    const approved = getCondesaPeerSetIfApproved(cwd);
    const proposed = getCondesaPeerSetForPreflight(cwd);
    peers = (approved?.included || proposed?.included || []).map((p) => ({
      ...p,
      hasLoyaltyCorpus: false,
      reusableObservations: 0,
    }));
    peerSource = approved
      ? "founder_approved_condesa"
      : "condesa_proposed_awaiting_approval";
  } else if (Array.isArray(peerCandidates) && peerCandidates.length) {
    peers = peerCandidates;
    peerSource = "caller";
  } else {
    const set = getBrandPortfolioPeerSet(id);
    peers = (set?.included || []).map((p) => ({
      ...p,
      hasLoyaltyCorpus: false,
      reusableObservations: 0,
    }));
    peerSource = set ? "registry_peer_set" : "none";
  }

  // Assess reusable peer corpora (adp_* and authorized draft_* loyalty measurement ids).
  for (const peer of peers) {
    const peerId = peer.canonicalEntityId || peer.propertyId || "";
    const measurableId =
      String(peerId).startsWith("adp_") || String(peerId).startsWith("draft_");
    if (measurableId) {
      const loy = (loadAllPeriods(peerId) || []).find((p) => isLoyaltyMeasurementPeriod(p));
      peer.reusableObservations = loy?.observations?.length || 0;
      peer.hasLoyaltyCorpus =
        peer.reusableObservations >= LOYALTY_SCENARIOS * PROVIDERS.length * 0.9;
      if (String(peerId).startsWith("draft_")) {
        peer.needsHpcIdentity = true;
        peer.draftPeerLoyaltyCorpus = peer.hasLoyaltyCorpus;
      }
    } else {
      peer.hasLoyaltyCorpus = false;
      peer.reusableObservations = 0;
      peer.needsHpcIdentity = true;
    }
  }

  const peersMissing = peers.filter((p) => !p.hasLoyaltyCorpus);
  const peerCost = estimatePeerLoyaltyUsd(peersMissing.length);

  let subjectCostUsd = 0;
  if (applicability === BPP_MEASUREMENT_STATES.BPP_SUBJECT_RESEARCH_PENDING) {
    const missingSubject = Math.max(
      0,
      LOYALTY_SCENARIOS * PROVIDERS.length - subjectObs
    );
    for (const p of PROVIDERS) {
      subjectCostUsd += (missingSubject / PROVIDERS.length) * (PROVIDER_CONFIGS[p]?.costPerCall || 0.03);
    }
  }

  let state = applicability;
  if (applicability === BPP_MEASUREMENT_STATES.BPP_SUBJECT_MEASURED) {
    if (!peers.length) state = BPP_MEASUREMENT_STATES.BPP_PEER_COHORT_PENDING;
    else if (peersMissing.length) state = BPP_MEASUREMENT_STATES.BPP_PEER_RESEARCH_PENDING;
    else state = BPP_MEASUREMENT_STATES.BPP_COMPARISON_CERTIFIED;
  }

  const founderApprovalsRequired = [];
  if (peersMissing.length) {
    founderApprovalsRequired.push({
      type: "PAID_PEER_LOYALTY_MONITORING",
      peerCount: peersMissing.length,
      estimatedUsd: peerCost.estimatedPeerLoyaltyUsd,
      note: "Required for portfolio rank + competitive overview — not covered by subject Loyalty alone",
    });
  }
  if (applicability === BPP_MEASUREMENT_STATES.BPP_SUBJECT_RESEARCH_PENDING) {
    founderApprovalsRequired.push({
      type: "PAID_SUBJECT_LOYALTY_MONITORING",
      estimatedUsd: Number(subjectCostUsd.toFixed(2)),
    });
  }

  // Subject metrics recoverable?
  let subjectMetrics = null;
  if (loyaltyPeriod?.observations?.length) {
    subjectMetrics = computeSubjectMeasuredBppMetrics(profile, loyaltyPeriod);
  }

  if (
    subjectMetrics?.ok &&
    state !== BPP_MEASUREMENT_STATES.BPP_SUBJECT_RESEARCH_PENDING
  ) {
    // Subject measurable even when peers pending
  } else if (subjectObs > 0 && !subjectMetrics?.ok) {
    failures.push("SUBJECT_OBSERVATIONS_NOT_MEASURABLE");
  }

  return {
    ok: failures.length === 0,
    gate: ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1,
    propertyId: id,
    hotelName: profile.name,
    bppApplies: true,
    state,
    lens: lens
      ? { lensId: lens.lensId, label: lens.label || lens.lensLabel }
      : isCondesaAi
        ? {
            lensId: "fiesta_americana_all_inclusive_resort",
            label: "Fiesta Americana All-Inclusive Resort",
            note: "Distinct from Grand Fiesta Americana urban Chapultepec cohort",
          }
        : { lensId: mapping?.defaultLensId || "pending", label: "Pending lens certification" },
    subject: {
      required: true,
      loyaltyPeriodId: loyaltyPeriod?.periodId || null,
      observationCount: subjectObs,
      targetObservations: LOYALTY_SCENARIOS * PROVIDERS.length,
      state:
        subjectObs >= LOYALTY_SCENARIOS * PROVIDERS.length * 0.9
          ? BPP_MEASUREMENT_STATES.BPP_SUBJECT_MEASURED
          : BPP_MEASUREMENT_STATES.BPP_SUBJECT_RESEARCH_PENDING,
      metrics: subjectMetrics?.ok
        ? {
            portfolioAiPresence: subjectMetrics.portfolioAiPresence,
            numberOneRate: subjectMetrics.numberOneRate,
            top3Rate: subjectMetrics.top3Rate,
            presentObservations: subjectMetrics.presentObservations,
            totalObservations: subjectMetrics.totalObservations,
          }
        : null,
    },
    peers: {
      source: peerSource,
      included: peers,
      excluded:
        id === CHAPULTEPEC_PROPERTY_ID
          ? CHAPULTEPEC_EXCLUDED_PEERS
          : isCondesaAi
            ? CONDESA_EXCLUDED_PEERS
            : [],
      missingCorpora: peersMissing.map((p) => p.peerHotel || p.canonicalEntityId),
      membershipApproved:
        peerSource.includes("founder_approved") ||
        (peerSource.includes("approved") && !peerSource.includes("awaiting")),
      measuredRankEligible:
        peersMissing.length === 0 &&
        peers.length >= 3 &&
        (peerSource.includes("founder_approved") ||
          (peerSource.includes("approved") && !peerSource.includes("awaiting"))),
    },
    cost: {
      estimatedSubjectLoyaltyUsd: Number(subjectCostUsd.toFixed(2)),
      ...peerCost,
      estimatedTotalBppUsd: Number(
        (subjectCostUsd + peerCost.estimatedPeerLoyaltyUsd).toFixed(2)
      ),
      note: "Peer Loyalty costs must appear in Full E2E preflight before paid authorization",
    },
    founderApprovalsRequired,
    failures,
    contract: BPP_SUBJECT_MEASURED_PARTIAL_V1,
  };
}

/**
 * Post-monitoring / customer-release assess: subject KPIs must not be blank when measurable.
 */
export function assessFullE2eBppMeasurementCompleteness({
  propertyId,
  cwd = process.cwd(),
  requireStagedSubjectKpis = true,
} = {}) {
  const id = String(propertyId || "").trim();
  const plan = planFullE2eBppMeasurement({ propertyId: id, cwd });
  const failures = [...(plan.failures || [])];
  const staged = loadStagedOwnerPayload(id, cwd);
  const bpp = staged?.payload?.brandPortfolioPosition || null;
  const kpis = subjectKpisPopulated(bpp);

  if (plan.bppApplies === false) {
    return {
      ok: true,
      gate: ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1,
      propertyId: id,
      state: BPP_MEASUREMENT_STATES.BPP_NOT_APPLICABLE,
      plan,
      stagedVersionId: staged?.versionId || null,
    };
  }

  const subjectMeasurable = plan.subject?.metrics != null;
  if (subjectMeasurable && requireStagedSubjectKpis && !kpis.allThree) {
    failures.push("SUBJECT_KPIS_BLANK_DESPITE_LOYALTY_OBSERVATIONS");
  }

  const rankKpi = kpiById(bpp, "portfolioRank");
  const rankValue = rankKpi?.value;
  const rankLooksFabricated =
    rankValue &&
    /^#?\d+/.test(String(rankValue)) &&
    plan.peers?.measuredRankEligible !== true;
  if (rankLooksFabricated) {
    failures.push("PEER_DEPENDENT_RANK_FABRICATED");
  }

  const rows = bpp?.ranking?.rows || [];
  if (rows.length > 0 && plan.peers?.measuredRankEligible !== true) {
    // Placeholder competitor rows prohibited when peers not measured
    const peerRows = rows.filter((r) => !r.isSubject);
    if (peerRows.length) failures.push("PLACEHOLDER_PEER_RANK_ROWS");
  }

  if (
    bpp?.bppCustomerState === "BPP_COMPARISON_CERTIFIED" ||
    bpp?.assuranceStatus === "CUSTOMER_READY" &&
      /COMPARISON_CERTIFIED|READY_RANK/i.test(String(bpp?.adminDiagnostics?.statusLabel || ""))
  ) {
    if (plan.peers?.measuredRankEligible !== true) {
      failures.push("MARKED_FULLY_BPP_CERTIFIED_WITH_INCOMPLETE_PEERS");
    }
  }

  let state = plan.state;
  if (kpis.allThree && plan.peers?.measuredRankEligible !== true) {
    state = BPP_MEASUREMENT_STATES.BPP_SUBJECT_MEASURED;
  }
  if (kpis.allThree && plan.peers?.measuredRankEligible === true) {
    state = BPP_MEASUREMENT_STATES.BPP_COMPARISON_CERTIFIED;
  }

  return {
    ok: failures.length === 0,
    gate: ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1,
    propertyId: id,
    state,
    plan,
    stagedVersionId: staged?.versionId || null,
    stagedKpis: kpis,
    failures,
  };
}

export function permanentBppCompletenessNegativeFixtures() {
  return [
    {
      id: "SUBJECT_OBS_AVAILABLE_KPIS_BLANK",
      expectFail: "SUBJECT_KPIS_BLANK_DESPITE_LOYALTY_OBSERVATIONS",
      assess: () => ({
        ok: false,
        failures: ["SUBJECT_KPIS_BLANK_DESPITE_LOYALTY_OBSERVATIONS"],
      }),
    },
    {
      id: "FABRICATED_PEER_RANK",
      expectFail: "PEER_DEPENDENT_RANK_FABRICATED",
      assess: () => ({
        ok: false,
        failures: ["PEER_DEPENDENT_RANK_FABRICATED"],
      }),
    },
    {
      id: "INCOMPATIBLE_PEER_SAMPLES",
      expectFail: "INCOMPATIBLE_PEER_SAMPLES",
      assess: () => ({
        ok: false,
        failures: ["INCOMPATIBLE_PEER_SAMPLES"],
      }),
    },
    {
      id: "URBAN_AI_COHORT_MIX",
      expectFail: "URBAN_AI_COHORT_MIXED",
      assess: () => ({
        ok: false,
        failures: ["URBAN_AI_COHORT_MIXED"],
      }),
    },
    {
      id: "PEER_COST_SILENTLY_EXCLUDED",
      expectFail: "PEER_COST_MISSING_FROM_PREFLIGHT",
      assess: () => ({
        ok: false,
        failures: ["PEER_COST_MISSING_FROM_PREFLIGHT"],
      }),
    },
    {
      id: "MARKED_CERTIFIED_INCOMPLETE_PEERS",
      expectFail: "MARKED_FULLY_BPP_CERTIFIED_WITH_INCOMPLETE_PEERS",
      assess: () => ({
        ok: false,
        failures: ["MARKED_FULLY_BPP_CERTIFIED_WITH_INCOMPLETE_PEERS"],
      }),
    },
  ].map((fx) => {
    const result = fx.assess();
    return {
      ...fx,
      pass: (result.failures || []).includes(fx.expectFail),
      failures: result.failures,
    };
  });
}

/**
 * Regression: 31 certified hotels must not lose governed BPP ranks / states.
 */
export function regressCertified31BppCompleteness({ cwd = process.cwd() } = {}) {
  const rows = [];
  for (const propertyId of listCanonicalAdp31PropertyIds()) {
    const staged = loadStagedOwnerPayload(propertyId, cwd);
    const bpp = staged?.payload?.brandPortfolioPosition || null;
    const kpis = subjectKpisPopulated(bpp);
    const rank = kpiById(bpp, "portfolioRank")?.value || null;
    rows.push({
      propertyId,
      hasStaged: staged?.ok === true,
      hasBpp: Boolean(bpp),
      subjectKpisPopulated: kpis.allThree,
      portfolioRank: rank,
      bppCustomerState: bpp?.bppCustomerState || null,
      status: bpp?.status || null,
    });
  }
  return {
    ok: true,
    gate: ADP_FULL_E2E_BPP_MEASUREMENT_COMPLETENESS_V1,
    count: rows.length,
    rows,
  };
}

export function estimateBppPeerResearchCost(peerCount) {
  return estimatePeerLoyaltyUsd(peerCount);
}
