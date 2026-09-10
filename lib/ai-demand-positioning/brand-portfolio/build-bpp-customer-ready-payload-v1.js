/**
 * Build a customer-ready Brand & Portfolio payload from period observations + frozen peer set.
 * Used to restore RANK_ONLY / FULL populated states into the deployable BPP pack.
 *
 * Does not change BPP methodology — packaging + customer-state fields only.
 */

import { buildBrandPortfolioPositionPayload } from "./build-brand-portfolio-position-payload-v1.js";
import { computeBrandPortfolioMetricsV1, buildPositiveEvidencePack, buildMissingEvidencePack } from "./brand-portfolio-first-cycle-metrics-v1.js";
import { getBrandPortfolioPeerSet } from "./brand-portfolio-peer-set-v1.js";
import { getPortfolioMapping } from "./brand-portfolio-affiliation-mapping-v1.js";
import {
  BPP_CUSTOMER_PUBLICATION_VERSION,
  BPP_KPI_CONTRACT_VERSION,
  BPP_METRICS_VERSION,
} from "./bpp-publication-meta-v1.js";
import {
  BPP_CUSTOMER_STATE,
  BPP_DISCLOSURE_REASON,
  classifyBppCustomerState,
  attachBppCustomerStateFields,
  ADP_BPP_CUSTOMER_STATE_V1,
} from "./adp-bpp-customer-state-v1.js";

export function buildBppCustomerReadyPayloadFromPeriod({
  profile,
  period,
  scenarios,
  publicationVersion = BPP_CUSTOMER_PUBLICATION_VERSION,
  remediationTag = null,
} = {}) {
  const propertyId = profile?.propertyId;
  if (!propertyId || !period?.observations?.length) {
    return { ok: false, error: "missing_profile_or_period" };
  }

  const peerSet = getBrandPortfolioPeerSet(propertyId);
  if (!peerSet) {
    return { ok: false, error: "no_certified_peer_set", propertyId };
  }

  const mapping = getPortfolioMapping(propertyId);
  const lens =
    (mapping?.lenses || []).find((l) => l.lensId === mapping.defaultLensId) ||
    (mapping?.lenses || [])[0] ||
    null;

  const metrics = computeBrandPortfolioMetricsV1({
    profile,
    peerSet,
    scenarios,
    observations: period.observations,
    lens,
  });

  if (!metrics?.kpis?.length || !metrics?.tableRows?.length) {
    return { ok: false, error: "metrics_empty", propertyId, adequacy: peerSet.adequacy };
  }

  const base = buildBrandPortfolioPositionPayload(profile, {
    certifiedMeasurement: true,
    kpis: metrics.kpis,
    ranking: { rows: metrics.tableRows },
    hasPriorPeriod: false,
  });

  const adequacy = peerSet.adequacy || {};
  const state = classifyBppCustomerState({
    affiliated: true,
    lensApplicable: true,
    affiliationResolved: true,
    peerCount: peerSet.peerCountExcludingSubject,
    canRank: adequacy.canRank === true,
    canBenchmark: adequacy.canBenchmark === true,
    canIndex: adequacy.canIndex === true,
    payloadReady: true,
    exceptionSuppressed: false,
  });

  const displacementEvidence = (metrics.displacement?.byPeer || []).flatMap((peer) =>
    (peer.scenarioIds || metrics.displacement?.scenarioIds || []).slice(0, 3).map((scenarioId) => ({
      scenarioId,
      competitorEntityId: peer.canonicalEntityId,
      competitorName: peer.name || peer.peerHotel,
      evidenceType: "bpp_displacement",
      lens: "bpp",
    }))
  );

  let payload = {
    ...base,
    customerPublished: true,
    periodId: period.periodId,
    peerSet: {
      peerSetId: peerSet.peerSetId,
      peerSetVersion: peerSet.peerSetVersion,
      adequacy: adequacy.status || null,
      peerCount: peerSet.peerCountExcludingSubject,
      included: (peerSet.included || []).map((p) => ({
        peerHotel: p.peerHotel,
        canonicalEntityId: p.canonicalEntityId,
        brand: p.brand,
        parentCompany: p.parentCompany,
        ecosystem: p.ecosystem,
        market: p.market,
      })),
    },
    measurement: {
      kpiContractVersion: BPP_KPI_CONTRACT_VERSION,
      metricsVersion: BPP_METRICS_VERSION,
      grain: "PROVIDER_OBSERVATION",
      periodId: period.periodId,
      priorPeriodId: null,
      comparisonMode: "PUBLICATION_TIME",
      publicationVersion,
      customerPublished: true,
      compatibleObservationReuse: "PERIOD_CORE_OBSERVATIONS_REUSED_FOR_PEER_RANKING",
      suppressedKpis: metrics.suppressedKpis || [],
      adequacyStatus: adequacy.status || null,
    },
    comparisonMode: "PUBLICATION_TIME",
    metricEligibility: {
      bppLensAvailable: state.bppLensAvailable,
      bppRankEligible: state.bppRankEligible,
      bppBenchmarkEligible: state.bppBenchmarkEligible,
      bppIndexEligible: state.bppIndexEligible,
    },
    evidence: {
      positive: buildPositiveEvidencePack({
        observations: period.observations,
        profile,
        limit: 20,
      }),
      missing: buildMissingEvidencePack({
        observations: period.observations,
        profile,
        limit: 20,
      }),
      displacement: displacementEvidence.slice(0, 20),
      measurementPeriodId: period.periodId,
    },
    contract: ADP_BPP_CUSTOMER_STATE_V1,
  };

  payload = attachBppCustomerStateFields(payload, state);

  if (state.customerState === BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY) {
    payload.benchmarkLimitation = {
      reason: BPP_DISCLOSURE_REASON.PEER_SET_INSUFFICIENT_FOR_BENCHMARK,
      customerNote:
        `${peerSet.peerCountExcludingSubject} comparable ${(lens?.label || peerSet.lensLabel || "loyalty")} alternatives are currently measured in this market. Portfolio rank is shown; benchmark/index metrics require a larger governed peer set.`,
      suppressedKpis: metrics.suppressedKpis || ["portfolioBenchmark", "portfolioPresenceIndex"],
    };
  }

  if (remediationTag) {
    payload.remediation = {
      tag: remediationTag,
      class: state.customerState,
      newProviderCalls: 0,
    };
  }

  return {
    ok: true,
    propertyId,
    customerState: state.customerState,
    peerCount: peerSet.peerCountExcludingSubject,
    kpiCount: payload.kpis?.length || 0,
    rowCount: payload.ranking?.rows?.length || 0,
    payload,
    metrics,
    state,
  };
}
