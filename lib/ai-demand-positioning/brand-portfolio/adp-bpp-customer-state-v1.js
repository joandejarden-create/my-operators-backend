/**
 * ADP_BPP_CUSTOMER_STATE_V1
 *
 * Canonical Brand & Portfolio customer state machine.
 * Analytical state === published state === rendered state.
 *
 * Doctrine:
 * BPP_SECTION_VISIBILITY_IS_INDEPENDENT_OF_BENCHMARK_INDEX_AVAILABILITY
 * READY_POPULATED_RANK_ONLY_MUST_RENDER_AS_POPULATED
 * BENCHMARK_INSUFFICIENCY_MAY_SUPPRESS_BENCHMARK_METRICS_NOT_THE_ENTIRE_BPP_LENS
 * ADP_BPP_SECTION_VISIBILITY_NOT_COUPLED_TO_BENCHMARK_THRESHOLD
 *
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN. — presentation/state only.
 */

export const ADP_BPP_CUSTOMER_STATE_V1 = "ADP_BPP_CUSTOMER_STATE_V1";
export const ADP_BPP_SECTION_VISIBILITY_NOT_COUPLED_TO_BENCHMARK_THRESHOLD =
  "ADP_BPP_SECTION_VISIBILITY_NOT_COUPLED_TO_BENCHMARK_THRESHOLD";
export const ADP_BPP_ANALYTICAL_PUBLISHED_RENDERED_STATE_PARITY =
  "ADP_BPP_ANALYTICAL_PUBLISHED_RENDERED_STATE_PARITY";
export const ADP_BPP_RANK_ONLY_RENDER_CONTRACT = "ADP_BPP_RANK_ONLY_RENDER_CONTRACT";
export const ADP_BPP_DISCLOSURE_REASON_PARITY = "ADP_BPP_DISCLOSURE_REASON_PARITY";
export const ADP_BPP_OWNER_SHARE_PAYLOAD_PARITY = "ADP_BPP_OWNER_SHARE_PAYLOAD_PARITY";

/** Allowed customer-facing BPP states (no internal setup states). */
export const BPP_CUSTOMER_STATE = Object.freeze({
  BPP_READY_POPULATED_FULL: "BPP_READY_POPULATED_FULL",
  BPP_READY_POPULATED_RANK_ONLY: "BPP_READY_POPULATED_RANK_ONLY",
  BPP_EXCEPTION_SUPPRESSED: "BPP_EXCEPTION_SUPPRESSED",
  BPP_NOT_APPLICABLE: "BPP_NOT_APPLICABLE",
});

/** Disclosure reason codes — drive customer copy; never invent generic fallbacks. */
export const BPP_DISCLOSURE_REASON = Object.freeze({
  PEER_SET_INSUFFICIENT_FOR_BENCHMARK: "PEER_SET_INSUFFICIENT_FOR_BENCHMARK",
  ECOSYSTEM_DISCOVERY_EXHAUSTED_NO_VALID_PEERS: "ECOSYSTEM_DISCOVERY_EXHAUSTED_NO_VALID_PEERS",
  AFFILIATION_UNRESOLVED: "AFFILIATION_UNRESOLVED",
  LENS_NOT_APPLICABLE: "LENS_NOT_APPLICABLE",
  NONE: "NONE",
});

const DISCLOSURE_COPY = Object.freeze({
  [BPP_DISCLOSURE_REASON.PEER_SET_INSUFFICIENT_FOR_BENCHMARK]: Object.freeze({
    headline: null,
    body: null,
    metricNote:
      "Comparable hotels in this loyalty ecosystem are measured for portfolio rank. Benchmark and index metrics require a larger governed peer set.",
  }),
  [BPP_DISCLOSURE_REASON.ECOSYSTEM_DISCOVERY_EXHAUSTED_NO_VALID_PEERS]: Object.freeze({
    headline: "Brand & Portfolio not shown",
    body:
      "Brand & Portfolio benchmarking is not shown for this period because the governed peer set does not yet meet the minimum comparability requirement.",
  }),
  [BPP_DISCLOSURE_REASON.AFFILIATION_UNRESOLVED]: Object.freeze({
    headline: "Brand & Portfolio not shown",
    body:
      "Brand & Portfolio benchmarking is not shown for this period because the governed affiliation and peer path are not yet resolved for display.",
  }),
  [BPP_DISCLOSURE_REASON.LENS_NOT_APPLICABLE]: Object.freeze({
    headline: "Brand & Portfolio not applicable",
    body: "Brand & Portfolio Position does not apply to this hotel’s current affiliation profile.",
  }),
});

/**
 * Forbidden collapse: peerCount < 5 or !benchmarkEligible ⇒ suppress entire BPP.
 */
export function assertBppSectionVisibilityNotCoupledToBenchmark({
  peerCount,
  benchmarkEligible,
  sectionVisible,
  customerState,
} = {}) {
  const forbiddenCollapse =
    sectionVisible === false &&
    Number(peerCount) >= 3 &&
    benchmarkEligible === false &&
    (customerState === BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY ||
      customerState === BPP_CUSTOMER_STATE.BPP_READY_POPULATED_FULL);
  return {
    gate: ADP_BPP_SECTION_VISIBILITY_NOT_COUPLED_TO_BENCHMARK_THRESHOLD,
    pass: !forbiddenCollapse,
    forbiddenCollapse,
  };
}

/**
 * Classify canonical customer state from adequacy + payload readiness.
 */
export function classifyBppCustomerState({
  affiliated = true,
  lensApplicable = true,
  affiliationResolved = true,
  peerCount = 0,
  canRank = false,
  canBenchmark = false,
  canIndex = false,
  payloadReady = false,
  exceptionSuppressed = false,
} = {}) {
  if (!lensApplicable || (!affiliated && !exceptionSuppressed)) {
    if (!affiliated && !lensApplicable) {
      return {
        customerState: BPP_CUSTOMER_STATE.BPP_NOT_APPLICABLE,
        disclosureReason: BPP_DISCLOSURE_REASON.LENS_NOT_APPLICABLE,
        bppLensAvailable: false,
        bppRankEligible: false,
        bppBenchmarkEligible: false,
        bppIndexEligible: false,
        sectionVisible: false,
      };
    }
  }

  if (!affiliationResolved) {
    return {
      customerState: BPP_CUSTOMER_STATE.BPP_EXCEPTION_SUPPRESSED,
      disclosureReason: BPP_DISCLOSURE_REASON.AFFILIATION_UNRESOLVED,
      bppLensAvailable: false,
      bppRankEligible: false,
      bppBenchmarkEligible: false,
      bppIndexEligible: false,
      sectionVisible: true,
      renderMode: "STATUS_ONLY",
    };
  }

  if (exceptionSuppressed || (affiliationResolved && peerCount < 3 && !payloadReady)) {
    return {
      customerState: BPP_CUSTOMER_STATE.BPP_EXCEPTION_SUPPRESSED,
      disclosureReason: BPP_DISCLOSURE_REASON.ECOSYSTEM_DISCOVERY_EXHAUSTED_NO_VALID_PEERS,
      bppLensAvailable: true,
      bppRankEligible: false,
      bppBenchmarkEligible: false,
      bppIndexEligible: false,
      sectionVisible: true,
      renderMode: "STATUS_ONLY",
    };
  }

  if (payloadReady && canRank && canBenchmark && canIndex) {
    return {
      customerState: BPP_CUSTOMER_STATE.BPP_READY_POPULATED_FULL,
      disclosureReason: BPP_DISCLOSURE_REASON.NONE,
      bppLensAvailable: true,
      bppRankEligible: true,
      bppBenchmarkEligible: true,
      bppIndexEligible: true,
      sectionVisible: true,
      renderMode: "POPULATED_FULL",
    };
  }

  if (payloadReady && canRank) {
    return {
      customerState: BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY,
      disclosureReason: BPP_DISCLOSURE_REASON.PEER_SET_INSUFFICIENT_FOR_BENCHMARK,
      bppLensAvailable: true,
      bppRankEligible: true,
      bppBenchmarkEligible: false,
      bppIndexEligible: false,
      sectionVisible: true,
      renderMode: "POPULATED_RANK_ONLY",
    };
  }

  // Affiliation + peers resolved but certified customer payload not yet attached
  // — still NOT the affiliation-unresolved copy.
  return {
    customerState: BPP_CUSTOMER_STATE.BPP_EXCEPTION_SUPPRESSED,
    disclosureReason: BPP_DISCLOSURE_REASON.ECOSYSTEM_DISCOVERY_EXHAUSTED_NO_VALID_PEERS,
    bppLensAvailable: true,
    bppRankEligible: canRank,
    bppBenchmarkEligible: canBenchmark,
    bppIndexEligible: canIndex,
    sectionVisible: true,
    renderMode: "STATUS_ONLY",
    awaitingCertifiedPayload: true,
  };
}

export function disclosureCopyForReason(reason) {
  return DISCLOSURE_COPY[reason] || DISCLOSURE_COPY[BPP_DISCLOSURE_REASON.AFFILIATION_UNRESOLVED];
}

/**
 * RANK_ONLY render contract fixture expectations.
 */
export function evaluateBppRankOnlyRenderContract({
  peerCount = 4,
  rankEligible = true,
  benchmarkEligible = false,
  sectionVisible,
  peersVisible,
  rankMetricsVisible,
  benchmarkNumeric,
  indexNumeric,
  suppressionCard,
} = {}) {
  const expected = {
    SECTION_VISIBLE: true,
    PEERS_VISIBLE: true,
    RANK_METRICS_VISIBLE: true,
    BENCHMARK_NUMERIC: false,
    INDEX_NUMERIC: false,
    SUPPRESSION_CARD: false,
  };
  const actual = {
    SECTION_VISIBLE: sectionVisible === true,
    PEERS_VISIBLE: peersVisible === true,
    RANK_METRICS_VISIBLE: rankMetricsVisible === true,
    BENCHMARK_NUMERIC: benchmarkNumeric === true,
    INDEX_NUMERIC: indexNumeric === true,
    SUPPRESSION_CARD: suppressionCard === true,
  };
  const pass =
    peerCount >= 3 &&
    rankEligible === true &&
    benchmarkEligible === false &&
    Object.keys(expected).every((k) => actual[k] === expected[k]);

  return {
    gate: ADP_BPP_RANK_ONLY_RENDER_CONTRACT,
    pass,
    expected,
    actual,
    peerCount,
    rankEligible,
    benchmarkEligible,
  };
}

/**
 * Attach customer-state fields onto a READY BPP payload (presentation only).
 */
export function attachBppCustomerStateFields(bpp, stateClassification) {
  if (!bpp || !stateClassification) return bpp;
  return {
    ...bpp,
    customerReadyClass: stateClassification.customerState,
    bppCustomerState: stateClassification.customerState,
    bppLensAvailable: stateClassification.bppLensAvailable,
    bppRankEligible: stateClassification.bppRankEligible,
    bppBenchmarkEligible: stateClassification.bppBenchmarkEligible,
    bppIndexEligible: stateClassification.bppIndexEligible,
    disclosureReason: stateClassification.disclosureReason,
    disclosure:
      stateClassification.disclosureReason &&
      stateClassification.disclosureReason !== BPP_DISCLOSURE_REASON.NONE
        ? disclosureCopyForReason(stateClassification.disclosureReason)
        : null,
    renderMode: stateClassification.renderMode,
  };
}
