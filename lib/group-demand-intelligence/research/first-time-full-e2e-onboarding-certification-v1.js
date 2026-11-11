/**
 * First-time Full E2E onboarding certification V1 (hotel-generic, zero-paid).
 *
 * Separates layers that must NEVER be collapsed into a single LIVE_COMPLETED flag:
 *   1. Technical Execution
 *   2. Discovery Coverage
 *   3. Research Depth
 *   4. Commercial Qualification
 *   5. Customer-Facing Readiness
 *   6. Business Value
 *
 * Does not authorize spend, mutate inventory, or promote customer-facing rows.
 */
export const FIRST_TIME_FULL_E2E_ONBOARDING_CERTIFICATION_V1 =
  "gdi-first-time-full-e2e-onboarding-certification-v1";

export const ONBOARDING_LAYER = Object.freeze({
  TECHNICAL_EXECUTION: "TECHNICAL_EXECUTION",
  DISCOVERY_COVERAGE: "DISCOVERY_COVERAGE",
  RESEARCH_DEPTH: "RESEARCH_DEPTH",
  COMMERCIAL_QUALIFICATION: "COMMERCIAL_QUALIFICATION",
  CUSTOMER_FACING_READINESS: "CUSTOMER_FACING_READINESS",
  BUSINESS_VALUE: "BUSINESS_VALUE",
});

export const LAYER_STATUS = Object.freeze({
  PASS: "PASS",
  PARTIAL: "PARTIAL",
  FAIL: "FAIL",
  NOT_EVALUATED: "NOT_EVALUATED",
});

const CANONICAL_FOURTEEN = Object.freeze([
  "association_events",
  "scaling_companies",
  "corporate_meetings_incentives",
  "government_public_sector",
  "sports_entertainment",
  "education_scientific",
  "projects_infrastructure",
  "social_special_events",
  "local_catering_day_meetings",
  "corporate_celebrations_private_events",
  "travel_trade_group_intermediaries",
  "relocation_extended_stay",
  "preferred_lodging",
  "professional_event_planners",
]);

function num(v, fb = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fb;
}

/**
 * @param {object} input
 * @param {Array<{workflowId:string,discoveryCalls?:number,deepCalls?:number,uniqueCandidatesAfterDedup?:number,rawResultsRetrieved?:number,liveStatus?:string}>} input.workflowCoverage
 * @param {object} [input.acceptance]
 * @param {number} [input.facingCount]
 * @param {boolean} [input.withinBudget]
 * @param {boolean} [input.paidProvidersAuthorized]
 * @param {object} [input.opts]
 */
export function certifyFirstTimeFullE2eOnboarding(input = {}) {
  const workflows = Array.isArray(input.workflowCoverage)
    ? input.workflowCoverage
    : [];
  const byId = new Map(workflows.map((w) => [String(w.workflowId), w]));
  const acceptance = input.acceptance || {};
  const salesReady = num(acceptance.salesReady ?? acceptance.SALES_READY);
  const researchContinuation = num(
    acceptance.researchContinuation ?? acceptance.RESEARCH_CONTINUATION_REQUIRED,
  );
  const watch = num(acceptance.watch ?? acceptance.WATCH);
  const facingCount = num(input.facingCount);
  const auditorLiveBookingSr = input.auditorLiveBookingSalesReady;
  const effectiveSr =
    auditorLiveBookingSr == null ? salesReady : num(auditorLiveBookingSr);

  const executed = CANONICAL_FOURTEEN.filter((id) => {
    const w = byId.get(id);
    return w && num(w.discoveryCalls) + num(w.deepCalls) > 0;
  });
  const withDiscoveryYield = CANONICAL_FOURTEEN.filter((id) => {
    const w = byId.get(id);
    return w && (num(w.uniqueCandidatesAfterDedup) > 0 || num(w.rawResultsRetrieved) > 0);
  });
  const withDeep = CANONICAL_FOURTEEN.filter((id) => {
    const w = byId.get(id);
    return w && num(w.deepCalls) > 0;
  });
  const discoveryOnlyDespiteCandidates = CANONICAL_FOURTEEN.filter((id) => {
    const w = byId.get(id);
    return (
      w &&
      num(w.deepCalls) === 0 &&
      num(w.uniqueCandidatesAfterDedup) > 0
    );
  });

  const technical = {
    layer: ONBOARDING_LAYER.TECHNICAL_EXECUTION,
    status:
      executed.length === 14
        ? LAYER_STATUS.PASS
        : executed.length >= 10
          ? LAYER_STATUS.PARTIAL
          : LAYER_STATUS.FAIL,
    evidence: {
      workflowsExecuted: executed.length,
      expected: 14,
      withinBudget: input.withinBudget !== false,
    },
    note: "Workflow entry points returned a live outcome. Not equivalent to market coverage.",
  };

  const discovery = {
    layer: ONBOARDING_LAYER.DISCOVERY_COVERAGE,
    status:
      withDiscoveryYield.length >= 12 &&
      input.singleDiscoveryPassOnly !== true
        ? LAYER_STATUS.PASS
        : withDiscoveryYield.length >= 10
          ? LAYER_STATUS.PARTIAL
          : LAYER_STATUS.FAIL,
    evidence: {
      segmentsWithDiscoveryYield: withDiscoveryYield.length,
      singleDiscoveryPassOnly: input.singleDiscoveryPassOnly === true,
      multilingualVerified: input.multilingualVerified === true,
      eighteenMonthHorizonVerified: input.eighteenMonthHorizonVerified === true,
    },
    note:
      input.singleDiscoveryPassOnly === true
        ? "Single discovery pass per segment is PARTIAL at best for first-time Full E2E."
        : "Discovery yield present across most segments.",
  };

  const unfinishedHighPriority = num(input.unfinishedHighPriorityEligible);
  const researchCompletenessStatuses = Array.isArray(
    input.researchCompletenessStatuses,
  )
    ? input.researchCompletenessStatuses
    : [];
  const partialUnderCap = researchCompletenessStatuses.some((s) =>
    /PARTIAL_CANDIDATE_CAP|PARTIAL_BUDGET_LIMIT|PARTIAL_PROVIDER/i.test(
      String(s),
    ),
  );
  const depth = {
    layer: ONBOARDING_LAYER.RESEARCH_DEPTH,
    status:
      unfinishedHighPriority > 0 || partialUnderCap
        ? LAYER_STATUS.PARTIAL
        : discoveryOnlyDespiteCandidates.length === 0 && withDeep.length >= 12
          ? LAYER_STATUS.PASS
          : withDeep.length >= 8
            ? LAYER_STATUS.PARTIAL
            : LAYER_STATUS.FAIL,
    evidence: {
      segmentsWithDeep: withDeep.length,
      discoveryOnlyDespiteCandidates,
      firstTimeDepthFloorApplied: input.firstTimeDepthFloorApplied === true,
      unfinishedHighPriorityEligible: unfinishedHighPriority,
      researchCompletenessStatuses,
      note:
        unfinishedHighPriority > 0
          ? "Cannot PASS research depth while high-priority eligible candidates remain unfinished solely due to caps/budget."
          : null,
    },
    note: "Promising candidates must receive deep research on first-time Full E2E. LIVE_COMPLETED ≠ research complete under candidate caps.",
  };

  const commercial = {
    layer: ONBOARDING_LAYER.COMMERCIAL_QUALIFICATION,
    status:
      effectiveSr >= 1 &&
      (auditorLiveBookingSr == null || num(auditorLiveBookingSr) === salesReady)
        ? LAYER_STATUS.PASS
        : researchContinuation + watch > 0
          ? LAYER_STATUS.PARTIAL
          : LAYER_STATUS.FAIL,
    evidence: {
      gateSalesReady: salesReady,
      auditorLiveBookingSalesReady:
        auditorLiveBookingSr == null ? null : num(auditorLiveBookingSr),
      researchContinuation,
      watch,
      completedEditionDemotions: num(input.completedEditionDemotions),
    },
    note: "Gate SR stamps are not sufficient when deep evidence shows completed/historical editions.",
  };

  const facing = {
    layer: ONBOARDING_LAYER.CUSTOMER_FACING_READINESS,
    status:
      facingCount > 0
        ? LAYER_STATUS.PASS
        : effectiveSr > 0
          ? LAYER_STATUS.PARTIAL
          : LAYER_STATUS.FAIL,
    evidence: {
      facingCount,
      autoPromote: false,
      note: "Customer-facing requires separate founder promotion — never inferred from LIVE_COMPLETED.",
    },
  };

  const businessValue = {
    layer: ONBOARDING_LAYER.BUSINESS_VALUE,
    status:
      effectiveSr >= 1
        ? LAYER_STATUS.PASS
        : researchContinuation >= 5
          ? LAYER_STATUS.PARTIAL
          : LAYER_STATUS.FAIL,
    evidence: {
      practicalSalesActionsAvailable: effectiveSr,
      researchContinuationPool: researchContinuation,
    },
    note: "Business value = practical sales action for the hotel team, not workflow exit codes.",
  };

  const layers = [technical, discovery, depth, commercial, facing, businessValue];
  const allPass = layers.every((l) => l.status === LAYER_STATUS.PASS);
  const anyFail = layers.some((l) => l.status === LAYER_STATUS.FAIL);

  let overallStatus = LAYER_STATUS.PARTIAL;
  if (allPass) overallStatus = LAYER_STATUS.PASS;
  else if (anyFail && technical.status === LAYER_STATUS.PASS)
    overallStatus = LAYER_STATUS.PARTIAL;
  else if (technical.status === LAYER_STATUS.FAIL) overallStatus = LAYER_STATUS.FAIL;

  return {
    version: FIRST_TIME_FULL_E2E_ONBOARDING_CERTIFICATION_V1,
    hotelId: input.hotelId || null,
    overallStatus,
    genuineFirstTimeFullE2e: allPass === true,
    layers,
    forbiddenCollapse:
      "Do not mark first-time onboarding complete solely because all workflow entry points returned LIVE_COMPLETED / TECHNICAL_EXECUTION PASS.",
    canonicalFourteen: [...CANONICAL_FOURTEEN],
  };
}

export function assertNotTechnicalEqualsComplete(certification = {}) {
  const tech = (certification.layers || []).find(
    (l) => l.layer === ONBOARDING_LAYER.TECHNICAL_EXECUTION,
  );
  const incompleteLayers = (certification.layers || []).filter(
    (l) => l.status !== LAYER_STATUS.PASS,
  );
  if (
    tech?.status === LAYER_STATUS.PASS &&
    incompleteLayers.length > 0 &&
    certification.genuineFirstTimeFullE2e === true
  ) {
    throw new Error("TECHNICAL_PASS_COLLAPSED_INTO_GENUINE_FIRST_TIME");
  }
  if (
    tech?.status === LAYER_STATUS.PASS &&
    incompleteLayers.length > 0 &&
    certification.genuineFirstTimeFullE2e !== false
  ) {
    throw new Error("TECHNICAL_PASS_MUST_NOT_IMPLY_GENUINE_FIRST_TIME");
  }
  return true;
}
