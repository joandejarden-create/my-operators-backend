/**
 * Jev maturation adapter — targeted follow-up *routing* only.
 *
 * - Prefers Jev when an opportunity already exists and a specific evidence gap remains.
 * - NEVER resolves blockers with Jev choices as facts.
 * - NEVER sets researchStatus / publishability / Ready from Jev.
 * - Normalizes output into shared research-memory / evidence model as
 *   RESEARCH_DIRECTION records (not demand evidence).
 * - Persists decisionId + providerRequestId for reuse / dedupe.
 */

import { decideEvidenceGapNextAction } from "../evidence-gap/jev-evidence-gap-next-action.js";
import { JEV_NEXT_ACTION } from "../evidence-gap/evidence-gap-model-v1.js";
import { isJevEnabled } from "../jev/jev-config.js";
import { BLOCKER_KEY } from "./research-constants-v1.js";
import {
  BLOCKER_TO_EVIDENCE_DIMENSION,
  PROVIDER_KIND,
  PROVIDER_COST_ESTIMATES_USD,
} from "./provider-cost-router-v1.js";

export const JEV_MATURATION_ADAPTER_VERSION = "gdi-jev-maturation-adapter-v1";

/** Jev next-action → supervisor strategy hint (executor still acquires evidence). */
export const JEV_ACTION_TO_STRATEGY = Object.freeze({
  [JEV_NEXT_ACTION.VERIFY_FUTURE_CYCLE]: "official_future_events",
  [JEV_NEXT_ACTION.VERIFY_DATES]: "official_future_events",
  [JEV_NEXT_ACTION.FIND_OFFICIAL_EVENT_PAGE]: "official_future_events",
  [JEV_NEXT_ACTION.FIND_FUTURE_HOST_PAGE]: "event_calendar",
  [JEV_NEXT_ACTION.FIND_REGISTRATION_PAGE]: "event_calendar",
  [JEV_NEXT_ACTION.FIND_EVENT_MANUAL]: "exhibitor_or_sponsor_prospectus",
  [JEV_NEXT_ACTION.FIND_TEAM_MANUAL]: "exhibitor_or_sponsor_prospectus",
  [JEV_NEXT_ACTION.FIND_OFFICIAL_HOUSING_PAGE]: "housing_block_or_booking_page",
  [JEV_NEXT_ACTION.FIND_TRAVEL_ACCOMMODATION_PAGE]: "housing_block_or_booking_page",
  [JEV_NEXT_ACTION.VERIFY_HOUSING_STATUS]: "housing_block_or_booking_page",
  [JEV_NEXT_ACTION.VERIFY_HOTEL_SELECTION_STATUS]: "event_venue_lodging_pattern",
  [JEV_NEXT_ACTION.VERIFY_ROOM_DEMAND]: "official_attendance_or_rooms",
  [JEV_NEXT_ACTION.VERIFY_EVENT_SCALE]: "prospectus_attendance",
  [JEV_NEXT_ACTION.VERIFY_OVERFLOW]: "prior_event_scale",
  [JEV_NEXT_ACTION.VERIFY_ORGANIZER_CONTROL]: "association_management_company",
  [JEV_NEXT_ACTION.VERIFY_MARKET]: "distance_and_catchment",
  [JEV_NEXT_ACTION.VERIFY_WHO]: "official_staff_directory",
  [JEV_NEXT_ACTION.VERIFY_WHO_ROLE]: "event_team_page",
  [JEV_NEXT_ACTION.VERIFY_ACTION_PATH]: "official_business_email_phone",
  [JEV_NEXT_ACTION.FIND_HOSTING_BID]: "cvb_convention_calendar",
  [JEV_NEXT_ACTION.WAIT_FOR_TRIGGER]: null,
  [JEV_NEXT_ACTION.STOP_NO_PUBLIC_PATH]: null,
});

export function jevRouteFingerprint({
  opportunityId,
  blockerKey,
  primaryBlocker,
  priorFailedStrategies = [],
}) {
  // Fingerprint = opportunity × blocker × failed public strategies — not prior Jev
  // actions (those would change the key after every call and defeat reuse).
  const failed = [...new Set(priorFailedStrategies || [])].sort().join(",");
  return `jev_route::${opportunityId || ""}::${blockerKey}::${primaryBlocker}::${failed}`;
}

/**
 * Build shared-model evidence row for a Jev *routing* decision.
 * Explicitly not material demand evidence.
 */
export function normalizeJevRouteToEvidenceModel(route = {}) {
  return {
    kind: "RESEARCH_DIRECTION",
    sourceFamily: "jev_typesafe",
    sourceAuthority: "routing_only",
    isMaterialDemandEvidence: false,
    cannotResolveBlockerAlone: true,
    cannotPromoteProspect: true,
    jevAction: route.appliedAction || route.jevAction || null,
    defaultAction: route.defaultAction || null,
    strategyHint: route.strategyHint || null,
    rationale: route.rationale || null,
    expectedEvidenceType: route.expectedEvidenceType || null,
    stopCondition: route.stopCondition || null,
    decisionId: route.decisionId || null,
    providerRequestId: route.providerRequestId || null,
    inputHash: route.inputHash || null,
    confidence: route.confidence ?? null,
    costUsd: Number(route.costUsd || 0),
    at: route.at || new Date().toISOString(),
    adapterVersion: JEV_MATURATION_ADAPTER_VERSION,
  };
}

function priorJevReuse(opportunity, fingerprint) {
  const attempts = opportunity.researchAttempts || [];
  return attempts.find(
    (a) =>
      a.provider === PROVIDER_KIND.JEV_ROUTE &&
      a.fingerprint === fingerprint &&
      a.providerRunId &&
      (a.disposition === "COMPLETED" || a.disposition === "REUSED_PROVIDER_RUN")
  );
}

/**
 * Ask Jev for the next research action for this blocker.
 * Returns a maturation researcher-compatible result that does NOT patch facts.
 *
 * @param {object} args
 * @param {object} args.opportunity
 * @param {{ key: string }} args.blocker
 * @param {object} [args.options]
 * @param {Function} [args.decideFn] inject for tests
 */
export async function executeJevRouteForBlocker({
  opportunity,
  blocker,
  options = {},
  decideFn = decideEvidenceGapNextAction,
} = {}) {
  if (!options.forceJev && !isJevEnabled()) {
    return {
      newMaterialEvidence: false,
      provider: PROVIDER_KIND.JEV_ROUTE,
      sourceFamilies: ["jev_typesafe"],
      costUsd: 0,
      disposition: "JEV_DISABLED",
      notes: "Jev not enabled — skip routing call",
      isCompletionAuthority: false,
    };
  }

  const blockerKey = blocker?.key || blocker;
  const primaryBlocker =
    BLOCKER_TO_EVIDENCE_DIMENSION[blockerKey] || "FUTURE_CYCLE_VALIDATION";
  const previousFailed = (opportunity.researchAttempts || [])
    .filter(
      (a) =>
        a.blocker === blockerKey &&
        !a.newMaterialEvidence &&
        a.provider !== PROVIDER_KIND.JEV_ROUTE
    )
    .map((a) => a.strategy)
    .filter(Boolean);
  const previousActions = (opportunity.researchAttempts || [])
    .filter((a) => a.provider === PROVIDER_KIND.JEV_ROUTE)
    .map((a) => a.jevAction || a.strategy)
    .filter(Boolean);

  const fingerprint = jevRouteFingerprint({
    opportunityId: opportunity.id || opportunity.opportunityId,
    blockerKey,
    primaryBlocker,
    priorFailedStrategies: previousFailed,
  });

  const prior = priorJevReuse(opportunity, fingerprint);
  if (prior && !options.forceResubmitProvider) {
    const reusedEvidence = normalizeJevRouteToEvidenceModel({
      appliedAction: prior.jevAction,
      strategyHint: prior.strategyHint || JEV_ACTION_TO_STRATEGY[prior.jevAction],
      decisionId: prior.providerRunId,
      providerRequestId: prior.providerRequestId,
      costUsd: 0,
      rationale: "reused_prior_jev_decision",
    });
    return {
      newMaterialEvidence: false,
      provider: PROVIDER_KIND.JEV_ROUTE,
      providerRunId: prior.providerRunId,
      providerRequestId: prior.providerRequestId,
      reusedProviderRun: true,
      fingerprint,
      sourceFamilies: ["jev_typesafe"],
      costUsd: 0,
      disposition: "REUSED_PROVIDER_RUN",
      jevAction: prior.jevAction,
      strategyHint: reusedEvidence.strategyHint,
      evidence: [reusedEvidence],
      evidenceIds: [prior.providerRunId].filter(Boolean),
      isCompletionAuthority: false,
      notes: "Jev decision reused — not resubmitted; does not resolve blocker",
      nextRecommendedAction: reusedEvidence.strategyHint
        ? { provider: PROVIDER_KIND.DIRECT_PUBLIC, strategy: reusedEvidence.strategyHint }
        : { provider: PROVIDER_KIND.NOOP, strategy: null },
    };
  }

  const packet = {
    hotel: opportunity.hotel?.displayName || opportunity.hotel?.name || null,
    market: opportunity.hotel?.market || opportunity.hotel?.city || null,
    event: opportunity.organizationName || opportunity.name || null,
    unresolvedDimensions: [primaryBlocker],
    resolvedDimensions: [],
    previousResearchActions: previousActions,
    previousFailedPaths: previousFailed,
    sourceUrls: (opportunity.evidence || []).map((e) => e.url).filter(Boolean).slice(0, 5),
    secondaryBlocker: null,
  };

  const route = await decideFn({
    packet,
    primaryBlocker,
    hotelShort: options.hotelShort || "",
    enableSafeApply: options.enableJevSafeApply !== false,
  });

  const decisionId =
    route.result?.decisionId || route.decisionId || `jev_local_${Date.now()}`;
  const providerRequestId =
    route.result?.providerRequestId || route.providerRequestId || null;
  const costUsd = Number(
    route.result?.costUsd ??
      route.costUsd ??
      PROVIDER_COST_ESTIMATES_USD[PROVIDER_KIND.JEV_ROUTE]
  );
  const strategyHint = JEV_ACTION_TO_STRATEGY[route.appliedAction] ?? null;
  const stopSuggested =
    route.appliedAction === JEV_NEXT_ACTION.STOP_NO_PUBLIC_PATH ||
    route.appliedAction === JEV_NEXT_ACTION.WAIT_FOR_TRIGGER;

  const evidenceRow = normalizeJevRouteToEvidenceModel({
    ...route,
    strategyHint,
    decisionId,
    providerRequestId,
    inputHash: route.result?.inputHash,
    costUsd,
    at: new Date().toISOString(),
  });

  return {
    newMaterialEvidence: false, // routing never counts as material demand evidence
    provider: PROVIDER_KIND.JEV_ROUTE,
    providerRunId: decisionId,
    providerRequestId,
    fingerprint,
    sourceFamilies: ["jev_typesafe"],
    costUsd,
    disposition: stopSuggested ? "JEV_SUGGEST_STOP" : "JEV_ROUTE_ONLY",
    jevAction: route.appliedAction,
    strategyHint,
    evidence: [evidenceRow],
    evidenceIds: [decisionId, providerRequestId].filter(Boolean),
    isCompletionAuthority: false,
    notes:
      "Jev routed next research action only — executor must acquire evidence; cannot promote or complete",
    // Soft hint for composite researcher — never a fact patch
    deepPatch: undefined,
    patch: undefined,
    nextRecommendedAction: stopSuggested
      ? { provider: PROVIDER_KIND.NOOP, strategy: null, stopHint: route.appliedAction }
      : {
          provider: PROVIDER_KIND.DIRECT_PUBLIC,
          strategy: strategyHint,
          jevAction: route.appliedAction,
        },
  };
}

/**
 * Assert Jev output cannot be treated as completion.
 */
export function assertJevNotCompletionAuthority(result = {}) {
  if (result.isCompletionAuthority === true) {
    throw new Error("Jev must not be completion authority");
  }
  if (result.deepPatch?.futureDemand || result.deepPatch?.demandScale) {
    throw new Error("Jev adapter must not patch demand facts");
  }
  if (result.newMaterialEvidence === true && result.provider === PROVIDER_KIND.JEV_ROUTE) {
    throw new Error("Jev routing must not claim newMaterialEvidence");
  }
  return true;
}
