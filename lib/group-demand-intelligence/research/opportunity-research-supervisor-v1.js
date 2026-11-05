/**
 * Iterative Opportunity Research Supervisor v1.1
 *
 * Blocker-driven maturation loop. Persists research memory. Does not publish,
 * invent evidence, or lower Ready/eligibility bars.
 */

import {
  RESEARCH_TERMINAL,
  PROSPECT_MATURITY,
  STOP_REASONS,
  BLOCKER_KEY,
  BLOCKER_PRIORITY,
  SUPERVISOR_DEFAULTS,
  SUPERVISOR_VERSION,
} from "./research-constants-v1.js";
import {
  HeuristicResearchPlanner,
  hasTriedParallelFollowup,
  hasCompletedParallelFollowup,
  canReuseParallelProviderRun,
  publicPathExhaustedForBlocker,
  remainingPublicStrategiesForBlocker,
  isParallelStrategy,
  PARALLEL_STRATEGY,
} from "./research-planner-v1.js";
import {
  applySalesIntelligencePacketIfNeeded,
  diagnoseSalesPacketGaps,
} from "../sales-intelligence-packet-v1.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { evaluateShadowPresourcing } from "./shadow-presourcing-evaluation-v1.js";
import { evaluateCommercialQualification } from "./commercial-pathway-v1.js";
import { writeMaturationCheckpointAtomic, maturationMemoryPath } from "./bethesda-maturation-checkpoint-v1.js";
import { planDynamicResearch, runEvidenceDrivenResearchLoop, assembleReplanningContext, resolvePlannerStall, RESEARCH_DISPOSITION } from "./dynamic-research-planner-v1.js";
import { prepareAutonomousCertification, executeAuthorizedCertification } from "./autonomous-research-certification-v1.js";
import fs from "node:fs";
import {
  decideParallelDelegation,
  planFindAllFollowUpTask,
  buildFindAllFollowUpInstruction,
  classifyStructuredTaskEvidence,
  formulateNextResearchStep,
  executeDelegatedParallelTask,
} from "./parallel-delegated-research-v1.js";
import { applySupervisorV2MaturationHooks } from "./ors-supervisor-v2-hooks.js";
import { selectNextAuthorizedLeadFollowUp } from "./autonomous-lead-following-v2.js";
import { STOPPING_TRIGGER } from "./commercial-source-selection-v1.js";
import { applyDiscoveryExpansionAfterPublicPathExhausted } from "./commercial-demand-discovery-v4.js";
import { enrichOpportunityWithSecondaryNamedContact } from "../secondary-named-contact-v1.js";
import { classifyResearchContinuationDisposition } from "../research-continuation-disposition-v1.js";
import { evaluateGdi2ShadowPublication } from "../gdi2-shadow-publication-evaluation-v1.js";
import {
  buildLeadPursueQueueFromState,
  drainRelatedEntityFollowUpsThroughOrs,
} from "./commercial-spine-restoration-v41.js";
import {
  resolveRelatedEntityFollowUpStop,
  applyRelatedEntityFollowUpDisposition,
  countRelatedEntityFollowUpAttempts,
} from "./related-entity-followup-disposition-v1.js";
import {
  selectNextAdaptedResearchStep,
  recordFailedResearchStrategy,
} from "./research-strategy-adaptation-v1.js";
import { assessCommercialResearchState } from "./commercial-research-assessment-v1.js";
import { buildResearchSelectionRecord } from "./adaptive-ors-exa-research-loop-v1.js";

function uniq(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function now() {
  return new Date().toISOString();
}

/**
 * Homepage / marketing URL / event listing pages are NOT usable sales routes.
 * Meetings/contact/enquiry paths, business email/phone, or named-buyer routes are.
 * Event calendar listings may still be research-lead evidence (see contactRouteQuality).
 */
export function isActionableContactRoute(contact = {}) {
  const c = contact || {};
  if (c.homepageOnly === true) return false;
  if (
    c.contactRouteQuality === "EVENT_LISTING_ONLY" ||
    c.intermediary?.type === "event_listing_evidence"
  ) {
    return false;
  }
  if (
    c.organizationHomepage &&
    !c.businessEmail &&
    !c.businessPhone &&
    !c.contactFormUrl &&
    !c.intermediary &&
    !c.organizationRouteUrl
  ) {
    return false;
  }
  const orgSales =
    c.intermediary?.type === "organization_sales_route" &&
    c.homepageOnly !== true &&
    c.contactRouteQuality !== "EVENT_LISTING_ONLY" &&
    Boolean(c.contactFormUrl || c.organizationRouteUrl);
  return Boolean(
    c.businessEmail ||
      c.businessPhone ||
      (c.contactFormUrl && c.contactRouteQuality !== "EVENT_LISTING_ONLY") ||
      orgSales ||
      c.namedBuyerEmail ||
      c.namedBuyerPhone
  );
}

/** Research-lead: verified event page without a sales desk. */
export function isEventListingResearchLead(contact = {}) {
  const c = contact || {};
  return (
    c.contactRouteQuality === "EVENT_LISTING_ONLY" ||
    c.intermediary?.type === "event_listing_evidence"
  );
}

export function hasNamedBuyer(contact = {}) {
  const c = contact || {};
  return Boolean(c.personName && (c.role || c.title));
}

/** Contact route ≠ buyer identity. */
export function contactRouteImpliesBuyerIdentity() {
  return false;
}

function evidenceFamilies(opportunity) {
  return uniq((opportunity.evidence || []).map((x) => x.sourceFamily || x.family));
}

function evidenceSupported(flag) {
  return Boolean(flag && (flag === true || flag.supported === true));
}

function shadowReading(opportunity) {
  const shadow = evaluateShadowPresourcing(opportunity);
  return {
    ...shadow,
    internalOnly: true,
    productionPublication: false,
    customerVisible: false,
  };
}

/**
 * Evidence-first evaluation. Confidence scores are diagnostic only —
 * they never alone make a prospect publishable.
 */
export function evaluateOpportunity(opportunity, options = {}) {
  const cfg = { ...SUPERVISOR_DEFAULTS, ...options };
  const blockers = [];
  const contact = opportunity.contact || {};

  if (opportunity.duplicateOf || opportunity.isDuplicate === true) {
    blockers.push({
      key: BLOCKER_KEY.DUPLICATE,
      severity: 1,
      recoverable: false,
      reason: STOP_REASONS.DUPLICATE_OPPORTUNITY,
    });
  }

  if (opportunity.eventStatus === "cancelled" || opportunity.cancelled === true) {
    blockers.push({
      key: BLOCKER_KEY.EVENT_STATUS,
      severity: 1,
      recoverable: false,
      reason: STOP_REASONS.EVENT_CANCELLED,
    });
  }

  if (opportunity.datesPast === true || opportunity.timingClass === "PAST") {
    blockers.push({
      key: BLOCKER_KEY.EVENT_STATUS,
      severity: 1,
      recoverable: false,
      reason: STOP_REASONS.DATES_ALREADY_PAST,
    });
  }

  if (opportunity.marketFit === false || opportunity.hotelCatchmentMismatch === true) {
    blockers.push({
      key: BLOCKER_KEY.GEOGRAPHY_FIT,
      severity: 1,
      recoverable: false,
      reason: opportunity.hotelCatchmentMismatch
        ? STOP_REASONS.HOTEL_CATCHMENT_MISMATCH
        : STOP_REASONS.OUTSIDE_HOTEL_MARKET,
    });
  }

  if (opportunity.sourceConflictUnresolved === true) {
    blockers.push({
      key: BLOCKER_KEY.SOURCE_CONFLICT,
      severity: 1,
      recoverable: false,
      reason: STOP_REASONS.SOURCE_CONFLICT_UNRESOLVED,
    });
  }

  if (opportunity.alreadyContracted === true || opportunity.venueContracted === true) {
    blockers.push({
      key: BLOCKER_KEY.EVENT_STATUS,
      severity: 1,
      recoverable: false,
      reason: STOP_REASONS.ALREADY_PLACED_OR_CONTRACTED,
    });
  }

  if (opportunity.organizationInactive === true) {
    blockers.push({
      key: BLOCKER_KEY.EVENT_STATUS,
      severity: 1,
      recoverable: false,
      reason: STOP_REASONS.ORGANIZATION_INACTIVE,
    });
  }

  if (cfg.requireFutureDemand && !evidenceSupported(opportunity.futureDemand)) {
    blockers.push({
      key: BLOCKER_KEY.FUTURE_DEMAND,
      severity: 0.95,
      recoverable: true,
    });
  }

  // Event-derived exploratory mechanisms (executive/VIP/small-group/offsite)
  // may prospect without a confirmed room block. Missing demandScale is then a
  // soft gap (blocks Ready/publishable, not exploratory). Event existence alone
  // still never qualifies — futureDemand + hotelFit + org route remain required.
  const e2dExploratoryScale =
    opportunity.eventToDemand?.exploratoryWithoutRoomBlock === true ||
    (opportunity.discoverySource === "event_to_demand_v1" &&
      [
        "EXECUTIVE_DELEGATION",
        "VIP_HOSPITALITY",
        "SMALL_GROUP_LODGING",
        "OFFSITE_MEETING",
        "ANCILLARY_EVENT_BUSINESS",
      ].includes(String(opportunity.eventDemandMechanism || "").toUpperCase()));

  if (!evidenceSupported(opportunity.demandScale)) {
    blockers.push({
      key: BLOCKER_KEY.DEMAND_SCALE,
      severity: e2dExploratoryScale ? 0.35 : 0.8,
      recoverable: true,
      softForExploratory: e2dExploratoryScale === true,
      note: e2dExploratoryScale
        ? "confirmed_room_block_not_required_for_e2d_exploratory"
        : null,
    });
  } else if (opportunity.demandScale?.hotelRelevant === false) {
    blockers.push({
      key: BLOCKER_KEY.DEMAND_SCALE,
      severity: 1,
      recoverable: false,
      reason: STOP_REASONS.INSUFFICIENT_DEMAND_SCALE,
    });
  }

  if (cfg.requireHotelFit && !evidenceSupported(opportunity.hotelFit)) {
    blockers.push({
      key: BLOCKER_KEY.HOTEL_FIT,
      severity: 0.8,
      recoverable: true,
    });
  } else if (
    evidenceSupported(opportunity.hotelFit) &&
    opportunity.hotelFit?.viable === false
  ) {
    blockers.push({
      key: BLOCKER_KEY.HOTEL_FIT,
      severity: 1,
      recoverable: false,
      reason: STOP_REASONS.OUTSIDE_HOTEL_MARKET,
    });
  }

  if (!hasNamedBuyer(contact)) {
    blockers.push({
      key: BLOCKER_KEY.BUYER_IDENTITY,
      severity: 0.55,
      recoverable: true,
      note: "contact_route_does_not_imply_buyer_identity",
    });
  }

  if (cfg.requireActionableContactRoute && !isActionableContactRoute(contact)) {
    blockers.push({
      key: BLOCKER_KEY.CONTACT_ROUTE,
      severity: 0.9,
      recoverable: true,
    });
  }

  const families = evidenceFamilies(opportunity);
  if ((opportunity.evidence || []).length < 2 || families.length < 2) {
    blockers.push({
      key: BLOCKER_KEY.CORROBORATION,
      severity: 0.45,
      recoverable: true,
    });
  }

  if (opportunity.drawer?.complete === false) {
    blockers.push({
      key: BLOCKER_KEY.DRAWER_COMPLETENESS,
      severity: 0.4,
      recoverable: true,
    });
  }

  // Unsupported / hallucinated evidence cannot promote
  if (opportunity.unsupportedPromotionAttempt === true) {
    blockers.push({
      key: BLOCKER_KEY.CORROBORATION,
      severity: 1,
      recoverable: false,
      reason: STOP_REASONS.SOURCE_CONFLICT_UNRESOLVED,
    });
  }

  blockers.sort(
    (a, b) =>
      BLOCKER_PRIORITY[b.key] * b.severity - BLOCKER_PRIORITY[a.key] * a.severity
  );

  const hardStop = blockers.find((b) => b.reason && b.recoverable === false);

  const softOnly = new Set([
    BLOCKER_KEY.BUYER_IDENTITY,
    BLOCKER_KEY.CORROBORATION,
    BLOCKER_KEY.DRAWER_COMPLETENESS,
  ]);

  const unresolvedHardBlockers = blockers.filter(
    (b) =>
      b.recoverable &&
      !softOnly.has(b.key) &&
      !(b.softForExploratory === true && b.key === BLOCKER_KEY.DEMAND_SCALE)
  );

  const publishable =
    !hardStop &&
    unresolvedHardBlockers.length === 0 &&
    isActionableContactRoute(contact) &&
    evidenceSupported(opportunity.futureDemand) &&
    evidenceSupported(opportunity.hotelFit) &&
    evidenceSupported(opportunity.demandScale) &&
    opportunity.demandScale?.hotelRelevant !== false &&
    !opportunity.unsupportedPromotionAttempt;

  // If requireNamedBuyerForPublishable, strip publishable when buyer missing
  const publishableFinal =
    publishable &&
    (!cfg.requireNamedBuyerForPublishable || hasNamedBuyer(contact));

  const researchLead =
    !hardStop &&
    !publishableFinal &&
    evidenceSupported(opportunity.futureDemand) &&
    evidenceSupported(opportunity.hotelFit) &&
    isEventListingResearchLead(contact) &&
    (opportunity.discoverySource === "event_to_demand_v1" ||
      opportunity.eventToDemand?.exploratoryWithoutRoomBlock === true) &&
    !opportunity.unsupportedPromotionAttempt;

  const exploratory =
    (!hardStop &&
      !publishableFinal &&
      evidenceSupported(opportunity.futureDemand) &&
      evidenceSupported(opportunity.hotelFit) &&
      isActionableContactRoute(contact) &&
      !opportunity.unsupportedPromotionAttempt) ||
    researchLead;

  let maturityHint = null;
  if (publishableFinal && hasNamedBuyer(contact) && families.length >= 2) {
    maturityHint = PROSPECT_MATURITY.CONFIRMED_DEMAND;
  } else if (publishableFinal) {
    maturityHint = PROSPECT_MATURITY.QUALIFIED_PROSPECT;
  } else if (exploratory) {
    maturityHint = PROSPECT_MATURITY.EXPLORATORY_PROSPECT;
  }

  const evaluation = {
    blockers,
    hardStop,
    publishable: publishableFinal,
    exploratory,
    researchLead: researchLead === true,
    maturityHint,
    namedBuyer: hasNamedBuyer(contact),
    actionableRoute: isActionableContactRoute(contact),
    sourceFamilies: families,
    supervisorVersion: SUPERVISOR_VERSION,
    shadow: shadowReading(opportunity),
    commercial: evaluateCommercialQualification(opportunity, {
      targetHotel: options.targetHotel || opportunity.targetHotel || opportunity.hotelName || "",
    }),
  };
  evaluation.parallelDelegation = decideParallelDelegation(opportunity, {
    commercial: evaluation.commercial,
    targetHotel: options.targetHotel || opportunity.targetHotel || opportunity.hotelName || "",
    hotelId: options.hotelId || opportunity.hotelId || null,
    env: options.env,
  });
  const discoveries = opportunity.discoveryCandidates || options.discoveryCandidates || [];
  if (discoveries.length) {
    evaluation.parallelFollowUp = planFindAllFollowUpTask(discoveries, {
      targetDomains: options.targetDomains,
      hotelId: options.hotelId || opportunity.hotelId || null,
      env: options.env,
      authorizePaidResearch: options.authorizePaidResearch === true,
    });
  }
  return evaluation;
}

export function chooseNextAction(opportunity, evaluation, planner, options = {}) {
  const blocker = evaluation.blockers.find((b) => b.recoverable);
  if (!blocker) return null;
  return planner.plan({ opportunity, blocker, options });
}

function deepMerge(target, patch) {
  for (const [key, value] of Object.entries(patch || {})) {
    if (value && typeof value === "object" && !Array.isArray(value)) {
      target[key] = target[key] || {};
      deepMerge(target[key], value);
    } else {
      target[key] = value;
    }
  }
  return target;
}

function mergeEvidence(opportunity, result) {
  if (!result) return opportunity;
  const next = structuredClone(opportunity);
  if (result.patch) Object.assign(next, result.patch);
  if (result.deepPatch) deepMerge(next, result.deepPatch);
  // Merge contact carefully so buyer fields accumulate
  if (result.deepPatch?.contact || result.patch?.contact) {
    next.contact = { ...(opportunity.contact || {}), ...(next.contact || {}) };
  }
  next.evidence = [...(next.evidence || []), ...(result.evidence || [])];
  if (result.reopenStrategies) {
    next.reopenStrategies = uniq([
      ...(next.reopenStrategies || []),
      ...result.reopenStrategies,
    ]);
  }
  next.updatedAt = now();
  return next;
}

/**
 * RESEARCH_EXHAUSTED requires meaningful breadth+depth — not a casual stop.
 */
export function researchExhausted(opportunity, cfg, blockerKey) {
  const attempts = opportunity.researchAttempts || [];
  const blockerAttempts = attempts.filter((a) => a.blocker === blockerKey);
  const recent = attempts.slice(-cfg.noNewEvidenceStopAfter);
  const noNewEvidence =
    recent.length >= cfg.noNewEvidenceStopAfter &&
    recent.every((a) => !a.newMaterialEvidence);
  const sourceFamilies = uniq(
    blockerAttempts.flatMap((a) => a.sourceFamilies || [])
  );
  const strategies = uniq(blockerAttempts.map((a) => a.strategy).filter(Boolean));
  const queryCount = blockerAttempts.reduce(
    (n, a) => n + (a.queryCount || (a.queries || []).length || 0),
    0
  );

  return (
    blockerAttempts.length >= cfg.maxAttemptsPerBlocker &&
    strategies.length >= cfg.minStrategiesForExhaustion &&
    sourceFamilies.length >= cfg.minSourceFamiliesForExhaustion &&
    queryCount >= cfg.minQueriesForExhaustion &&
    noNewEvidence
  );
}

/**
 * Distinguish false RESEARCH_EXHAUSTED from public-only / paid-available / contact-pending.
 * A candidate cannot be fully RESEARCH_EXHAUSTED while a justified untried paid path remains.
 */
export function classifyExhaustionStop({
  opportunity,
  blockerKey,
  paidBlocked = false,
  parallelAvailable = true,
} = {}) {
  const contact = opportunity?.contact || {};
  if (
    (blockerKey === BLOCKER_KEY.BUYER_IDENTITY ||
      blockerKey === BLOCKER_KEY.CONTACT_ROUTE) &&
    isActionableContactRoute(contact) &&
    !hasNamedBuyer(contact)
  ) {
    return STOP_REASONS.CONTACT_RESEARCH_PENDING;
  }

  // Public path is done only when every applicable public strategy was tried
  // or explicitly skipped — never via the old min-strategy researchExhausted
  // heuristic, which allowed Parallel / NO_FUTURE while CVB / trade-press /
  // recurrence remained untried.
  const remPublic = remainingPublicStrategiesForBlocker(
    opportunity,
    blockerKey
  );
  const publicDone =
    remPublic.length === 0 &&
    publicPathExhaustedForBlocker(opportunity, blockerKey);
  const parallelTried = hasTriedParallelFollowup(opportunity, blockerKey);

  if (publicDone && !parallelTried) {
    if (paidBlocked || parallelAvailable === false) {
      return paidBlocked
        ? STOP_REASONS.PUBLIC_PATH_EXHAUSTED
        : STOP_REASONS.PROVIDER_BLOCKED;
    }
    // Paid path still available — not full exhaustion
    return STOP_REASONS.PAID_FOLLOWUP_AVAILABLE;
  }

  if (publicDone && parallelTried) {
    const lastParallel = [...(opportunity.researchAttempts || [])]
      .reverse()
      .find(
        (a) =>
          a.blocker === blockerKey &&
          (a.provider === "parallel" || isParallelStrategy(a.strategy))
      );
    if (
      lastParallel &&
      (String(lastParallel.disposition || "").toUpperCase() === "BLOCKED" ||
        String(lastParallel.failureReason || "").includes("not_allowed") ||
        String(lastParallel.failureReason || "").includes("kill_switch") ||
        String(lastParallel.failureReason || "").includes("key_missing"))
    ) {
      return STOP_REASONS.PROVIDER_BLOCKED;
    }
    if (
      blockerKey === BLOCKER_KEY.FUTURE_DEMAND &&
      !evidenceSupported(opportunity.futureDemand)
    ) {
      return STOP_REASONS.NO_FUTURE_DEMAND_EVIDENCE;
    }
    // E2D exploratory: exhausting room-block search is not a hard reject when
    // future demand + hotel fit + org route already support prospecting.
    if (
      blockerKey === BLOCKER_KEY.DEMAND_SCALE &&
      (opportunity.eventToDemand?.exploratoryWithoutRoomBlock === true ||
        opportunity.discoverySource === "event_to_demand_v1") &&
      evidenceSupported(opportunity.futureDemand) &&
      evidenceSupported(opportunity.hotelFit) &&
      isActionableContactRoute(opportunity.contact || {})
    ) {
      return STOP_REASONS.CONTACT_RESEARCH_PENDING;
    }
    return STOP_REASONS.RESEARCH_EXHAUSTED;
  }

  // Public path not fully exhausted — never claim RESEARCH_EXHAUSTED.
  // Resumable stop so the orchestrator continues remaining public strategies
  // before any paid escalation.
  return STOP_REASONS.PUBLIC_PATH_EXHAUSTED;
}

export function estimateParallelFollowupExpectedValue({
  opportunity,
  blockerKey,
  remainingBudgetUsd = 0,
  estimatedCostUsd = 0.05,
} = {}) {
  if (remainingBudgetUsd < estimatedCostUsd) {
    return { justified: false, reason: "BUDGET_INSUFFICIENT", score: 0 };
  }
  if (
    hasCompletedParallelFollowup(opportunity, blockerKey) ||
    hasTriedParallelFollowup(opportunity, blockerKey)
  ) {
    return { justified: false, reason: "PARALLEL_ALREADY_TRIED", score: 0 };
  }
  if (!publicPathExhaustedForBlocker(opportunity, blockerKey)) {
    return { justified: false, reason: "PUBLIC_NOT_EXHAUSTED", score: 0.2 };
  }
  const hasIdentity = Boolean(
    opportunity.organizationName || opportunity.officialUrl || opportunity.officialDomain
  );
  const hasSomeEvidence = (opportunity.evidence || []).length >= 2;
  const softReject =
    opportunity.hotelFit?.viable === false ||
    opportunity.demandScale?.hotelRelevant === false;
  if (softReject || !hasIdentity) {
    return { justified: false, reason: "LOW_IDENTITY_OR_FIT", score: 0.1 };
  }
  const score =
    (hasIdentity ? 0.35 : 0) +
    (hasSomeEvidence ? 0.25 : 0) +
    (blockerKey === BLOCKER_KEY.FUTURE_DEMAND ||
    blockerKey === BLOCKER_KEY.DEMAND_SCALE
      ? 0.25
      : 0.1);
  return {
    justified: score >= 0.6,
    reason: score >= 0.6 ? "PUBLIC_EXHAUSTED_WITH_IDENTITY" : "EV_BELOW_THRESHOLD",
    score,
  };
}

function spentUsd(opportunity) {
  return (opportunity.researchAttempts || []).reduce(
    (n, a) => n + Number(a.costUsd || 0),
    0
  );
}

/** Already hotel-facing / customer-visible — deepen buyer depth; do not RESEARCH_EXHAUST. */
export function isHotelFacingAdvisory(opportunity = {}) {
  return (
    opportunity.customerVisible === true ||
    opportunity.prospectHotelFacing === true ||
    opportunity.prospectPublicationEligibility === "HOTEL_FACING" ||
    opportunity.startingState === "HOTEL_FACING"
  );
}

async function preserveFacingOrExploratory(state, persist, note) {
  state.researchStatus = RESEARCH_TERMINAL.EXPLORATORY_PROSPECT;
  state.prospectMaturity = PROSPECT_MATURITY.EXPLORATORY_PROSPECT;
  state.stopReason = null;
  state.notes = [...(state.notes || []), note].filter(Boolean);
  state.statusHistory = state.statusHistory || [];
  state.statusHistory.push({
    at: now(),
    status: RESEARCH_TERMINAL.EXPLORATORY_PROSPECT,
    note,
  });
  await persist?.(state);
  return state;
}

function checkpointInvestigation(state) {
  if (state?.checkpointDir) writeMaturationCheckpointAtomic(state.checkpointDir, state);
}

function stopWith(state, reason, persist, stopOpts = {}) {
  let finalReason = reason;
  // PUBLIC_PATH_EXHAUSTED = current org-owned path only. Plan independent/program
  // searches; if they cannot run, prefer PROVIDER_RESTRICTED_NOT_EXHAUSTION.
  if (reason === STOP_REASONS.PUBLIC_PATH_EXHAUSTED) {
    const hotel = stopOpts.hotel || {
      hotelId: state.hotelId,
      displayName: state.hotelName || state.targetHotel,
      city: state.city,
    };
    const { opportunity: expanded, plan } = applyDiscoveryExpansionAfterPublicPathExhausted(
      state,
      hotel,
      {
        paidBlocked: stopOpts.paidBlocked !== false,
        parallelAvailable: stopOpts.parallelAvailable === true,
        remainingBudgetUsd: stopOpts.remainingBudgetUsd ?? 0,
      },
    );
    Object.assign(state, expanded);
    state.notes = [
      ...(state.notes || []),
      "public_path_exhausted_means_org_owned_path_only",
      plan.executableNow.length
        ? `independent_source_expansion_pending:${plan.executableNow.length}`
        : `independent_source_blocked:${plan.stopReasonIfCannotContinue || "none"}`,
    ];
    if (!plan.executableNow.length && plan.stopReasonIfCannotContinue === STOP_REASONS.PROVIDER_RESTRICTED_NOT_EXHAUSTION) {
      finalReason = STOP_REASONS.PROVIDER_RESTRICTED_NOT_EXHAUSTION;
      state.commercialStoppingTrigger = STOPPING_TRIGGER.PROVIDER_RESTRICTED_NOT_EXHAUSTION;
    } else if (plan.executableNow.length) {
      finalReason = STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING;
    }
  }
  state.researchStatus = RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON;
  state.stopReason = finalReason;
  // Phase 2B shadow: map stop reason → continuation disposition (does not change terminal status).
  try {
    const providerFailed =
      finalReason === STOP_REASONS.TEMPORARY_FAILURE ||
      finalReason === STOP_REASONS.PROVIDER_BLOCKED ||
      finalReason === STOP_REASONS.PROVIDER_RESTRICTED_NOT_EXHAUSTION;
    const budgetExhausted = finalReason === STOP_REASONS.BUDGET_BLOCKED;
    const strategiesExhausted =
      finalReason === STOP_REASONS.PUBLIC_PATH_EXHAUSTED ||
      finalReason === STOP_REASONS.RESEARCH_EXHAUSTED;
    state.gdi2ContinuationDispositionV1 = classifyResearchContinuationDisposition({
      opportunity: state,
      hotel: stopOpts.hotel || {},
      providerFailed,
      strategyEmptyResults: strategiesExhausted,
      authorizedBudgetExhausted: budgetExhausted,
      laterMilestonePending:
        finalReason === STOP_REASONS.PAID_FOLLOWUP_AVAILABLE ||
        finalReason === STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING ||
        finalReason === STOP_REASONS.CONTACT_RESEARCH_PENDING,
    });
    state.gdi2ShadowEvaluationV1 = evaluateGdi2ShadowPublication(state, {
      hotel: stopOpts.hotel,
      existingSalesReady: false,
      existingResearchContinuation: true,
      providerFailed,
      strategyEmptyResults: strategiesExhausted,
      authorizedBudgetExhausted: budgetExhausted,
    });
    state.notes = [
      ...(state.notes || []),
      `gdi2_shadow_continuation:${state.gdi2ContinuationDispositionV1?.disposition || "n/a"}`,
      `gdi2_shadow_exhausted_false:${state.gdi2ContinuationDispositionV1?.commerciallyExhausted !== true}`,
    ];
  } catch {
    state.notes = [...(state.notes || []), "gdi2_shadow_continuation_attach_skipped"];
  }
  checkpointInvestigation(state);
  state.statusHistory = state.statusHistory || [];
  state.statusHistory.push({
    at: now(),
    status: RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON,
    reason: finalReason,
  });
  return persist?.(state).then?.(() => state) ?? Promise.resolve(state);
}

export async function matureOpportunity({
  opportunity,
  planner,
  researcher,
  persist,
  options = {},
}) {
  const cfg = { ...SUPERVISOR_DEFAULTS, ...options };
  const activePlanner = planner || new HeuristicResearchPlanner();
  let state = structuredClone(opportunity);
  if (cfg.checkpointDir) state.checkpointDir = cfg.checkpointDir;
  state.researchAttempts = state.researchAttempts || [];
  state.statusHistory = state.statusHistory || [];
  state.researchStatus = RESEARCH_TERMINAL.IN_PROGRESS;
  state.startedAt ||= now();
  state.supervisorVersion = SUPERVISOR_VERSION;

  // Resume: if already terminal, do not re-run unless force
  if (
    !cfg.forceRemature &&
    [
      RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT,
      RESEARCH_TERMINAL.EXPLORATORY_PROSPECT,
      RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON,
    ].includes(opportunity.researchStatus)
  ) {
    return structuredClone(opportunity);
  }

  for (let iteration = 1; iteration <= cfg.maxIterations; iteration++) {
    const evalBefore = evaluateOpportunity(state, cfg);

    if (evalBefore.hardStop) {
      await stopWith(state, evalBefore.hardStop.reason, persist);
      return state;
    }

    const followUp = evalBefore.parallelFollowUp;
    const followUpFingerprint = followUp?.task?.fingerprint || null;
    const followUpCompleted = (state.researchAttempts || []).some(
      (attempt) => attempt.fingerprint === followUpFingerprint && attempt.outcome === "completed"
    );
    if (followUp?.task && !followUp.task.executable && !followUpCompleted) {
      state.nextResearchStep = {
        action: "AWAITING_AUTHORIZATION",
        question: followUp.task.questions?.[0] || null,
        evidenceSought: "Hotel placement, programme evidence, and a contact on the firm's own domain.",
        provider: "parallel_pro_task",
        estimatedUsd: followUp.task.estimatedUsd,
        executable: false,
        paidCalls: 0,
        publicationEligibilityChanged: false,
        stoppingCriteria: "Do not start the Task until flags, the hotel allowlist, and explicit budget authorization are present.",
      };
      await stopWith(state, "AWAITING_PAID_RESEARCH_AUTHORIZATION", persist);
      return state;
    }
    if (followUp?.task?.executable && followUpFingerprint && !followUpCompleted) {
      const executed = await executeDelegatedParallelTask({
        opportunity: { ...state, organizationName: followUp.selected },
        decision: {
          justified: true,
          executable: true,
          questions: followUp.task.questions,
          fingerprint: followUpFingerprint,
          processor: "pro",
          publicationAuthority: "supervisor",
          customerVisible: false,
        },
        hotelId: state.hotelId,
        processor: "pro",
        env: cfg.env || process.env,
        client: cfg.parallelClient,
        timeoutMs: cfg.parallelTimeoutMs || 900000,
        instruction: buildFindAllFollowUpInstruction(followUp.task, cfg.targetHotel || state.targetHotel || ""),
      });
      const separated = classifyStructuredTaskEvidence(executed.validation, executed.basis, {
        targetDomains: cfg.targetDomains,
      });
      if (separated.organizationOwned.length) {
        state.evidence = [
          ...(state.evidence || []),
          ...separated.organizationOwned.map((item) => ({
            sourceFamily: "parallel_structured_task",
            url: item.evidenceUrls[0] || null,
            quote: item.excerpt || item.statement,
            organizationOwned: true,
            publicationReady: false,
            customerVisible: false,
          })),
        ];
      }
      for (const item of executed.validation?.sourceRecovery || []) {
        if (item.path !== "stored_basis" || !item.excerpt) continue;
        const held = (state.evidence || []).some((row) => row.url === item.url && row.quote === item.excerpt);
        if (held) continue;
        state.evidence.push({
          sourceFamily: "parallel_stored_basis",
          url: item.url,
          quote: item.excerpt,
          fact: item.fact,
          organizationOwned: true,
          publicationReady: false,
          customerVisible: false,
        });
      }
      state.nextResearchStep = formulateNextResearchStep({
        organizationName: followUp.selected || state.organizationName,
        targetHotel: cfg.targetHotel || state.targetHotel || "",
        evidenceText: [state.evidenceQuote, ...(state.evidence || []).map((item) => item.quote)].filter(Boolean).join("\n"),
        sourceRecovery: executed.validation?.sourceRecovery || [],
        attempts: state.researchAttempts,
        investigatedUrls: (executed.basis || []).flatMap((item) => (item.citations || []).map((citation) => citation.url)),
        remainingUsd: cfg.remainingBudgetUsd ?? 0,
        commercial: evalBefore.commercial,
      });
      checkpointInvestigation(state);
      if (cfg.dynamicPlanner === true) {
        try {
          const replanningContext = assembleReplanningContext({
            url: (state.evidence || []).map((item) => item.url).filter(Boolean).at(-1) || "",
            pageText: (state.evidence || []).map((item) => item.quote).filter(Boolean).join(" "),
            targetHotel: cfg.targetHotel || state.targetHotel || "",
            investigated: (state.evidence || []).map((item) => item.url).filter(Boolean),
            answeredQuestions: state.nextResearchStep?.question ? [state.nextResearchStep.question] : [],
            failedAttempts: state.rejectedProposals || [],
            authorizedProviders: cfg.authorizePaidResearch === true ? ["direct_public", "parallel_pro_task"] : ["direct_public"],
          });
          const evidenceFindings = (state.evidence || []).map((item) => ({ excerpt: item.quote, url: item.url }));
          state.dynamicPlan = await planDynamicResearch({
            objective: {
              hotelId: state.hotelId,
              hotelName: cfg.targetHotel || state.targetHotel || "",
              segmentId: state.segmentId || null,
              commercialQuestion: state.nextResearchStep?.question || null,
            },
            hotel: { name: cfg.targetHotel || state.targetHotel || "", market: state.market || "" },
            opportunity: {
              organizationName: followUp.selected || state.organizationName,
              commercialStage: evalBefore.commercial?.commercialStage || null,
              sourcingStatus: evalBefore.commercial?.sourcingStatus || null,
            },
            findings: replanningContext.plannerFindings.length ? replanningContext.plannerFindings : evidenceFindings,
            replanningContext,
            openQuestions: state.nextResearchStep?.missingFacts || replanningContext.unresolvedBlockers,
            attempts: state.researchAttempts || [],
            investigated: { urls: (state.evidence || []).map((item) => item.url).filter(Boolean) },
            remainingUsd: cfg.remainingBudgetUsd ?? 0,
            authorizePaidResearch: cfg.authorizePaidResearch === true,
            authorizePlannerModel: cfg.authorizePlannerModel === true,
            complete: cfg.plannerComplete,
            commercial: evalBefore.commercial,
            deterministicStep: state.nextResearchStep,
            memory: state.researchMemory || null,
          });
          state.researchDisposition = state.dynamicPlan.researchDisposition;
          state.researchMemory = state.dynamicPlan.researchMemory;
          state.nextPermittedAction = state.dynamicPlan.nextPermittedAction;
          state.rejectedProposals = state.dynamicPlan.rejectedProposals;
          if (state.researchDisposition === RESEARCH_DISPOSITION.PLANNER_STALLED) {
            const stall = resolvePlannerStall({
              targetHotel: cfg.targetHotel || state.targetHotel || "",
              destination: replanningContext.destination || state.market || "",
              hotelMarket: state.market || "",
              hotelCommerciallyUnsuitable: replanningContext.hotelCommerciallyUnsuitable === true,
              hotelSpecificFit: replanningContext.hotelSpecificFit,
              unresolvedFact: (state.nextResearchStep?.missingFacts || replanningContext.unresolvedBlockers || [])[0] || "host hotel name",
              alreadyInvestigated: true,
              contactPresent: Boolean(state.primaryContactEmail || replanningContext.contactPresent),
              investigatedUrls: (state.evidence || []).map((item) => item.url).filter(Boolean),
              organizationName: followUp.selected || state.organizationName,
              commercial: evalBefore.commercial,
              alternativeSources: cfg.alternativeSources || [],
              openHostQuestion: true,
              sourcesExhausted: cfg.sourcesExhausted === true,
            });
            state.stallResolution = stall;
            state.valueOfInformation = stall.valueOfInformation;
            state.researchDisposition = stall.researchDisposition;
            state.nextPermittedAction = stall.nextPermittedAction;
            if (stall.proposals?.length) {
              state.dynamicPlan = {
                ...state.dynamicPlan,
                proposals: stall.proposals,
                researchDisposition: stall.researchDisposition,
                dispositionNote: stall.dispositionNote,
                nextPermittedAction: stall.nextPermittedAction,
                publicationEligibilityChanged: false,
              };
            }
          }
        } catch (err) {
          state.dynamicPlan = { fallbackReason: err.code || "planner_unavailable", paidCalls: 0, publicationEligibilityChanged: false };
        }
      }
      if (separated.contact?.email && !state.primaryContactEmail) {
        state.primaryContactEmail = separated.contact.email;
        state.officialSource = separated.contact.url || state.officialSource;
      }
      state.parallelEvidenceSeparation = {
        organizationOwned: separated.organizationOwned.length,
        targetHotelMarketing: separated.targetHotelMarketing.length,
        sourceRecovery: separated.sourceRecovery,
        publicationEligibilityChanged: false,
      };
      state.delegatedResearch = {
        status: executed.status,
        providerRunId: executed.providerRunId || null,
        resumed: executed.resumed === true,
        actualUsd: executed.actualUsd || 0,
        acceptedClaims: executed.validation?.accepted?.length || 0,
        rejectedClaims: executed.validation?.rejected?.length || 0,
        validation: executed.validation || null,
        output: executed.output || null,
        basis: executed.basis || [],
        separation: separated,
        publicationEligibilityChanged: false,
        customerVisible: false,
      };
      state.researchAttempts.push({
        at: now(),
        iteration,
        blocker: null,
        strategy: "parallel_findall_followup",
        fingerprint: executed.status === "completed" ? followUpFingerprint : null,
        outcome: executed.status,
        paid: executed.paidCalls > 0,
        countsAsResearchDepth: executed.paidCalls > 0,
        publicationEligibilityChanged: false,
      });
      continue;
    }

    const delegation = evalBefore.parallelDelegation;
    const delegationAlreadyTried = (state.researchAttempts || []).some(
      (attempt) => attempt.fingerprint && attempt.fingerprint === delegation?.fingerprint
    );
    if (delegation?.executable && !delegationAlreadyTried && !followUp?.selected) {
      const executed = await executeDelegatedParallelTask({
        opportunity: state,
        decision: delegation,
        hotelId: state.hotelId,
        env: cfg.env || process.env,
        client: cfg.parallelClient,
      });
      state.researchAttempts.push({
        at: now(),
        iteration,
        blocker: null,
        strategy: "parallel_structured_task",
        fingerprint: delegation.fingerprint,
        outcome: executed.status,
        paid: executed.paidCalls > 0,
        countsAsResearchDepth: executed.paidCalls > 0,
        publicationEligibilityChanged: false,
      });
      if (executed.evidence?.length) {
        state.evidence = [...(state.evidence || []), ...executed.evidence];
      }
      state.delegatedResearch = {
        status: executed.status,
        acceptedClaims: executed.validation?.accepted?.length || 0,
        rejectedClaims: executed.validation?.rejected?.length || 0,
        publicationEligibilityChanged: false,
        customerVisible: false,
      };
      continue;
    }

    // Blocker-driven sales packet: when research evidence already supports
    // customer Ready fields, synthesize before spending on another web search.
    // At most one synthesis attempt per opportunity (prevents retry loops).
    const alreadyPacketAttempted = (state.researchAttempts || []).some(
      (a) =>
        a.disposition === "SALES_PACKET_SYNTHESIS" ||
        a.strategy === "sales_intelligence_packet_v1"
    );
    if (
      !alreadyPacketAttempted &&
      cfg.salesPacketSynthesis !== false &&
      (evalBefore.publishable ||
        evalBefore.exploratory ||
        isHotelFacingAdvisory(state))
    ) {
      const readyBefore = isGdiCustomerOpportunityReady(state, cfg);
      if (!readyBefore.ok) {
        const { next } = diagnoseSalesPacketGaps(state, readyBefore.failed);
        const canSynth = next.some((d) => d.canSynthesizeWithoutSearch);
        if (
          canSynth ||
          readyBefore.failed.some((f) =>
            /summary|why_now|recommended_action|account_quality|hotel_fit/i.test(f)
          )
        ) {
          const applied = applySalesIntelligencePacketIfNeeded(state, {
            reason: "supervisor_blocker_driven_packet",
          });
          if (applied.changed && applied.opportunity) {
            state = applied.opportunity;
          }
          state.researchAttempts = [
            ...(state.researchAttempts || []),
            {
              at: now(),
              iteration,
              blocker: "customer_packet_fields",
              strategy: "sales_intelligence_packet_v1",
              fingerprint: `sales_packet::${state.opportunityId || state.id || "opp"}`,
              newMaterialEvidence: applied.changed === true,
              costUsd: 0,
              disposition: "SALES_PACKET_SYNTHESIS",
              notes: applied.reason || "packet_synthesis",
              gaps: applied.gaps || next.map((d) => d.field),
            },
          ];
          await persist?.(state);
          if (applied.changed) continue;
        }
      }
    }

    if (evalBefore.publishable) {
      // Priority B: hotel-facing org-route prospects may already clear advisory
      // publishable without a named buyer — optionally keep deepening buyer/contact.
      const deepenBuyer =
        cfg.deepenNamedBuyer !== false &&
        isHotelFacingAdvisory(state) &&
        !hasNamedBuyer(state.contact || {}) &&
        evalBefore.blockers.some(
          (b) => b.recoverable && b.key === BLOCKER_KEY.BUYER_IDENTITY
        ) &&
        (state.researchAttempts || []).filter((a) => a.blocker === BLOCKER_KEY.BUYER_IDENTITY)
          .length < Number(cfg.maxBuyerDepthAttempts ?? 3);

      if (!deepenBuyer) {
        state.researchStatus = RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT;
        state.prospectMaturity = evalBefore.maturityHint;
        state.publishability = {
          level: evalBefore.maturityHint || PROSPECT_MATURITY.QUALIFIED_PROSPECT,
          note: "supervisor_advisory_does_not_set_customer_ready",
        };
        state.statusHistory.push({
          at: now(),
          status: RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT,
          maturity: state.prospectMaturity,
        });
        await persist?.(state);
        return state;
      }
      state.notes = [
        ...(state.notes || []),
        "priority_b_deepen_named_buyer_despite_advisory_publishable",
      ];
    }

    const paidBlockedEarly =
      Boolean(cfg.paidBlocked) ||
      spentUsd(state) >= Number(cfg.maxPaidCostUsd ?? cfg.maxCostUsd);

    if ((state.researchAttempts || []).length >= cfg.maxTotalAttempts) {
      if (evalBefore.exploratory || isHotelFacingAdvisory(state)) {
        return preserveFacingOrExploratory(
          state,
          persist,
          "facing_or_exploratory_preserved_after_max_attempts"
        );
      }
      const blk = evalBefore.blockers.find((b) => b.recoverable);
      await stopWith(
        state,
        classifyExhaustionStop({
          opportunity: state,
          blockerKey: blk?.key || BLOCKER_KEY.FUTURE_DEMAND,
          paidBlocked: paidBlockedEarly,
          parallelAvailable: cfg.parallelAvailable !== false,
        }),
        persist
      );
      return state;
    }

    if (spentUsd(state) >= cfg.maxCostUsd) {
      await stopWith(state, STOP_REASONS.BUDGET_BLOCKED, persist);
      return state;
    }

    // Prefer researchable blockers. Soft demand_scale on E2D exploratory
    // mechanisms must not force room-block hunting when the prospect already
    // qualifies as exploratory (future + fit + org route).
    const blocker = evalBefore.blockers.find(
      (b) =>
        b.recoverable &&
        !(
          evalBefore.exploratory &&
          b.softForExploratory === true &&
          b.key === BLOCKER_KEY.DEMAND_SCALE
        )
    );
    if (!blocker) {
      if (evalBefore.exploratory || isHotelFacingAdvisory(state)) {
        return preserveFacingOrExploratory(
          state,
          persist,
          "facing_or_exploratory_no_recoverable_blocker"
        );
      }
      await stopWith(state, STOP_REASONS.LEGITIMATELY_REJECTED, persist);
      return state;
    }

    // Named-buyer gap alone: keep deepening, but E2D with a true org *sales*
    // route may stop at exploratory without inventing contacts.
    if (
      evalBefore.exploratory &&
      blocker.key === BLOCKER_KEY.BUYER_IDENTITY &&
      isActionableContactRoute(state.contact || {}) &&
      (state.discoverySource === "event_to_demand_v1" ||
        state.eventToDemand?.exploratoryWithoutRoomBlock === true) &&
      cfg.allowE2dExploratoryWithoutNamedBuyer !== false
    ) {
      return preserveFacingOrExploratory(
        state,
        persist,
        "e2d_exploratory_org_route_without_named_buyer"
      );
    }

    // E2D research-lead (event listing only): preserve exploratory research-memory
    // without treating the listing URL as a sales contact or publishable route.
    if (
      (state.discoverySource === "event_to_demand_v1" ||
        state.eventToDemand?.exploratoryWithoutRoomBlock === true) &&
      isEventListingResearchLead(state.contact || {}) &&
      evidenceSupported(state.futureDemand) &&
      evidenceSupported(state.hotelFit) &&
      cfg.allowE2dResearchLeadExploratory !== false &&
      (blocker.key === BLOCKER_KEY.CONTACT_ROUTE ||
        blocker.key === BLOCKER_KEY.BUYER_IDENTITY ||
        blocker.key === BLOCKER_KEY.DEMAND_SCALE)
    ) {
      state.prospectPublicationEligibility = "ENRICHMENT_QUEUE";
      state.contactRouteQuality = "EVENT_LISTING_ONLY";
      state.notes = [
        ...(state.notes || []),
        "e2d_research_lead_event_listing_not_sales_route",
      ];
      return preserveFacingOrExploratory(
        state,
        persist,
        "e2d_exploratory_research_lead_pending_sales_route"
      );
    }

    const paidBlocked = paidBlockedEarly;

    // Do not OR researchExhausted here — that min-strategy heuristic prematurely
    // stops while reopenStrategies / remaining public strategies are still open.
    const remPublicHere = remainingPublicStrategiesForBlocker(
      state,
      blocker.key
    );
    const publicDone =
      remPublicHere.length === 0 &&
      publicPathExhaustedForBlocker(state, blocker.key);

    if (publicDone) {
      if (evalBefore.exploratory || isHotelFacingAdvisory(state)) {
        return preserveFacingOrExploratory(
          state,
          persist,
          `facing_or_exploratory_preserved_after_blocker_exhaustion:${blocker.key}`
        );
      }
      const reason = classifyExhaustionStop({
        opportunity: state,
        blockerKey: blocker.key,
        paidBlocked,
        parallelAvailable: cfg.parallelAvailable !== false,
      });
      // When paid follow-up is available, do not hard-stop — inject Parallel strategy next
      if (
        reason === STOP_REASONS.PAID_FOLLOWUP_AVAILABLE &&
        !paidBlocked &&
        !hasTriedParallelFollowup(state, blocker.key)
      ) {
        const ev = estimateParallelFollowupExpectedValue({
          opportunity: state,
          blockerKey: blocker.key,
          remainingBudgetUsd:
            cfg.remainingBudgetUsd ??
            Number(cfg.maxPaidCostUsd ?? cfg.maxCostUsd) - spentUsd(state),
        });
        if (ev.justified) {
          state.notes = [
            ...(state.notes || []),
            `escalate_parallel_followup:${blocker.key}:${ev.reason}`,
          ];
          // Fall through — force Parallel action below
        } else {
          state.stopDetail = { prior: reason, parallelEv: ev };
          await stopWith(state, STOP_REASONS.LEGITIMATELY_REJECTED, persist);
          return state;
        }
      } else if (reason === STOP_REASONS.PAID_FOLLOWUP_AVAILABLE && paidBlocked) {
        await stopWith(state, STOP_REASONS.PUBLIC_PATH_EXHAUSTED, persist);
        return state;
      } else {
        await stopWith(state, reason, persist);
        return state;
      }
    }

    let action = chooseNextAction(state, evalBefore, activePlanner, {
      paidBlocked,
    });

    // Force Parallel escalation when notes mark it and planner did not yield it yet
    if (
      !paidBlocked &&
      !isParallelStrategy(action?.strategy) &&
      (state.notes || []).some((n) =>
        String(n).startsWith("escalate_parallel_followup:")
      ) &&
      !hasTriedParallelFollowup(state, blocker.key)
    ) {
      const org = state.organizationName || state.name || "";
      const queries = [
        `"${org}" future demand lodging`,
        `"${org}" official housing contact`,
      ];
      action = {
        strategy: PARALLEL_STRATEGY,
        queries,
        provider: "router",
        fingerprint: `${blocker.key}::${PARALLEL_STRATEGY}::forced`,
        paid: true,
      };
    }

    if (!action || !action.strategy) {
      if (action?.skipReason === "DUPLICATE_STRATEGY_REJECTED") {
        state.researchAttempts.push({
          at: now(),
          iteration,
          blocker: blocker.key,
          strategy: "rejected_duplicate",
          fingerprint: action.fingerprint || null,
          newMaterialEvidence: false,
          costUsd: 0,
          disposition: "DUPLICATE_STRATEGY_REJECTED",
          notes: "Identical research fingerprint rejected",
        });
        await persist?.(state);
        // Continue to next recoverable or exhaust
        if (researchExhausted(state, cfg, blocker.key) || iteration >= cfg.maxIterations) {
          if (evalBefore.exploratory || isHotelFacingAdvisory(state)) {
            return preserveFacingOrExploratory(
              state,
              persist,
              "facing_or_exploratory_preserved_after_duplicate_strategy_reject"
            );
          }
          await stopWith(
            state,
            classifyExhaustionStop({
              opportunity: state,
              blockerKey: blocker.key,
              paidBlocked,
              parallelAvailable: cfg.parallelAvailable !== false,
            }),
            persist
          );
          return state;
        }
        continue;
      }
      if (
        action?.skipReason === "ALL_STRATEGIES_ATTEMPTED" ||
        (paidBlocked && cfg.allowPublicWhenPaidBlocked)
      ) {
        const nextRecoverable = evalBefore.blockers.find(
          (b) => b.recoverable && b.key !== blocker.key
        );
        if (!nextRecoverable) {
          await stopWith(
            state,
            classifyExhaustionStop({
              opportunity: state,
              blockerKey: blocker.key,
              paidBlocked,
              parallelAvailable: cfg.parallelAvailable !== false,
            }),
            persist
          );
          return state;
        }
        continue;
      }
      throw new Error(
        `No research action produced for blocker ${blocker.key}: ${action?.skipReason || "none"}`
      );
    }

    // Reuse provider run if same fingerprint OR same blocker+strategy already completed
    const priorSame = (state.researchAttempts || []).find(
      (a) =>
        a.providerRunId &&
        a.disposition === "COMPLETED" &&
        !a.reusedProviderRun &&
        (a.fingerprint === action.fingerprint ||
          (a.blocker === blocker.key && a.strategy === action.strategy))
    );
    // Only gate Parallel *strategy* reuse on post-public timing. A prior row
    // that merely set provider=parallel on a public strategy still reuses.
    const mayReuse =
      priorSame &&
      !cfg.forceResubmitProvider &&
      (!isParallelStrategy(action.strategy) ||
        canReuseParallelProviderRun(state, blocker.key, priorSame));
    if (mayReuse) {
      state.researchAttempts.push({
        at: now(),
        iteration,
        blocker: blocker.key,
        strategy: action.strategy,
        fingerprint: action.fingerprint,
        queries: action.queries || [],
        queryCount: (action.queries || []).length,
        provider: priorSame.provider,
        providerRunId: priorSame.providerRunId,
        reusedProviderRun: true,
        newMaterialEvidence: false,
        costUsd: 0,
        disposition: "REUSED_PROVIDER_RUN",
        notes: "Completed provider run reused — not resubmitted",
      });
      await persist?.(state);
      continue;
    }

    const started = Date.now();
    const result = await researcher.execute({
      opportunity: state,
      blocker,
      action,
      options: { ...cfg, paidBlocked },
    });

    const attempt = {
      at: now(),
      iteration,
      blocker: blocker.key,
      strategy: action.strategy,
      fingerprint: action.fingerprint,
      queries: action.queries || [],
      queryCount: (action.queries || []).length,
      sourceFamilies: result?.sourceFamilies || [],
      provider: result?.provider || action.provider || "unknown",
      providerRunId: result?.providerRunId || null,
      reusedProviderRun: Boolean(result?.reusedProviderRun),
      newMaterialEvidence: Boolean(result?.newMaterialEvidence),
      evidenceIds: result?.evidenceIds || (result?.evidence || []).map((e) => e.url || e.id).filter(Boolean),
      costUsd: Number(result?.costUsd || 0),
      durationMs: Date.now() - started,
      disposition: result?.disposition || "COMPLETED",
      failureReason: result?.failureReason || null,
      nextRecommendedAction: result?.nextRecommendedAction || null,
      notes: result?.notes || null,
      paid: Boolean(action.paid),
      // Jev routing metadata (Jev is never material evidence / completion authority)
      jevAction: result?.jevAction || result?.jevRouting?.action || null,
      jevDecisionId:
        result?.jevRouting?.decisionId ||
        (result?.provider === "jev_route" ? result?.providerRunId : null),
      jevProviderRequestId: result?.jevRouting?.providerRequestId || null,
      jevCostUsd: Number(result?.jevRouting?.costUsd || 0),
      providerSelection: result?.providerSelection || null,
    };

    state.researchAttempts.push(attempt);
    state = mergeEvidence(state, result);
    state.researchAttempts = state.researchAttempts || [];
    // Ensure the attempt we just pushed survives merge clone quirks
    if (!(state.researchAttempts || []).some((a) => a.at === attempt.at && a.strategy === attempt.strategy)) {
      state.researchAttempts.push(attempt);
    }
    state.statusHistory = state.statusHistory || [];

    // Supervisor V2 hooks — same ORS loop; not a separate pipeline.
    if (cfg.enableSupervisorV2 !== false) {
      state = applySupervisorV2MaturationHooks(state, {
        enableSupervisorV2: true,
        targetHotel: cfg.targetHotel || state.targetHotel || "",
        remainingBudgetUsd: cfg.remainingBudgetUsd,
        retrievalCount: (state.researchAttempts || []).filter((a) => a.newMaterialEvidence).length,
        maxRetrievals: cfg.maxIterations,
        forbidPublication: cfg.forbidPublication !== false,
        forbidAirtable: cfg.forbidAirtable !== false,
        checkpointDir: cfg.checkpointDir || state.checkpointDir || null,
        generateDossier: cfg.generateSupervisorV2Dossier !== false,
        entityUrlHints: cfg.entityUrlHints || state.entityUrlHints || {},
      });
    }

    // Bounded related-entity lead-following through ORS researcher (not a side pipeline).
    if (
      cfg.enableSupervisorV2 !== false
      && cfg.executeLeadFollowUps !== false
      && researcher?.execute
    ) {
      const followedSoFar = (state.researchAttempts || []).filter(
        (a) => a.strategy === "related_entity_followup",
      ).length;
      const leadPick = selectNextAuthorizedLeadFollowUp({
        pursueQueue: state.supervisorV2?.leadFollowing?.pursue || [],
        investigatedUrls: [
          ...(state.evidence || []).map((e) => e.url).filter(Boolean),
          ...(state.investigatedLeadUrls || []),
        ],
        alreadyFollowedCount: followedSoFar,
        maxFollowUps: cfg.maxLeadFollowUpsPerOpportunity ?? 2,
        remainingBudgetUsd: cfg.remainingBudgetUsd ?? Infinity,
        commerciallyUsefulInternal:
          state.supervisorV2?.qualification?.commerciallyUsefulInternal === true
          || state.internalAccountDevelopment === true,
      });
      if (leadPick.lead) {
        const leadStarted = Date.now();
        state.investigatedLeadUrls = [
          ...new Set([...(state.investigatedLeadUrls || []), leadPick.lead.suggestedUrl].filter(Boolean)),
        ];
        const leadResult = await researcher.execute({
          opportunity: state,
          blocker,
          action: {
            strategy: "related_entity_followup",
            queries: [leadPick.lead.suggestedUrl],
            provider: "direct_public",
            fingerprint: leadPick.fingerprint,
            paid: false,
            lead: leadPick.lead,
          },
          options: { ...cfg, paidBlocked },
        });
        const leadAttempt = {
          at: now(),
          iteration,
          blocker: blocker.key,
          strategy: "related_entity_followup",
          fingerprint: leadPick.fingerprint,
          queries: [leadPick.lead.suggestedUrl],
          queryCount: 1,
          sourceFamilies: leadResult?.sourceFamilies || ["related_entity"],
          provider: leadResult?.provider || "direct_public",
          providerRunId: leadResult?.providerRunId || null,
          reusedProviderRun: Boolean(leadResult?.reusedProviderRun),
          newMaterialEvidence: Boolean(leadResult?.newMaterialEvidence),
          evidenceIds: leadResult?.evidenceIds
            || (leadResult?.evidence || []).map((e) => e.url || e.id).filter(Boolean),
          costUsd: Number(leadResult?.costUsd || 0),
          durationMs: Date.now() - leadStarted,
          disposition: leadResult?.disposition || "COMPLETED",
          notes: `related_entity_followup:${leadPick.lead.entityName}`,
          paid: false,
          leadEntityName: leadPick.lead.entityName,
          commercialValueScore: leadPick.lead.commercialValueScore ?? null,
        };
        state.researchAttempts.push(leadAttempt);
        state = mergeEvidence(state, leadResult);
        if (!(state.researchAttempts || []).some((a) => a.at === leadAttempt.at && a.strategy === leadAttempt.strategy)) {
          state.researchAttempts.push(leadAttempt);
        }
        // Preserve attempted lead URLs across merge clone
        state.investigatedLeadUrls = [
          ...new Set([...(state.investigatedLeadUrls || []), leadPick.lead.suggestedUrl].filter(Boolean)),
        ];
        state = applySupervisorV2MaturationHooks(state, {
          enableSupervisorV2: true,
          targetHotel: cfg.targetHotel || state.targetHotel || "",
          remainingBudgetUsd: cfg.remainingBudgetUsd,
          retrievalCount: (state.researchAttempts || []).filter((a) => a.newMaterialEvidence).length,
          maxRetrievals: cfg.maxIterations,
          forbidPublication: cfg.forbidPublication !== false,
          forbidAirtable: cfg.forbidAirtable !== false,
          checkpointDir: cfg.checkpointDir || state.checkpointDir || null,
          generateDossier: cfg.generateSupervisorV2Dossier !== false,
          entityUrlHints: cfg.entityUrlHints || state.entityUrlHints || {},
          leadRetrievalResult: {
            url: leadPick.lead.suggestedUrl,
            text: (leadResult?.evidence || []).map((e) => e.quote || "").join("\n"),
          },
        });
        state.notes = [
          ...(state.notes || []),
          `ors_executed_related_entity_followup:${leadPick.lead.entityName}`,
        ];
      } else if (leadPick.skipReason && leadPick.skipReason !== "no_uninvestigated_authorized_leads") {
        state.notes = [
          ...(state.notes || []),
          `ors_lead_followup_skipped:${leadPick.skipReason}`,
        ];
      }
    }

    if (result?.disposition === "TEMPORARY_FAILURE" || result?.stopHint === "TEMPORARY_FAILURE") {
      await stopWith(state, STOP_REASONS.TEMPORARY_FAILURE, persist);
      return state;
    }

    // Provider/paid blocks are not commercial exhaustion — and pending lead follow-ups must run first.
    if (result?.disposition === "PAID_BLOCKED" || result?.stopHint === "PUBLIC_PATH_EXHAUSTED") {
      const investigatedForLeads = [
        ...(state.evidence || []).map((e) => e.url).filter(Boolean),
        ...(state.investigatedLeadUrls || []),
      ];
      const pendingLead = selectNextAuthorizedLeadFollowUp({
        pursueQueue: state.supervisorV2?.leadFollowing?.pursue || [],
        investigatedUrls: investigatedForLeads,
        alreadyFollowedCount: (state.researchAttempts || []).filter((a) => a.strategy === "related_entity_followup").length,
        maxFollowUps: cfg.maxLeadFollowUpsPerOpportunity ?? 2,
        remainingBudgetUsd: cfg.remainingBudgetUsd ?? Infinity,
        commerciallyUsefulInternal:
          state.supervisorV2?.qualification?.commerciallyUsefulInternal === true
          || state.internalAccountDevelopment === true,
      });
      if (pendingLead.lead) {
        state.notes = [
          ...(state.notes || []),
          "public_path_hint_deferred_for_related_entity_followup",
        ];
        await persist?.(state);
        continue;
      }
      const stopReason = result?.disposition === "PAID_BLOCKED"
        ? STOP_REASONS.PROVIDER_RESTRICTED_NOT_EXHAUSTION
        : (paidBlocked
          ? STOP_REASONS.PROVIDER_RESTRICTED_NOT_EXHAUSTION
          : STOP_REASONS.PUBLIC_PATH_EXHAUSTED);
      state.commercialStoppingTrigger = result?.disposition === "PAID_BLOCKED"
        ? STOPPING_TRIGGER.PROVIDER_RESTRICTED_NOT_EXHAUSTION
        : STOPPING_TRIGGER.PROVIDER_BLOCKED;
      await stopWith(state, stopReason, persist);
      return state;
    }

    await persist?.(state);
  }

  // V4.1: maxIterations is NOT commercial exhaustion. Drain bounded related-entity /
  // pendingDiscoverySearches through the same ORS researcher when follow-ups remain.
  if (
    cfg.executeLeadFollowUps !== false
    && cfg.enableSupervisorV2 !== false
    && researcher?.execute
  ) {
    const queuePack = buildLeadPursueQueueFromState(state, {
      targetHotel: cfg.targetHotel || state.targetHotel || state.hotelName || "",
      remainingBudgetUsd: cfg.remainingBudgetUsd ?? Infinity,
    });
    state.supervisorV2 = {
      ...(state.supervisorV2 || {}),
      leadFollowing: {
        ...(state.supervisorV2?.leadFollowing || {}),
        pursue: queuePack.pursue,
        v41DrainQueued: queuePack.pursue.length,
      },
    };
    const drain = await drainRelatedEntityFollowUpsThroughOrs({
      state,
      researcher,
      targetHotel: cfg.targetHotel || state.targetHotel || state.hotelName || "",
      remainingBudgetUsd: cfg.remainingBudgetUsd ?? Infinity,
      maxFollowUps: cfg.maxLeadFollowUpsPerOpportunity ?? 3,
      paidBlocked: Boolean(cfg.paidBlocked),
      checkpoint: {
        persist: async (s) => {
          Object.assign(state, s);
          await persist?.(state);
          checkpointInvestigation(state);
        },
      },
    });
    Object.assign(state, drain.state);
    state.notes = [
      ...(state.notes || []),
      `ors_v41_lead_drain_executed:${drain.relatedEntityLeadsFollowed}`,
      ...(drain.unexecutedRemaining || []).map(
        (u) => `ors_lead_unexecuted_not_exhaustion:${u.reason}:${u.entityName}`,
      ),
    ];
    if (drain.relatedEntityLeadsFollowed > 0 && cfg.enableSupervisorV2 !== false) {
      state = applySupervisorV2MaturationHooks(state, {
        enableSupervisorV2: true,
        targetHotel: cfg.targetHotel || state.targetHotel || "",
        remainingBudgetUsd: cfg.remainingBudgetUsd,
        forbidPublication: cfg.forbidPublication !== false,
        forbidAirtable: cfg.forbidAirtable !== false,
        checkpointDir: cfg.checkpointDir || state.checkpointDir || null,
        generateDossier: cfg.generateSupervisorV2Dossier !== false,
        entityUrlHints: cfg.entityUrlHints || state.entityUrlHints || {},
      });
    }
  }

  const finalEval = evaluateOpportunity(state, cfg);
  const pendingAfterLoop = selectNextAuthorizedLeadFollowUp({
    pursueQueue: state.supervisorV2?.leadFollowing?.pursue
      || buildLeadPursueQueueFromState(state, {
        targetHotel: cfg.targetHotel || state.targetHotel || "",
        remainingBudgetUsd: cfg.remainingBudgetUsd ?? Infinity,
      }).pursue,
    investigatedUrls: [
      ...(state.evidence || []).map((e) => e.url).filter(Boolean),
      ...(state.investigatedLeadUrls || []),
    ],
    alreadyFollowedCount: (state.researchAttempts || []).filter((a) => a.strategy === "related_entity_followup").length,
    maxFollowUps: cfg.maxLeadFollowUpsPerOpportunity ?? 3,
    remainingBudgetUsd: cfg.remainingBudgetUsd ?? Infinity,
    commerciallyUsefulInternal:
      state.supervisorV2?.qualification?.commerciallyUsefulInternal === true
      || state.internalAccountDevelopment === true,
  });

  if (finalEval.publishable) {
    state.researchStatus = RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT;
    state.prospectMaturity = finalEval.maturityHint;
    state.commercialStoppingTrigger = STOPPING_TRIGGER.RESEARCH_SUFFICIENT;
  } else if (finalEval.exploratory || isHotelFacingAdvisory(state)) {
    state.researchStatus = RESEARCH_TERMINAL.EXPLORATORY_PROSPECT;
    state.prospectMaturity = PROSPECT_MATURITY.EXPLORATORY_PROSPECT;
    state.stopReason = null;
  } else if (pendingAfterLoop.lead) {
    // run-049: PENDING only when executable; cap / heuristic exhaustion → truthful disposition.
    const followUpResolution = resolveRelatedEntityFollowUpStop({
      state,
      pendingLead: pendingAfterLoop.lead,
      maxFollowUps: cfg.maxLeadFollowUpsPerOpportunity ?? 3,
      remainingBudgetUsd: cfg.remainingBudgetUsd ?? Infinity,
      paidBlocked: Boolean(cfg.paidBlocked),
    });
    state = applyRelatedEntityFollowUpDisposition(state, followUpResolution);
    state.researchStatus = RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON;
    if (!state.stopReason) {
      state.stopReason = STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING;
      state.commercialStoppingTrigger = STOPPING_TRIGGER.RELATED_ENTITY_FOLLOWUPS_PENDING;
    }
  } else if (
    countRelatedEntityFollowUpAttempts(state) > 0 &&
    !(state.relatedEntityFollowUpDispositions || []).length
  ) {
    // Follow-ups were executed but queue is empty — do not leave a stale PENDING
    // and do not silently drop the investigation trail.
    const followUpResolution = resolveRelatedEntityFollowUpStop({
      state,
      pendingLead: null,
      maxFollowUps: cfg.maxLeadFollowUpsPerOpportunity ?? 3,
      remainingBudgetUsd: cfg.remainingBudgetUsd ?? Infinity,
      paidBlocked: Boolean(cfg.paidBlocked),
    });
    state = applyRelatedEntityFollowUpDisposition(state, followUpResolution);
    // run-050: when heuristic follow-ups unresolved, adapt to a different strategy
    // instead of silently ending as INVESTIGATED_UNRESOLVED with no next step.
    state = recordFailedResearchStrategy(state, "HEURISTIC_ORG_PATH", "related_entity_drain_complete");
    const adapted = selectNextAdaptedResearchStep(state, {
      displayName: cfg.targetHotel || state.hotelName || "",
    });
    state.researchStrategyAdaptation = adapted;
    // Stage E: record ORS ↔ Exa adaptive selection (does not auto-spend).
    const adaptiveSelection = buildResearchSelectionRecord({
      opportunity: {
        ...state,
        unresolvedResearchTasks: adapted.question
          ? [{ question: adapted.question, priority: "P0" }]
          : state.unresolvedResearchTasks,
      },
      hotel: {
        hotelId: state.hotelId || cfg.hotelId,
        displayName: cfg.targetHotel || state.hotelName || "",
        name: cfg.targetHotel || state.hotelName || "",
      },
      remainingAuthorizedUsd: Number(cfg.remainingBudgetUsd) || 0,
      authorizeExaAgentLive: Boolean(cfg.authorizeExaAgentLive),
    });
    state.adaptiveOrsExaSelection = adaptiveSelection;
    // Gold-drawer control surface: persist commercial gaps that drove adaptation.
    if (adapted.assessment) {
      state.commercialResearchAssessment = adapted.assessment;
    }
    if (adapted.executable && adapted.nextResearchStep) {
      state.nextResearchStep = {
        ...adapted.nextResearchStep,
        adaptiveProviderFamily: adaptiveSelection.providerFamily,
        adaptiveStrategy: adaptiveSelection.selectedStrategy,
        canExecuteLive: adaptiveSelection.canExecuteLive,
      };
      state.stopReason = STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING;
      state.commercialStoppingTrigger = STOPPING_TRIGGER.RELATED_ENTITY_FOLLOWUPS_PENDING;
      state.researchStatus = RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON;
      state.notes = [
        ...(state.notes || []),
        `ors_strategy_adapted:${adapted.strategy}`,
        `adaptive_ors_exa:${adaptiveSelection.providerFamily}:${adaptiveSelection.selectedStrategy}`,
      ];
    } else if (followUpResolution.stopReason) {
      state.researchStatus = RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON;
      if (adapted.action === "SCHEDULE_FUTURE_REFRESH") {
        state.nextResearchStep = {
          action: "SCHEDULE_FUTURE_REFRESH",
          question: adapted.question,
          executable: false,
          followUpStatus: adapted.status,
        };
      }
    } else if (
      state.supervisorV2?.qualification?.commerciallyUsefulInternal === true
      || state.internalAccountDevelopment === true
    ) {
      state.researchStatus = RESEARCH_TERMINAL.EXPLORATORY_PROSPECT;
      state.prospectMaturity = PROSPECT_MATURITY.EXPLORATORY_PROSPECT;
      state.stopReason = null;
      state.commercialStoppingTrigger = STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL;
    } else {
      state.researchStatus = RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON;
      state.stopReason = STOP_REASONS.ITERATION_CEILING_HIT;
      state.commercialStoppingTrigger = STOPPING_TRIGGER.ITERATION_CEILING_HIT;
    }
  } else if (
    state.supervisorV2?.qualification?.commerciallyUsefulInternal === true
    || state.internalAccountDevelopment === true
  ) {
    state.researchStatus = RESEARCH_TERMINAL.EXPLORATORY_PROSPECT;
    state.prospectMaturity = PROSPECT_MATURITY.EXPLORATORY_PROSPECT;
    state.stopReason = null;
    state.commercialStoppingTrigger = STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL;
  } else {
    const blk = finalEval.blockers.find((b) => b.recoverable);
    state.researchStatus = RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON;
    // Soft iteration ceiling is not research exhaustion.
    state.stopReason = STOP_REASONS.ITERATION_CEILING_HIT;
    state.commercialStoppingTrigger = STOPPING_TRIGGER.ITERATION_CEILING_HIT;
    state.stopDetail = {
      ...(state.stopDetail || {}),
      priorClassifyWouldHaveBeen: classifyExhaustionStop({
        opportunity: state,
        blockerKey: blk?.key || BLOCKER_KEY.FUTURE_DEMAND,
        paidBlocked: Boolean(cfg.paidBlocked),
        parallelAvailable: cfg.parallelAvailable !== false,
      }),
      note: "maxIterations / soft ceiling reached with unresolved commercial questions — resumable, not exhausted",
    };
  }
  if (cfg.enableSupervisorV2 !== false) {
    state = applySupervisorV2MaturationHooks(state, {
      enableSupervisorV2: true,
      targetHotel: cfg.targetHotel || state.targetHotel || "",
      remainingBudgetUsd: cfg.remainingBudgetUsd,
      forbidPublication: cfg.forbidPublication !== false,
      forbidAirtable: cfg.forbidAirtable !== false,
      checkpointDir: cfg.checkpointDir || state.checkpointDir || null,
      generateDossier: cfg.generateSupervisorV2Dossier !== false,
      stoppingTrigger: state.commercialStoppingTrigger || state.stopReason || null,
      entityUrlHints: cfg.entityUrlHints || state.entityUrlHints || {},
    });
  }
  checkpointInvestigation(state);
  // Universal secondary named contact (all segments) — optional; never blocks publication
  state = enrichOpportunityWithSecondaryNamedContact(state, {
    candidates: state.namedStakeholders || state.namedStakeholdersFound || [],
  });
  // Always attach commercial assessment so gold-drawer / continuation can see gaps
  // even when strategy adaptation did not fire on this pass.
  if (!state.commercialResearchAssessment) {
    state.commercialResearchAssessment = assessCommercialResearchState(state, {
      displayName: cfg.targetHotel || state.hotelName || "",
    });
  }
  await persist?.(state);
  return state;
}

export async function resumeMatureOpportunity({
  opportunity,
  planner,
  researcher,
  persist,
  options = {},
} = {}) {
  const checkpointDir = options.checkpointDir;
  const id = opportunity?.id || opportunity?.opportunityId;
  const saved = JSON.parse(fs.readFileSync(maturationMemoryPath(checkpointDir, id), "utf8"));
  const interrupted = saved.delegatedResearch?.status === "interrupted" && Boolean(saved.delegatedResearch?.providerRunId);
  return matureOpportunity({
    opportunity: saved,
    planner,
    researcher,
    persist,
    options: { ...options, checkpointDir, forceRemature: interrupted },
  });
}

export async function matureOpportunityQueue({
  opportunities,
  planner,
  researcher,
  persist,
  options = {},
  onProgress,
}) {
  const results = [];
  const seenIds = new Set();
  for (let i = 0; i < opportunities.length; i++) {
    const opp = opportunities[i];
    const id = String(opp.id || opp.opportunityId || "");
    if (id && seenIds.has(id)) {
      results.push({
        ...structuredClone(opp),
        researchStatus: RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON,
        stopReason: STOP_REASONS.DUPLICATE_OPPORTUNITY,
        note: "duplicate_id_in_queue_skipped",
      });
      continue;
    }
    if (id) seenIds.add(id);
    const result = await matureOpportunity({
      opportunity: opp,
      planner,
      researcher,
      persist,
      options,
    });
    results.push(result);
    await onProgress?.({ index: i, total: opportunities.length, result });
  }
  return results;
}

export {
  RESEARCH_TERMINAL,
  PROSPECT_MATURITY,
  STOP_REASONS,
  SUPERVISOR_VERSION,
};

export function prepareLiveResearchCertification() {
  return prepareAutonomousCertification();
}

export async function executeLiveResearchCertification(input = {}) {
  return executeAuthorizedCertification(input);
}

export async function runSupervisorEvidenceLoop(input = {}) {
  if (input.executePaidProviders === true) {
    const err = new Error("paid_provider_execution_refused");
    err.code = "PAID_EXECUTION_REFUSED";
    throw err;
  }
  return runEvidenceDrivenResearchLoop({ ...input, executePaidProviders: false });
}

export {
  runAdaptiveInvestigation,
  replayFrozenInvestigation,
  interpretInvestigationEvidence,
} from "./adaptive-investigation-v1.js";
