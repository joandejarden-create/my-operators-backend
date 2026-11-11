/**
 * Adaptive Deep Research Continuation Contract V1 (hotel-generic).
 *
 * Replaces premature fixed-cap "research complete" with commercially justified
 * continuation planning. Zero-paid by default — execute only when authorized.
 *
 * Does NOT weaken Sales Ready acceptance gates.
 * Does NOT authorize provider spend.
 */
import { createHash } from "node:crypto";
import {
  scoreDeepResearchCandidate,
  assessContinuationEligibility,
} from "./adaptive-deep-research-prioritization-v1.js";
import {
  RESEARCH_COMPLETENESS_STATUS,
  classifyWorkflowResearchCompleteness,
} from "./research-completeness-status-contract-v1.js";
import { EXA_MEDIUM_USD, EXA_HIGH_USD } from "./first-time-full-e2e-research-plan-v1.js";

export const ADAPTIVE_DEEP_RESEARCH_CONTINUATION_V1 =
  "gdi-adaptive-deep-research-continuation-v1";

export const CONTINUATION_STOP_CONDITION = Object.freeze({
  SUFFICIENT_FOR_QUALIFICATION: "SUFFICIENT_FOR_QUALIFICATION",
  THESIS_DISPROVEN: "THESIS_DISPROVEN",
  STRATEGIES_EXHAUSTED: "STRATEGIES_EXHAUSTED",
  MARGINAL_VALUE_TOO_LOW: "MARGINAL_VALUE_TOO_LOW",
  PER_CANDIDATE_CEILING: "PER_CANDIDATE_CEILING",
  AGGREGATE_CAMPAIGN_CEILING: "AGGREGATE_CAMPAIGN_CEILING",
  PROVIDER_OR_ELIGIBILITY_FAILURE: "PROVIDER_OR_ELIGIBILITY_FAILURE",
  AWAITING_PAID_AUTHORIZATION: "AWAITING_PAID_AUTHORIZATION",
});

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

export function mintDiscoveryCandidateId({
  hotelKey = "",
  workflowId = "",
  organizationName = "",
  programmeName = "",
  activityDate = "",
} = {}) {
  const raw = [
    hotelKey,
    workflowId,
    text(organizationName).toLowerCase(),
    text(programmeName).toLowerCase(),
    text(activityDate),
  ].join("|");
  const hash = createHash("sha256").update(raw).digest("hex").slice(0, 12);
  return `gdi_v26disc_${hash}`;
}

/**
 * Identify the unresolved commercial question for a candidate.
 */
export function identifyUnresolvedCommercialQuestion(candidate = {}) {
  const gaps = []
    .concat(candidate.researchGaps || [])
    .concat(candidate.uncertaintyRegister || [])
    .map(text)
    .filter(Boolean);
  const contacts =
    candidate.organizerContactCandidates || candidate.namedContacts || [];
  const named = contacts.filter((c) => text(c?.name));
  const demand = text(
    candidate.demandMechanismSummary ||
      candidate.hotelDemandThesis ||
      candidate.evidenceOfHospitalityDemand ||
      "",
  );
  const fit = text(candidate.preliminaryHotelFit || "");

  if (!named.length) {
    return {
      question:
        "Who is the named, commercially relevant buyer or planner for this demand?",
      gapType: "CONTACT_GAP",
      recoverable: true,
      recommendedEffort: "high",
      quotedUsd: EXA_HIGH_USD,
    };
  }
  if (!demand || demand.length < 24) {
    return {
      question:
        "What specific hotel revenue mechanism (rooms, meetings, catering, buyout) is evidenced?",
      gapType: "DEMAND_MECHANISM_GAP",
      recoverable: true,
      recommendedEffort: "medium",
      quotedUsd: EXA_MEDIUM_USD,
    };
  }
  if (!fit || /generic|unclear|unknown/i.test(fit)) {
    return {
      question:
        "Does this demand fit this specific property's product, geography, and capacity?",
      gapType: "HOTEL_FIT_GAP",
      recoverable: true,
      recommendedEffort: "medium",
      quotedUsd: EXA_MEDIUM_USD,
    };
  }
  if (gaps.length) {
    return {
      question: gaps[0],
      gapType: "STATED_RESEARCH_GAP",
      recoverable: true,
      recommendedEffort: "medium",
      quotedUsd: EXA_MEDIUM_USD,
    };
  }
  if (
    !/202[6-9]|2030|upcoming|next edition|confirmed/i.test(
      text(candidate.verifiedDates || candidate.futureEditionDate || ""),
    )
  ) {
    return {
      question: "Are there confirmed upcoming dates or a recurring programme cycle?",
      gapType: "TIMING_GAP",
      recoverable: true,
      recommendedEffort: "medium",
      quotedUsd: EXA_MEDIUM_USD,
    };
  }
  return {
    question:
      "Can remaining evidence strengthen commercial accessibility for sales action?",
    gapType: "ACCESSIBILITY_GAP",
    recoverable: true,
    recommendedEffort: "medium",
    quotedUsd: EXA_MEDIUM_USD,
  };
}

/**
 * Plan one adaptive continuation step (planning only — no spend).
 */
export function planAdaptiveContinuationStep(candidate = {}, ctx = {}) {
  const eligibility = assessContinuationEligibility(candidate, ctx);
  const scored = scoreDeepResearchCandidate(candidate, ctx);
  const unresolved = identifyUnresolvedCommercialQuestion(candidate);
  const priorStrategies = new Set(
    (ctx.priorStrategies || candidate.priorResearchStrategies || []).map(String),
  );
  const strategyKey = `${unresolved.gapType}:${unresolved.recommendedEffort}`;
  if (priorStrategies.has(strategyKey)) {
    return {
      version: ADAPTIVE_DEEP_RESEARCH_CONTINUATION_V1,
      authorizeSpend: false,
      proceed: false,
      stopCondition: CONTINUATION_STOP_CONDITION.STRATEGIES_EXHAUSTED,
      note: "Identical strategy already attempted — do not repeat the same search.",
      eligibility,
      scored,
      unresolved,
    };
  }
  if (!eligibility.eligible) {
    return {
      version: ADAPTIVE_DEEP_RESEARCH_CONTINUATION_V1,
      authorizeSpend: false,
      proceed: false,
      stopCondition: CONTINUATION_STOP_CONDITION.MARGINAL_VALUE_TOO_LOW,
      eligibility,
      scored,
      unresolved,
    };
  }

  const perCandidateCeiling = Number(ctx.perCandidateCeilingUsd ?? 1.1);
  const spentOnCandidate = Number(ctx.spentOnCandidateUsd || 0);
  if (spentOnCandidate + unresolved.quotedUsd > perCandidateCeiling + 1e-9) {
    return {
      version: ADAPTIVE_DEEP_RESEARCH_CONTINUATION_V1,
      authorizeSpend: false,
      proceed: false,
      stopCondition: CONTINUATION_STOP_CONDITION.PER_CANDIDATE_CEILING,
      eligibility,
      scored,
      unresolved,
      perCandidateCeilingUsd: perCandidateCeiling,
      spentOnCandidateUsd: spentOnCandidate,
    };
  }

  const campaignRemaining = Number(ctx.campaignRemainingUsd ?? Infinity);
  if (unresolved.quotedUsd > campaignRemaining + 1e-9) {
    return {
      version: ADAPTIVE_DEEP_RESEARCH_CONTINUATION_V1,
      authorizeSpend: false,
      proceed: false,
      stopCondition: CONTINUATION_STOP_CONDITION.AGGREGATE_CAMPAIGN_CEILING,
      eligibility,
      scored,
      unresolved,
    };
  }

  const authorized = ctx.paidResearchAuthorized === true;
  return {
    version: ADAPTIVE_DEEP_RESEARCH_CONTINUATION_V1,
    authorizeSpend: authorized,
    proceed: authorized,
    stopCondition: authorized
      ? null
      : CONTINUATION_STOP_CONDITION.AWAITING_PAID_AUTHORIZATION,
    provider: "exa",
    effort: unresolved.recommendedEffort,
    quotedUsd: unresolved.quotedUsd,
    strategyKey,
    commercialQuestion: unresolved.question,
    gapType: unresolved.gapType,
    eligibility,
    scored,
    unresolved,
    note: authorized
      ? "Authorized — execute once, evaluate evidence, then reassess continuation."
      : "Planning only — founder GO required before paid execution.",
  };
}

/**
 * Build ranked continuation queue from undeepened / partial candidates.
 */
/**
 * Drop queue rows whose account family is already customer-facing, founder-blocked
 * internal-only, or previously promoted — prevents re-spend on resolved inventory.
 * Hotel-generic; org keys are normalized tokens only (no hotel-specific hardcoding).
 */
export function filterContinuationQueueAgainstResolvedInventory(
  queue = [],
  opts = {},
) {
  const facingOrgKeys = new Set(
    [...(opts.facingOrgKeys || [])]
      .map((k) => text(k).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim())
      .filter(Boolean),
  );
  const blockedIds = new Set(
    [...(opts.blockedOpportunityIds || [])].map(String).filter(Boolean),
  );
  const blockedOrgKeys = new Set(
    [...(opts.blockedOrgKeys || [])]
      .map((k) => text(k).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim())
      .filter(Boolean),
  );
  const out = [];
  const excluded = [];
  for (const row of queue || []) {
    const id = String(row.opportunityId || row.id || "");
    const org = text(row.organization || row.organizationName)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    let reason = null;
    if (id && blockedIds.has(id)) reason = "BLOCKED_INTERNAL_ONLY_ID";
    else if (org && blockedOrgKeys.has(org)) reason = "BLOCKED_INTERNAL_ONLY_ORG";
    else if (
      org &&
      [...blockedOrgKeys].some(
        (bk) =>
          bk.length >= 4 &&
          (org === bk ||
            org.startsWith(`${bk} `) ||
            org.includes(` ${bk} `) ||
            org.includes(bk)),
      )
    ) {
      reason = "BLOCKED_INTERNAL_ONLY_ORG_FAMILY";
    } else if (org && facingOrgKeys.has(org)) reason = "ALREADY_CUSTOMER_FACING_ORG";
    else if (
      org &&
      [...facingOrgKeys].some(
        (fk) =>
          fk.length >= 6 &&
          (org.includes(fk) || fk.includes(org) || org.startsWith(fk.slice(0, 12))),
      )
    ) {
      reason = "ALREADY_CUSTOMER_FACING_ORG_FAMILY";
    }
    if (reason) excluded.push({ opportunityId: id || null, organization: org || null, reason });
    else out.push(row);
  }
  return {
    version: ADAPTIVE_DEEP_RESEARCH_CONTINUATION_V1,
    queue: out.map((r, i) => ({ ...r, rank: i + 1 })),
    excluded,
    excludedCount: excluded.length,
  };
}

export function buildRankedContinuationQueue(candidates = [], ctx = {}) {
  const perCandidateMax = Number(ctx.perCandidateCeilingUsd ?? 1.1);
  const rows = [];
  for (const c of candidates || []) {
    const eligibility = assessContinuationEligibility(c, ctx);
    if (!eligibility.eligible) continue;
    const unresolved = identifyUnresolvedCommercialQuestion(c);
    const scored = scoreDeepResearchCandidate(c, ctx);
    const contactGap = !(c.organizerContactCandidates || c.namedContacts || []).some(
      (x) => text(x?.name),
    );
    const firstPass = unresolved.quotedUsd;
    const conditionalDeeper =
      unresolved.gapType === "CONTACT_GAP" ? 0 : EXA_HIGH_USD;
    const contactRecovery = contactGap ? EXA_HIGH_USD : 0;
    const estimatedCost = Math.min(
      perCandidateMax,
      Number((firstPass + Math.min(conditionalDeeper, contactRecovery || conditionalDeeper)).toFixed(2)),
    );
    rows.push({
      opportunityId:
        c.canonicalId ||
        c.id ||
        mintDiscoveryCandidateId({
          hotelKey: ctx.hotelKey,
          workflowId: c.workflowId,
          organizationName: c.organizationName || c.name,
          programmeName: c.programmeName,
          activityDate: c.activityDate || c.verifiedDates,
        }),
      organization: text(c.organizationName || c.name) || null,
      demandMechanism: text(
        c.demandMechanismSummary || c.hotelDemandThesis || "",
      ).slice(0, 240),
      hotelFit: text(c.preliminaryHotelFit || "").slice(0, 200),
      existingEvidence: (c.supportingSources || c.evidenceUrls || [])
        .slice(0, 3)
        .map((s) => (typeof s === "string" ? s : s?.url || s?.sourceUrl))
        .filter(Boolean),
      missingEvidence: unresolved.question,
      contactGap,
      commercialQuestion: unresolved.question,
      recommendedProvider: "exa",
      estimatedCostUsd: firstPass,
      maximumResearchCostUsd: perCandidateMax,
      expectedCommercialValue:
        scored.score >= 60
          ? "HIGH"
          : scored.score >= 48
            ? "MEDIUM_HIGH"
            : "MEDIUM",
      stopCondition: CONTINUATION_STOP_CONDITION.AWAITING_PAID_AUTHORIZATION,
      score: scored.score,
      highPriority: eligibility.highPriority === true,
      workflowId: c.workflowId || null,
      reasons: scored.reasons,
      gapType: unresolved.gapType,
      estimatedContinuationCostUsd: estimatedCost,
    });
  }
  rows.sort((a, b) => (b.score || 0) - (a.score || 0));
  return rows.map((r, i) => ({ rank: i + 1, ...r }));
}

/**
 * Classify allocation outcome — caps must not imply COMPLETE.
 */
export function classifyAllocationCompleteness(allocation = {}, opts = {}) {
  const selected = allocation.selected || [];
  const deferred = allocation.deferred || [];
  const capDeferred = deferred.filter((d) =>
    /workflow_cap|global_cap/i.test(String(d.disposition || "")),
  );
  const eligibleUninvestigated = deferred.filter((d) => {
    const el = assessContinuationEligibility(d, opts);
    return el.eligible && el.highPriority;
  });
  const status = classifyWorkflowResearchCompleteness({
    uniqueCandidates: Number(opts.uniqueCandidates ?? selected.length + deferred.length),
    deepAttempted: selected.length,
    deepCompleted: selected.length,
    eligibleUninvestigatedHighPriority: eligibleUninvestigated.length,
    candidateCapHit: capDeferred.length > 0,
    budgetCapHit: opts.budgetCapHit === true,
    stopReason: opts.stopReason || (capDeferred.length ? "CANDIDATE_CAP" : ""),
    budgetRemainingUsd: opts.budgetRemainingUsd,
  });
  return {
    ...status,
    preserveResearchQueue: status.preserveResearchQueue === true,
    unfinishedHighPriorityIds: eligibleUninvestigated
      .map((d) => d.canonicalId || d.candidateKey || d.name)
      .filter(Boolean)
      .slice(0, 80),
    deferredCount: deferred.length,
    capDeferredCount: capDeferred.length,
    forbiddenCompleteUnderCap:
      status.status === RESEARCH_COMPLETENESS_STATUS.COMPLETE &&
      eligibleUninvestigated.length > 0,
  };
}

export {
  RESEARCH_COMPLETENESS_STATUS,
  classifyWorkflowResearchCompleteness,
};
