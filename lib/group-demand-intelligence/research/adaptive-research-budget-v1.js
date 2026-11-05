/**
 * Adaptive research budget recommendations V1.
 *
 * Recommends — never authorizes — incremental paid research.
 * Distinguishes research sufficiency from financial authorization.
 */
import { quoteExaAgentRun, EXA_AGENT_EFFORT } from "./exa-agent/exa-agent-provider-adapter-v1.js";

export const ADAPTIVE_RESEARCH_BUDGET_V1 = "gdi-adaptive-research-budget-v1";

export const RESEARCH_STOP_STATE = Object.freeze({
  RESEARCH_COMPLETE: "RESEARCH_COMPLETE",
  RESEARCH_EXHAUSTED: "RESEARCH_EXHAUSTED",
  BUDGET_PAUSED: "BUDGET_PAUSED",
  SCHEDULED_REFRESH: "SCHEDULED_REFRESH",
  RESEARCH_CONTINUATION_REQUIRED: "RESEARCH_CONTINUATION_REQUIRED",
});

/** Map Stage D stop concepts ↔ existing Exa / Scaling dispositions. */
export const RESEARCH_STOP_STATE_MAPPING = Object.freeze({
  [RESEARCH_STOP_STATE.RESEARCH_COMPLETE]: {
    exaDispositions: ["SUFFICIENT_FOR_CIB_DRAFT", "NOT_COMMERCIALLY_ATTRACTIVE"],
    note: "SUFFICIENT_FOR_CIB_DRAFT when commercially actionable draft is supported; NOT_COMMERCIALLY_ATTRACTIVE is a complete negative outcome",
  },
  [RESEARCH_STOP_STATE.RESEARCH_CONTINUATION_REQUIRED]: {
    exaDispositions: ["RESEARCH_CONTINUATION_REQUIRED"],
    note: "Material unanswered questions remain and further research is justified",
  },
  [RESEARCH_STOP_STATE.SCHEDULED_REFRESH]: {
    exaDispositions: ["WATCHLIST_OR_MONITOR"],
    note: "Important evidence may become available later",
  },
  [RESEARCH_STOP_STATE.RESEARCH_EXHAUSTED]: {
    exaDispositions: ["BLOCKED_IDENTITY_OR_EVIDENCE"],
    note: "Reasonable strategies exhausted or low expected value — map carefully; not identical to identity block",
  },
  [RESEARCH_STOP_STATE.BUDGET_PAUSED]: {
    exaDispositions: ["RESEARCH_CONTINUATION_REQUIRED"],
    note: "Continuation still justified but approved spend unavailable — overlay flag, do not replace disposition",
  },
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Assess one remaining research gap for adaptive allocation.
 */
export function assessResearchGap(gap = {}, ctx = {}) {
  const question = text(gap.question || gap.commercialQuestion);
  const priority = text(gap.priority || "P2");
  const importance =
    priority === "P0" ? 1 : priority === "P1" ? 0.7 : priority === "P2" ? 0.4 : 0.2;

  const touchesBuyer = /buyer|contact|authority|procurement|travel owner|approver/i.test(
    question,
  );
  const touchesDemand = /demand|room|lodging|meeting|itinerary|overnight/i.test(question);
  const touchesFit = /hotel|corridor|geography|chapultepec|fit|address/i.test(question);
  const changesAction = touchesBuyer || touchesDemand || touchesFit;

  const priorFailures = Number(ctx.priorFailedAttempts || gap.priorFailedAttempts || 0);
  const likelihood = Math.max(0.15, 0.75 - priorFailures * 0.2);

  let strategy = "EXA_SEARCH_OR_CONTENTS";
  let effort = EXA_AGENT_EFFORT.LOW;
  if (touchesBuyer && changesAction) {
    strategy = "EXA_AGENT_SCALING_DEEP_RESEARCH";
    effort = EXA_AGENT_EFFORT.MEDIUM;
  } else if (importance >= 0.7 && changesAction) {
    strategy = "EXA_AGENT_SCALING_DEEP_RESEARCH";
    effort = EXA_AGENT_EFFORT.MEDIUM;
  }

  const quote = quoteExaAgentRun({ effort });
  const estimatedUsd = quote.bounded ? quote.maxUsd : null;
  const expectedValue =
    importance * likelihood * (changesAction ? 1 : 0.4) * (ctx.commercialPotentialScore || 0.6);

  // Commercial justification is independent of remaining authorization.
  const justifyMore = expectedValue >= 0.25 && estimatedUsd != null;
  const remaining = ctx.remainingAuthorizedUsd;
  const authorizationStatus =
    remaining == null
      ? "NO_LIVE_AUTHORIZATION"
      : Number(remaining) <= 0
        ? "BUDGET_EXHAUSTED"
        : "AUTHORIZED_REMAINING";
  const affordableNow =
    remaining == null
      ? false
      : Number(remaining) + 1e-9 >= Number(estimatedUsd || 0);

  return {
    question,
    priority,
    commercialImportance: importance,
    likelihoodOfResolution: likelihood,
    expectedValueScore: Number(expectedValue.toFixed(3)),
    changesQualificationOrSalesAction: changesAction,
    suitableStrategy: strategy,
    suggestedEffort: effort,
    estimatedIncrementalUsd: estimatedUsd,
    justifyAdditionalInvestigation: justifyMore,
    affordableUnderCurrentAuthorization: affordableNow,
    authorizationStatus,
    previousFailedStrategies: gap.previousFailedStrategies || [],
  };
}

/**
 * Recommend stop state + optional budget extension request (not self-authorized).
 */
export function recommendAdaptiveResearchPlan({
  disposition = null,
  contactPack = null,
  unresolvedTasks = [],
  hotelContextStatus = null,
  commercialPotentialScore = 0.6,
  remainingAuthorizedUsd = 0,
  spentUsd = 0,
} = {}) {
  const gaps = (unresolvedTasks || []).map((t) =>
    assessResearchGap(t, {
      commercialPotentialScore,
      remainingAuthorizedUsd,
    }),
  );
  gaps.sort((a, b) => b.expectedValueScore - a.expectedValueScore);

  const hasOneContact = contactPack?.meetsMinimumOneRelevantNamed === true;
  const hasTwoContacts = contactPack?.meetsPreferredTwoDistinct === true;
  const hasAuthority = (contactPack?.primaryContact?.contractingAuthorityStatus || "") ===
    "CONTRACTING_AUTHORITY_VERIFIED";
  const demandOk = commercialPotentialScore >= 0.45;

  let stopState = RESEARCH_STOP_STATE.RESEARCH_CONTINUATION_REQUIRED;
  let rationale = "";

  if (disposition === "NOT_COMMERCIALLY_ATTRACTIVE") {
    stopState = RESEARCH_STOP_STATE.RESEARCH_COMPLETE;
    rationale = "Negative commercial outcome complete — do not spend further";
  } else if (disposition === "WATCHLIST_OR_MONITOR") {
    stopState = RESEARCH_STOP_STATE.SCHEDULED_REFRESH;
    rationale = "Monitor / refresh later";
  } else if (
    hasOneContact &&
    demandOk &&
    gaps.every((g) => g.expectedValueScore < 0.2)
  ) {
    stopState = RESEARCH_STOP_STATE.RESEARCH_EXHAUSTED;
    rationale =
      "Minimum contact + demand thesis present; remaining gaps low expected value from public research";
  } else if (
    gaps.some((g) => g.justifyAdditionalInvestigation) &&
    (remainingAuthorizedUsd == null || Number(remainingAuthorizedUsd) <= 0)
  ) {
    stopState = RESEARCH_STOP_STATE.BUDGET_PAUSED;
    rationale =
      "Further investigation justified but no approved remaining spend — founder extension required";
  } else if (disposition === "SUFFICIENT_FOR_CIB_DRAFT" && hasOneContact && demandOk) {
    stopState = RESEARCH_STOP_STATE.RESEARCH_COMPLETE;
    rationale = "Disposition sufficient with minimum contact relevance";
  } else {
    stopState = RESEARCH_STOP_STATE.RESEARCH_CONTINUATION_REQUIRED;
    rationale = "Material commercial questions remain";
  }

  if (hotelContextStatus === "HOTEL_CONTEXT_CONFLICT") {
    gaps.unshift(
      assessResearchGap(
        {
          question: "Resolve authoritative hotel street address before property-specific geography claims",
          priority: "P0",
        },
        { commercialPotentialScore: 1, remainingAuthorizedUsd },
      ),
    );
  }

  const top = gaps[0] || null;
  const proposedExtensionUsd = top?.justifyAdditionalInvestigation
    ? top.estimatedIncrementalUsd
    : null;

  return {
    version: ADAPTIVE_RESEARCH_BUDGET_V1,
    stopState,
    stopStateMapping: RESEARCH_STOP_STATE_MAPPING[stopState],
    rationale,
    dispositionPreserved: disposition,
    contactMinimumMet: hasOneContact,
    contactPreferredTwoMet: hasTwoContacts,
    contractingAuthorityVerified: hasAuthority,
    gaps,
    highestValueUnansweredQuestion: top?.question || null,
    recommendedStrategy: top?.suitableStrategy || null,
    proposedAdditionalSpendUsd: proposedExtensionUsd,
    expectedCommercialImprovement: top
      ? `Expected-value score ${top.expectedValueScore}; may ${
          top.changesQualificationOrSalesAction ? "" : "not "
        }change qualification/sales action`
      : null,
    spentUsd,
    remainingAuthorizedUsd,
    founderAuthorizationRequired:
      stopState === RESEARCH_STOP_STATE.BUDGET_PAUSED ||
      (proposedExtensionUsd != null && Number(remainingAuthorizedUsd) < proposedExtensionUsd),
    canSelfAuthorize: false,
  };
}
