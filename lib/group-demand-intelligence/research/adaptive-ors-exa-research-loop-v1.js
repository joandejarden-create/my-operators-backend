/**
 * Adaptive ORS ↔ Exa research loop V1.
 *
 * Coordinates existing ORS/search strategies with Exa Agent without creating
 * a competing supervisor. Zero-paid by default; live Agent still dual-gated.
 */
import {
  selectExaAgentStrategy,
  EXA_AGENT_RESEARCH_STRATEGIES,
} from "./exa-agent/exa-agent-ors-strategy-v1.js";
import { selectNextAdaptedResearchStep } from "./research-strategy-adaptation-v1.js";
import { RESEARCH_STRATEGIES } from "./research-action-contract-v1.js";
import { recommendAdaptiveResearchPlan, RESEARCH_STOP_STATE } from "./adaptive-research-budget-v1.js";
import { buildAuthoritativeHotelResearchContext } from "./authoritative-hotel-research-context-v1.js";
import { evaluateContactIntelligenceFromDossier } from "./scaling-contact-intelligence-v1.js";
import {
  prioritizeUnresolvedResearchTasks,
  scoreCommercialResearchQuestion,
} from "./commercial-research-priority-v1.js";

export const ADAPTIVE_ORS_EXA_RESEARCH_LOOP_V1 =
  "gdi-adaptive-ors-exa-research-loop-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

const SIMPLE_QUESTION_RE =
  /verify (an? )?(office )?address|confirm (company |legal )?name|press[- ]?release date|official website|contact form|title of |current title|registration name|published date/i;

const COMPLEX_QUESTION_RE =
  /travel.?sourcing|travel.?management|buyer|procurement|multi[- ]?market|project.?team|chronology|decision.?maker|secondary contact|hotel.?demand|compete for|sourcing structure|tmc\b|preferred.?hotel/i;

/**
 * Classify whether a commercial question is simple (targeted search) or complex (Agent).
 */
export function classifyResearchQuestionComplexity(question = "") {
  const q = text(question);
  if (!q) return { complexity: "unknown", prefer: "ORS_ASSESS" };
  if (SIMPLE_QUESTION_RE.test(q) && !COMPLEX_QUESTION_RE.test(q)) {
    return { complexity: "simple", prefer: "ORS_SEARCH_OR_CONTENTS" };
  }
  if (COMPLEX_QUESTION_RE.test(q)) {
    return { complexity: "complex", prefer: "EXA_AGENT_DEEP_OR_DISCOVERY" };
  }
  return { complexity: "mixed", prefer: "ORS_FIRST_THEN_ESCALATE" };
}

/**
 * Build a mandatory selection record for the next investigation.
 */
export function buildResearchSelectionRecord({
  opportunity = {},
  hotel = {},
  remainingAuthorizedUsd = 0,
  authorizeExaAgentLive = false,
  contactPack = null,
} = {}) {
  const hotelCtx = buildAuthoritativeHotelResearchContext(hotel);
  const adapted = selectNextAdaptedResearchStep(opportunity, hotel);
  const prioritizedTask = prioritizeUnresolvedResearchTasks(
    opportunity.unresolvedResearchTasks || [],
  );
  const question =
    text(adapted.question) ||
    text(adapted.researchAction?.question) ||
    text(prioritizedTask?.question) ||
    text(opportunity.unresolvedResearchTasks?.[0]?.question) ||
    text(contactPack?.contactResearchGaps?.[0]?.question) ||
    "Identify highest-value unanswered commercial question";

  const taskQuestion =
    text(prioritizedTask?.question) ||
    text(opportunity.unresolvedResearchTasks?.[0]?.question);
  const commercialPriority = scoreCommercialResearchQuestion(taskQuestion || question);
  const complexity = classifyResearchQuestionComplexity(question);
  const taskComplexity = classifyResearchQuestionComplexity(taskQuestion || question);
  const effectiveComplexity =
    taskComplexity.complexity === "complex" ? taskComplexity : complexity;
  const exaSel = selectExaAgentStrategy({
    gap: {
      commercialQuestion: taskQuestion || question,
      drawerSection: "who_to_contact",
    },
    opportunity,
    authorizeExaAgentLive,
    remainingBudgetUsd: remainingAuthorizedUsd,
    priorExaSearchAttempts: Number(opportunity.priorExaSearchAttempts || 0),
    priorAgentAttempts: Number(opportunity.priorAgentAttempts || 0),
  });

  let selectedStrategy = adapted.strategy || RESEARCH_STRATEGIES.PRIMARY_SOURCE_RECOVERY;
  let reason = adapted.note || "ors_adapted_strategy";
  let providerFamily = "ORS_SEARCH";

  if (effectiveComplexity.complexity === "simple") {
    selectedStrategy =
      adapted.strategy && adapted.strategy !== RESEARCH_STRATEGIES.SCHEDULE_FUTURE_REFRESH
        ? adapted.strategy
        : RESEARCH_STRATEGIES.PRIMARY_SOURCE_RECOVERY;
    reason = "simple_verification_prefers_targeted_ors_search";
    providerFamily = "ORS_SEARCH";
  } else if (
    effectiveComplexity.complexity === "complex" &&
    exaSel.useExaAgent &&
    Number(remainingAuthorizedUsd) > 0
  ) {
    selectedStrategy = exaSel.strategyId;
    reason = `complex_gap_escalates_to_exa_agent:${exaSel.reason}`;
    providerFamily = "EXA_AGENT";
  } else if (
    effectiveComplexity.complexity === "complex" &&
    Number(remainingAuthorizedUsd) <= 0
  ) {
    selectedStrategy = adapted.strategy || RESEARCH_STRATEGIES.SCHEDULE_FUTURE_REFRESH;
    reason = "complex_gap_justifies_agent_but_budget_paused";
    providerFamily = "BUDGET_PAUSED";
  } else if (effectiveComplexity.complexity === "mixed") {
    if (Number(remainingAuthorizedUsd) <= 0 && /hotel.?demand|travel|buyer|sourcing/i.test(taskQuestion || question)) {
      selectedStrategy = adapted.strategy || RESEARCH_STRATEGIES.SCHEDULE_FUTURE_REFRESH;
      reason = "commercially_important_mixed_gap_budget_paused";
      providerFamily = "BUDGET_PAUSED";
    } else {
      selectedStrategy =
        adapted.strategy || RESEARCH_STRATEGIES.INDEPENDENT_PRESS_CORROBORATION;
      reason = "mixed_gap_ors_first_then_escalate_if_needed";
      providerFamily = "ORS_FIRST";
    }
  }

  const plan = recommendAdaptiveResearchPlan({
    disposition: opportunity.exaAgentResearchDisposition || opportunity.researchDisposition,
    contactPack,
    unresolvedTasks: [
      { question: taskQuestion || question, priority: "P0" },
      ...(opportunity.unresolvedResearchTasks || []),
    ],
    hotelContextStatus: hotelCtx.status,
    remainingAuthorizedUsd,
    spentUsd: Number(opportunity.stageResearchSpentUsd || 0),
    commercialPotentialScore: 0.55,
  });

  const stopCriteria =
    providerFamily === "BUDGET_PAUSED"
      ? RESEARCH_STOP_STATE.BUDGET_PAUSED
      : plan.stopState;

  return {
    version: ADAPTIVE_ORS_EXA_RESEARCH_LOOP_V1,
    researchQuestion: taskQuestion || question,
    commercialImportance: "material_to_qualification_or_sales_action",
    selectedStrategy,
    providerFamily,
    reasonForChoosingStrategy: reason,
    complexity: effectiveComplexity,
    commercialPriority,
    commercialPriorityBucket: commercialPriority.bucket,
    mapsToPrioritySections: commercialPriority.mapsToPrioritySections,
    existingEvidenceConsidered: {
      hasCib: Boolean(opportunity.commercialIntelligenceBrief),
      sourceCount: Array.isArray(opportunity.sources) ? opportunity.sources.length : 0,
      namedContact: Boolean(opportunity.primaryContact?.name || opportunity.contactName),
      hotelContextStatus: hotelCtx.status,
    },
    expectedResult: adapted.researchAction?.evidenceSought || "verified_fact_or_honest_unknown",
    estimatedCostUsd:
      providerFamily === "EXA_AGENT"
        ? exaSel.suggestedEffort === "high"
          ? 0.5
          : 0.1
        : providerFamily === "BUDGET_PAUSED"
          ? 0.1
          : 0,
    authorizedSpendingCapacityUsd: Number(remainingAuthorizedUsd) || 0,
    stopCriteria,
    adaptivePlan: plan,
    orsAdaptation: {
      executable: adapted.executable,
      strategy: adapted.strategy,
      note: adapted.note,
    },
    exaSelection: {
      useExaAgent: exaSel.useExaAgent,
      reason: exaSel.reason,
      strategyId: exaSel.strategyId || null,
    },
    hotelContext: {
      officialStreetAddress: hotelCtx.officialStreetAddress,
      status: hotelCtx.status,
    },
    canExecuteLive:
      providerFamily !== "BUDGET_PAUSED" &&
      (providerFamily !== "EXA_AGENT" ||
        (authorizeExaAgentLive && Number(remainingAuthorizedUsd) > 0)),
  };
}

/**
 * Evaluate an investigation result and recommend the next loop action.
 */
export function evaluateResearchStepResult({
  selectionRecord = {},
  result = {},
} = {}) {
  const resolved = result.resolved === true;
  const partial = result.partial === true;
  const contradictory = result.contradictory === true;
  const newQuestion = text(result.newCommercialQuestion);
  const failed = result.failed === true || result.ok === false;

  let nextAction = "STOP_OR_REASSESS";
  let note = "";

  if (failed) {
    nextAction =
      selectionRecord.providerFamily === "ORS_SEARCH" ||
      selectionRecord.providerFamily === "ORS_FIRST"
        ? "ESCALATE_TO_EXA_AGENT_IF_AUTHORIZED"
        : "MARK_STRATEGY_FAILED_AND_ADAPT";
    note = "investigation_failed";
  } else if (contradictory) {
    nextAction = "ORS_TARGETED_VERIFICATION";
    note = "contradiction_requires_targeted_check";
  } else if (resolved && !newQuestion) {
    nextAction = "UPDATE_SHADOW_CIB_AND_REEVALUATE";
    note = "question_resolved";
  } else if (partial || newQuestion) {
    nextAction =
      selectionRecord.providerFamily === "EXA_AGENT"
        ? "ORS_TARGETED_VERIFICATION"
        : "CONTINUE_OR_ESCALATE";
    note = newQuestion || "partial_resolution";
  } else if (selectionRecord.stopCriteria === RESEARCH_STOP_STATE.BUDGET_PAUSED) {
    nextAction = "REQUEST_FOUNDER_BUDGET";
    note = "budget_paused";
  }

  return {
    version: ADAPTIVE_ORS_EXA_RESEARCH_LOOP_V1,
    resolved,
    partial,
    contradictory,
    failed,
    newCommercialQuestion: newQuestion || null,
    nextAction,
    note,
    priorSelection: {
      strategy: selectionRecord.selectedStrategy,
      providerFamily: selectionRecord.providerFamily,
      question: selectionRecord.researchQuestion,
    },
  };
}

/**
 * Zero-paid maturation assessor for an opportunity + optional Exa dossier.
 */
export function assessOpportunityForAdaptiveLoop({
  opportunity = {},
  hotel = {},
  dossier = null,
  remainingAuthorizedUsd = 0,
} = {}) {
  const contactPack = dossier
    ? evaluateContactIntelligenceFromDossier(dossier, {
        hotelName: hotel.displayName || hotel.name,
        companyName: opportunity.organizationName,
      })
    : opportunity.scalingContactIntelligence || null;

  const selection = buildResearchSelectionRecord({
    opportunity: {
      ...opportunity,
      unresolvedResearchTasks:
        opportunity.unresolvedResearchTasks ||
        dossier?.unresolvedResearchTasks ||
        [],
      exaAgentResearchDisposition: dossier?.researchDisposition,
    },
    hotel,
    remainingAuthorizedUsd,
    authorizeExaAgentLive: false,
    contactPack,
  });

  return {
    version: ADAPTIVE_ORS_EXA_RESEARCH_LOOP_V1,
    opportunityId: opportunity.id || null,
    organizationName: opportunity.organizationName || null,
    contactPackSummary: contactPack
      ? {
          meetsMinimumOneRelevantNamed: contactPack.meetsMinimumOneRelevantNamed,
          meetsPreferredTwoDistinct: contactPack.meetsPreferredTwoDistinct,
          primary: contactPack.primaryContact?.name || null,
          secondary: contactPack.secondaryContact?.name || null,
        }
      : null,
    selection,
    liveExecutionAuthorized: false,
  };
}

export { EXA_AGENT_RESEARCH_STRATEGIES, RESEARCH_STOP_STATE };
