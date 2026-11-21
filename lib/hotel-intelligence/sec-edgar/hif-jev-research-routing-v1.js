/**
 * HIF V5 — Jev research escalation router (reuses GDI TypeSafe System One).
 *
 * Jev chooses research *direction* only. It must never:
 * - write contractual facts
 * - declare verification gates satisfied
 * - authorize HPC creates/updates
 * - publish customer intelligence
 *
 * Default mode: shadow (compare to deterministic; do not change production routing
 * until founder-approved measured benefit).
 */

import {
  decide,
  describeJevConfig,
  isJevEnabled,
  isJevShadowMode,
  JEV_DECISION_TYPE,
  CHOICES,
  HARD_GATES,
  sanitizeJevState,
} from "../../group-demand-intelligence/jev/index.js";

export const HIF_JEV_RESEARCH_ROUTING_V1 = "hif-jev-research-routing-v1";

export const HIF_RESEARCH_ACTION = Object.freeze({
  ACCEPT_EXISTING_EVIDENCE: "ACCEPT_EXISTING_EVIDENCE",
  SEARCH_ADDITIONAL_SOURCES: "SEARCH_ADDITIONAL_SOURCES",
  INVESTIGATE_WITH_EXA_AGENT: "INVESTIGATE_WITH_EXA_AGENT",
  VERIFY_CONTRACT_DATE: "VERIFY_CONTRACT_DATE",
  VERIFY_RENEWAL_STATUS: "VERIFY_RENEWAL_STATUS",
  VERIFY_OWNER_OPERATOR: "VERIFY_OWNER_OPERATOR",
  HOLD_INSUFFICIENT_EVIDENCE: "HOLD_INSUFFICIENT_EVIDENCE",
  STOP_LOW_VALUE_RESEARCH: "STOP_LOW_VALUE_RESEARCH",
});

export const HIF_JEV_FORBIDDEN = Object.freeze([
  "write_contract_facts",
  "declare_verification_gates",
  "authorize_hpc_changes",
  "publish_customer_intelligence",
  "invent_renewals_or_end_dates",
]);

const LOW_VALUE_NAME =
  /\b(rv\s*resort|campground|casino|bellagio|portfolio|trs subsidiary|unnamed)\b/i;

/**
 * Deterministic HIF escalation (baseline). No provider spend.
 */
export function decideHifResearchEscalationDeterministic(caseRow = {}) {
  const hotel = String(caseRow.hotel || caseRow.hotelName || "").trim();
  const cohort = String(caseRow.cohort || "").toUpperCase();
  const baseline = caseRow.searchBaseline || {};
  const hasEnd = Boolean(baseline.contractEndDate || caseRow.verifiedEndDate);
  const hasNotice =
    baseline.noticePeriodDays != null || caseRow.noticePeriodDays != null;
  const statusVerified = Boolean(baseline.currentStatusVerified);
  const termsVerified = Boolean(baseline.contractTermsVerified);
  const existenceVerified = Boolean(baseline.agreementExistenceVerified);
  const ownerGap = !(baseline.owner || caseRow.owner) || !(baseline.managementCompany || caseRow.managementCompany);
  const statusUnconfirmed = /UNCONFIRMED|EXPIRED_STATUS|HISTORICAL|STATUS_UNCONFIRMED|NOT_DISCLOSED/i.test(
    String(baseline.currentStatus || caseRow.renewalStatus || caseRow.endDateStatus || ""),
  );
  const agentBudgetRemaining = Number(caseRow.agentBudgetRemainingUsd);
  const searchExhausted = Boolean(caseRow.searchExhausted);

  if (!hotel) {
    return {
      action: HIF_RESEARCH_ACTION.STOP_LOW_VALUE_RESEARCH,
      source: "DETERMINISTIC",
      rationale: "missing_hotel_identity",
      acceptedByPolicy: true,
    };
  }
  if (LOW_VALUE_NAME.test(hotel) || caseRow.lowValue === true) {
    return {
      action: HIF_RESEARCH_ACTION.STOP_LOW_VALUE_RESEARCH,
      source: "DETERMINISTIC",
      rationale: "low_value_or_non_target_property_pattern",
      acceptedByPolicy: true,
    };
  }
  if (statusVerified && termsVerified && existenceVerified) {
    return {
      action: HIF_RESEARCH_ACTION.ACCEPT_EXISTING_EVIDENCE,
      source: "DETERMINISTIC",
      rationale: "gates_already_satisfied_on_baseline",
      acceptedByPolicy: true,
    };
  }
  if (Number.isFinite(agentBudgetRemaining) && agentBudgetRemaining <= 0) {
    return {
      action: HIF_RESEARCH_ACTION.STOP_LOW_VALUE_RESEARCH,
      source: "DETERMINISTIC",
      rationale: "agent_budget_exhausted",
      acceptedByPolicy: true,
    };
  }

  // Cohort-aware commercial prioritization
  if (cohort === "A" || (hasEnd && statusUnconfirmed && !statusVerified)) {
    return {
      action: HIF_RESEARCH_ACTION.INVESTIGATE_WITH_EXA_AGENT,
      source: "DETERMINISTIC",
      rationale: "historical_end_date_unresolved_current_status",
      acceptedByPolicy: true,
      preferredAgentQuestion: "VERIFY_RENEWAL_STATUS",
    };
  }
  if (cohort === "B" || !hasEnd || !hasNotice) {
    if (!hasEnd) {
      return {
        action: searchExhausted
          ? HIF_RESEARCH_ACTION.INVESTIGATE_WITH_EXA_AGENT
          : HIF_RESEARCH_ACTION.VERIFY_CONTRACT_DATE,
        source: "DETERMINISTIC",
        rationale: searchExhausted
          ? "missing_end_date_after_search_exhausted"
          : "missing_expiration_prefer_verify_date_before_agent",
        acceptedByPolicy: true,
      };
    }
    if (!hasNotice) {
      return {
        action: searchExhausted
          ? HIF_RESEARCH_ACTION.INVESTIGATE_WITH_EXA_AGENT
          : HIF_RESEARCH_ACTION.SEARCH_ADDITIONAL_SOURCES,
        source: "DETERMINISTIC",
        rationale: searchExhausted
          ? "missing_notice_after_search_exhausted"
          : "missing_notice_search_first",
        acceptedByPolicy: true,
      };
    }
  }
  if (cohort === "C" || ownerGap) {
    return {
      action: HIF_RESEARCH_ACTION.VERIFY_OWNER_OPERATOR,
      source: "DETERMINISTIC",
      rationale: "ownership_or_management_gap",
      acceptedByPolicy: true,
    };
  }
  if (searchExhausted) {
    return {
      action: HIF_RESEARCH_ACTION.HOLD_INSUFFICIENT_EVIDENCE,
      source: "DETERMINISTIC",
      rationale: "search_exhausted_no_clear_escalation",
      acceptedByPolicy: true,
    };
  }
  return {
    action: HIF_RESEARCH_ACTION.SEARCH_ADDITIONAL_SOURCES,
    source: "DETERMINISTIC",
    rationale: "default_cheap_search_before_agent",
    acceptedByPolicy: true,
  };
}

function buildHifJevContext(caseRow = {}, deterministic = {}) {
  const baseline = caseRow.searchBaseline || {};
  return {
    // No raw hotel marketing blobs — sanitize will also drop names if present
    caseId: caseRow.caseId || null,
    cohort: caseRow.cohort || null,
    agreementType: caseRow.agreementType || null,
    prospectList: caseRow.prospectList || null,
    hasEndDate: Boolean(baseline.contractEndDate || caseRow.verifiedEndDate),
    hasNoticePeriod: baseline.noticePeriodDays != null || caseRow.noticePeriodDays != null,
    statusUnconfirmed: /UNCONFIRMED|EXPIRED|HISTORICAL|NOT_DISCLOSED/i.test(
      String(baseline.currentStatus || caseRow.renewalStatus || ""),
    ),
    existenceVerified: Boolean(baseline.agreementExistenceVerified),
    termsVerified: Boolean(baseline.contractTermsVerified),
    currentStatusVerified: Boolean(baseline.currentStatusVerified),
    ownerPresent: Boolean(baseline.owner || caseRow.owner),
    managementPresent: Boolean(baseline.managementCompany || caseRow.managementCompany),
    searchExhausted: Boolean(caseRow.searchExhausted),
    agentBudgetRemainingUsd:
      caseRow.agentBudgetRemainingUsd != null ? Number(caseRow.agentBudgetRemainingUsd) : null,
    lowValuePattern: LOW_VALUE_NAME.test(String(caseRow.hotel || caseRow.hotelName || "")),
    deterministicAction: deterministic.action || null,
    deterministicRationale: deterministic.rationale || null,
    evidenceSummary: String(caseRow.researchQuestion || deterministic.rationale || "").slice(0, 400),
    publicationState: "CUSTOMER_BLOCKED",
    hpcMutationAllowed: false,
    factWriteAllowed: false,
  };
}

/**
 * Jev-assisted escalation. Reuses GDI `decide()` + System One client.
 * Shadow by default: finalAction mirrors deterministic unless forceApplyRouting.
 */
export async function decideHifResearchEscalation(caseRow = {}, opts = {}) {
  const deterministic = decideHifResearchEscalationDeterministic(caseRow);
  const shadow = opts.forceApplyRouting === true ? false : opts.forceShadow != null ? Boolean(opts.forceShadow) : isJevShadowMode();
  const decisionType = JEV_DECISION_TYPE.HIF_CONTRACT_RESEARCH_ESCALATION;

  // Do NOT put fact-write forbids into Jev hardGates — that would null every routing
  // recommendation. Enforce write/HPC/publication forbids in this HIF wrapper instead.
  const policyContext = {
    hpcMutationAllowed: false,
    customerPublicationAllowed: false,
    factWriteAllowed: false,
  };

  const context = buildHifJevContext(caseRow, deterministic);

  // Hard stop: never escalate Agent for low-value patterns even if Jev disagrees in apply mode
  if (deterministic.action === HIF_RESEARCH_ACTION.STOP_LOW_VALUE_RESEARCH && deterministic.rationale === "low_value_or_non_target_property_pattern") {
    return {
      version: HIF_JEV_RESEARCH_ROUTING_V1,
      caseId: caseRow.caseId || null,
      deterministic,
      jev: null,
      finalAction: deterministic.action,
      source: "DETERMINISTIC_HARD_STOP",
      shadow,
      forbidden: [...HIF_JEV_FORBIDDEN],
      notes: ["Jev not invoked — low-value hard stop"],
    };
  }

  if (opts.deterministicOnly || !isJevEnabled()) {
    return {
      version: HIF_JEV_RESEARCH_ROUTING_V1,
      caseId: caseRow.caseId || null,
      deterministic,
      jev: null,
      finalAction: deterministic.action,
      source: "DETERMINISTIC",
      shadow,
      forbidden: [...HIF_JEV_FORBIDDEN],
      jevConfig: describeJevConfig(),
    };
  }

  const jev = await decide({
    decisionType,
    context,
    existingDecision: deterministic.action,
    policyContext,
    runContext: { runId: opts.runId || "hif-jev-routing" },
    forceShadow: shadow,
    choices: CHOICES[decisionType],
  });

  // Shadow default: finalAction = deterministic. Apply routing only with explicit founder flag.
  let finalAction = deterministic.action;
  let source = shadow ? "DETERMINISTIC_SHADOW" : "DETERMINISTIC";
  const jevChoiceValid =
    jev?.selected &&
    CHOICES[decisionType].includes(jev.selected) &&
    typeof jev.confidence === "number" &&
    jev.confidence >= 0.45 &&
    !jev.technicalFallback;

  if (opts.forceApplyRouting === true && jevChoiceValid) {
    finalAction = jev.selected;
    source = "JEV_ROUTING";
  }

  return {
    version: HIF_JEV_RESEARCH_ROUTING_V1,
    caseId: caseRow.caseId || null,
    deterministic,
    jev: {
      selected: jev.selected,
      confidence: jev.confidence,
      costUsd: jev.costUsd || 0,
      fallbackUsed: jev.fallbackUsed,
      technicalFallback: jev.technicalFallback,
      policyFallback: jev.policyFallback,
      fallbackCause: jev.fallbackCause,
      matchExisting: jev.matchExisting,
      decisionId: jev.decisionId,
      latencyMs: jev.latencyMs,
      error: jev.error,
      policyOutcome: jev.policy?.policyOutcome || null,
    },
    finalAction,
    source,
    shadow,
    forbidden: [...HIF_JEV_FORBIDDEN],
    hardForbids: [
      HARD_GATES.HIF_CUSTOMER_PUBLICATION_FORBIDDEN,
      HARD_GATES.HIF_HPC_MUTATION_FORBIDDEN,
      HARD_GATES.HIF_FACT_WRITE_FORBIDDEN,
    ],
    agreementWithDeterministic: jev?.selected === deterministic.action,
  };
}

export function mapActionToFactoryStage(action) {
  switch (action) {
    case HIF_RESEARCH_ACTION.INVESTIGATE_WITH_EXA_AGENT:
      return "EXA_AGENT_DEEP_VERIFICATION";
    case HIF_RESEARCH_ACTION.SEARCH_ADDITIONAL_SOURCES:
    case HIF_RESEARCH_ACTION.VERIFY_CONTRACT_DATE:
    case HIF_RESEARCH_ACTION.VERIFY_RENEWAL_STATUS:
      return "ENRICH_CONTRACT_TERMS";
    case HIF_RESEARCH_ACTION.VERIFY_OWNER_OPERATOR:
      return "ENRICH_CONTRACT_TERMS";
    case HIF_RESEARCH_ACTION.ACCEPT_EXISTING_EVIDENCE:
      return "RECONCILE";
    case HIF_RESEARCH_ACTION.HOLD_INSUFFICIENT_EVIDENCE:
    case HIF_RESEARCH_ACTION.STOP_LOW_VALUE_RESEARCH:
    default:
      return "REPORT";
  }
}

export function describeHifJevRoutingContract() {
  return {
    version: HIF_JEV_RESEARCH_ROUTING_V1,
    decisionType: JEV_DECISION_TYPE.HIF_CONTRACT_RESEARCH_ESCALATION,
    choices: CHOICES[JEV_DECISION_TYPE.HIF_CONTRACT_RESEARCH_ESCALATION],
    reuses: [
      "lib/group-demand-intelligence/jev/jev-client.js",
      "lib/group-demand-intelligence/jev/jev-decision-service.js",
      "lib/group-demand-intelligence/jev/jev-config.js",
    ],
    forbidden: [...HIF_JEV_FORBIDDEN],
    defaultMode: "shadow",
    jevConfig: describeJevConfig(),
    sanitizePreview: sanitizeJevState({
      hotelName: "Example Hotel",
      evidenceSummary: "test",
      webpageHtml: "<html>",
    }),
  };
}

export default {
  decideHifResearchEscalationDeterministic,
  decideHifResearchEscalation,
  mapActionToFactoryStage,
  HIF_RESEARCH_ACTION,
  HIF_JEV_FORBIDDEN,
};
