/**
 * Commercial research completion contract V1
 *
 * Separates:
 *   EXECUTION_FINISHED — bounded job ended
 *   RESEARCH_CONTINUATION — material unanswered questions remain
 *   RESEARCH_EXHAUSTED — strategies tried; low EV to continue now
 *   COMMERCIAL_RESEARCH_COMPLETE — defensible sales / resolved research outcome
 *
 * Publication qualification MUST NOT independently terminate worthwhile
 * commercial investigation. Customer-publication gates stay strict and separate.
 */
import { assessSixCommercialOutcomes } from "./autonomous-portfolio-ownership-v1.js";
import { DOS_GRADE } from "../commercial-sales-quality-v1.js";

export const COMMERCIAL_RESEARCH_COMPLETION_CONTRACT_V1 =
  "gdi-commercial-research-completion-contract-v1";

export const RESEARCH_COMPLETION_STATE = Object.freeze({
  EXECUTION_FINISHED: "EXECUTION_FINISHED",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  RESEARCH_EXHAUSTED: "RESEARCH_EXHAUSTED",
  COMMERCIAL_RESEARCH_COMPLETE: "COMMERCIAL_RESEARCH_COMPLETE",
});

export const DRAWER_ACCEPTANCE = Object.freeze({
  SALES_BRIEF_ACCEPTED: "SALES_BRIEF_ACCEPTED",
  RESEARCH_OUTCOME_ACCEPTED: "RESEARCH_OUTCOME_ACCEPTED",
  CONTINUATION_OPEN: "CONTINUATION_OPEN",
  BLOCKED: "BLOCKED",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

const GENERIC_FIT =
  /same city|same commercial market|in the (same )?city|located in|generic hotel|meeting rooms and|executive facilities|mexico city hotel with/i;

/**
 * Material commercial gaps that survive publication qualification.
 * These justify continued investigation even when publicationQualified=true.
 */
export function materialCommercialGaps(opportunity = {}, hotel = {}) {
  const assess = assessSixCommercialOutcomes(opportunity, hotel);
  const gaps = [...(assess.gaps || [])];
  const title = text(opportunity.title);
  const why = text(
    opportunity.whyThisHotel ||
      opportunity.hotelOpportunityThesis ||
      opportunity.associationEventsGoldBrief?.whyThisHotel ||
      opportunity.commercialIntelligenceBrief?.whyThisHotel ||
      "",
  );
  if (why.length < 60 || GENERIC_FIT.test(why)) {
    if (!gaps.includes("HOTEL_FIT_GENERIC_OR_MISSING")) gaps.push("HOTEL_FIT_GENERIC_OR_MISSING");
  }
  const sources = [
    ...(opportunity.sources || []),
    ...(opportunity.evidenceUrls || []),
  ].filter(Boolean);
  if (sources.length === 0 && !gaps.includes("NO_SOURCE_EVIDENCE")) {
    gaps.push("NO_SOURCE_EVIDENCE");
  }
  if (
    (/^Speakers\b/i.test(title) ||
      /revista magazzine|poder y finanzas|- en - cumbre|busca abrir nuevos mercados/i.test(title)) &&
    !gaps.includes("EVENT_IDENTITY_UNCLEAR")
  ) {
    gaps.push("EVENT_IDENTITY_UNCLEAR");
  }
  if (
    text(opportunity.contactRole) &&
    !text(opportunity.contactName) &&
    !gaps.includes("ORPHAN_CONTACT_ROLE")
  ) {
    gaps.push("ORPHAN_CONTACT_ROLE");
  }
  if (
    opportunity.contactName &&
    /sai\s+derecho|research\s+\w+|m[eé]dico\s*$/i.test(
      `${opportunity.contactName} ${opportunity.contactRole || ""}`,
    ) &&
    !gaps.includes("CONTACT_RELATIONSHIP_UNSUPPORTED")
  ) {
    gaps.push("CONTACT_RELATIONSHIP_UNSUPPORTED");
  }
  const unresolved = opportunity.associationEventsUnresolvedQuestions ||
    opportunity.investigationLoopUnresolved ||
    [];
  for (const q of unresolved) {
    const qText = typeof q === "string" ? q : q?.id || q?.question || "";
    if (
      /HOTEL_SOURCING|COMPETITIVE|ACCOMMODATION|CONTACT_ROUTE|NEXT_EDITION|SPEAKER_TRAVEL|IDENTITY/i.test(
        qText,
      )
    ) {
      const tag = `UNRESOLVED:${String(qText).slice(0, 80)}`;
      if (!gaps.includes(tag)) gaps.push(tag);
    }
  }
  return {
    gaps: [...new Set(gaps)],
    assess,
    sourceCount: sources.length,
  };
}

/**
 * True when publicationQualified alone would wrongly stop investigation.
 */
export function publicationMustNotStopInvestigation(opportunity = {}, hotel = {}) {
  const { gaps } = materialCommercialGaps(opportunity, hotel);
  const material = gaps.filter(
    (g) =>
      /HOTEL_FIT|DEMAND|NO_SOURCE|NO_CREDIBLE|EVENT_IDENTITY|SALES_STRATEGY|UNRESOLVED:/i.test(
        g,
      ),
  );
  return material.length > 0;
}

/**
 * Classify commercial research completion for an opportunity.
 * Does not change publicationQualified.
 */
export function assessCommercialResearchCompletion(opportunity = {}, hotel = {}, opts = {}) {
  const {
    executionStopReason = null,
    strategiesExhausted = false,
    iterationCeiling = false,
    blockedReason = null,
  } = opts;

  const { gaps, assess, sourceCount } = materialCommercialGaps(opportunity, hotel);
  const disp = opportunity.run044LifecycleDisposition || "";
  const dos = opportunity.dosGrade || null;
  const past =
    assess.past === true ||
    disp === "HISTORICAL_INTELLIGENCE" ||
    disp === "REJECTED_OR_IRRELEVANT" ||
    dos === DOS_GRADE.DO_NOT_PURSUE;
  const watchlist = disp === "FUTURE_CYCLE_WATCHLIST";
  const pub =
    opportunity.publicationQualified === true ||
    opportunity.associationEventsPublicationQualified === true ||
    /PUBLICATION_QUALIFIED/i.test(String(executionStopReason || ""));

  if (blockedReason) {
    return {
      version: COMMERCIAL_RESEARCH_COMPLETION_CONTRACT_V1,
      state: RESEARCH_COMPLETION_STATE.EXECUTION_FINISHED,
      acceptance: DRAWER_ACCEPTANCE.BLOCKED,
      publicationQualified: pub,
      commerciallyComplete: false,
      gaps,
      sourceCount,
      assess,
      reason: blockedReason,
      nextResearchAction: opportunity.run044NextAction || "Resume when blocker clears",
    };
  }

  // Resolved non-sales outcomes — contact/sales gaps do not keep historical rows open
  if (past) {
    return {
      version: COMMERCIAL_RESEARCH_COMPLETION_CONTRACT_V1,
      state: RESEARCH_COMPLETION_STATE.COMMERCIAL_RESEARCH_COMPLETE,
      acceptance: DRAWER_ACCEPTANCE.RESEARCH_OUTCOME_ACCEPTED,
      publicationQualified: false,
      commerciallyComplete: true,
      gaps,
      sourceCount,
      assess,
      reason: "historical_or_rejected_resolved_for_cycle",
      nextResearchAction: opportunity.run044NextAction || "Monitor only if series recurs",
    };
  }

  if (watchlist && sourceCount >= 1 && assess.outcomes?.opportunity?.addressed) {
    const materialOpen = gaps.filter((g) =>
      /DEMAND|NO_SOURCE|EVENT_IDENTITY|UNRESOLVED:IS_THERE_A_CONFIRMED/i.test(g),
    );
    if (materialOpen.length === 0 || strategiesExhausted) {
      return {
        version: COMMERCIAL_RESEARCH_COMPLETION_CONTRACT_V1,
        state: strategiesExhausted
          ? RESEARCH_COMPLETION_STATE.RESEARCH_EXHAUSTED
          : RESEARCH_COMPLETION_STATE.COMMERCIAL_RESEARCH_COMPLETE,
        acceptance: DRAWER_ACCEPTANCE.RESEARCH_OUTCOME_ACCEPTED,
        publicationQualified: pub,
        commerciallyComplete: !strategiesExhausted || materialOpen.length === 0,
        gaps,
        sourceCount,
        assess,
        reason: "future_cycle_watchlist_documented",
        nextResearchAction:
          opportunity.run044NextAction || "Refresh before next confirmed edition",
      };
    }
  }

  // Sales-ready commercial complete: identity + demand + non-generic fit + evidence + route/action
  const salesReady =
    assess.outcomes?.opportunity?.addressed &&
    assess.outcomes?.demand?.addressed &&
    assess.outcomes?.hotelFit?.addressed &&
    assess.outcomes?.evidence?.addressed &&
    (assess.outcomes?.people?.addressed || assess.outcomes?.salesStrategy?.addressed) &&
    sourceCount >= 1 &&
    !/^Speakers\b/i.test(text(opportunity.title));

  if (salesReady && gaps.length === 0) {
    return {
      version: COMMERCIAL_RESEARCH_COMPLETION_CONTRACT_V1,
      state: RESEARCH_COMPLETION_STATE.COMMERCIAL_RESEARCH_COMPLETE,
      acceptance: DRAWER_ACCEPTANCE.SALES_BRIEF_ACCEPTED,
      publicationQualified: pub,
      commerciallyComplete: true,
      gaps,
      sourceCount,
      assess,
      reason: "six_outcomes_addressed_with_evidence",
      nextResearchAction: opportunity.recommendedAction || "Execute assigned sales action",
    };
  }

  if (strategiesExhausted && gaps.length > 0) {
    return {
      version: COMMERCIAL_RESEARCH_COMPLETION_CONTRACT_V1,
      state: RESEARCH_COMPLETION_STATE.RESEARCH_EXHAUSTED,
      acceptance: DRAWER_ACCEPTANCE.RESEARCH_OUTCOME_ACCEPTED,
      publicationQualified: pub,
      commerciallyComplete: false,
      gaps,
      sourceCount,
      assess,
      reason: "strategies_exhausted_material_gaps_remain",
      nextResearchAction:
        gaps[0] ||
        opportunity.run044NextAction ||
        "Revisit when new public sources appear",
    };
  }

  if (gaps.length > 0) {
    return {
      version: COMMERCIAL_RESEARCH_COMPLETION_CONTRACT_V1,
      state: RESEARCH_COMPLETION_STATE.RESEARCH_CONTINUATION,
      acceptance: DRAWER_ACCEPTANCE.CONTINUATION_OPEN,
      publicationQualified: pub,
      commerciallyComplete: false,
      gaps,
      sourceCount,
      assess,
      reason: pub
        ? "publication_qualified_but_commercial_gaps_remain"
        : "commercially_material_questions_open",
      nextResearchAction: gaps[0] || opportunity.run044NextAction || "Continue FullE2E/ORS",
    };
  }

  if (iterationCeiling || executionStopReason) {
    return {
      version: COMMERCIAL_RESEARCH_COMPLETION_CONTRACT_V1,
      state: RESEARCH_COMPLETION_STATE.EXECUTION_FINISHED,
      acceptance: DRAWER_ACCEPTANCE.CONTINUATION_OPEN,
      publicationQualified: pub,
      commerciallyComplete: false,
      gaps,
      sourceCount,
      assess,
      reason: `execution_bound:${executionStopReason || "ITERATION_CEILING"}`,
      nextResearchAction: "Resume next wave — execution bound ≠ commercial complete",
    };
  }

  return {
    version: COMMERCIAL_RESEARCH_COMPLETION_CONTRACT_V1,
    state: RESEARCH_COMPLETION_STATE.RESEARCH_CONTINUATION,
    acceptance: DRAWER_ACCEPTANCE.CONTINUATION_OPEN,
    publicationQualified: pub,
    commerciallyComplete: false,
    gaps,
    sourceCount,
    assess,
    reason: "default_continuation",
    nextResearchAction: opportunity.run044NextAction || "Continue research",
  };
}

const FOUNDER_LOCKED_GRADES = new Set([
  DOS_GRADE.ASSIGN_NOW,
  DOS_GRADE.ASSIGN_FOR_DISCOVERY,
  DOS_GRADE.RESEARCH_FIRST,
  DOS_GRADE.DO_NOT_PURSUE,
]);

/**
 * Attach completion fields onto opportunity without touching publication gates.
 * Aligns DOS grade with commercial completion (does not overwrite founder reviews).
 */
export function applyCommercialResearchCompletion(opportunity = {}, hotel = {}, opts = {}) {
  const completion = assessCommercialResearchCompletion(opportunity, hotel, opts);
  const founder = opportunity.founderReview || opportunity.localFounderReview || null;
  let dosGrade = opportunity.dosGrade || null;
  const founderLocked =
    founder &&
    (founder.decision || founder.dosGrade) &&
    FOUNDER_LOCKED_GRADES.has(String(founder.decision || founder.dosGrade || ""));

  if (!founderLocked) {
    if (
      completion.acceptance === DRAWER_ACCEPTANCE.CONTINUATION_OPEN ||
      completion.state === RESEARCH_COMPLETION_STATE.RESEARCH_CONTINUATION
    ) {
      // Never present ASSIGN_NOW while material commercial research remains open
      if (dosGrade === DOS_GRADE.ASSIGN_NOW) {
        dosGrade = DOS_GRADE.RESEARCH_FIRST;
      }
      if (
        /^Speakers\b/i.test(text(opportunity.title)) ||
        completion.gaps.some((g) => /EVENT_IDENTITY/i.test(g))
      ) {
        dosGrade = DOS_GRADE.RESEARCH_FIRST;
      }
    }
    if (
      completion.acceptance === DRAWER_ACCEPTANCE.RESEARCH_OUTCOME_ACCEPTED &&
      (opportunity.run044LifecycleDisposition === "HISTORICAL_INTELLIGENCE" ||
        opportunity.run044LifecycleDisposition === "REJECTED_OR_IRRELEVANT")
    ) {
      dosGrade = DOS_GRADE.DO_NOT_PURSUE;
    }
  }

  return {
    ...opportunity,
    dosGrade,
    commercialResearchCompletionVersion: COMMERCIAL_RESEARCH_COMPLETION_CONTRACT_V1,
    commercialResearchCompletionState: completion.state,
    commercialDrawerAcceptance: completion.acceptance,
    commerciallyResearchComplete: completion.commerciallyComplete === true,
    commercialMaterialGaps: completion.gaps,
    commercialCompletionReason: completion.reason,
    commercialNextResearchAction: completion.nextResearchAction,
    // Never let publication alone imply complete
    researchContinuation:
      completion.state === RESEARCH_COMPLETION_STATE.RESEARCH_CONTINUATION ||
      completion.state === RESEARCH_COMPLETION_STATE.EXECUTION_FINISHED,
    founderReview: founder || opportunity.founderReview,
    localFounderReview: founder || opportunity.localFounderReview,
    commercialCompletionAssessedAt: new Date().toISOString(),
  };
}

/**
 * Investigation-loop helper: may we treat publication as a stop?
 */
export function mayStopForPublicationQualification(opportunity = {}, hotel = {}) {
  return !publicationMustNotStopInvestigation(opportunity, hotel);
}
