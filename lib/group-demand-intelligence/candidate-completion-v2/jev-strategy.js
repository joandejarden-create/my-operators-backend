/**
 * Jev research strategy review — advisory only.
 * Jev decides WHERE / HOW FAR to look. Dealality decides WHAT is true.
 */

import { isJevEnabled } from "../jev/jev-config.js";
import { decide } from "../jev/jev-decision-service.js";
import { JEV_DECISION_TYPE } from "../jev/jev-types.js";
import { RESEARCH_DEPTH } from "./depth-policy.js";
import { GAP_STATE } from "./evidence-gap.js";

export const JEV_LOOP_ACTION = Object.freeze({
  CONTINUE: "CONTINUE",
  SWITCH_SOURCE: "SWITCH_SOURCE",
  SWITCH_LANGUAGE: "SWITCH_LANGUAGE",
  SWITCH_FEEDER_MARKET: "SWITCH_FEEDER_MARKET",
  SWITCH_BLOCKER: "SWITCH_BLOCKER",
  STOP_LOW_INFORMATION_GAIN: "STOP_LOW_INFORMATION_GAIN",
  STOP_PUBLIC_DATA_CEILING: "STOP_PUBLIC_DATA_CEILING",
});

const SOURCE_BY_BLOCKER = Object.freeze({
  TIMING: "official_event_or_cycle_page",
  LODGING: "official_housing_accommodation_page",
  WHO: "official_contact_or_secretariat",
  CONTACT: "official_contact_page",
  ENTITY: "official_organization_page",
  GEOGRAPHY: "official_venue_location_page",
  PLACEMENT: "official_venue_or_host_page",
  FIT: "hotel_fit_evidence_page",
  SUMMARY: "official_program_page",
  SURFACE: "lodging_or_thesis_evidence_page",
  PROCUREMENT: "official_procurement_notice",
});

/**
 * Deterministic advisory baseline (used when Jev off / fallback).
 */
export function buildDeterministicJevStrategy(packet = {}) {
  const blocker = packet.primaryBlocker || packet.smallestMissingFact || "TIMING";
  const blockerKey = String(blocker).toUpperCase().replace(/[^A-Z_]/g, "").split("_")[0] || "TIMING";
  const lang =
    packet.languagesAttempted?.length && !packet.languagesAttempted.includes("en")
      ? "en"
      : packet.recommendedLanguage || packet.queryLanguage || "en";
  const feeder =
    packet.feederMarketsAttempted?.length === 0
      ? packet.defaultFeederMarket || null
      : null;

  let depth = RESEARCH_DEPTH.DEPTH_1_ONE_MORE_STEP;
  let steps = 1;
  let gain = "MEDIUM";
  let promo = "LOW";
  if (packet.completionPriority === "P0_HIGH_COMPLETION_POTENTIAL") {
    depth = RESEARCH_DEPTH.DEPTH_2_TWO_STEP_COMPLETION;
    steps = 2;
    gain = "HIGH";
    promo = packet.lodgingHint ? "MEDIUM" : "LOW";
  }
  if (packet.completionPriority === "P2_LOW_DEFER") {
    depth = RESEARCH_DEPTH.DEPTH_0_STOP_NOW;
    steps = 0;
    gain = "LOW";
  }
  if (
    packet.entityPass &&
    packet.lodgingHint &&
    Number(packet.completionScore || 0) >= 7 &&
    Number(packet.hotelFit || 0) >= 50
  ) {
    depth = RESEARCH_DEPTH.DEPTH_3_DEEP_RESEARCH_JUSTIFIED;
    steps = 3;
    promo = "MEDIUM";
  }

  const stop =
    steps === 0
      ? "STOP_NOW_LOW_PRIORITY"
      : `STOP_AFTER_${steps}_STEPS_OR_BLOCKER_RESOLVED_OR_PUBLIC_CEILING`;

  return {
    jevNextQuestion: `What is the public ${blockerKey.toLowerCase()} evidence for ${packet.title || "this candidate"}?`,
    jevRecommendedSourceFamily: SOURCE_BY_BLOCKER[blockerKey] || SOURCE_BY_BLOCKER.TIMING,
    jevRecommendedLanguage: lang,
    jevRecommendedFeederMarket: feeder,
    jevExpectedInformationGain: gain,
    jevEstimatedPromotionPotential: promo,
    jevRecommendedDepth: depth,
    jevRecommendedAdditionalSteps: steps,
    jevStopCondition: stop,
    jevRationale: `Deterministic advisory: attack ${blockerKey} via ${SOURCE_BY_BLOCKER[blockerKey] || "official_page"}; depth ${depth}.`,
    source: "DETERMINISTIC",
    advisoryOnly: true,
  };
}

/**
 * Ask Jev for research strategy (advisory). Falls back to deterministic.
 */
export async function reviewJevResearchStrategy(packet = {}) {
  const baseline = buildDeterministicJevStrategy(packet);
  if (!isJevEnabled()) {
    return { ...baseline, jevCalled: false };
  }

  try {
    const result = await decide({
      decisionType: JEV_DECISION_TYPE.EVIDENCE_GAP_NEXT_ACTION,
      context: {
        hotel: packet.hotelLabel || packet.hotelKey,
        market: packet.marketId,
        eventProgram: packet.title,
        primaryBlocker: packet.primaryBlocker,
        unresolvedDimensions: packet.unresolvedDimensions || [],
        previousResearchActions: packet.sourcesAttempted || [],
        costSoFarUsd: packet.estimatedRemainingResearchCost ?? packet.estimatedCost ?? 0,
        lodgingEvidence: packet.lodgingState,
        futureDateStatus: packet.timingState,
        missingFields: packet.unresolvedDimensions || [],
        researchDirectionOnly: true,
        noInventFacts: true,
        noPromote: true,
        decisionIntent: "CANDIDATE_COMPLETION_STRATEGY_V2",
      },
      existingDecision: baseline.jevRecommendedSourceFamily,
      policyContext: {
        researchDirectionOnly: true,
        noMutateFacts: true,
        noPromote: true,
        noInventPerson: true,
      },
      fallbackDecision: baseline.jevRecommendedSourceFamily,
    });

    const meta = result.rawResponse?.metadata || result.metadata || {};
    const choice = result.selected || result.effectiveDecision;

    // Map Jev choice → source family if recognizable; never accept facts
    let sourceFamily = baseline.jevRecommendedSourceFamily;
    if (typeof choice === "string" && /HOUSING|LODGING|CONTACT|CYCLE|SOURCE|PROCURE|VENUE/i.test(choice)) {
      if (/HOUSING|LODGING/.test(choice)) sourceFamily = SOURCE_BY_BLOCKER.LODGING;
      else if (/CONTACT|WHO/.test(choice)) sourceFamily = SOURCE_BY_BLOCKER.WHO;
      else if (/CYCLE|TIMING|DATE/.test(choice)) sourceFamily = SOURCE_BY_BLOCKER.TIMING;
      else if (/PROCURE|RFP|TENDER/.test(choice)) sourceFamily = SOURCE_BY_BLOCKER.PROCUREMENT;
      else if (/VENUE|HOST/.test(choice)) sourceFamily = SOURCE_BY_BLOCKER.PLACEMENT;
    }

    let depth = baseline.jevRecommendedDepth;
    if (result.confidence != null && Number(result.confidence) < 0.35) {
      depth = RESEARCH_DEPTH.DEPTH_0_STOP_NOW;
    } else if (meta.recommendedDepth && RESEARCH_DEPTH[meta.recommendedDepth]) {
      depth = meta.recommendedDepth;
    }

    return {
      ...baseline,
      jevNextQuestion:
        String(meta.nextQuestion || baseline.jevNextQuestion).slice(0, 400),
      jevRecommendedSourceFamily: sourceFamily,
      jevRecommendedLanguage:
        String(meta.language || baseline.jevRecommendedLanguage).slice(0, 16),
      jevRecommendedFeederMarket:
        meta.feederMarket != null ? String(meta.feederMarket).slice(0, 80) : baseline.jevRecommendedFeederMarket,
      jevExpectedInformationGain:
        String(meta.informationGain || baseline.jevExpectedInformationGain).slice(0, 24),
      jevEstimatedPromotionPotential:
        String(meta.promotionPotential || baseline.jevEstimatedPromotionPotential).slice(0, 24),
      jevRecommendedDepth: depth,
      jevRecommendedAdditionalSteps: baseline.jevRecommendedAdditionalSteps,
      jevStopCondition: String(meta.stopCondition || baseline.jevStopCondition).slice(0, 200),
      jevRationale: String(meta.rationale || result.rationale || baseline.jevRationale).slice(0, 500),
      source: "JEV_ADVISORY+DETERMINISTIC",
      advisoryOnly: true,
      jevCalled: true,
      jevChoice: choice,
      technicalFallback: Boolean(result.technicalFallback || result.policyFallback),
    };
  } catch (err) {
    return {
      ...baseline,
      jevCalled: true,
      jevError: String(err?.message || err).slice(0, 160),
      source: "DETERMINISTIC_FALLBACK",
    };
  }
}

/**
 * After a research step — Jev next-best action (advisory).
 */
export function decideJevNextActionDeterministic({
  gapMap,
  stepsDone = 0,
  maxSteps = 1,
  blockersResolvedThisStep = 0,
  contactAttempted = false,
  hasContact = false,
  pagesEmpty = false,
} = {}) {
  if (stepsDone >= maxSteps) {
    return {
      action: JEV_LOOP_ACTION.STOP_LOW_INFORMATION_GAIN,
      rationale: "Depth budget exhausted",
      acceptedByPolicy: true,
      source: "DETERMINISTIC",
    };
  }
  if (contactAttempted && !hasContact) {
    return {
      action: JEV_LOOP_ACTION.STOP_PUBLIC_DATA_CEILING,
      rationale: "WHO researched; public contact ceiling reached",
      acceptedByPolicy: true,
      source: "DETERMINISTIC",
    };
  }
  if (pagesEmpty && stepsDone >= 1) {
    return {
      action: JEV_LOOP_ACTION.SWITCH_SOURCE,
      rationale: "Page empty/blocked — switch source family",
      acceptedByPolicy: true,
      source: "DETERMINISTIC",
    };
  }
  if (blockersResolvedThisStep === 0 && stepsDone >= 1) {
    const next = gapMap?.smallestMissingFact;
    if (next && gapMap?.gaps?.TIMING === GAP_STATE.FAIL) {
      return {
        action: JEV_LOOP_ACTION.SWITCH_BLOCKER,
        rationale: "No gain on prior blocker; switch to timing",
        acceptedByPolicy: true,
        source: "DETERMINISTIC",
      };
    }
    return {
      action: JEV_LOOP_ACTION.STOP_LOW_INFORMATION_GAIN,
      rationale: "Zero blockers resolved; marginal gain poor",
      acceptedByPolicy: true,
      source: "DETERMINISTIC",
    };
  }
  if (gapMap?.failOrUnknownCount > 0 && stepsDone < maxSteps) {
    return {
      action: JEV_LOOP_ACTION.CONTINUE,
      rationale: "Residual blockers remain within depth budget",
      acceptedByPolicy: true,
      source: "DETERMINISTIC",
    };
  }
  return {
    action: JEV_LOOP_ACTION.STOP_LOW_INFORMATION_GAIN,
    rationale: "No residual high-value blockers",
    acceptedByPolicy: true,
    source: "DETERMINISTIC",
  };
}

export async function reviewJevNextAction(input = {}) {
  const det = decideJevNextActionDeterministic(input);
  if (!isJevEnabled()) return { ...det, jevCalled: false };

  try {
    const result = await decide({
      decisionType: JEV_DECISION_TYPE.STOP_CONTINUE,
      context: {
        researchDirectionOnly: true,
        noInventFacts: true,
        noPromote: true,
        costSoFarUsd: input.estimatedCost || 0,
        missingFields: input.gapMap?.smallestMissingFact
          ? [input.gapMap.smallestMissingFact]
          : [],
        priorRunResults: [`steps=${input.stepsDone}`, `resolved=${input.blockersResolvedThisStep}`],
        decisionIntent: "CANDIDATE_COMPLETION_LOOP_V2",
      },
      existingDecision: det.action === JEV_LOOP_ACTION.CONTINUE ? "CONTINUE" : "STOP",
      policyContext: { researchDirectionOnly: true, noMutateFacts: true, noPromote: true },
      fallbackDecision: det.action === JEV_LOOP_ACTION.CONTINUE ? "CONTINUE" : "STOP",
    });

    const selected = String(result.selected || result.effectiveDecision || "").toUpperCase();
    let action = det.action;
    if (/STOP/.test(selected) && det.action === JEV_LOOP_ACTION.CONTINUE) {
      // Jev may recommend stop; policy accepts if gain was already weak
      if ((input.blockersResolvedThisStep || 0) === 0) {
        action = JEV_LOOP_ACTION.STOP_LOW_INFORMATION_GAIN;
      }
    } else if (/CONTINUE/.test(selected) && input.stepsDone < input.maxSteps) {
      action = JEV_LOOP_ACTION.CONTINUE;
    }

    const accepted =
      action === det.action ||
      (action === JEV_LOOP_ACTION.STOP_LOW_INFORMATION_GAIN &&
        det.action === JEV_LOOP_ACTION.CONTINUE &&
        (input.blockersResolvedThisStep || 0) === 0);

    return {
      action,
      rationale: String(result.rationale || det.rationale).slice(0, 400),
      acceptedByPolicy: accepted,
      rejectedByPolicy: !accepted,
      source: "JEV_ADVISORY+DETERMINISTIC",
      jevCalled: true,
      jevChoice: selected,
      deterministicAction: det.action,
    };
  } catch (err) {
    return { ...det, jevCalled: true, jevError: String(err?.message || err).slice(0, 120) };
  }
}
