/**
 * Active Jev advisory research controller — WHERE / HOW FAR only.
 * forceShadow:false for research-routing SAFE APPLY; never writes facts / promotes.
 */

import { isJevEnabled } from "../jev/jev-config.js";
import { decide } from "../jev/jev-decision-service.js";
import { JEV_DECISION_TYPE } from "../jev/jev-types.js";
import { RESEARCH_PRIORITY } from "./score.js";

export const JEV_DECISION = Object.freeze({
  RESEARCH_NOW: "RESEARCH_NOW",
  RESEARCH_LATER: "RESEARCH_LATER",
  STOP_LOW_YIELD: "STOP_LOW_YIELD",
});

export const JEV_DEPTH = Object.freeze({
  DEPTH_0_STOP: "DEPTH_0_STOP",
  DEPTH_1_ONE_STEP: "DEPTH_1_ONE_STEP",
  DEPTH_2_TWO_STEPS: "DEPTH_2_TWO_STEPS",
  DEPTH_3_DEEP: "DEPTH_3_DEEP",
});

export const JEV_LOOP = Object.freeze({
  CONTINUE: "CONTINUE",
  SWITCH_SOURCE: "SWITCH_SOURCE",
  SWITCH_LANGUAGE: "SWITCH_LANGUAGE",
  SWITCH_FEEDER_MARKET: "SWITCH_FEEDER_MARKET",
  SWITCH_BLOCKER: "SWITCH_BLOCKER",
  STOP_LOW_INFORMATION_GAIN: "STOP_LOW_INFORMATION_GAIN",
  STOP_PUBLIC_DATA_CEILING: "STOP_PUBLIC_DATA_CEILING",
});

const SOURCE_BY_BLOCKER = {
  TIMING: "official_event_or_cycle_page",
  LODGING: "official_housing_accommodation_page",
  WHO: "official_contact_or_secretariat",
  CONTACT: "official_contact_page",
  ENTITY: "official_organization_page",
  PLACEMENT: "official_venue_page",
  PROCUREMENT: "official_procurement_notice",
  ROTATION: "historic_host_cycle_pages",
};

function primaryBlocker(item = {}) {
  const b = item.currentBlockers || [];
  if (b.includes("TIMING")) return "TIMING";
  if (b.includes("LODGING")) return "LODGING";
  if (b.includes("WHO") || b.includes("CONTACT")) return "WHO";
  return b[0] || "TIMING";
}

/**
 * Deterministic advisory baseline (always available).
 */
export function buildDeterministicActiveAdvice(item = {}) {
  const blocker = primaryBlocker(item);
  const isP0 = item.researchPriority === RESEARCH_PRIORITY.P0_HIGH;
  const isP1 = item.researchPriority === RESEARCH_PRIORITY.P1_MEDIUM;
  const noise = (item.completionPotentialScore || 0) < 3;

  let jevDecision = JEV_DECISION.STOP_LOW_YIELD;
  let jevDepth = JEV_DEPTH.DEPTH_0_STOP;
  if (noise || item.researchPriority === RESEARCH_PRIORITY.P2_LOW) {
    jevDecision = JEV_DECISION.STOP_LOW_YIELD;
    jevDepth = JEV_DEPTH.DEPTH_0_STOP;
  } else if (isP0) {
    jevDecision = JEV_DECISION.RESEARCH_NOW;
    jevDepth =
      item.signalType === "PRE_RFP" || item.signalType === "ROTATION_SERIES"
        ? JEV_DEPTH.DEPTH_3_DEEP
        : JEV_DEPTH.DEPTH_2_TWO_STEPS;
  } else if (isP1) {
    jevDecision = JEV_DECISION.RESEARCH_NOW;
    jevDepth = JEV_DEPTH.DEPTH_1_ONE_STEP;
  }

  const lang =
    item.language && item.language !== "en" && blocker === "LODGING" ? "en" : item.language || "en";

  return {
    jevDecision,
    jevPriority: item.researchPriority,
    jevTargetBlocker: blocker,
    jevResearchQuestion: `What public ${blocker.toLowerCase()} evidence exists for ${item.organization || item.title}?`,
    jevSourceFamily: SOURCE_BY_BLOCKER[blocker] || SOURCE_BY_BLOCKER.TIMING,
    jevLanguage: lang,
    jevFeederMarket: item.feederMarket || null,
    jevDepth,
    jevStopCondition:
      jevDepth === JEV_DEPTH.DEPTH_0_STOP
        ? "STOP_NOW"
        : "STOP_AFTER_DEPTH_OR_BLOCKER_RESOLVED_OR_PUBLIC_CEILING",
    jevExpectedInformationGain: isP0 ? "HIGH" : isP1 ? "MEDIUM" : "LOW",
    jevPromotionPotential: "LOW",
    jevRationale: `Deterministic active baseline: ${jevDecision} / ${blocker} / ${jevDepth}`,
    evidenceThatWouldChangeClassification: `${blocker} public confirmation + lodging or contact path`,
    source: "DETERMINISTIC",
    advisoryOnly: true,
    writesVerifiedFacts: false,
    promotesOpportunities: false,
  };
}

function mapPriorityChoice(choice, baseline) {
  const c = String(choice || "").toUpperCase();
  if (/RESEARCH_NOW/.test(c)) return JEV_DECISION.RESEARCH_NOW;
  if (/DEFER|LOWER_PRIORITY|PAUSE|RESEARCH_LATER/.test(c)) return JEV_DECISION.RESEARCH_LATER;
  if (/RETIRE|STOP|STOP_NO/.test(c)) return JEV_DECISION.STOP_LOW_YIELD;
  return baseline.jevDecision;
}

function mapBlockerFromChoice(choice, baseline) {
  const c = String(choice || "").toUpperCase();
  if (/HOUSING|LODGING|ROOM/.test(c)) return "LODGING";
  if (/CONTACT|WHO|PERSON/.test(c)) return "WHO";
  if (/CYCLE|TIMING|DATE|FORWARD/.test(c)) return "TIMING";
  if (/VENUE|HOST|PLACE/.test(c)) return "PLACEMENT";
  if (/PROCURE|RFP|TENDER/.test(c)) return "PROCUREMENT";
  return baseline.jevTargetBlocker;
}

/**
 * Active Jev strategy review — real API when enabled; advisory apply for routing.
 */
export async function reviewJevActiveStrategy(item = {}) {
  const baseline = buildDeterministicActiveAdvice(item);

  if (!isJevEnabled()) {
    return { ...baseline, jevCalled: false, jevLive: false };
  }

  try {
    // 1) Worth researching?
    const priorityDec = await decide({
      decisionType: JEV_DECISION_TYPE.TARGET_RESEARCH_PRIORITY,
      context: {
        targetType: "SIGNAL_OR_SERIES",
        entityType: item.signalType,
        hotel: item.hotelLabel,
        market: item.marketId,
        eventProgram: item.eventProgram || item.title,
        missingFields: item.currentBlockers || [],
        costSoFarUsd: item.priorCost || 0,
        lodgingEvidence: item.lodgingState,
        futureDateStatus: item.timingState,
        researchDirectionOnly: true,
        noInventFacts: true,
        noPromote: true,
        decisionIntent: "JEV_ACTIVE_RESEARCH_CONTROLLER_V2",
        decisionHint: "ROUTING_ONLY_NO_TRUTH",
        opportunityPriority: item.researchPriority,
        evidenceSummary: JSON.stringify({
          score: item.completionPotentialScore,
          signalType: item.signalType,
          engine: item.demandEngine,
        }),
      },
      existingDecision: baseline.jevDecision === JEV_DECISION.RESEARCH_NOW ? "RESEARCH_NOW" : "DEFER",
      policyContext: {
        researchDirectionOnly: true,
        noMutateFacts: true,
        noPromote: true,
        noInventPerson: true,
        noInventOpportunity: true,
      },
      forceShadow: false,
      runContext: { runId: "jev_active_research_v2" },
    });

    // 2) Next evidence action
    const gapDec = await decide({
      decisionType: JEV_DECISION_TYPE.EVIDENCE_GAP_NEXT_ACTION,
      context: {
        hotel: item.hotelLabel,
        market: item.marketId,
        eventProgram: item.title,
        primaryBlocker: baseline.jevTargetBlocker,
        unresolvedDimensions: item.currentBlockers || [],
        researchDirectionOnly: true,
        noInventFacts: true,
        noPromote: true,
        decisionIntent: "JEV_ACTIVE_BLOCKER_ROUTING_V2",
      },
      existingDecision: baseline.jevSourceFamily,
      policyContext: {
        researchDirectionOnly: true,
        noMutateFacts: true,
        noPromote: true,
      },
      forceShadow: false,
      runContext: { runId: "jev_active_research_v2" },
    });

    const pChoice = priorityDec.selected || priorityDec.finalPolicyDecision || priorityDec.effectiveDecision;
    const gChoice = gapDec.selected || gapDec.finalPolicyDecision || gapDec.effectiveDecision;
    const jevDecision = mapPriorityChoice(pChoice, baseline);
    const blocker = mapBlockerFromChoice(gChoice, baseline);

    let jevDepth = baseline.jevDepth;
    if (jevDecision === JEV_DECISION.STOP_LOW_YIELD) jevDepth = JEV_DEPTH.DEPTH_0_STOP;
    else if (jevDecision === JEV_DECISION.RESEARCH_LATER) jevDepth = JEV_DEPTH.DEPTH_0_STOP;
    else if (priorityDec.confidence != null && Number(priorityDec.confidence) >= 0.7 && item.researchPriority === RESEARCH_PRIORITY.P0_HIGH) {
      jevDepth = JEV_DEPTH.DEPTH_3_DEEP;
    } else if (priorityDec.confidence != null && Number(priorityDec.confidence) < 0.4) {
      jevDepth = JEV_DEPTH.DEPTH_1_ONE_STEP;
    }

    const meta = gapDec.rawResponse?.metadata || gapDec.metadata || {};

    return {
      ...baseline,
      jevDecision,
      jevTargetBlocker: blocker,
      jevSourceFamily: SOURCE_BY_BLOCKER[blocker] || baseline.jevSourceFamily,
      jevResearchQuestion: String(meta.nextQuestion || baseline.jevResearchQuestion).slice(0, 400),
      jevLanguage: String(meta.language || baseline.jevLanguage).slice(0, 16),
      jevFeederMarket:
        meta.feederMarket != null ? String(meta.feederMarket).slice(0, 80) : baseline.jevFeederMarket,
      jevDepth,
      jevExpectedInformationGain:
        jevDecision === JEV_DECISION.RESEARCH_NOW
          ? baseline.jevExpectedInformationGain
          : "LOW",
      jevRationale: String(
        meta.rationale ||
          priorityDec.rationale ||
          `Jev active: priority=${pChoice} gap=${gChoice} → ${jevDecision}/${blocker}`
      ).slice(0, 500),
      jevStopCondition: String(meta.stopCondition || baseline.jevStopCondition).slice(0, 200),
      source: "JEV_ACTIVE_ADVISORY",
      jevCalled: true,
      jevLive: true,
      jevPriorityChoice: pChoice,
      jevGapChoice: gChoice,
      jevPriorityConfidence: priorityDec.confidence,
      jevGapConfidence: gapDec.confidence,
      technicalFallback: Boolean(priorityDec.technicalFallback || gapDec.technicalFallback),
      costUsd: (priorityDec.costUsd || 0) + (gapDec.costUsd || 0),
    };
  } catch (err) {
    return {
      ...baseline,
      jevCalled: true,
      jevLive: false,
      jevError: String(err?.message || err).slice(0, 160),
      source: "DETERMINISTIC_FALLBACK",
    };
  }
}

export async function reviewJevActiveLoop(item = {}, stepCtx = {}) {
  const det =
    stepCtx.blockersResolvedThisStep > 0 && stepCtx.stepsDone < stepCtx.maxSteps
      ? JEV_LOOP.CONTINUE
      : stepCtx.contactAttempted && !stepCtx.hasContact
        ? JEV_LOOP.STOP_PUBLIC_DATA_CEILING
        : stepCtx.blockersResolvedThisStep === 0 && stepCtx.stepsDone >= 1
          ? JEV_LOOP.STOP_LOW_INFORMATION_GAIN
          : stepCtx.pageEmpty
            ? JEV_LOOP.SWITCH_SOURCE
            : stepCtx.stepsDone < stepCtx.maxSteps
              ? JEV_LOOP.CONTINUE
              : JEV_LOOP.STOP_LOW_INFORMATION_GAIN;

  if (!isJevEnabled()) {
    return {
      action: det,
      rationale: "deterministic_loop",
      source: "DETERMINISTIC",
      jevCalled: false,
    };
  }

  try {
    const result = await decide({
      decisionType: JEV_DECISION_TYPE.STOP_CONTINUE,
      context: {
        researchDirectionOnly: true,
        noInventFacts: true,
        noPromote: true,
        costSoFarUsd: stepCtx.estimatedCost || 0,
        missingFields: item.currentBlockers || [],
        priorRunResults: [
          `steps=${stepCtx.stepsDone}`,
          `resolved=${stepCtx.blockersResolvedThisStep}`,
        ],
        decisionIntent: "JEV_ACTIVE_LOOP_V2",
        decisionHint: "ROUTING_ONLY_NO_TRUTH",
      },
      existingDecision: /CONTINUE|SWITCH/.test(det) ? "CONTINUE" : "STOP",
      policyContext: { researchDirectionOnly: true, noMutateFacts: true, noPromote: true },
      forceShadow: false,
      runContext: { runId: "jev_active_research_v2" },
    });

    const selected = String(result.selected || result.finalPolicyDecision || "").toUpperCase();
    let action = det;
    if (/STOP/.test(selected) && det === JEV_LOOP.CONTINUE && (stepCtx.blockersResolvedThisStep || 0) === 0) {
      action = JEV_LOOP.STOP_LOW_INFORMATION_GAIN;
    } else if (/CONTINUE/.test(selected) && stepCtx.stepsDone < stepCtx.maxSteps) {
      action = JEV_LOOP.CONTINUE;
    }

    return {
      action,
      rationale: String(result.rationale || det).slice(0, 400),
      source: "JEV_ACTIVE_ADVISORY",
      jevCalled: true,
      jevChoice: selected,
      deterministicAction: det,
      costUsd: result.costUsd || 0,
    };
  } catch (err) {
    return {
      action: det,
      rationale: String(err?.message || err).slice(0, 120),
      source: "DETERMINISTIC_FALLBACK",
      jevCalled: true,
      jevError: true,
    };
  }
}

/**
 * Engine-level coverage advice (deterministic + optional Jev).
 */
export function adviseEngineCoverage(coverageRows = []) {
  const under = (coverageRows || [])
    .filter((r) => r.underResearched || r.researched === 0)
    .sort((a, b) => (b.priorityRank || 0) - (a.priorityRank || 0));
  const saturated = (coverageRows || []).filter(
    (r) => (r.candidates || 0) >= 8 && (r.useful || 0) === 0
  );
  const decisions = [];
  for (const s of saturated.slice(0, 3)) {
    decisions.push({
      action: "STOP_ENGINE_SATURATED",
      demandEngine: s.demandEngine || s.key,
      rationale: "Zero useful on high candidate volume",
    });
  }
  if (under[0]) {
    const eng = under[0].demandEngine || under[0].key;
    decisions.push({
      action: `CONTINUE_${String(eng).replace(/_LIFE_SCIENCES|_INTL_ORGANIZATIONS|_ENTERTAINMENT_SOCIAL|_CREW_EXTENDED_GROUP|_DMC_INCENTIVE|_RFP/g, "").slice(0, 24)}`,
      demandEngine: eng,
      rationale: "Under-researched engine still deserves investment",
    });
  }
  return decisions;
}
