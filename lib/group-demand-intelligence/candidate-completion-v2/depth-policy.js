/**
 * Adaptive research depth — hard caps, no infinite loops.
 */

import { COMPLETION_PRIORITY } from "./priority.js";

export const RESEARCH_DEPTH = Object.freeze({
  DEPTH_0_STOP_NOW: "DEPTH_0_STOP_NOW",
  DEPTH_1_ONE_MORE_STEP: "DEPTH_1_ONE_MORE_STEP",
  DEPTH_2_TWO_STEP_COMPLETION: "DEPTH_2_TWO_STEP_COMPLETION",
  DEPTH_3_DEEP_RESEARCH_JUSTIFIED: "DEPTH_3_DEEP_RESEARCH_JUSTIFIED",
});

export const HARD_CAP_STEPS = 3;

/**
 * @param {object} candidate
 * @param {object} deterministicState
 * @param {object} jevAdvice
 */
export function resolveGdiResearchDepth(candidate = {}, deterministicState = {}, jevAdvice = {}) {
  const priority = candidate.completionPriority || COMPLETION_PRIORITY.P2_LOW_DEFER;
  const stepsDone = Number(deterministicState.researchSteps || 0);
  const fit = Number(candidate.hotelFitScore ?? candidate.hotelFit ?? deterministicState.hotelFitScore ?? 0);
  const entityOk = deterministicState.entityPass === true || deterministicState.ENTITY === "PASS";
  const geoOk = deterministicState.geographyPass === true || deterministicState.GEOGRAPHY === "PASS";
  const lodgingPartial =
    deterministicState.LODGING === "PARTIAL" || deterministicState.lodgingHint === true;
  const commercial = Number(deterministicState.commercialPotential || candidate.completionScore || 0);
  const cost = Number(deterministicState.estimatedCost || 0);

  let depth = RESEARCH_DEPTH.DEPTH_0_STOP_NOW;
  let maxSteps = 0;
  let rationale = "default_stop";

  if (priority === COMPLETION_PRIORITY.P2_LOW_DEFER) {
    if (
      jevAdvice?.jevRecommendedDepth === RESEARCH_DEPTH.DEPTH_2_TWO_STEP_COMPLETION &&
      jevAdvice?.jevEstimatedPromotionPotential === "HIGH" &&
      entityOk
    ) {
      depth = RESEARCH_DEPTH.DEPTH_1_ONE_MORE_STEP;
      maxSteps = 1;
      rationale = "P2_exception_high_jev_potential";
    } else {
      return {
        depth: RESEARCH_DEPTH.DEPTH_0_STOP_NOW,
        maxSteps: 0,
        rationale: "P2_defer",
        hardCap: HARD_CAP_STEPS,
      };
    }
  } else if (priority === COMPLETION_PRIORITY.P1_MEDIUM_COMPLETION_POTENTIAL) {
    depth = RESEARCH_DEPTH.DEPTH_1_ONE_MORE_STEP;
    maxSteps = 1;
    rationale = "P1_default_one_step";
  } else if (priority === COMPLETION_PRIORITY.P0_HIGH_COMPLETION_POTENTIAL) {
    depth = RESEARCH_DEPTH.DEPTH_2_TWO_STEP_COMPLETION;
    maxSteps = 2;
    rationale = "P0_default_two_steps";
  }

  // DEPTH_3 only when strong conditions met
  const depth3Ok =
    entityOk &&
    geoOk &&
    lodgingPartial &&
    fit >= 50 &&
    commercial >= 7 &&
    cost < 1.5 &&
    (jevAdvice?.jevRecommendedDepth === RESEARCH_DEPTH.DEPTH_3_DEEP_RESEARCH_JUSTIFIED ||
      priority === COMPLETION_PRIORITY.P0_HIGH_COMPLETION_POTENTIAL);

  if (depth3Ok) {
    depth = RESEARCH_DEPTH.DEPTH_3_DEEP_RESEARCH_JUSTIFIED;
    maxSteps = HARD_CAP_STEPS;
    rationale = "depth3_justified_strong_fit_lodging_partial";
  }

  // Honor Jev shallower stop
  if (jevAdvice?.jevRecommendedDepth === RESEARCH_DEPTH.DEPTH_0_STOP_NOW && stepsDone >= 1) {
    return {
      depth: RESEARCH_DEPTH.DEPTH_0_STOP_NOW,
      maxSteps: stepsDone,
      rationale: "jev_stop_after_step",
      hardCap: HARD_CAP_STEPS,
    };
  }

  if (jevAdvice?.jevRecommendedDepth === RESEARCH_DEPTH.DEPTH_1_ONE_MORE_STEP && maxSteps > 1) {
    maxSteps = Math.min(maxSteps, 1);
    depth = RESEARCH_DEPTH.DEPTH_1_ONE_MORE_STEP;
    rationale = `${rationale}+jev_limit_one`;
  }

  maxSteps = Math.min(maxSteps, HARD_CAP_STEPS);
  if (stepsDone >= maxSteps) {
    return {
      depth: RESEARCH_DEPTH.DEPTH_0_STOP_NOW,
      maxSteps,
      rationale: "depth_exhausted",
      hardCap: HARD_CAP_STEPS,
      stepsDone,
    };
  }

  return { depth, maxSteps, rationale, hardCap: HARD_CAP_STEPS, stepsDone };
}
