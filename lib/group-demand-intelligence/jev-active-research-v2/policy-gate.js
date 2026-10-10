/**
 * Deterministic policy gate for Jev research-routing advice.
 * Never allows promotion / fact writes / threshold changes.
 */

export const POLICY_VERDICT = Object.freeze({
  JEV_ACCEPTED: "JEV_ACCEPTED",
  JEV_REJECTED_POLICY: "JEV_REJECTED_POLICY",
  JEV_REJECTED_LOW_VALUE: "JEV_REJECTED_LOW_VALUE",
});

const DISALLOWED_SOURCE = /\b(surfe|linkedin.?sales|scraped_pii|dark_web)\b/i;
const PROMOTE_RE = /\b(PROMOTE|CUSTOMER_READY|FORCE_READY|BYPASS_GATE)\b/i;

/**
 * @returns {{ verdict, acceptedAdvice, reason }}
 */
export function gateJevResearchAdvice(item = {}, advice = {}, opts = {}) {
  const hardCap = opts.hardCapSteps ?? 3;
  const budgetLeft = opts.budgetLeftUsd ?? 999;
  const stepsDone = Number(item.priorResearchSteps || item.researchSteps || 0);

  if (PROMOTE_RE.test(JSON.stringify(advice))) {
    return {
      verdict: POLICY_VERDICT.JEV_REJECTED_POLICY,
      acceptedAdvice: null,
      reason: "jev_proposed_unsupported_promotion",
    };
  }

  if (DISALLOWED_SOURCE.test(String(advice.jevSourceFamily || advice.jevResearchQuestion || ""))) {
    return {
      verdict: POLICY_VERDICT.JEV_REJECTED_POLICY,
      acceptedAdvice: null,
      reason: "disallowed_source_family",
    };
  }

  if (item.duplicateCanonical === true) {
    return {
      verdict: POLICY_VERDICT.JEV_REJECTED_POLICY,
      acceptedAdvice: null,
      reason: "duplicate_candidate",
    };
  }

  if (item.timingState === "HISTORICAL_ONLY" && advice.jevDecision === "RESEARCH_NOW") {
    return {
      verdict: POLICY_VERDICT.JEV_REJECTED_POLICY,
      acceptedAdvice: { ...advice, jevDecision: "STOP_LOW_YIELD", jevDepth: "DEPTH_0_STOP" },
      reason: "historical_only_block",
    };
  }

  if (/IMPOSSIBLE|NO_FIT/i.test(String(item.fitState || "")) && advice.jevDecision === "RESEARCH_NOW") {
    return {
      verdict: POLICY_VERDICT.JEV_REJECTED_POLICY,
      acceptedAdvice: null,
      reason: "hotel_fit_clearly_impossible",
    };
  }

  const depthMap = {
    DEPTH_0_STOP: 0,
    DEPTH_1_ONE_STEP: 1,
    DEPTH_2_TWO_STEPS: 2,
    DEPTH_3_DEEP: 3,
  };
  let maxSteps = depthMap[advice.jevDepth] ?? 1;
  if (maxSteps + stepsDone > hardCap) {
    maxSteps = Math.max(0, hardCap - stepsDone);
    if (maxSteps === 0) {
      return {
        verdict: POLICY_VERDICT.JEV_REJECTED_POLICY,
        acceptedAdvice: { ...advice, jevDecision: "STOP_LOW_YIELD", jevDepth: "DEPTH_0_STOP" },
        reason: "depth_exceeds_hard_cap",
      };
    }
  }

  if (budgetLeft < 0.05 && advice.jevDecision === "RESEARCH_NOW") {
    return {
      verdict: POLICY_VERDICT.JEV_REJECTED_LOW_VALUE,
      acceptedAdvice: { ...advice, jevDecision: "RESEARCH_LATER", jevDepth: "DEPTH_0_STOP" },
      reason: "cost_exceeds_allowed_budget",
    };
  }

  if (advice.jevDecision === "STOP_LOW_YIELD" || advice.jevDepth === "DEPTH_0_STOP") {
    return {
      verdict: POLICY_VERDICT.JEV_ACCEPTED,
      acceptedAdvice: { ...advice, jevDepth: "DEPTH_0_STOP", maxSteps: 0 },
      reason: "jev_stop_low_yield",
    };
  }

  if (advice.jevDecision === "RESEARCH_LATER") {
    return {
      verdict: POLICY_VERDICT.JEV_REJECTED_LOW_VALUE,
      acceptedAdvice: { ...advice, maxSteps: 0 },
      reason: "jev_defer_low_value_this_tranche",
    };
  }

  // RESEARCH_NOW
  return {
    verdict: POLICY_VERDICT.JEV_ACCEPTED,
    acceptedAdvice: {
      ...advice,
      maxSteps,
      jevDepth:
        maxSteps >= 3
          ? "DEPTH_3_DEEP"
          : maxSteps === 2
            ? "DEPTH_2_TWO_STEPS"
            : maxSteps === 1
              ? "DEPTH_1_ONE_STEP"
              : "DEPTH_0_STOP",
    },
    reason: "research_routing_accepted",
  };
}

export function gateJevLoopAction(action, item = {}, opts = {}) {
  const hardCap = opts.hardCapSteps ?? 3;
  const stepsDone = Number(item.researchSteps || 0);
  if (stepsDone >= hardCap && /CONTINUE|SWITCH_/i.test(action)) {
    return {
      verdict: POLICY_VERDICT.JEV_REJECTED_POLICY,
      action: "STOP_LOW_INFORMATION_GAIN",
      reason: "hard_cap_reached",
    };
  }
  if (action === "STOP_PUBLIC_DATA_CEILING" || action === "STOP_LOW_INFORMATION_GAIN") {
    return { verdict: POLICY_VERDICT.JEV_ACCEPTED, action, reason: "stop_accepted" };
  }
  return { verdict: POLICY_VERDICT.JEV_ACCEPTED, action, reason: "loop_action_accepted" };
}
