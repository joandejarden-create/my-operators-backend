/**
 * Next-best-action + stop/continue — Iteration 2 (external tool escalation).
 */

import { TERMINATION_REASONS } from "./bounds.js";
import { nextOpenPlanStep } from "./planner.js";
import { touchLatency } from "./research-state.js";
import { evaluateEscalation } from "./tools/escalation-policy.js";

export const NEXT_ACTION_VERSION = "native-next-action-v2";

export function chooseNextAction(state) {
  touchLatency(state);

  if (state.identity_lock.status === "PENDING") {
    return { type: "IDENTITY_LOCK", reason: "identity_required_first" };
  }
  if (state.identity_lock.identity_match === "FALSE" || state.identity_lock.identity_match === "UNRESOLVED") {
    return { type: "STOP", termination_reason: TERMINATION_REASONS.IDENTITY_UNRESOLVED };
  }

  if (!state._internal_loaded) {
    return { type: "LOAD_INTERNAL_KNOWLEDGE", reason: "cheapest_trustworthy_first" };
  }

  if (state.lane === "OWNER_PORTFOLIO" && !state._portfolio_loaded) {
    return { type: "LOAD_PORTFOLIO", reason: "portfolio_lane_priority" };
  }

  if (state.contradictions.some((c) => c.status === "OPEN") && !state._contra_searched) {
    return { type: "RESOLVE_CONTRADICTION", reason: "open_contradiction" };
  }

  // Contradiction-driven external research after one internal contra pass
  if (
    state.contradictions.some((c) => c.status === "OPEN") &&
    state._contra_searched &&
    !(state._external_contra_pass)
  ) {
    const esc = evaluateEscalation(state);
    if (esc.escalate) {
      return {
        type: esc.preferred_tool === "USE_PARALLEL" ? "USE_PARALLEL" : "SEARCH_WEB",
        reason: "contradiction_driven_external",
        reasons: esc.reasons,
        step: { field: "contradiction", id: "q_contra_ext", question: "Resolve open contradiction" },
      };
    }
  }

  const step = nextOpenPlanStep(state);
  if (!step) {
    return { type: "VERIFY_THEN_STOP", reason: "plan_exhausted" };
  }

  if (step.field === "verification" || step.id === "q_verify") {
    // Multi-step residual synthesis before final verify
    if (state.lane === "OPEN_RESEARCH" && (state._parallel_calls || 0) < 1) {
      return {
        type: "USE_PARALLEL",
        step,
        reason: "multi_step_operator_residual_pass",
        reasons: ["multi_step_open", "operator_residual"],
      };
    }
    // Before verify: escalate if single-source / unresolved critical fields
    const esc = evaluateEscalation(state);
    if (esc.escalate && (state._search_calls || 0) + (state._parallel_calls || 0) < 2) {
      return {
        type: esc.preferred_tool,
        step,
        reason: "pre_verify_escalation",
        reasons: esc.reasons,
      };
    }
    return { type: "VERIFY_CRITICAL", step, reason: "verification_pass" };
  }

  if (!state._corpus_passes) state._corpus_passes = 0;
  if (state._corpus_passes < 2) {
    return { type: "SEARCH_CORPUS", step, reason: "domain_corpus_retrieval" };
  }

  // After internal corpus: gap-driven external tools
  const esc = evaluateEscalation(state);
  const alreadyVerifiedOwner = state.claims.some(
    (c) =>
      c.relationship === "OWNED_BY" &&
      ["VERIFIED", "HIGH"].includes(c.status) &&
      c.object &&
      !c.unresolved
  );
  const openContra = state.contradictions.some((c) => c.status === "OPEN");

  // Do not keep spending external tools once ownership is verified and no open contradiction
  // Exception: OPEN_RESEARCH / multi-step still needs one Parallel pass for operator residual synthesis
  const needsMultiStepPass =
    state.lane === "OPEN_RESEARCH" && (state._parallel_calls || 0) < 1;
  if (
    alreadyVerifiedOwner &&
    !openContra &&
    !needsMultiStepPass &&
    ((state._search_calls || 0) > 0 || (state._parallel_calls || 0) > 0)
  ) {
    return { type: "VERIFY_THEN_STOP", reason: "verified_after_external_corroboration" };
  }

  if (needsMultiStepPass && alreadyVerifiedOwner) {
    return {
      type: "USE_PARALLEL",
      step,
      reason: "multi_step_operator_residual_pass",
      reasons: ["multi_step_open", "operator_residual"],
    };
  }

  if (esc.escalate && esc.preferred_tool !== "NONE") {
    if (
      esc.preferred_tool === "SEARCH_WEB" &&
      (state._promising_urls || []).length &&
      !(state._page_fetches || 0) &&
      (state._search_calls || 0) > 0
    ) {
      return { type: "FETCH_PAGE_EVIDENCE", step, reason: "inspect_promising_sources", reasons: esc.reasons };
    }
    // Cap redundant search after first Parallel pass when still unresolved
    if (
      esc.preferred_tool === "SEARCH_WEB" &&
      (state._search_calls || 0) >= 2 &&
      (state._parallel_calls || 0) >= 1 &&
      !openContra
    ) {
      return { type: "VERIFY_THEN_STOP", reason: "external_pass_exhausted_still_unresolved" };
    }
    return {
      type: esc.preferred_tool,
      step,
      reason: "gap_driven_external",
      reasons: esc.reasons,
    };
  }

  if (state._corpus_passes < 3) {
    return { type: "SEARCH_CORPUS", step, reason: "domain_corpus_retrieval" };
  }

  return { type: "VERIFY_THEN_STOP", reason: "no_more_high_value_actions" };
}

export function evaluateStop(state) {
  touchLatency(state);
  const b = state.bounds;

  if (state.latency.elapsed_ms > b.max_elapsed_ms) {
    return { stop: true, reason: TERMINATION_REASONS.TIME_LIMIT };
  }
  if (state.iteration >= b.max_iterations) {
    return { stop: true, reason: TERMINATION_REASONS.MAX_ITERATIONS };
  }
  if (state.action_count >= b.max_actions) {
    return { stop: true, reason: TERMINATION_REASONS.MAX_ACTIONS };
  }
  if (state.cost.total_usd > b.max_total_cost_usd && b.max_total_cost_usd > 0) {
    return { stop: true, reason: TERMINATION_REASONS.BUDGET_REACHED };
  }
  if (
    Number(b.max_external_cost_usd || 0) > 0 &&
    Number(state.cost.external_usd || 0) > Number(b.max_external_cost_usd)
  ) {
    return { stop: true, reason: TERMINATION_REASONS.BUDGET_REACHED };
  }
  if (state.identity_lock.identity_match === "UNRESOLVED" || state.identity_lock.identity_match === "FALSE") {
    return { stop: true, reason: TERMINATION_REASONS.IDENTITY_UNRESOLVED };
  }

  const openContra = state.contradictions.filter((c) => c.status === "OPEN");
  const criticalOwned = state.claims.filter(
    (c) =>
      c.relationship === "OWNED_BY" &&
      (c.status === "VERIFIED" || c.status === "HIGH") &&
      !c.unresolved &&
      c.object
  );
  if (criticalOwned.length && !openContra.length && b.verification_required_for_high_confidence) {
    if (state._verified_pass) {
      return { stop: true, reason: TERMINATION_REASONS.VERIFIED };
    }
  }

  // Contact: person+role+domain resolved with email intentionally unverified → sufficient
  if (state.lane === "CONTACT_INTELLIGENCE") {
    const personOk = state.claims.some(
      (c) => c.relationship === "PERSON_AFFILIATED_WITH" && c.object && !c.unresolved
    );
    const domainOk = state.claims.some((c) => c.relationship === "HAS_DOMAIN" && c.object);
    if (personOk && domainOk && state._verified_pass) {
      return { stop: true, reason: TERMINATION_REASONS.SUFFICIENT_EVIDENCE };
    }
  }

  // Portfolio lane satisfied
  if (
    state.lane === "OWNER_PORTFOLIO" &&
    state.claims.some((c) => c.relationship === "HAS_PORTFOLIO" && c.object) &&
    state._verified_pass
  ) {
    return { stop: true, reason: TERMINATION_REASONS.SUFFICIENT_EVIDENCE };
  }

  const openSteps = state.research_plan.filter((p) => p.status === "OPEN" || p.status === "CONTESTED");
  if (!openSteps.length && state._verified_pass) {
    return { stop: true, reason: TERMINATION_REASONS.SUFFICIENT_EVIDENCE };
  }

  // After external passes + verified critical claim: stop even if minor plan steps remain
  if (
    criticalOwned.length &&
    !openContra.length &&
    ((state._parallel_calls || 0) > 0 || (state._search_calls || 0) > 0) &&
    state._verified_pass
  ) {
    return { stop: true, reason: TERMINATION_REASONS.SUFFICIENT_EVIDENCE };
  }

  return { stop: false };
}
