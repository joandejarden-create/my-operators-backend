/**
 * Canonical research state for Dealality native iterative loop (Iteration 1).
 */

import crypto from "node:crypto";
import { NATIVE_LOOP_BOUNDS_VERSION, resolveNativeLoopBounds } from "./bounds.js";

export const RESEARCH_STATE_VERSION = "dealality-research-state-v1";

export function createResearchState(input = {}) {
  const run_id = input.run_id || `nrs_${crypto.randomBytes(8).toString("hex")}`;
  const bounds = resolveNativeLoopBounds(input);
  return {
    version: RESEARCH_STATE_VERSION,
    bounds_version: NATIVE_LOOP_BOUNDS_VERSION,
    run_id,
    started_at: new Date().toISOString(),
    completed_at: null,
    lane: input.lane || "HOTEL_OWNERSHIP",
    objective: {
      research_objective: input.research_objective || null,
      template_id: input.template_id || null,
      requested_fields: input.requested_fields || [],
      case_id: input.case_id || null,
    },
    entity: input.entity || {},
    identity_lock: {
      status: "PENDING",
      identity_match: null,
      subject: null,
      failure_reason: null,
    },
    known_facts: Array.isArray(input.known_facts) ? input.known_facts.slice() : [],
    research_plan: [],
    actions: [],
    queries: [],
    sources: [],
    evidence: [],
    claims: [],
    contradictions: [],
    open_questions: [],
    working_notes: [],
    providers_used: [],
    cost: { provider_usd: 0, external_usd: 0, total_usd: 0 },
    latency: { started_ms: Date.now(), elapsed_ms: 0 },
    iteration: 0,
    action_count: 0,
    termination_reason: null,
    bounds,
    synthesis: null,
    result: null,
  };
}

export function appendAction(state, action) {
  state.actions.push({
    action_id: `act_${state.actions.length + 1}`,
    at: new Date().toISOString(),
    iteration: state.iteration,
    ...action,
  });
  state.action_count = state.actions.length;
  return state;
}

export function touchLatency(state) {
  state.latency.elapsed_ms = Date.now() - state.latency.started_ms;
  return state;
}
