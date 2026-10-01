/**
 * Dealality native iterative research orchestrator — Iteration 2.
 * Owns planning, state, claims, contradictions, verification, synthesis.
 * Tools: internal knowledge, corpus, SerpAPI/search, Parallel, page evidence.
 * Parallel/SerpAPI are tools — never the research engine.
 */

import {
  buildCanonicalSubjectInput,
  assertSubjectIdentityMatch,
} from "../providers/parallel/subject-identity.js";
import { createResearchState, appendAction, touchLatency } from "./research-state.js";
import { createInitialPlan, refreshPlanFromState, resolveLane } from "./planner.js";
import { chooseNextAction, evaluateStop } from "./next-action.js";
import { generateQueriesForStep } from "./query-strategies.js";
import { detectContradictions, classifyContradictionHeuristics } from "./contradictions.js";
import { verifyCriticalClaims } from "./verification.js";
import { synthesizeResult } from "./synthesis.js";
import { addWorkingNote, upsertTopicNote } from "./working-notes.js";
import { TERMINATION_REASONS } from "./bounds.js";
import {
  loadOwnershipSurfaceKnowledge,
  searchPacket25Corpus,
  loadPortfolioKnowledge,
} from "./tools/internal-knowledge.js";
import { executeSearchTool } from "./tools/search-tool.js";
import { executeParallelTool } from "./tools/parallel-tool.js";
import { executePageEvidenceTool } from "./tools/page-evidence.js";

export const NATIVE_LOOP_VERSION = "dealality-native-loop-v2";

function applyIdentityLock(state) {
  const seed = {
    hotel_name: state.entity?.name,
    name: state.entity?.name,
    city: state.entity?.city,
    country: state.entity?.country,
    hotel_id: state.entity?.hotel_id || state.entity?.census_id,
    census_record_id: state.entity?.census_id,
    aliases: state.entity?.aliases || [],
    address: state.entity?.address,
    coordinates: state.entity?.coordinates,
  };
  const subject = buildCanonicalSubjectInput(seed);
  const resolved = {
    resolved_name: subject.name,
    resolved_city: subject.city,
    resolved_country: subject.country,
    resolved_coordinates: subject.coordinates,
    identity_match: subject.name && subject.country ? "TRUE" : "UNRESOLVED",
  };
  // Person contact lane: lock on person name + geography when hotel fields absent
  if (state.lane === "CONTACT_INTELLIGENCE" && state.entity?.type === "person") {
    resolved.identity_match = subject.name ? "TRUE" : "UNRESOLVED";
  }
  const gate = assertSubjectIdentityMatch(subject, resolved);
  // Soft-pass person identity when country-only seed
  if (
    !gate.ok &&
    state.lane === "CONTACT_INTELLIGENCE" &&
    state.entity?.type === "person" &&
    state.entity?.name
  ) {
    state.identity_lock = {
      status: "LOCKED",
      identity_match: "TRUE",
      subject: { ...subject, name: state.entity.name },
      failure_reason: null,
      checks: { ...(gate.checks || {}), person_soft_lock: true },
    };
    addWorkingNote(state, {
      topic: "identity",
      observation: `Person identity soft-lock for ${state.entity.name}`,
      status: "SUPPORTED",
    });
    appendAction(state, { type: "IDENTITY_LOCK", result: "TRUE" });
    return { ok: true, identity_match: "TRUE" };
  }

  state.identity_lock = {
    status: gate.ok ? "LOCKED" : "FAILED",
    identity_match: gate.identity_match,
    subject,
    failure_reason: gate.failure_reason || null,
    checks: gate.checks,
  };
  upsertTopicNote(state, {
    topic: "identity",
    current_hypothesis: `Identity ${gate.identity_match} for ${subject.name}`,
    status: gate.ok ? "SUPPORTED" : "UNRESOLVED",
    unresolved_question: gate.ok ? null : gate.failure_reason,
    next_research_path: gate.ok ? "load_internal_knowledge" : "stop",
  });
  appendAction(state, { type: "IDENTITY_LOCK", result: gate.identity_match });
  return gate;
}

async function executeAction(state, action) {
  if (action.type === "IDENTITY_LOCK") {
    return applyIdentityLock(state);
  }
  if (action.type === "LOAD_INTERNAL_KNOWLEDGE") {
    const r = loadOwnershipSurfaceKnowledge(state);
    state._internal_loaded = true;
    state.providers_used.push("dealality_internal_knowledge");
    appendAction(state, { type: "LOAD_INTERNAL_KNOWLEDGE", result: r });
    detectContradictions(state);
    refreshPlanFromState(state);
    return r;
  }
  if (action.type === "LOAD_PORTFOLIO") {
    const r = loadPortfolioKnowledge(state);
    state._portfolio_loaded = true;
    state.providers_used.push("dealality_owner_portfolio");
    appendAction(state, { type: "LOAD_PORTFOLIO", result: r });
    refreshPlanFromState(state);
    return r;
  }
  if (action.type === "SEARCH_CORPUS" || action.type === "RESOLVE_CONTRADICTION") {
    const step = action.step || { field: "contradiction", id: "q_contra" };
    const queries = generateQueriesForStep(state, step);
    const r = searchPacket25Corpus(state, queries);
    state._corpus_passes = (state._corpus_passes || 0) + 1;
    if (action.type === "RESOLVE_CONTRADICTION") state._contra_searched = true;
    state.providers_used.push("packet25_corpus");
    appendAction(state, { type: action.type, queries: queries.slice(0, 6), result: r });
    detectContradictions(state);
    for (const c of state.contradictions.filter((x) => x.status === "OPEN")) {
      classifyContradictionHeuristics(state, c);
      if ((c.heuristic_notes || []).includes("temporal_or_residual_signal")) {
        c.status = "MITIGATED_TEMPORAL";
        addWorkingNote(state, {
          topic: "contradictions",
          observation: `Contradiction mitigated as temporal/residual: ${c.summary}`,
          status: "SUPPORTED",
        });
      }
    }
    refreshPlanFromState(state);
    return r;
  }
  if (action.type === "SEARCH_WEB") {
    const r = await executeSearchTool(state, action);
    if (action.reason === "contradiction_driven_external") state._external_contra_pass = true;
    appendAction(state, {
      type: "SEARCH_WEB",
      reason: action.reason,
      reasons: action.reasons,
      result: r,
    });
    detectContradictions(state);
    refreshPlanFromState(state);
    return r;
  }
  if (action.type === "USE_PARALLEL") {
    const r = await executeParallelTool(state, action);
    if (action.reason === "contradiction_driven_external") state._external_contra_pass = true;
    appendAction(state, {
      type: "USE_PARALLEL",
      reason: action.reason,
      reasons: action.reasons,
      result: r,
    });
    detectContradictions(state);
    refreshPlanFromState(state);
    return r;
  }
  if (action.type === "FETCH_PAGE_EVIDENCE") {
    const r = await executePageEvidenceTool(state, action);
    appendAction(state, { type: "FETCH_PAGE_EVIDENCE", result: r });
    refreshPlanFromState(state);
    return r;
  }
  if (action.type === "VERIFY_CRITICAL" || action.type === "VERIFY_THEN_STOP") {
    const r = verifyCriticalClaims(state);
    appendAction(state, { type: "VERIFY_CRITICAL", result: r });
    refreshPlanFromState(state);
    return r;
  }
  if (action.type === "STOP") {
    state.termination_reason = action.termination_reason;
    appendAction(state, { type: "STOP", termination_reason: action.termination_reason });
    return action;
  }
  appendAction(state, { type: action.type, result: "noop" });
  return null;
}

/**
 * @param {object} input
 * @returns {Promise<object>}
 */
export async function runNativeIterativeResearch(input = {}) {
  const entity = input.entity || {
    type: "hotel",
    name: input.hotel_name || input.hotel_seed?.hotel_name,
    hotel_id: input.hotel_id || input.hotel_seed?.hotel_id,
    census_id: input.census_id || input.hotel_seed?.airtable_record_id,
    country: input.country || input.hotel_seed?.country,
    city: input.city || input.hotel_seed?.city,
    aliases: input.aliases || input.hotel_seed?.former_names || [],
    owner_id: input.owner_id,
  };

  const state = createResearchState({
    ...input,
    entity,
    lane: resolveLane(input),
    research_objective: input.research_objective,
    template_id: input.template_id,
    requested_fields: input.requested_fields || ["economic_owner", "propco", "operator"],
    known_facts: input.known_facts || [],
    case_id: input.case_id,
    bounds: input.bounds,
  });

  // Ensure cost.external_usd exists
  state.cost.external_usd = state.cost.external_usd || 0;

  createInitialPlan(state);

  while (true) {
    state.iteration += 1;
    const stopEarly = evaluateStop(state);
    if (stopEarly.stop) {
      state.termination_reason = stopEarly.reason;
      break;
    }

    const action = chooseNextAction(state);
    if (action.type === "STOP") {
      await executeAction(state, action);
      break;
    }

    await executeAction(state, action);

    if (action.type === "VERIFY_THEN_STOP" || action.type === "VERIFY_CRITICAL") {
      const after = evaluateStop(state);
      if (after.stop || action.type === "VERIFY_THEN_STOP") {
        state.termination_reason =
          after.reason ||
          (state.claims.some((c) => c.status === "VERIFIED")
            ? TERMINATION_REASONS.VERIFIED
            : TERMINATION_REASONS.INSUFFICIENT_EVIDENCE);
        break;
      }
    }

    if (state.action_count >= state.bounds.max_actions) {
      state.termination_reason = TERMINATION_REASONS.MAX_ACTIONS;
      break;
    }
  }

  if (!state._verified_pass) verifyCriticalClaims(state);
  if (!state.termination_reason) {
    state.termination_reason = TERMINATION_REASONS.NO_MORE_HIGH_VALUE_ACTIONS;
  }

  touchLatency(state);
  state.completed_at = new Date().toISOString();
  const synthesis = synthesizeResult(state);

  const externalActions = state.actions.filter((a) =>
    ["SEARCH_WEB", "USE_PARALLEL", "FETCH_PAGE_EVIDENCE"].includes(a.type)
  );

  return {
    ok: true,
    version: NATIVE_LOOP_VERSION,
    run_id: state.run_id,
    termination_reason: state.termination_reason,
    result: synthesis.machine.result,
    claims: state.claims,
    evidence: state.evidence,
    sources: state.sources,
    contradictions: state.contradictions,
    confidence: synthesis.machine.confidence,
    unresolved: synthesis.machine.unresolved,
    human_output: synthesis.human,
    audit: {
      ...synthesis.machine.audit,
      external_actions: externalActions.map((a) => ({
        type: a.type,
        reason: a.reason,
        reasons: a.reasons,
        material_improvement: a.result?.material_improvement,
        mode: a.result?.mode,
        cost_usd: a.result?.cost_usd,
      })),
      providers_used: [...new Set(state.providers_used)],
      parallel_request: state._last_parallel_request || null,
    },
    state,
    claim_handoff: { auto_promote: false },
    webhound_network_calls: 0,
  };
}

export default runNativeIterativeResearch;
