/**
 * Parallel-as-tool — Iteration 2.
 * Native loop decides USE_PARALLEL; Parallel output never becomes final answer directly.
 */

import { addEvidence, upsertClaim } from "../claim-graph.js";
import { addWorkingNote, upsertTopicNote } from "../working-notes.js";
import { compileGapDrivenParallelRequest } from "./compile-gap-request.js";
import { getFixtureBundle, resolveExternalMode } from "./fixture-external.js";
import { remainingExternalBudget } from "./escalation-policy.js";

export const PARALLEL_TOOL_VERSION = "native-parallel-tool-v1";

function ingestParallelClaims(state, claims = [], meta = {}) {
  const added = [];
  for (const c of claims) {
    const ev = addEvidence(state, {
      url: c.url || null,
      source_type: c.source_type || "parallel_research",
      authority_tier: c.authority_tier || "supporting",
      excerpt: c.excerpt || c.text || "",
      title: c.title || `Parallel ${c.relationship}`,
      entity_association: "ASSUMED_SUBJECT",
      provider: meta.provider || "parallel",
      retrieval_timestamp: new Date().toISOString(),
    });
    const row = upsertClaim(state, {
      subject: c.subject || state.entity?.name,
      relationship: c.relationship,
      object: c.object,
      field: c.field || null,
      status: c.status || "CANDIDATE",
      confidence: c.confidence,
      evidence_ids: [ev.evidence_id],
      unresolved: c.unresolved === true || c.status === "UNRESOLVED",
      unknown_code: c.unknown_code || null,
      inference: false,
    });
    added.push(row.claim_id);
  }
  return added;
}

function ingestContradictions(state, rows = []) {
  for (const row of rows) {
    const already = state.contradictions.some((c) => c.summary === row.summary);
    if (already) continue;
    state.contradictions.push({
      contradiction_id: `contra_ext_${state.contradictions.length + 1}`,
      summary: row.summary,
      claim_ids: [],
      hypotheses: [row.hypothesis || "unresolved_conflict"],
      status: "OPEN",
      research_query: row.research_query || row.summary,
      source: "parallel_tool",
    });
  }
}

async function liveParallel(state, compiled) {
  const budget = remainingExternalBudget(state);
  const maxExt = Number(state.bounds?.max_external_cost_usd ?? 0);
  if (maxExt <= 0 && state.bounds?.force_live !== true) {
    return { ok: false, reason: "EXTERNAL_COST_CAP_ZERO", cost_usd: 0 };
  }
  if (budget <= 0 && maxExt > 0) {
    return { ok: false, reason: "EXTERNAL_BUDGET", cost_usd: 0 };
  }

  const authorized =
    state.bounds?.confirm_spend === true ||
    process.env.HI_NATIVE_EXTERNAL_CONFIRM_SPEND === "1";
  if (!authorized) {
    return { ok: false, reason: "CONFIRM_SPEND_REQUIRED", cost_usd: 0 };
  }

  try {
    const { createParallelProvider } = await import("../../providers/parallel/provider.js");
    const { compileParallelTask } = await import("../../providers/parallel/compile-parallel-task.js");

    // Enrich Parallel compiler with gap-driven objective + identity
    const provider = createParallelProvider({ force_enabled: true });
    if (!provider.isAvailable() && !process.env.HI_NATIVE_PARALLEL_FORCE) {
      return { ok: false, reason: "PARALLEL_UNAVAILABLE", cost_usd: 0 };
    }

    const job = compileParallelTask({
      template_id: compiled.template_id,
      hotel_seed: {
        hotel_name: compiled.subject.name,
        city: compiled.subject.city,
        country: compiled.subject.country,
        address: compiled.subject.address,
        coordinates: compiled.subject.coordinates,
        airtable_record_id: compiled.subject.census_record_id,
        aliases: compiled.subject.aliases,
        former_names: compiled.subject.aliases,
      },
      known_facts: compiled.known_facts,
      unresolved_questions: compiled.unresolved_questions,
      negative_screens: compiled.negative_screens,
      include_contacts: compiled.include_contacts,
      attempt_reason: compiled.attempt_reason,
      blind_mode: false,
    });

    // Override research objective with gap-driven text (exact-match / owner-vs-operator contract)
    if (job.parallel_input) {
      job.parallel_input.research_objective = compiled.research_objective;
      job.parallel_input.identity_lock = compiled.identity_lock;
      job.parallel_input.known_facts = compiled.known_facts;
      job.parallel_input.unresolved_questions = compiled.unresolved_questions;
      job.parallel_input.negative_screens = compiled.negative_screens;
    }

    const started = await provider.start({
      template_id: compiled.template_id,
      hotel_seed: {
        hotel_name: compiled.subject.name,
        city: compiled.subject.city,
        country: compiled.subject.country,
        address: compiled.subject.address,
        coordinates: compiled.subject.coordinates,
        airtable_record_id: compiled.subject.census_record_id,
        aliases: compiled.subject.aliases,
      },
      known_facts: compiled.known_facts,
      unresolved_questions: compiled.unresolved_questions,
      include_contacts: compiled.include_contacts,
      authorized: true,
      confirm_spend: true,
      explicit_user_action: true,
      budget_usd: Math.min(budget || maxExt, maxExt || budget || 5),
      attempt_reason: compiled.attempt_reason,
    });

    const collected = await provider.collectCompleted(started.provider_run_id, {
      timeout_ms: Number(state.bounds?.parallel_timeout_ms || 600_000),
    });

    const cost = Number(collected.provider_cost_usd || 0);
    return {
      ok: collected.status === "COMPLETED" || collected.status === "SUCCEEDED",
      cost_usd: cost,
      raw: collected,
      compiled_gap: compiled,
      mode: "live",
      reason: collected.error?.code || null,
    };
  } catch (err) {
    return { ok: false, reason: err.code || err.message, cost_usd: 0 };
  }
}

function fixtureParallel(state, compiled) {
  const bundle = getFixtureBundle(state.objective?.case_id);
  if (!bundle?.parallel) {
    return { ok: false, reason: "NO_FIXTURE", cost_usd: 0, compiled_gap: compiled };
  }
  return {
    ok: true,
    cost_usd: 0,
    mode: "fixture",
    compiled_gap: compiled,
    fixture: bundle.parallel,
    reason: null,
  };
}

/**
 * Execute USE_PARALLEL tool under native control.
 */
export async function executeParallelTool(state, action = {}) {
  const compiled = compileGapDrivenParallelRequest(state, {
    reasons: action.reasons || [],
    include_contacts: state.lane === "CONTACT_INTELLIGENCE",
  });

  // Persist compiled request for audit
  state._last_parallel_request = {
    subject: compiled.subject,
    attempt_reason: compiled.attempt_reason,
    unresolved_questions: compiled.unresolved_questions,
    candidate_owners: compiled.candidate_owners,
    research_objective_preview: compiled.research_objective.slice(0, 800),
  };

  addWorkingNote(state, {
    topic: "ownership",
    observation: `Compiled gap-driven Parallel request (${compiled.attempt_reason}) with ${compiled.unresolved_questions.length} open questions`,
    status: "OBSERVED",
    open_question: compiled.unresolved_questions[0] || null,
  });

  const mode = resolveExternalMode(state);
  let result = { ok: false, cost_usd: 0, reason: "SKIPPED", compiled_gap: compiled };

  if (mode === "live" || (mode === "auto" && Number(state.bounds?.max_external_cost_usd || 0) > 0 && state.bounds?.confirm_spend)) {
    result = await liveParallel(state, compiled);
  }
  if (!result.ok && mode !== "live" && mode !== "off") {
    result = fixtureParallel(state, compiled);
  }

  state._parallel_calls = (state._parallel_calls || 0) + 1;
  state.providers_used.push(result.mode === "fixture" ? "parallel_fixture" : "PARALLEL");
  state.cost.external_usd = (state.cost.external_usd || 0) + (result.cost_usd || 0);
  state.cost.provider_usd = (state.cost.provider_usd || 0) + (result.cost_usd || 0);
  state.cost.total_usd = (state.cost.total_usd || 0) + (result.cost_usd || 0);

  let claimIds = [];
  let material = false;

  if (result.fixture) {
    claimIds = ingestParallelClaims(state, result.fixture.claims || [], { provider: "parallel_fixture" });
    ingestContradictions(state, result.fixture.contradictions || []);
    material = Boolean(result.fixture.material_improvement) && claimIds.length > 0;
    upsertTopicNote(state, {
      topic: state.lane === "CONTACT_INTELLIGENCE" ? "contacts" : "ownership",
      current_hypothesis: result.fixture.notes,
      strongest_support: result.fixture.claims?.[0]?.excerpt || null,
      strongest_contradiction: result.fixture.contradictions?.[0]?.summary || null,
      unresolved_question: compiled.unresolved_questions[0] || null,
      next_research_path: "verify_then_decide",
      status: material ? "SUPPORTED" : "OBSERVED",
      supporting_evidence_ids: state.evidence.slice(-claimIds.length).map((e) => e.evidence_id),
    });
  } else if (result.raw?.raw_artifact || result.raw?.normalized) {
    // Live path: best-effort claim extraction from normalized artifact if present
    const art = result.raw.normalized || result.raw.raw_artifact || {};
    const findings = art.findings || art.claims || [];
    const mapped = findings.slice(0, 8).map((f) => ({
      relationship: f.relationship || "RESEARCH_FINDING",
      object: f.object || f.headline || null,
      status: f.status || "CANDIDATE",
      confidence: f.confidence || "LOW",
      excerpt: f.body || f.text || f.headline || "",
      url: (f.sources && f.sources[0]) || null,
      authority_tier: "supporting",
    }));
    claimIds = ingestParallelClaims(state, mapped, { provider: "PARALLEL" });
    material = claimIds.length > 0;
  }

  addWorkingNote(state, {
    topic: "unresolved",
    observation: `USE_PARALLEL ${result.ok ? "ok" : "miss"} mode=${result.mode || mode}; material=${material}; ${result.reason || ""}`,
    status: material ? "SUPPORTED" : "OBSERVED",
  });

  return {
    ok: result.ok,
    mode: result.mode || mode,
    cost_usd: result.cost_usd || 0,
    claims_added: claimIds.length,
    material_improvement: material,
    reason: result.reason,
    compiled_preview: state._last_parallel_request,
  };
}
