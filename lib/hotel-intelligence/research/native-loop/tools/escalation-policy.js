/**
 * External-tool escalation policy — Iteration 2.
 * Parallel / SerpAPI / page fetch are tools; Dealality owns the loop.
 */

export const ESCALATION_POLICY_VERSION = "native-escalation-policy-v1";

function hasVerifiedOwner(state) {
  return state.claims.some(
    (c) =>
      (c.relationship === "OWNED_BY" || c.relationship === "ECONOMIC_OWNER") &&
      ["VERIFIED", "HIGH"].includes(c.status) &&
      c.object &&
      !c.unresolved
  );
}

function singleSourceOwnership(state) {
  const owners = state.claims.filter(
    (c) =>
      (c.relationship === "OWNED_BY" || c.relationship === "ECONOMIC_OWNER") &&
      c.object &&
      c.status !== "REJECTED"
  );
  if (!owners.length) return false;
  return owners.every((c) => (c.evidence_ids || []).length <= 1);
}

function ownerOperatorAmbiguity(state) {
  const owners = new Set(
    state.claims
      .filter((c) => c.relationship === "OWNED_BY" && c.object)
      .map((c) => String(c.object).toLowerCase())
  );
  const ops = new Set(
    state.claims
      .filter((c) => c.relationship === "OPERATED_BY" && c.object)
      .map((c) => String(c.object).toLowerCase())
  );
  for (const o of owners) {
    if (ops.has(o)) return true;
  }
  return state.contradictions.some((c) => c.status === "OPEN");
}

function contactGaps(state) {
  if (state.lane !== "CONTACT_INTELLIGENCE") return false;
  const person = state.claims.find((c) => c.relationship === "PERSON_AFFILIATED_WITH" && c.object);
  const email = state.claims.find((c) => c.relationship === "HAS_EMAIL" && c.object && !c.unresolved);
  const domain = state.claims.find((c) => c.relationship === "HAS_DOMAIN" && c.object);
  const role = state.claims.find((c) => c.relationship === "HAS_ROLE" && c.object);
  if (!person) return true;
  if (!domain || !role) return true;
  // Email missing is a gap but must not force paid enrichment alone
  if (!email && (state.objective?.requested_fields || []).includes("email")) return true;
  return false;
}

/**
 * @returns {{ escalate: boolean, reasons: string[], preferred_tool: 'SEARCH_WEB'|'USE_PARALLEL'|'NONE', priority: number }}
 */
export function evaluateEscalation(state) {
  const reasons = [];
  const b = state.bounds || {};
  const externalOff =
    b.allow_external === false ||
    String(b.external_mode || process.env.HI_NATIVE_EXTERNAL_MODE || "auto").toLowerCase() === "off";

  if (externalOff) {
    return { escalate: false, reasons: ["external_disabled"], preferred_tool: "NONE", priority: 0 };
  }

  const openSteps = (state.research_plan || []).filter(
    (p) => p.status === "OPEN" || p.status === "CONTESTED" || p.status === "PARTIAL"
  );
  const openContra = (state.contradictions || []).filter((c) => c.status === "OPEN");
  const wantsOwner = (state.objective?.requested_fields || []).some((f) =>
    /owner|propco|economic/i.test(String(f))
  );

  if (openContra.length) reasons.push("contradiction_unresolved");
  if (wantsOwner && !hasVerifiedOwner(state)) reasons.push("required_field_unresolved");
  if (singleSourceOwnership(state)) reasons.push("single_source_ownership");
  // Same entity as both owner and operator is common in hospitality — not an escalation trigger alone
  const owners = new Set(
    state.claims
      .filter((c) => c.relationship === "OWNED_BY" && c.object && !c.unresolved)
      .map((c) => String(c.object).toLowerCase())
  );
  const ops = new Set(
    state.claims
      .filter((c) => c.relationship === "OPERATED_BY" && c.object && !c.unresolved)
      .map((c) => String(c.object).toLowerCase())
  );
  let ownerOpAmbiguity = state.contradictions.some((c) => c.status === "OPEN");
  for (const o of owners) {
    // Only flag when operator differs OR open contradiction — not when owner===operator
    for (const op of ops) {
      if (op !== o) ownerOpAmbiguity = true;
    }
  }
  if (ownerOpAmbiguity) reasons.push("owner_operator_ambiguity");
  if (contactGaps(state)) reasons.push("contact_role_or_domain_gap");
  if (openSteps.some((s) => s.field === "verification" || s.field === "alternatives")) {
    reasons.push("planner_open_question");
  }
  if ((state._corpus_passes || 0) >= 2 && openSteps.length && !hasVerifiedOwner(state)) {
    reasons.push("internal_evidence_insufficient");
  }
  if (state.lane === "OPEN_RESEARCH" && openSteps.length) {
    reasons.push("multi_step_open");
  }

  // Do not escalate merely because providers exist
  if (!reasons.length) {
    return { escalate: false, reasons: ["no_escalation_trigger"], preferred_tool: "NONE", priority: 0 };
  }

  const searchCalls = state._search_calls || 0;
  const parallelCalls = state._parallel_calls || 0;
  const maxSearch = Number(b.max_search_calls ?? 4);
  const maxParallel = Number(b.max_parallel_calls ?? 1);

  // Prefer low-cost search first; Parallel when contradiction / hard ownership / contact after search
  const hard =
    reasons.includes("contradiction_unresolved") ||
    reasons.includes("single_source_ownership") ||
    reasons.includes("owner_operator_ambiguity") ||
    (state.lane === "OPEN_RESEARCH" && reasons.includes("multi_step_open"));

  if (searchCalls < maxSearch && (!hard || searchCalls === 0)) {
    return { escalate: true, reasons, preferred_tool: "SEARCH_WEB", priority: 70 };
  }
  if (parallelCalls < maxParallel && (hard || searchCalls >= Math.min(2, maxSearch))) {
    return { escalate: true, reasons, preferred_tool: "USE_PARALLEL", priority: 85 };
  }
  if (searchCalls < maxSearch) {
    return { escalate: true, reasons, preferred_tool: "SEARCH_WEB", priority: 60 };
  }

  return { escalate: false, reasons: [...reasons, "external_call_budget_exhausted"], preferred_tool: "NONE", priority: 0 };
}

export function remainingExternalBudget(state) {
  const b = state.bounds || {};
  const maxExt = Number(b.max_external_cost_usd ?? 0);
  const spent = Number(state.cost?.external_usd || 0);
  return Math.max(0, maxExt - spent);
}
