/**
 * Gap-driven external research request compiler — Iteration 2.
 * Never emit generic "Find the owner of Hotel X."
 */

import {
  buildCanonicalSubjectInput,
  buildSubjectIdentityLockText,
} from "../../providers/parallel/subject-identity.js";

export const GAP_REQUEST_COMPILER_VERSION = "native-gap-request-compiler-v1";

function hotelSubject(state) {
  const ent = state.entity || {};
  const lock = state.identity_lock?.subject || {};
  return buildCanonicalSubjectInput({
    hotel_name: lock.name || ent.name,
    name: lock.name || ent.name,
    city: lock.city || ent.city,
    country: lock.country || ent.country,
    address: lock.address || ent.address,
    coordinates: lock.coordinates || ent.coordinates,
    hotel_id: ent.hotel_id || lock.canonical_id,
    census_record_id: ent.census_id || lock.census_record_id,
    aliases: [...(ent.aliases || []), ...(lock.aliases || [])],
    brand: ent.brand || null,
  });
}

function candidateOwners(state) {
  return state.claims
    .filter(
      (c) =>
        /OWNED_BY|ECONOMIC_OWNER|PROPCO/.test(c.relationship || "") &&
        c.object &&
        c.status !== "REJECTED"
    )
    .map((c) => ({
      object: c.object,
      relationship: c.relationship,
      status: c.status,
      evidence_count: (c.evidence_ids || []).length,
    }));
}

function operators(state) {
  return state.claims
    .filter((c) => /OPERATED_BY|MANAGED_BY/.test(c.relationship || "") && c.object)
    .map((c) => c.object);
}

function openQuestions(state) {
  const fromPlan = (state.research_plan || [])
    .filter((p) => p.status === "OPEN" || p.status === "CONTESTED" || p.status === "PARTIAL")
    .map((p) => p.question);
  const fromContra = (state.contradictions || [])
    .filter((c) => c.status === "OPEN")
    .map((c) => c.research_query || c.summary);
  const fromNotes = (state.working_notes || [])
    .filter((n) => n.open_question)
    .map((n) => n.open_question);
  return [...new Set([...fromPlan, ...fromContra, ...fromNotes])].slice(0, 14);
}

function knownFacts(state) {
  const facts = [];
  for (const c of state.claims) {
    if (!c.object || c.status === "REJECTED") continue;
    if (c.unresolved) {
      facts.push(`CANDIDATE (unverified): ${c.relationship} → ${c.object}`);
    } else {
      facts.push(`${c.relationship} → ${c.object} [${c.status}]`);
    }
  }
  for (const f of state.known_facts || []) {
    facts.push(typeof f === "string" ? f : JSON.stringify(f));
  }
  return facts.slice(0, 20);
}

function negativeScreens(state) {
  const screens = [
    "Do not equate brand affiliation with ownership.",
    "Do not equate operator / management company with economic owner.",
    "Do not treat lender or developer as deed owner without explicit evidence.",
    "Reject evidence about a differently named / differently located hotel.",
    "False matches are worse than UNKNOWN — return unresolved if identity or ownership cannot be verified.",
  ];
  for (const op of operators(state)) {
    screens.push(`Operator exclusion / do not treat as owner without deed evidence: ${op}`);
  }
  if (state.lane === "CONTACT_INTELLIGENCE") {
    screens.push("Never invent or infer email as VERIFIED; mark inferred separately.");
    screens.push("Separate person identified / role verified / email discovered / email verified.");
  }
  return screens;
}

/**
 * Build gap-aware Parallel / deep-research task from native research state.
 */
export function compileGapDrivenParallelRequest(state, opts = {}) {
  const subject = hotelSubject(state);
  // Contact lane may be a person entity — still lock company geography when present
  if (state.lane === "CONTACT_INTELLIGENCE" && state.entity?.type === "person") {
    subject.name = state.entity.name;
    subject.aliases = state.entity.aliases || [];
  }

  const identityLock = buildSubjectIdentityLockText(subject);
  const candidates = candidateOwners(state);
  const questions = openQuestions(state);
  const facts = knownFacts(state);
  const screens = negativeScreens(state);
  const contra = (state.contradictions || []).filter((c) => c.status === "OPEN");

  const lines = [
    "Dealality gap-driven research request (not a generic ownership search).",
    "",
    `Lane: ${state.lane}`,
    `Objective: ${state.objective?.research_objective || "Resolve open research gaps"}`,
    "",
    "IDENTITY IS LOCKED TO THE SUBJECT BELOW. Research only this entity.",
    identityLock,
    "",
    "CURRENT CANDIDATE CLAIMS (test; do not assume true):",
    candidates.length
      ? candidates.map((c) => `- ${c.relationship} ${c.object} (${c.status}, ${c.evidence_count} evidence)`).join("\n")
      : "- None yet",
    "",
    "KNOWN FACTS FROM PRIOR NATIVE RESEARCH:",
    facts.length ? facts.map((f) => `- ${f}`).join("\n") : "- None",
    "",
    "OPEN QUESTIONS TO RESOLVE:",
    questions.length ? questions.map((q) => `- ${q}`).join("\n") : "- Resolve primary requested fields",
    "",
    contra.length
      ? `OPEN CONTRADICTIONS:\n${contra.map((c) => `- ${c.summary}`).join("\n")}\nResearch specifically to resolve whether ownership/operator changed temporally or sources conflict.`
      : "No open contradictions recorded.",
    "",
    "REQUIRED EVIDENCE STANDARD:",
    "- Prefer official company, regulatory/government, transaction announcements, lender filings, reputable hospitality/business press.",
    "- Snippets alone are not decisive when the underlying page can be inspected.",
    "- Distinguish owner vs operator vs management company vs brand.",
    "- If ownership cannot be independently verified for this exact property, return UNRESOLVED.",
    "- Return supporting AND contradicting evidence.",
    "",
    "NEGATIVE SCREENS:",
    screens.map((s) => `- ${s}`).join("\n"),
    "",
    "OUTPUT: structured findings with sources, claim status/confidence, contradictions, and unresolved items. Do not invent contacts.",
  ];

  const research_objective = lines.join("\n");

  return {
    compiler_version: GAP_REQUEST_COMPILER_VERSION,
    subject,
    identity_lock: identityLock,
    research_objective,
    known_facts: facts,
    unresolved_questions: questions,
    negative_screens: screens,
    candidate_owners: candidates,
    operators: operators(state),
    contradictions: contra.map((c) => ({ summary: c.summary, research_query: c.research_query })),
    template_id: state.objective?.template_id || opts.template_id || "FULL_HOTEL_INTELLIGENCE",
    include_contacts: state.lane === "CONTACT_INTELLIGENCE" || opts.include_contacts === true,
    attempt_reason: (opts.reasons || []).join(",") || "gap_escalation",
  };
}

/**
 * Build targeted search queries from research state gaps (lane-aware).
 */
export function compileGapDrivenSearchQueries(state, step = null) {
  const subject = hotelSubject(state);
  const hotel = subject.name || state.entity?.name || "";
  const loc = [subject.city, subject.country].filter(Boolean).join(" ");
  const base = `${hotel} ${loc}`.trim();
  const candidates = candidateOwners(state);
  const queries = [];

  if (state.lane === "CONTACT_INTELLIGENCE") {
    const person = state.entity?.name || hotel;
    const aff =
      state.claims.find((c) => c.relationship === "PERSON_AFFILIATED_WITH")?.object ||
      "Dovetail";
    queries.push(
      `${person} ${aff}`,
      `${person} ${aff} CEO`,
      `${aff} leadership team`,
      `${aff} official site contact`,
      `site:linkedin.com ${person} ${aff}`
    );
  } else if (state.lane === "OWNER_PORTFOLIO") {
    const org = state.entity?.name || candidates[0]?.object || hotel;
    queries.push(
      `${org} hotel portfolio`,
      `${org} owned hotels`,
      `${org} subsidiaries hotels`,
      `${org} annual report hotels`
    );
  } else {
    queries.push(
      `${base} owner`,
      `${base} acquisition`,
      `${base} sold`,
      `${base} ownership LLC`,
      `${base} developer`,
      `${base} financing`,
      `${base} portfolio`,
      `${base} management company`
    );
    for (const c of candidates.slice(0, 2)) {
      queries.push(
        `${c.object} ${hotel}`,
        `${c.object} acquisition ${hotel}`,
        `${c.object} portfolio ${loc}`
      );
    }
    for (const c of (state.contradictions || []).filter((x) => x.status === "OPEN")) {
      if (c.research_query) queries.push(c.research_query);
    }
  }

  if (step?.field === "dispute" || step?.field === "residual") {
    queries.push(
      `${base} operator`,
      `${base} managed by`,
      `${base} Pyramid`,
      `${base} Benchmark Hospitality`,
      `${base} Dovetail self operated`
    );
  }

  const deduped = [];
  const seen = new Set();
  for (const q of queries) {
    const k = String(q).toLowerCase().replace(/\s+/g, " ").trim();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    deduped.push(q);
  }
  return deduped.slice(0, 10);
}
