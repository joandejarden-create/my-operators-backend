/**
 * Dealality-owned synthesis from research state — Iteration 1.
 */

export const SYNTHESIS_VERSION = "native-synthesis-v1";

function bestClaim(state, relationships) {
  const rels = Array.isArray(relationships) ? relationships : [relationships];
  const ranked = state.claims
    .filter(
      (c) =>
        rels.includes(c.relationship) &&
        c.status !== "REJECTED" &&
        c.unknown_code !== "UNKNOWN_OWNER"
    )
    .sort((a, b) => score(b) - score(a));
  // Prefer non-unresolved
  const verified = ranked.find((c) => !c.unresolved && c.status !== "UNRESOLVED");
  return verified || ranked[0] || null;
}

function score(c) {
  const s = { VERIFIED: 5, HIGH: 4, PROBABLE: 3, CONTESTED: 2, CANDIDATE: 1, UNRESOLVED: 0, ANNOUNCED: 2 };
  return (s[c.status] || 0) + (c.evidence_ids?.length || 0) * 0.1;
}

function evidenceLines(state, claim) {
  if (!claim) return [];
  return (claim.evidence_ids || [])
    .map((id) => state.evidence.find((e) => e.evidence_id === id))
    .filter(Boolean)
    .map((e) => ({
      url: e.url,
      source_type: e.source_type,
      excerpt: String(e.excerpt || "").slice(0, 280),
      authority_tier: e.authority_tier,
    }));
}

export function synthesizeResult(state) {
  const owner = bestClaim(state, ["OWNED_BY", "ECONOMIC_OWNER"]);
  const propco = bestClaim(state, ["PROPCO"]);
  const operator = bestClaim(state, ["OPERATED_BY"]);
  const person = bestClaim(state, ["PERSON_AFFILIATED_WITH"]);
  const portfolio = bestClaim(state, ["HAS_PORTFOLIO", "REGISTERED_BUSINESS"]);

  let finding = "INSUFFICIENT_EVIDENCE";
  let confidence = "UNRESOLVED";
  let unknown_code = null;

  if (state.lane === "CONTACT_INTELLIGENCE") {
    const role = bestClaim(state, ["HAS_ROLE"]);
    const domain = bestClaim(state, ["HAS_DOMAIN"]);
    const email = bestClaim(state, ["HAS_EMAIL"]);
    const parts = [];
    if (person && !person.unresolved) {
      parts.push(`${person.subject} affiliated with ${person.object}`);
    }
    if (role?.object) parts.push(`role=${role.object}`);
    if (domain?.object) parts.push(`domain=${domain.object}`);
    if (email?.object && !email.unresolved) {
      parts.push(`email=${email.object} (${email.status})`);
    } else {
      parts.push("email=UNVERIFIED");
    }
    if (person && !person.unresolved) {
      finding = parts.join("; ");
      confidence = person.confidence || person.status || "MEDIUM";
      // Email still unknown is correct — do not invent; keep unknown_code for email gap
      if (!email?.object || email.unresolved) {
        unknown_code = "UNVERIFIED_CONTACT";
      }
    } else {
      finding = "UNVERIFIED_CONTACT";
      unknown_code = "UNVERIFIED_CONTACT";
      confidence = "UNRESOLVED";
    }
  } else if (state.lane === "OPEN_RESEARCH") {
    const formerOp = bestClaim(state, ["FORMERLY_OPERATED_BY"]);
    if (owner && owner.object && !owner.unresolved) {
      finding = `${state.entity?.name || "Hotel"} OWNED_BY ${owner.object}`;
      confidence = owner.confidence || owner.status || "MEDIUM";
      if (operator?.object) finding += `; current operator assessment: ${operator.object} (${operator.status})`;
      if (formerOp?.object) {
        finding += `; historical/announced operator: ${formerOp.object}`;
      }
      const openOpContra = (state.contradictions || []).filter(
        (c) => /OPERATED_BY|operator/i.test(c.summary) && c.status === "OPEN"
      );
      if (openOpContra.length) {
        finding += "; residual operator uncertainty retained";
        confidence = confidence === "HIGH" || confidence === "VERIFIED" ? "MEDIUM" : confidence;
      }
    } else if (owner?.unresolved && owner?.object) {
      finding = `UNRESOLVED_OWNERSHIP (candidate ${owner.object} not verified)`;
      unknown_code = "UNRESOLVED_OWNERSHIP";
      confidence = "UNRESOLVED";
    }
  } else if (state.lane === "OWNER_PORTFOLIO") {
    if (portfolio) {
      finding = `Portfolio evidence assembled for ${state.entity?.name}`;
      confidence = portfolio.confidence || "MEDIUM";
    } else {
      finding = "INSUFFICIENT_EVIDENCE";
      unknown_code = "INSUFFICIENT_EVIDENCE";
    }
  } else if (owner?.unknown_code === "UNKNOWN_OWNER" || (owner?.unresolved && !owner?.object)) {
    finding = "UNKNOWN_OWNER";
    unknown_code = "UNKNOWN_OWNER";
    confidence = "UNRESOLVED";
  } else if (owner?.unresolved && owner?.object) {
    finding = `UNRESOLVED_OWNERSHIP (candidate ${owner.object} not verified)`;
    unknown_code = "UNRESOLVED_OWNERSHIP";
    confidence = "UNRESOLVED";
  } else if (owner && owner.object && !owner.unresolved) {
    finding = `${state.entity?.name || "Hotel"} OWNED_BY ${owner.object}`;
    confidence = owner.confidence || owner.status || "MEDIUM";
    if (propco?.object) finding += `; PropCo ${propco.object}`;
    if (operator?.object) finding += `; OPERATED_BY ${operator.object}`;
  } else if (operator?.object && !owner) {
    finding = `Operator evidenced (${operator.object}); economic owner UNRESOLVED`;
    unknown_code = "UNRESOLVED_OWNERSHIP";
    confidence = "LOW";
  }

  const contradictions = state.contradictions.map((c) => ({
    summary: c.summary,
    status: c.status,
    claim_ids: c.claim_ids,
  }));

  const unresolved = [
    ...state.open_questions,
    ...state.research_plan
      .filter((p) => p.status === "OPEN" || p.status === "CONTESTED")
      .map((p) => ({ code: "OPEN_PLAN_STEP", question: p.question })),
    ...state.claims.filter((c) => c.unresolved).map((c) => ({
      code: c.unknown_code || "UNRESOLVED_CLAIM",
      question: `${c.relationship} for ${c.subject}`,
    })),
  ];

  const why = evidenceLines(state, owner || operator || person || portfolio);

  const human = [
    `## Finding`,
    finding,
    ``,
    `## Confidence`,
    String(confidence),
    ``,
    `## Why`,
    why.length ? why.map((e) => `- (${e.authority_tier}) ${e.excerpt}`).join("\n") : "- No strong supporting evidence retained.",
    ``,
    `## Evidence`,
    why.length
      ? why.map((e) => `- ${e.url || "internal"} — ${e.source_type}`).join("\n")
      : "- None",
    ``,
    contradictions.length ? `## Contradictions / Caveats\n${contradictions.map((c) => `- ${c.summary}`).join("\n")}` : "",
    ``,
    unresolved.length
      ? `## What remains unresolved\n${unresolved
          .slice(0, 8)
          .map((u) => `- ${u.question || u.code}`)
          .join("\n")}`
      : "",
    ``,
    `## Research trail`,
    `- Iterations: ${state.iteration}`,
    `- Actions: ${state.actions.map((a) => a.type).join(" → ")}`,
    `- Queries: ${state.queries.length}`,
    `- Working notes: ${state.working_notes.length}`,
    `- Providers/tools: ${(state.providers_used || []).join(", ") || "internal"}`,
  ]
    .filter(Boolean)
    .join("\n");

  const machine = {
    result: {
      finding,
      confidence,
      unknown_code,
      owner: owner?.object || null,
      propco: propco?.object || null,
      operator: operator?.object || null,
      person: person ? { name: person.subject, org: person.object } : null,
      role: bestClaim(state, ["HAS_ROLE"])?.object || null,
      domain: bestClaim(state, ["HAS_DOMAIN"])?.object || null,
      email_status: (() => {
        const email = bestClaim(state, ["HAS_EMAIL"]);
        if (email?.object && !email.unresolved) return email.status || "DISCOVERED";
        return "UNVERIFIED";
      })(),
      former_operator: bestClaim(state, ["FORMERLY_OPERATED_BY"])?.object || null,
    },
    claims: state.claims,
    evidence: state.evidence,
    sources: state.sources,
    contradictions,
    confidence: { overall: confidence, source: "dealality_native_loop" },
    unresolved,
    audit: {
      run_id: state.run_id,
      lane: state.lane,
      iteration: state.iteration,
      actions: state.actions,
      queries: state.queries,
      working_notes: state.working_notes,
      termination_reason: state.termination_reason,
      cost: state.cost,
      latency: state.latency,
      identity_lock: state.identity_lock,
      synthesis_version: SYNTHESIS_VERSION,
    },
  };

  state.synthesis = { human, machine };
  state.result = machine.result;
  return state.synthesis;
}
