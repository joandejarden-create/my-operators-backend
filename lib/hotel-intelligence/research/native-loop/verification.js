/**
 * Verification pass — Iteration 1.
 * High confidence requires identity lock + direct evidence + no open contradictions.
 */

export const VERIFICATION_VERSION = "native-verification-v1";

export function verifyCriticalClaims(state) {
  const results = [];
  for (const claim of state.claims) {
    if (!["OWNED_BY", "PROPCO", "OPERATED_BY", "ECONOMIC_OWNER"].includes(claim.relationship)) {
      continue;
    }
    const checks = {
      identity_locked: state.identity_lock.identity_match === "TRUE",
      has_evidence: (claim.evidence_ids || []).length > 0,
      evidence_direct: true,
      no_open_contradiction: !(claim.contradiction_ids || []).some((id) =>
        state.contradictions.some((c) => c.contradiction_id === id && c.status === "OPEN")
      ),
      not_inference: claim.inference !== true,
      authority_ok: false,
    };

    for (const eid of claim.evidence_ids || []) {
      const ev = state.evidence.find((e) => e.evidence_id === eid);
      if (ev && (ev.authority_tier === "high" || ev.authority_tier === "strong_secondary")) {
        checks.authority_ok = true;
      }
      if (ev && ev.entity_association === "UNCLEAR") checks.evidence_direct = false;
    }

    // Owner vs operator confusion: if OWNED_BY object equals only operator evidence without ownership language
    if (claim.relationship === "OWNED_BY") {
      const excerpts = (claim.evidence_ids || [])
        .map((id) => state.evidence.find((e) => e.evidence_id === id)?.excerpt || "")
        .join(" ")
        .toLowerCase();
      if (/operat|managed by|manag/.test(excerpts) && !/own|adquir|propiet|inmueble|sold to|acquired/.test(excerpts)) {
        checks.possible_operator_confusion = true;
        claim.status = "UNRESOLVED";
        claim.confidence = null;
        claim.unresolved = true;
        results.push({ claim_id: claim.claim_id, action: "downgrade_operator_confusion" });
        continue;
      }
    }

    const pass =
      checks.identity_locked &&
      checks.has_evidence &&
      checks.evidence_direct &&
      checks.no_open_contradiction &&
      checks.not_inference &&
      checks.authority_ok &&
      !checks.possible_operator_confusion;

    if (pass && (claim.status === "PROBABLE" || claim.status === "CANDIDATE" || claim.status === "HIGH")) {
      claim.status = "VERIFIED";
      claim.confidence = "HIGH";
      claim.confidence_source = "dealality_verification_pass";
      results.push({ claim_id: claim.claim_id, action: "upgrade_verified" });
    } else if (!pass && (claim.status === "VERIFIED" || claim.status === "HIGH")) {
      claim.status = "PROBABLE";
      claim.confidence = "MEDIUM";
      claim.confidence_source = "dealality_verification_pass";
      results.push({ claim_id: claim.claim_id, action: "downgrade_failed_checks", checks });
    } else {
      results.push({ claim_id: claim.claim_id, action: "unchanged", checks });
    }
  }

  // Ensure UNKNOWN for missing critical ownership when requested
  const wantsOwner = (state.objective.requested_fields || []).some((f) =>
    /owner|propco/i.test(String(f))
  );
  const hasOwner = state.claims.some(
    (c) =>
      (c.relationship === "OWNED_BY" || c.relationship === "ECONOMIC_OWNER") &&
      !c.unresolved &&
      c.status !== "UNRESOLVED" &&
      c.status !== "REJECTED"
  );
  if (wantsOwner && !hasOwner) {
    state.open_questions.push({
      code: "UNKNOWN_OWNER",
      question: "Economic owner not established with sufficient evidence.",
    });
    state.claims.push({
      claim_id: `claim_unknown_owner_${state.run_id.slice(-4)}`,
      subject: state.entity?.name,
      relationship: "OWNED_BY",
      object: null,
      field: "economic_owner",
      status: "UNRESOLVED",
      confidence: null,
      evidence_ids: [],
      contradiction_ids: [],
      inference: false,
      unresolved: true,
      unknown_code: "UNKNOWN_OWNER",
    });
  }

  state._verified_pass = true;
  return results;
}
