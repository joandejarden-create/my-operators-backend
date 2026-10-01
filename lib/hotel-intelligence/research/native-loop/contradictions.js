/**
 * Contradiction detection — Iteration 1.
 */

import crypto from "node:crypto";

export const CONTRADICTION_ENGINE_VERSION = "native-contradiction-v1";

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Detect conflicting OWNED_BY / OPERATED_BY objects for same subject.
 */
export function detectContradictions(state) {
  const byRel = new Map();
  for (const c of state.claims) {
    if (!c.relationship || c.status === "REJECTED") continue;
    if (!["OWNED_BY", "OPERATED_BY", "PROPCO", "ECONOMIC_OWNER"].includes(c.relationship)) continue;
    const key = `${c.relationship}::${norm(c.subject)}`;
    if (!byRel.has(key)) byRel.set(key, []);
    byRel.get(key).push(c);
  }

  for (const [, group] of byRel) {
    const objects = [...new Set(group.map((c) => norm(c.object)).filter(Boolean))];
    if (objects.length < 2) continue;
    const summary = `${group[0].relationship} conflict: ${group.map((c) => c.object).join(" vs ")}`;
    const already = state.contradictions.some((x) => x.summary === summary);
    if (already) continue;
    const contradiction_id = `contra_${crypto.randomBytes(3).toString("hex")}`;
    const row = {
      contradiction_id,
      summary,
      claim_ids: group.map((c) => c.claim_id),
      hypotheses: [
        "temporal_difference",
        "owner_vs_operator_confusion",
        "subsidiary_vs_ultimate_owner",
        "stale_source",
        "incorrect_entity",
        "unresolved_conflict",
      ],
      status: "OPEN",
      research_query: `resolve ownership operator conflict ${group.map((c) => c.object).join(" ")} ${state.entity?.name || ""}`,
    };
    state.contradictions.push(row);
    for (const c of group) {
      c.contradiction_ids = [...new Set([...(c.contradiction_ids || []), contradiction_id])];
      if (c.status === "VERIFIED" || c.status === "HIGH") {
        c.status = "CONTESTED";
        c.confidence = "MEDIUM";
      }
    }
  }

  // Residual operator vs self-op: if OPERATED_BY has two distinct objects
  return state.contradictions;
}

export function classifyContradictionHeuristics(state, contradiction) {
  const claims = state.claims.filter((c) => contradiction.claim_ids.includes(c.claim_id));
  const texts = claims
    .flatMap((c) =>
      (c.evidence_ids || []).map((id) => state.evidence.find((e) => e.evidence_id === id)?.excerpt || "")
    )
    .join(" ")
    .toLowerCase();
  const notes = [];
  if (/former|previously|legacy|residual|historical/.test(texts)) notes.push("temporal_or_residual_signal");
  if (/operat|manag|steward/.test(texts) && /own|acquir|propco|inmueble/.test(texts)) {
    notes.push("possible_owner_operator_mix");
  }
  contradiction.heuristic_notes = notes;
  return contradiction;
}
