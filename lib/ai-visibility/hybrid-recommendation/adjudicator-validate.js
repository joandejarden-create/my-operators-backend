/**
 * Deterministic validation of adjudicator output.
 */

import { GOVERNED_RECOMMENDATION_ROLES } from "./taxonomy.js";

export const ADJUDICATION_VALIDATION_FAILED = "ADJUDICATION_VALIDATION_FAILED";

/**
 * @param {object} raw - parsed adjudicator JSON
 * @param {{ evidence: object, entityPresent: boolean, allowedEvidenceRefIds?: string[] }} ctx
 */
export function validateAdjudicatorOutput(raw, ctx = {}) {
  const errors = [];
  if (!raw || typeof raw !== "object") {
    return { ok: false, code: ADJUDICATION_VALIDATION_FAILED, errors: ["missing_object"] };
  }

  const role = String(raw.selectedRole || "").trim();
  if (!GOVERNED_RECOMMENDATION_ROLES.includes(role)) {
    errors.push(`invalid_enum:${role || "empty"}`);
  }

  const refs = Array.isArray(raw.evidenceRefs) ? raw.evidenceRefs.map(String) : null;
  if (!refs || refs.length === 0) errors.push("evidence_refs_required");

  if (refs && Array.isArray(ctx.allowedEvidenceRefIds) && ctx.allowedEvidenceRefIds.length) {
    const anyKnown = refs.some((r) => ctx.allowedEvidenceRefIds.includes(r));
    if (!anyKnown) {
      errors.push(
        `evidence_refs_must_cite_known_ref:allowed=${ctx.allowedEvidenceRefIds.join(",")}`
      );
    }
    // Unknown extra refs are ignored (model may cite sub-paths); do not fail.
  }

  if (!String(raw.taxonomyRule || "").trim()) errors.push("taxonomy_rule_required");
  if (!String(raw.ambiguityResolved || "").trim()) errors.push("ambiguity_resolved_required");

  const evidence = ctx.evidence || {};
  const ev = evidence.recommendationEvidence || {};
  const st = evidence.structure || {};
  const entityPresent = ctx.entityPresent !== false;

  if (role === "no_mention" && entityPresent) {
    errors.push("no_mention_cannot_override_entity_present");
  }
  const localText = String(ctx.entityLocalEvidence || evidence.localSentence || evidence.localListItem || "");
  const LEAD_IN_TEXT =
    /\b(first\s+(?:choice|call|option|recommendation)|top\s+(?:choice|recommendation)|primary\s+recommendation|#\s*1\b|rank\s*1\b|1st\s+(?:choice|recommendation)|primera\s+opci|recomendaci[oó]n\s+principal|leading\s+recommendation)\b/i;
  const RANK_IN_TEXT =
    /\b(#\s*[2-9]\b|rank\s*[2-9]\b|second\s+choice|third\s+choice|segunda\s+opci|tercera\s+opci|priority\s*[2-9]|recommended\s+in\s+(?:the\s+)?(?:following\s+)?order|orden\s+de\s+prioridad)\b/i;

  if (role === "first_recommendation") {
    const pos =
      st.orderedPosition != null
        ? st.orderedPosition
        : evidence.rankPosition != null
          ? evidence.rankPosition
          : null;
    const hasLead = Boolean(ev.leadCue) || LEAD_IN_TEXT.test(localText);
    const hasRank1 = Boolean(
      ((st.confirmedRankStructure || evidence.confirmedRankStructure) && pos === 1) ||
        /#\s*1\b|rank\s*1\b|1st\s+/i.test(localText)
    );
    if (!hasLead && !hasRank1) errors.push("first_requires_lead_or_rank1_evidence");
  }
  if (role === "ranked_recommendation") {
    const pos =
      st.orderedPosition != null
        ? st.orderedPosition
        : evidence.rankPosition != null
          ? evidence.rankPosition
          : null;
    const ok =
      (Boolean(st.confirmedRankStructure || evidence.confirmedRankStructure) &&
        pos != null &&
        pos > 1) ||
      RANK_IN_TEXT.test(localText);
    if (!ok) errors.push("ranked_requires_meaningful_order_evidence");
  }
  if (role === "source_only") {
    if (ev.directPositiveCue || ev.leadCue || ev.considerationSetCue || ev.descriptiveCue) {
      errors.push("source_only_invalid_with_substantive_discussion");
    }
  }

  // Extra keys beyond contract
  const allowed = new Set([
    "selectedRole",
    "evidenceRefs",
    "taxonomyRule",
    "ambiguityResolved",
  ]);
  for (const k of Object.keys(raw)) {
    if (!allowed.has(k)) errors.push(`extra_key:${k}`);
  }

  if (errors.length) {
    return { ok: false, code: ADJUDICATION_VALIDATION_FAILED, errors, selectedRole: role || null };
  }

  return {
    ok: true,
    selectedRole: role,
    evidenceRefs: refs,
    taxonomyRule: String(raw.taxonomyRule).trim(),
    ambiguityResolved: String(raw.ambiguityResolved).trim(),
  };
}

export function parseAdjudicatorText(text) {
  const raw = String(text || "").trim();
  const unfenced = raw.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, "").trim();
  try {
    return { ok: true, value: JSON.parse(unfenced) };
  } catch (e) {
    // Prefer innermost/last JSON object containing selectedRole
    const matches = [...unfenced.matchAll(/\{[\s\S]*?\}/g)];
    for (let i = matches.length - 1; i >= 0; i--) {
      const chunk = matches[i][0];
      if (!/"selectedRole"\s*:/.test(chunk)) continue;
      try {
        return { ok: true, value: JSON.parse(chunk) };
      } catch {
        /* continue */
      }
    }
    const i = unfenced.indexOf("{");
    const j = unfenced.lastIndexOf("}");
    if (i >= 0 && j > i) {
      try {
        return { ok: true, value: JSON.parse(unfenced.slice(i, j + 1)) };
      } catch (e2) {
        return { ok: false, error: String(e2.message || e2) };
      }
    }
    return { ok: false, error: String(e.message || e) };
  }
}
