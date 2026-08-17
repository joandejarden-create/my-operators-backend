/**
 * Colombia RNT → Hotel Property Census match + gated insert plan (dry-run).
 *
 * Rules:
 * - Dedupe SoT = Hotel Property Census only (legacy Hotel Census forbidden).
 * - No Airtable writes from this module.
 * - No Owner Name / Operator writes (ownership_signal stays sidecar).
 * - RNT Source URL is government registry evidence — NOT Official Property URL.
 * - Therefore: never auto_insert from RNT alone; inserts require steward gate.
 */

import { AUTOPILOT_FORBIDDEN_FIELDS } from "./census-autopilot-field-allowlist.js";
import {
  MAP_COLOMBIA_RNT,
  COLOMBIA_RNT_HOSTEL_LIKE,
  normalizeColombiaText,
} from "./colombia-rnt-open-data-adapter.js";

export const COLOMBIA_RNT_HPC_PLAN_VERSION = "colombia-rnt-hpc-match-plan-v1";

export const COLOMBIA_RNT_PLAN_DECISIONS = Object.freeze({
  AUTO_ENRICH_ONLY: "auto_enrich_only",
  STEWARD_HOLD: "steward_hold",
  STEWARD_HOLD_INSERT_CANDIDATE: "steward_hold_insert_candidate",
  REJECT: "reject",
});

/**
 * Convert adapter candidate → HPC matcher input shape.
 * @param {object} candidate
 */
export function toColombiaRntHpcMatchInput(candidate) {
  const f = candidate?.fields || {};
  return {
    sourceRecordId: String(candidate?.codigo_rnt || candidate?.identity_key || ""),
    rawHotelName: f[MAP_COLOMBIA_RNT.propertyName] || "",
    rawCity: f[MAP_COLOMBIA_RNT.city] || "",
    rawCountry: f[MAP_COLOMBIA_RNT.countryField] || "Colombia",
    rawLatitude: "",
    rawLongitude: "",
    rawWebsite: "",
    rawPhone: "",
    proposedIdentityKey: candidate?.identity_key || f[MAP_COLOMBIA_RNT.propertyIdentityKey] || "",
  };
}

/**
 * @param {object} candidate — mapColombiaRntRowToCensusCandidate result
 * @param {object} hpcMatch — matchCandidateToHotelPropertyCensus result (+ proposedIdentityKey)
 */
export function evaluateColombiaRntInsertGate(candidate, hpcMatch = {}) {
  const reasons = [];
  const f = candidate?.fields || {};
  const sub = normalizeColombiaText(candidate?.raw?.sub_categoria).toUpperCase();
  const hpcAction = String(hpcMatch.recommendedAction || "");

  if (!candidate?.validation?.ok) {
    return {
      decision: COLOMBIA_RNT_PLAN_DECISIONS.REJECT,
      reasons: ["rnt_validation_failed", ...(candidate?.validation?.failed || [])],
      production_writable_insert: false,
      human_review_required: true,
      hpc_recommended_action: hpcAction,
    };
  }

  for (const field of AUTOPILOT_FORBIDDEN_FIELDS) {
    if (f[field] != null && f[field] !== "") {
      return {
        decision: COLOMBIA_RNT_PLAN_DECISIONS.REJECT,
        reasons: [`forbidden_field_present:${field}`],
        production_writable_insert: false,
        human_review_required: true,
        hpc_recommended_action: hpcAction,
      };
    }
  }

  if (COLOMBIA_RNT_HOSTEL_LIKE.includes(sub)) {
    return {
      decision: COLOMBIA_RNT_PLAN_DECISIONS.REJECT,
      reasons: ["hostel_or_hostal_out_of_scope"],
      production_writable_insert: false,
      human_review_required: false,
      hpc_recommended_action: hpcAction,
    };
  }

  if (candidate?.raw?.rooms_parse?.hold) {
    reasons.push("rooms_sanity_hold");
  }

  if (hpcAction === "likely_existing") {
    return {
      decision: COLOMBIA_RNT_PLAN_DECISIONS.AUTO_ENRICH_ONLY,
      reasons: ["hpc_likely_existing_no_insert", ...reasons],
      production_writable_insert: false,
      human_review_required: false,
      hpc_recommended_action: hpcAction,
      hpc_record_id: hpcMatch.matchedCensusRecordId || null,
    };
  }

  if (hpcAction === "possible_duplicate_review") {
    return {
      decision: COLOMBIA_RNT_PLAN_DECISIONS.STEWARD_HOLD,
      reasons: ["hpc_possible_duplicate_review", ...reasons],
      production_writable_insert: false,
      human_review_required: true,
      hpc_recommended_action: hpcAction,
      hpc_record_id: hpcMatch.matchedCensusRecordId || null,
    };
  }

  if (hpcAction === "skip_missing_name") {
    return {
      decision: COLOMBIA_RNT_PLAN_DECISIONS.REJECT,
      reasons: ["missing_property_name"],
      production_writable_insert: false,
      human_review_required: false,
      hpc_recommended_action: hpcAction,
    };
  }

  const city = normalizeColombiaText(f.City);
  if (!city || /^unknown$/i.test(city)) {
    return {
      decision: COLOMBIA_RNT_PLAN_DECISIONS.STEWARD_HOLD,
      reasons: ["missing_city", ...reasons],
      production_writable_insert: false,
      human_review_required: true,
      hpc_recommended_action: hpcAction,
    };
  }

  // Government registry alone is not enough for High auto_insert (no Official Property URL).
  reasons.push("government_rnt_requires_steward_insert_gate");
  reasons.push("no_official_property_url");
  if (hpcAction === "needs_research") reasons.push("hpc_needs_research");

  return {
    decision: COLOMBIA_RNT_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE,
    reasons,
    production_writable_insert: false,
    human_review_required: true,
    hpc_recommended_action: hpcAction || "likely_new_candidate",
    insert_payload_preview: buildColombiaRntInsertPreview(candidate),
  };
}

/**
 * Sanitized insert preview for steward review (still dry-run; no Owner fields).
 * @param {object} candidate
 */
export function buildColombiaRntInsertPreview(candidate) {
  const f = { ...(candidate?.fields || {}) };
  for (const field of AUTOPILOT_FORBIDDEN_FIELDS) {
    delete f[field];
  }
  f["Current Brand"] = f["Current Brand"] || "Independent / Unconfirmed";
  f["Affiliation Status"] = f["Affiliation Status"] || "Unknown";
  f["Human Review Required"] = true;
  f["Production Use Status"] = "Candidate";
  f["VIC Freeze Hash"] = `colombia_rnt_${new Date().toISOString().slice(0, 10)}`;
  return {
    fields: f,
    ownership_signal: candidate?.ownership_signal || null,
    field_mapping: MAP_COLOMBIA_RNT,
    notes: [
      "Source URL is datos.gov.co RNT evidence — not Official Property URL",
      "Owner Name must remain empty; NIT lives only on ownership_signal",
      "Apply requires separate confirm flags (not implemented in this dry-run)",
    ],
  };
}

/**
 * @param {object[]} candidates
 * @param {object[]} hpcMatchRows — from matchAllCandidatesToHotelPropertyCensus
 */
export function buildColombiaRntHpcPlan(candidates = [], hpcMatchRows = []) {
  const bySource = new Map(
    hpcMatchRows.map((m) => [String(m.sourceRecordId || m.proposedIdentityKey || ""), m])
  );

  const decisions = {
    [COLOMBIA_RNT_PLAN_DECISIONS.AUTO_ENRICH_ONLY]: 0,
    [COLOMBIA_RNT_PLAN_DECISIONS.STEWARD_HOLD]: 0,
    [COLOMBIA_RNT_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE]: 0,
    [COLOMBIA_RNT_PLAN_DECISIONS.REJECT]: 0,
  };

  const rows = [];
  for (const c of candidates) {
    const key = String(c.codigo_rnt || c.identity_key || "");
    const hpc =
      bySource.get(key) ||
      bySource.get(String(c.identity_key || "")) ||
      {};
    const gate = evaluateColombiaRntInsertGate(c, hpc);
    decisions[gate.decision] = (decisions[gate.decision] || 0) + 1;
    rows.push({
      identity_key: c.identity_key,
      codigo_rnt: c.codigo_rnt,
      property_name: c.fields?.[MAP_COLOMBIA_RNT.propertyName] || "",
      city: c.fields?.City || "",
      state: c.fields?.["State / Region"] || "",
      rooms: c.fields?.["Rooms / Keys"] ?? null,
      nit_signal: c.ownership_signal?.tax_id || null,
      hpc_recommended_action: hpc.recommendedAction || null,
      hpc_match_confidence: hpc.matchConfidence || null,
      hpc_matched_record_id: hpc.matchedCensusRecordId || null,
      hpc_matched_name: hpc.matchedCensusName || null,
      decision: gate.decision,
      reasons: gate.reasons,
      production_writable_insert: gate.production_writable_insert,
      human_review_required: gate.human_review_required,
      insert_payload_preview:
        gate.decision === COLOMBIA_RNT_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE
          ? gate.insert_payload_preview
          : null,
    });
  }

  const insertCandidates = rows.filter(
    (r) => r.decision === COLOMBIA_RNT_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE
  );

  return {
    version: COLOMBIA_RNT_HPC_PLAN_VERSION,
    dry_run: true,
    airtable_writes: false,
    ownership_writes: false,
    auto_insert_enabled: false,
    auto_insert_blocked_reason:
      "Colombia RNT provides government Source URL only; Official Property URL required for High auto_insert",
    required_future_apply_confirms: [
      "--confirm-colombia-rnt-steward-insert",
      "--confirm-no-owner-operator-writes",
      "--confirm-hotel-property-census-only",
      "--confirm-no-legacy-census-writes",
    ],
    summary: {
      candidates: candidates.length,
      decisions,
      steward_hold_insert_candidates: insertCandidates.length,
      auto_enrich_only: decisions[COLOMBIA_RNT_PLAN_DECISIONS.AUTO_ENRICH_ONLY],
      steward_hold: decisions[COLOMBIA_RNT_PLAN_DECISIONS.STEWARD_HOLD],
      reject: decisions[COLOMBIA_RNT_PLAN_DECISIONS.REJECT],
    },
    rows,
    insert_candidate_sample: insertCandidates.slice(0, 25),
  };
}
