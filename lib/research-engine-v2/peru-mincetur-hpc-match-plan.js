/**
 * Peru MINCETUR → Hotel Property Census match + gated insert plan (dry-run).
 *
 * Rules:
 * - Dedupe SoT = Hotel Property Census only (legacy Hotel Census forbidden).
 * - No Airtable writes from this module.
 * - No Owner Name / Operator writes (ownership_signal / RUC stays sidecar).
 * - Source URL is government catalog evidence — Official Property URL only when
 *   PAGINA_WEB normalizes to a property site (still never auto_insert here).
 * - Inserts require steward gate + future apply confirms.
 */

import { AUTOPILOT_FORBIDDEN_FIELDS } from "./census-autopilot-field-allowlist.js";
import {
  MAP_PERU_MINCETUR,
  PERU_MINCETUR_HOSTEL_LIKE,
  normalizePeruText,
} from "./peru-mincetur-open-data-adapter.js";

export const PERU_MINCETUR_HPC_PLAN_VERSION = "peru-mincetur-hpc-match-plan-v1";

export const PERU_MINCETUR_PLAN_DECISIONS = Object.freeze({
  AUTO_ENRICH_ONLY: "auto_enrich_only",
  STEWARD_HOLD: "steward_hold",
  STEWARD_HOLD_INSERT_CANDIDATE: "steward_hold_insert_candidate",
  REJECT: "reject",
});

/**
 * Convert adapter candidate → HPC matcher input shape.
 * @param {object} candidate
 */
export function toPeruMinceturHpcMatchInput(candidate) {
  const f = candidate?.fields || {};
  return {
    sourceRecordId: String(candidate?.nro_certificado || candidate?.identity_key || ""),
    rawHotelName: f[MAP_PERU_MINCETUR.propertyName] || "",
    rawCity: f[MAP_PERU_MINCETUR.city] || "",
    rawCountry: f[MAP_PERU_MINCETUR.countryField] || "Peru",
    rawLatitude: "",
    rawLongitude: "",
    rawWebsite: f[MAP_PERU_MINCETUR.officialPropertyUrl] || "",
    rawPhone: f[MAP_PERU_MINCETUR.phone] || "",
    proposedIdentityKey: candidate?.identity_key || f[MAP_PERU_MINCETUR.propertyIdentityKey] || "",
  };
}

/**
 * @param {object} candidate — mapPeruMinceturRowToCensusCandidate result
 * @param {object} hpcMatch — matchCandidateToHotelPropertyCensus result
 */
export function evaluatePeruMinceturInsertGate(candidate, hpcMatch = {}) {
  const reasons = [];
  const f = candidate?.fields || {};
  const clase = normalizePeruText(candidate?.raw?.clase).toUpperCase();
  const hpcAction = String(hpcMatch.recommendedAction || "");
  const hasOfficialUrl = Boolean(f[MAP_PERU_MINCETUR.officialPropertyUrl]);

  if (!candidate?.validation?.ok) {
    return {
      decision: PERU_MINCETUR_PLAN_DECISIONS.REJECT,
      reasons: ["mincetur_validation_failed", ...(candidate?.validation?.failed || [])],
      production_writable_insert: false,
      human_review_required: true,
      hpc_recommended_action: hpcAction,
    };
  }

  for (const field of AUTOPILOT_FORBIDDEN_FIELDS) {
    if (f[field] != null && f[field] !== "") {
      return {
        decision: PERU_MINCETUR_PLAN_DECISIONS.REJECT,
        reasons: [`forbidden_field_present:${field}`],
        production_writable_insert: false,
        human_review_required: true,
        hpc_recommended_action: hpcAction,
      };
    }
  }

  if (PERU_MINCETUR_HOSTEL_LIKE.includes(clase)) {
    return {
      decision: PERU_MINCETUR_PLAN_DECISIONS.REJECT,
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
      decision: PERU_MINCETUR_PLAN_DECISIONS.AUTO_ENRICH_ONLY,
      reasons: ["hpc_likely_existing_no_insert", ...reasons],
      production_writable_insert: false,
      human_review_required: false,
      hpc_recommended_action: hpcAction,
      hpc_record_id: hpcMatch.matchedCensusRecordId || null,
    };
  }

  if (hpcAction === "possible_duplicate_review") {
    return {
      decision: PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD,
      reasons: ["hpc_possible_duplicate_review", ...reasons],
      production_writable_insert: false,
      human_review_required: true,
      hpc_recommended_action: hpcAction,
      hpc_record_id: hpcMatch.matchedCensusRecordId || null,
    };
  }

  if (hpcAction === "skip_missing_name") {
    return {
      decision: PERU_MINCETUR_PLAN_DECISIONS.REJECT,
      reasons: ["missing_property_name"],
      production_writable_insert: false,
      human_review_required: false,
      hpc_recommended_action: hpcAction,
    };
  }

  const city = normalizePeruText(f.City);
  if (!city || /^unknown$/i.test(city)) {
    return {
      decision: PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD,
      reasons: ["missing_city", ...reasons],
      production_writable_insert: false,
      human_review_required: true,
      hpc_recommended_action: hpcAction,
    };
  }

  reasons.push("government_mincetur_requires_steward_insert_gate");
  if (hasOfficialUrl) {
    reasons.push("official_property_url_from_pagina_web");
  } else {
    reasons.push("no_official_property_url");
  }
  if (hpcAction === "needs_research") reasons.push("hpc_needs_research");

  return {
    decision: PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE,
    reasons,
    production_writable_insert: false,
    human_review_required: true,
    hpc_recommended_action: hpcAction || "likely_new_candidate",
    insert_payload_preview: buildPeruMinceturInsertPreview(candidate),
  };
}

/**
 * Sanitized insert preview for steward review (still dry-run; no Owner fields).
 * @param {object} candidate
 */
export function buildPeruMinceturInsertPreview(candidate) {
  const f = { ...(candidate?.fields || {}) };
  for (const field of AUTOPILOT_FORBIDDEN_FIELDS) {
    delete f[field];
  }
  f["Current Brand"] = f["Current Brand"] || "Independent / Unconfirmed";
  f["Affiliation Status"] = f["Affiliation Status"] || "Unknown";
  f["Human Review Required"] = true;
  f["Production Use Status"] = "Candidate";
  f["VIC Freeze Hash"] = `peru_mincetur_${new Date().toISOString().slice(0, 10)}`;
  return {
    fields: f,
    ownership_signal: candidate?.ownership_signal || null,
    field_mapping: MAP_PERU_MINCETUR,
    notes: [
      "Source URL is datosabiertos / MINCETUR catalog evidence",
      "Official Property URL may come from PAGINA_WEB when present — still steward-gated",
      "Owner Name must remain empty; RUC lives only on ownership_signal",
      "Apply requires separate confirm flags (not implemented in this dry-run)",
    ],
  };
}

/**
 * @param {object[]} candidates
 * @param {object[]} hpcMatchRows — from matchAllCandidatesToHotelPropertyCensus
 */
export function buildPeruMinceturHpcPlan(candidates = [], hpcMatchRows = []) {
  const bySource = new Map(
    hpcMatchRows.map((m) => [String(m.sourceRecordId || m.proposedIdentityKey || ""), m])
  );

  const decisions = {
    [PERU_MINCETUR_PLAN_DECISIONS.AUTO_ENRICH_ONLY]: 0,
    [PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD]: 0,
    [PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE]: 0,
    [PERU_MINCETUR_PLAN_DECISIONS.REJECT]: 0,
  };

  const rows = [];
  for (const c of candidates) {
    const key = String(c.nro_certificado || c.identity_key || "");
    const hpc =
      bySource.get(key) ||
      bySource.get(String(c.identity_key || "")) ||
      {};
    const gate = evaluatePeruMinceturInsertGate(c, hpc);
    decisions[gate.decision] = (decisions[gate.decision] || 0) + 1;
    rows.push({
      identity_key: c.identity_key,
      nro_certificado: c.nro_certificado,
      property_name: c.fields?.[MAP_PERU_MINCETUR.propertyName] || "",
      city: c.fields?.City || "",
      state: c.fields?.["State / Region"] || "",
      rooms: c.fields?.["Rooms / Keys"] ?? null,
      ruc_signal: c.ownership_signal?.tax_id || null,
      official_property_url: c.fields?.[MAP_PERU_MINCETUR.officialPropertyUrl] || null,
      hpc_recommended_action: hpc.recommendedAction || null,
      hpc_match_confidence: hpc.matchConfidence || null,
      hpc_matched_record_id: hpc.matchedCensusRecordId || null,
      hpc_matched_name: hpc.matchedCensusName || null,
      decision: gate.decision,
      reasons: gate.reasons,
      production_writable_insert: gate.production_writable_insert,
      human_review_required: gate.human_review_required,
      insert_payload_preview:
        gate.decision === PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE
          ? gate.insert_payload_preview
          : null,
    });
  }

  const insertCandidates = rows.filter(
    (r) => r.decision === PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD_INSERT_CANDIDATE
  );
  const withOfficialUrl = insertCandidates.filter((r) => r.official_property_url).length;

  return {
    version: PERU_MINCETUR_HPC_PLAN_VERSION,
    dry_run: true,
    airtable_writes: false,
    ownership_writes: false,
    auto_insert_enabled: false,
    auto_insert_blocked_reason:
      "Peru MINCETUR inserts remain steward-gated until apply confirms exist (even when PAGINA_WEB → Official Property URL)",
    required_future_apply_confirms: [
      "--confirm-peru-mincetur-steward-insert",
      "--confirm-no-owner-operator-writes",
      "--confirm-hotel-property-census-only",
      "--confirm-no-legacy-census-writes",
    ],
    summary: {
      candidates: candidates.length,
      decisions,
      steward_hold_insert_candidates: insertCandidates.length,
      insert_candidates_with_official_url: withOfficialUrl,
      auto_enrich_only: decisions[PERU_MINCETUR_PLAN_DECISIONS.AUTO_ENRICH_ONLY],
      steward_hold: decisions[PERU_MINCETUR_PLAN_DECISIONS.STEWARD_HOLD],
      reject: decisions[PERU_MINCETUR_PLAN_DECISIONS.REJECT],
    },
    rows,
    insert_candidate_sample: insertCandidates.slice(0, 25),
  };
}
