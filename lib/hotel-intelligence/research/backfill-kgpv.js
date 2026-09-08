/**
 * Packet 2.6C — KGPV Full Investigation as immutable Research Run #1.
 * No Webhound rerun. Links existing dossier + historical cost from artifacts.
 */

import { getDossierForHotelRecord, getDossierById } from "../dossier/index.js";
import { REPORT_KINDS } from "./report-export-contract.js";

export const KGPV_HOTEL_ID = "recUNycnMwOVFX0hc";
export const KGPV_RUN1_REQUEST_ID = "req_kgpv_full_hi_run1";
export const KGPV_RUN1_RUN_ID = "run_kgpv_full_hi_run1";

/**
 * Historical provider cost from archived Webhound meta / dossier fields.
 * Source of truth: fixtures meta research_cost_usd_approx + dossier.research_cost_usd = 5.4
 * Do not guess if values diverge — prefer dossier field when present.
 */
export function resolveKgpvHistoricalCostUsd(dossier) {
  if (dossier && dossier.research_cost_usd != null && Number.isFinite(Number(dossier.research_cost_usd))) {
    return Number(dossier.research_cost_usd);
  }
  return 5.4;
}

export function ensureKgpvRun1Backfill(repository) {
  if (!repository) throw new Error("repository_required");

  // Packet 2.6C-R1: strip any simulation pollution from live KGPV store.
  if (typeof repository.purgeSimulations === "function") {
    repository.purgeSimulations(KGPV_HOTEL_ID);
  }

  if (repository.getRequest(KGPV_HOTEL_ID, KGPV_RUN1_REQUEST_ID)) {
    return repository.getRequest(KGPV_HOTEL_ID, KGPV_RUN1_REQUEST_ID);
  }

  const dossier =
    getDossierForHotelRecord(KGPV_HOTEL_ID) || getDossierById("dossier_kgpv_full_hi_v1");
  if (!dossier) {
    throw new Error("kgpv_dossier_missing_for_backfill");
  }

  const cost = resolveKgpvHistoricalCostUsd(dossier);
  const completedAt = dossier.completed_at || "2026-09-04T08:56:40.569Z";
  const startedAt = dossier.started_at || dossier.created_at || completedAt;

  const request = repository.createRequest({
    request_id: KGPV_RUN1_REQUEST_ID,
    hotel_id: KGPV_HOTEL_ID,
    hotel_name: dossier.hotel_name || "Krystal Grand Puerto Vallarta",
    requested_by: "historical_backfill",
    template_id: "FULL_HOTEL_INTELLIGENCE",
    template_version: "1.0.0",
    question: "What do we know — and what remains unresolved — about this hotel?",
    scope: [
      "Ownership",
      "Corporate Structure",
      "Brand & Operator",
      "People",
      "Development Intelligence",
    ],
    status:
      Number(dossier.open_question_count || 0) > 0
        ? "COMPLETED_WITH_OPEN_QUESTIONS"
        : "COMPLETED",
    created_at: dossier.created_at || startedAt,
    started_at: startedAt,
    completed_at: completedAt,
    provider_strategy: "WEBHOUND",
    provider_budget_usd: cost,
    provider_actual_cost_usd: cost,
    total_internal_cost_usd: cost,
    provider_run_id: dossier.research_run_id || "23969ba8-60d2-402d-887d-c94f555d3e3b",
    raw_artifact_id: "art_kgpv_webhound_modules_v1",
    normalized_research_id: "norm_kgpv_full_hi_v1",
    report_id: dossier.dossier_id,
    pdf_id: dossier.dossier_id,
    report_type: REPORT_KINDS.FULL_INVESTIGATION,
    source_count: dossier.source_count ?? 20,
    finding_count: dossier.finding_count ?? 8,
    open_question_count: dossier.open_question_count ?? 6,
    simulated: false,
    immutable_historical: true,
    estimated_research_units: 10,
    pricing_tier_key: "full_investigation_v1",
    claim_handoff: dossier.claim_handoff || { auto_promote: false },
    lineage: {
      hotel_id: KGPV_HOTEL_ID,
      research_request_id: KGPV_RUN1_REQUEST_ID,
      research_run_id: KGPV_RUN1_RUN_ID,
      parent_report_id: null,
      template_id: "FULL_HOTEL_INTELLIGENCE",
      template_version: "1.0.0",
      historical_provider: "WEBHOUND",
      historical_provider_label_internal: dossier.research_provider || "webhound_kgpv_modules_archive",
    },
  });

  repository.createRun({
    run_id: KGPV_RUN1_RUN_ID,
    request_id: KGPV_RUN1_REQUEST_ID,
    hotel_id: KGPV_HOTEL_ID,
    provider: "WEBHOUND",
    provider_version: "historical",
    provider_run_id: request.provider_run_id,
    started_at: startedAt,
    completed_at: completedAt,
    budget: cost,
    actual_cost: cost,
    raw_artifact_id: request.raw_artifact_id,
    provider_status: "COMPLETED",
    simulated: false,
    immutable_historical: true,
    report_version: dossier.version || "1",
    template_id: "FULL_HOTEL_INTELLIGENCE",
    template_version: "1.0.0",
  });

  try {
    repository.writeArtifact(KGPV_HOTEL_ID, {
      artifact_id: "art_kgpv_webhound_modules_v1",
      kind: "RAW_RESEARCH",
      provider: "WEBHOUND",
      immutable: true,
      historical: true,
      reference: {
        path: "fixtures/hotel-intelligence/dossier/source-archive",
        session_id: request.provider_run_id,
        research_cost_usd: cost,
      },
      note: "Immutable historical Webhound archive reference. Not a rerun.",
    });
  } catch (err) {
    if (err.code !== "artifact_already_exists") throw err;
  }

  const openQs = Array.isArray(dossier.open_questions) ? dossier.open_questions : [];
  repository.upsertOpenQuestionLineage(
    KGPV_HOTEL_ID,
    openQs.map((q) => ({
      open_question_id: q.id || q.open_question_id,
      origin_report_id: dossier.dossier_id,
      origin_finding_id: null,
      recommended_template_id: null,
      resolved_by_run_id: null,
    }))
  );

  return request;
}
