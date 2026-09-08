/**
 * Mexico Explorer Full HI — immutable Research Run #1 backfill.
 * Links Sheraton GDL + Real Inn Cancún dossiers (no Webhound rerun).
 */

import { getDossierForHotelRecord, getDossierById } from "../dossier/index.js";
import { REPORT_KINDS } from "./report-export-contract.js";
import {
  SHERATON_GDL_AIRTABLE_ID,
  REAL_INN_CANCUN_AIRTABLE_ID,
} from "../ownership/golden-demo/mexico-explorer-demo-cohort.js";
import {
  MEXICO_EXPLORER_DOSSIER_META,
  SHERATON_DOSSIER_ID,
  REAL_INN_DOSSIER_ID,
} from "../dossier/adapters/from-mexico-explorer-deep-research.js";

export const MEXICO_EXPLORER_FULL_HI_HOTEL_IDS = Object.freeze([
  SHERATON_GDL_AIRTABLE_ID,
  REAL_INN_CANCUN_AIRTABLE_ID,
]);

function resolveCostUsd(dossier) {
  if (dossier && dossier.research_cost_usd != null && Number.isFinite(Number(dossier.research_cost_usd))) {
    return Number(dossier.research_cost_usd);
  }
  return 5;
}

function ensureOneMexicoExplorerRun1Backfill(repository, hotelId) {
  const meta = MEXICO_EXPLORER_DOSSIER_META[hotelId];
  if (!meta) throw new Error(`mexico_explorer_meta_missing:${hotelId}`);

  if (repository.getRequest(hotelId, meta.request_id)) {
    return repository.getRequest(hotelId, meta.request_id);
  }

  const dossier =
    getDossierForHotelRecord(hotelId) ||
    getDossierById(meta.dossier_id) ||
    getDossierById(hotelId === SHERATON_GDL_AIRTABLE_ID ? SHERATON_DOSSIER_ID : REAL_INN_DOSSIER_ID);

  if (!dossier) {
    throw new Error(`mexico_explorer_dossier_missing_for_backfill:${hotelId}`);
  }

  const cost = resolveCostUsd(dossier);
  const completedAt = dossier.completed_at || "2026-09-08T18:00:00.000Z";
  const startedAt = dossier.started_at || dossier.created_at || completedAt;

  const request = repository.createRequest({
    request_id: meta.request_id,
    hotel_id: hotelId,
    hotel_name: dossier.hotel_name || meta.hotel_name,
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
    provider_run_id: dossier.research_run_id || null,
    raw_artifact_id: meta.raw_artifact_id,
    normalized_research_id: meta.normalized_research_id,
    report_id: dossier.dossier_id,
    pdf_id: dossier.dossier_id,
    report_type: REPORT_KINDS.FULL_INVESTIGATION,
    source_count: dossier.source_count ?? null,
    finding_count: dossier.finding_count ?? null,
    open_question_count: dossier.open_question_count ?? null,
    simulated: false,
    immutable_historical: true,
    estimated_research_units: 10,
    pricing_tier_key: "full_investigation_v1",
    claim_handoff: dossier.claim_handoff || { auto_promote: false },
    lineage: {
      hotel_id: hotelId,
      research_request_id: meta.request_id,
      research_run_id: meta.run_id,
      parent_report_id: null,
      template_id: "FULL_HOTEL_INTELLIGENCE",
      template_version: "1.0.0",
      historical_provider: "WEBHOUND",
      historical_provider_label_internal: "webhound_mexico_explorer_full_hi_v1",
    },
  });

  repository.createRun({
    run_id: meta.run_id,
    request_id: meta.request_id,
    hotel_id: hotelId,
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
    repository.writeArtifact(hotelId, {
      artifact_id: meta.raw_artifact_id,
      kind: "RAW_RESEARCH",
      provider: "WEBHOUND",
      immutable: true,
      historical: true,
      reference: {
        path: meta.deep_path,
        session_id: request.provider_run_id,
        research_cost_usd: cost,
      },
      note: "Immutable historical Webhound Full HI reference. Not a rerun.",
    });
  } catch (err) {
    if (err.code !== "artifact_already_exists") throw err;
  }

  const openQs = Array.isArray(dossier.open_questions) ? dossier.open_questions : [];
  repository.upsertOpenQuestionLineage(
    hotelId,
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

export function ensureMexicoExplorerRun1Backfill(repository, hotelId) {
  if (!repository) throw new Error("repository_required");
  const id = String(hotelId || "").trim();
  if (!MEXICO_EXPLORER_FULL_HI_HOTEL_IDS.includes(id)) return null;
  return ensureOneMexicoExplorerRun1Backfill(repository, id);
}

export function ensureAllMexicoExplorerRun1Backfills(repository) {
  return MEXICO_EXPLORER_FULL_HI_HOTEL_IDS.map((id) =>
    ensureMexicoExplorerRun1Backfill(repository, id)
  );
}

export function isMexicoExplorerFullHiHotel(hotelId) {
  return MEXICO_EXPLORER_FULL_HI_HOTEL_IDS.includes(String(hotelId || "").trim());
}
