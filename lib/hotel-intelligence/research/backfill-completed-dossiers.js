/**
 * Generic completed-dossier → Research Archive backfill.
 * Links any client-visible completed dossier (fixture or addendum store)
 * into the hotel research repository as immutable historical requests.
 * No Webhound / provider rerun.
 */

import { listDossiersForHotel, getDossierById } from "../dossier/index.js";
import {
  DOSSIER_TYPE,
  DOSSIER_TYPE_RESEARCH_ADDENDUM,
} from "../dossier/statuses.js";
import { REPORT_KINDS } from "./report-export-contract.js";
import { getTemplate } from "./templates.js";

/** Synthetic template id for unknown addenda — still archives with a readable title. */
export const GENERIC_FOLLOW_UP_TEMPLATE_ID = "RESEARCH_FOLLOW_UP";

const FULL_SCOPE = Object.freeze([
  "Ownership",
  "Corporate Structure",
  "Brand & Operator",
  "People",
  "Development Intelligence",
]);

const COMPLETED_STATUSES = new Set([
  "COMPLETED",
  "COMPLETED_WITH_OPEN_QUESTIONS",
  "ARCHIVED",
]);

export function dossierBackfillRequestId(dossierId) {
  return `req_dossier_backfill_${String(dossierId || "").trim()}`;
}

export function dossierBackfillRunId(dossierId) {
  return `run_dossier_backfill_${String(dossierId || "").trim()}`;
}

function resolveCostUsd(dossier) {
  if (dossier && dossier.research_cost_usd != null && Number.isFinite(Number(dossier.research_cost_usd))) {
    return Number(dossier.research_cost_usd);
  }
  return null;
}

function isClientVisibleCompletedCard(card) {
  if (!card || !card.dossier_id) return false;
  if (card.customer_visible === false) return false;
  const status = String(card.status || "").toUpperCase();
  if (/SUPERSEDED/i.test(status)) return false;
  if (status && !COMPLETED_STATUSES.has(status) && status !== "PARTIAL") {
    // Allow empty status only when dossier loads as completed-shaped.
    if (!status) return true;
    return false;
  }
  return true;
}

/**
 * Map dossier type / template_id → research template_id.
 */
export function resolveDossierArchiveTemplateId(dossierOrCard) {
  const type = String(dossierOrCard?.dossier_type || dossierOrCard?.report_type || "").toUpperCase();
  const rawTid = String(dossierOrCard?.template_id || "").trim().toUpperCase();
  const idHint = String(dossierOrCard?.dossier_id || "");

  if (
    type === DOSSIER_TYPE ||
    type === "FULL_HOTEL_INTELLIGENCE" ||
    type === "FULL_INVESTIGATION" ||
    /full_hi|full.?hotel.?intelligence/i.test(idHint) ||
    /Full Hotel Intelligence/i.test(String(dossierOrCard?.title || ""))
  ) {
    if (!rawTid || rawTid === "FULL_HOTEL_INTELLIGENCE" || rawTid === "FULL_HOTEL_INTELLIGENCE_INVESTIGATION") {
      return "FULL_HOTEL_INTELLIGENCE";
    }
  }

  if (rawTid) {
    const t = getTemplate(rawTid);
    if (t?.template_id) return t.template_id;
    if (rawTid === "CHANGE_OPPORTUNITY") return "CHANGE_OPPORTUNITY";
  }

  if (
    type === DOSSIER_TYPE_RESEARCH_ADDENDUM ||
    type === "RESEARCH_ADDENDUM" ||
    /CHANGE.?OPPORTUNITY/i.test(String(dossierOrCard?.title || ""))
  ) {
    if (/CHANGE.?OPPORTUNITY/i.test(String(dossierOrCard?.title || "")) || rawTid === "CHANGE_OPPORTUNITY") {
      return "CHANGE_OPPORTUNITY";
    }
    return GENERIC_FOLLOW_UP_TEMPLATE_ID;
  }

  if (type === DOSSIER_TYPE || /full_hi/i.test(idHint)) {
    return "FULL_HOTEL_INTELLIGENCE";
  }

  return GENERIC_FOLLOW_UP_TEMPLATE_ID;
}

function findExistingArchiveForReport(repository, hotelId, reportId) {
  const research = repository.listHotelResearch(hotelId);
  return (research.requests || []).find(
    (r) => r && String(r.report_id || "") === String(reportId || "")
  ) || null;
}

function ensureOneDossierArchiveBackfill(repository, hotelId, card) {
  const dossierId = String(card.dossier_id || "").trim();
  if (!dossierId) return null;

  const existingByReport = findExistingArchiveForReport(repository, hotelId, dossierId);
  if (existingByReport) return existingByReport;

  const requestId = dossierBackfillRequestId(dossierId);
  if (repository.getRequest(hotelId, requestId)) {
    return repository.getRequest(hotelId, requestId);
  }

  const dossier = getDossierById(dossierId);
  if (!dossier) {
    console.warn("[hi-research] dossier_backfill_missing", hotelId, dossierId);
    return null;
  }

  const templateId = resolveDossierArchiveTemplateId(dossier);
  const template = getTemplate(templateId);
  const cost = resolveCostUsd(dossier);
  const completedAt = dossier.completed_at || card.completed_at || new Date().toISOString();
  const startedAt = dossier.started_at || dossier.created_at || completedAt;
  const runId = dossierBackfillRunId(dossierId);
  const isFull = templateId === "FULL_HOTEL_INTELLIGENCE";
  const reportType = isFull ? REPORT_KINDS.FULL_INVESTIGATION : REPORT_KINDS.RESEARCH_ADDENDUM;
  const parentReportId =
    dossier.parent_report_id ||
    dossier.origin_report_id ||
    (dossier.lineage && dossier.lineage.parent_report_id) ||
    null;
  const artifactId = `art_dossier_backfill_${dossierId}`;

  const request = repository.createRequest({
    request_id: requestId,
    hotel_id: hotelId,
    hotel_name: dossier.hotel_name || card.hotel_name || null,
    requested_by: "historical_backfill",
    template_id: templateId,
    template_version: template?.version || "1.0.0",
    question:
      template?.customer_question ||
      dossier.title ||
      "Completed research report (historical archive).",
    scope: isFull ? [...FULL_SCOPE] : template?.scope_labels || null,
    status:
      Number(dossier.open_question_count || 0) > 0
        ? "COMPLETED_WITH_OPEN_QUESTIONS"
        : String(dossier.status || "").toUpperCase() === "COMPLETED_WITH_OPEN_QUESTIONS"
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
    raw_artifact_id: artifactId,
    normalized_research_id: null,
    report_id: dossier.dossier_id,
    pdf_id: dossier.dossier_id,
    parent_report_id: parentReportId,
    report_type: reportType,
    source_count: dossier.source_count ?? card.source_count ?? null,
    finding_count: dossier.finding_count ?? card.finding_count ?? null,
    open_question_count: dossier.open_question_count ?? card.open_question_count ?? null,
    simulated: false,
    immutable_historical: true,
    estimated_research_units: isFull ? 10 : 3,
    pricing_tier_key: isFull ? "full_investigation_v1" : "research_addendum_v1",
    claim_handoff: dossier.claim_handoff || { auto_promote: false },
    display_name: template?.display_name || dossier.title || "Research Report",
    lineage: {
      hotel_id: hotelId,
      research_request_id: requestId,
      research_run_id: runId,
      parent_report_id: parentReportId,
      template_id: templateId,
      template_version: template?.version || "1.0.0",
      historical_provider: "WEBHOUND",
      historical_provider_label_internal:
        dossier.research_provider || "dossier_registry_historical_backfill",
      dossier_backfill: true,
    },
  });

  if (!repository.listHotelResearch(hotelId).runs.find((r) => r.run_id === runId)) {
    repository.createRun({
      run_id: runId,
      request_id: requestId,
      hotel_id: hotelId,
      provider: "WEBHOUND",
      provider_version: "historical",
      provider_run_id: request.provider_run_id,
      started_at: startedAt,
      completed_at: completedAt,
      budget: cost,
      actual_cost: cost,
      raw_artifact_id: artifactId,
      provider_status: "COMPLETED",
      simulated: false,
      immutable_historical: true,
      report_version: dossier.version != null ? String(dossier.version) : "1",
      template_id: templateId,
      template_version: template?.version || "1.0.0",
    });
  }

  try {
    repository.writeArtifact(hotelId, {
      artifact_id: artifactId,
      kind: "RAW_RESEARCH",
      provider: "WEBHOUND",
      immutable: true,
      historical: true,
      reference: {
        dossier_id: dossierId,
        session_id: request.provider_run_id,
        research_cost_usd: cost,
      },
      note: "Immutable historical dossier archive reference. Not a rerun.",
    });
  } catch (err) {
    if (err.code !== "artifact_already_exists") throw err;
  }

  return request;
}

/**
 * Ensure every client-visible completed dossier for hotelId has an archive request.
 * Idempotent by report_id and by req_dossier_backfill_<dossier_id>.
 * @returns {{ created: number, skipped: number, requests: object[] }}
 */
export function ensureCompletedDossierArchiveBackfill(repository, hotelId) {
  if (!repository) throw new Error("repository_required");
  const id = String(hotelId || "").trim();
  if (!id) return { created: 0, skipped: 0, requests: [] };

  const cards = listDossiersForHotel({ airtableRecordId: id });
  const requests = [];
  let created = 0;
  let skipped = 0;

  for (const card of cards) {
    if (!isClientVisibleCompletedCard(card)) {
      skipped += 1;
      continue;
    }
    const before = findExistingArchiveForReport(repository, id, card.dossier_id);
    const req = ensureOneDossierArchiveBackfill(repository, id, card);
    if (!req) {
      skipped += 1;
      continue;
    }
    if (!before) created += 1;
    else skipped += 1;
    requests.push(req);
  }

  return { created, skipped, requests };
}
