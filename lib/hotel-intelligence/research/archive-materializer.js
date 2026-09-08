/**
 * Packet 2.6C-R3 — Research Archive materializer.
 * Archive is a customer-facing projection of durable request/run/report records.
 */

import { createResearchRepository } from "./repository.js";
import {
  filterLiveArchiveRequests,
  filterLiveActiveRequests,
  canExposeViewReport,
  canExposeDownloadPdf,
  isSimulationRequest,
} from "./archive-integrity.js";
import { getTemplate } from "./templates.js";
import { customerStageLabel } from "./statuses.js";
import { reportTypeLabel } from "./report-export-contract.js";

function archiveEntryKey(request) {
  if (request.report_id) return `report:${request.report_id}`;
  return `run:${request.request_id}:${request.report_type || "REPORT"}`;
}

/**
 * Deterministic, hotel-scoped, idempotent archive projection.
 * Newest-first. Dedupes by report_id (or request+report_type).
 */
export function materializeResearchArchive(hotelId, opts = {}) {
  const repo = opts.repo || createResearchRepository({ env: opts.env });
  const includeSimulations = Boolean(opts.includeSimulations);
  const index = repo.listHotelResearch(hotelId);
  const eligible = filterLiveArchiveRequests(index.requests || [], { includeSimulations });

  const byKey = new Map();
  for (const req of eligible) {
    const key = archiveEntryKey(req);
    const prev = byKey.get(key);
    if (!prev) {
      byKey.set(key, req);
      continue;
    }
    const prevTs = String(prev.completed_at || prev.created_at || "");
    const nextTs = String(req.completed_at || req.created_at || "");
    if (nextTs > prevTs) byKey.set(key, req);
  }

  const entries = [...byKey.values()].sort((a, b) =>
    String(b.completed_at || b.created_at || "").localeCompare(
      String(a.completed_at || a.created_at || "")
    )
  );

  return {
    hotel_id: hotelId,
    archive: entries.map((request) => {
      const template = getTemplate(request.template_id);
      const labels = reportTypeLabel(request.report_type, template?.display_name);
      return {
        archive_entry_id: archiveEntryKey(request),
        request_id: request.request_id,
        report_id: request.report_id,
        pdf_id: request.pdf_id || request.report_id,
        template_id: request.template_id,
        template_version: request.template_version,
        display_name: template?.display_name || request.template_id,
        report_type: request.report_type,
        report_labels: labels,
        status: request.status,
        status_label: customerStageLabel(request.status),
        completed_at: request.completed_at,
        source_count: request.source_count,
        finding_count: request.finding_count,
        open_question_count: request.open_question_count,
        actions: {
          view_report: canExposeViewReport(request),
          download_pdf: canExposeDownloadPdf(request),
        },
        simulated: isSimulationRequest(request),
        recovered_legacy_orphan: Boolean(request.recovered_legacy_orphan),
      };
    }),
    count: entries.length,
    active_count: filterLiveActiveRequests(index.requests || [], { includeSimulations }).length,
    latest_research_at: entries.map((e) => e.completed_at).filter(Boolean).sort().reverse()[0] || null,
  };
}

/**
 * Internal research history — all requests/runs including failed/partial.
 */
export function listResearchHistory(hotelId, opts = {}) {
  const repo = opts.repo || createResearchRepository({ env: opts.env });
  const index = repo.listHotelResearch(hotelId);
  return {
    hotel_id: hotelId,
    requests: index.requests || [],
    runs: index.runs || [],
    open_question_lineage: index.open_question_lineage || [],
  };
}
