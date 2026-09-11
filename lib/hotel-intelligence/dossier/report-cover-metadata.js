/**
 * Packet 2.6C publishing hotfix — shared cover / publishing metadata.
 * Values come from one canonical object; document types choose which to display.
 */

import { enrichReportForPublishing, resolveReportHotelIdentity } from "./report-hotel-identity.js";
import { reportTypeLabel } from "../research/report-export-contract.js";

/**
 * Build client-safe cover metadata for any intelligence report.
 * Never includes record/run/provider/cost/claim internals.
 */
export function buildReportCoverMetadata(dossier = {}) {
  const enriched = enrichReportForPublishing(dossier);
  const identity = enriched.report_hotel_identity || resolveReportHotelIdentity(enriched);
  const isAddendum =
    String(enriched.dossier_type || enriched.report_type || "").toUpperCase() ===
    "RESEARCH_ADDENDUM";
  const labels = reportTypeLabel(
    isAddendum ? "RESEARCH_ADDENDUM" : "FULL_INVESTIGATION",
    enriched.cover?.investigation_name || enriched.report_type_label || enriched.title
  );

  return {
    hotel_name: identity.hotel_name || enriched.hotel_name || null,
    display_location: identity.display_location,
    location_available: identity.location_available,
    report_title: isAddendum
      ? enriched.cover?.investigation_name || labels.title
      : labels.report_type_label,
    report_subtitle: isAddendum ? "Research Addendum" : null,
    document_type: isAddendum ? "RESEARCH ADDENDUM" : "FULL HOTEL INTELLIGENCE INVESTIGATION",
    follow_up_parent: enriched.lineage?.follow_up_to
      ? {
          label: enriched.lineage.follow_up_to.label || null,
          completed_display: enriched.lineage.follow_up_to.completed_display || null,
        }
      : null,
    completed_at: enriched.completed_at || null,
    source_count: Number(enriched.source_count || (enriched.sources || []).length || 0),
    finding_count: Number(
      enriched.finding_count || (enriched.key_findings || []).length || 0
    ),
    open_question_count: Number(
      enriched.open_question_count || (enriched.open_questions || []).length || 0
    ),
    status: enriched.investigation_status || enriched.status || null,
    template_id: enriched.template_id || null,
    report_type: enriched.dossier_type || enriched.report_type || null,
  };
}
