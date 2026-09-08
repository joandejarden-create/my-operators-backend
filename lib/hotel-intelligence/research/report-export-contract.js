/**
 * Packet 2.6C — report / PDF export interface contract.
 * Uses existing R9 dossier PDF endpoints — does not fork PDF generation.
 */

export const REPORT_EXPORT_CONTRACT_VERSION = "hotel-intelligence-report-export-v1";

export const REPORT_KINDS = Object.freeze({
  FULL_INVESTIGATION: "FULL_INVESTIGATION",
  RESEARCH_ADDENDUM: "RESEARCH_ADDENDUM",
});

/**
 * Resolve customer-facing report + PDF links for a research request.
 * Prefer existing dossier PDF routes from Packet R9.
 */
export function resolveReportExportLinks(request = {}) {
  const reportId = request.report_id || null;
  const hotelId = request.hotel_id || null;
  const kind =
    request.report_type ||
    (String(request.template_id || "").toUpperCase() === "FULL_HOTEL_INTELLIGENCE"
      ? REPORT_KINDS.FULL_INVESTIGATION
      : REPORT_KINDS.RESEARCH_ADDENDUM);

  if (!reportId) {
    return {
      contract_version: REPORT_EXPORT_CONTRACT_VERSION,
      report_kind: kind,
      report_id: null,
      view_report: null,
      download_pdf: null,
      pdf_endpoint: null,
    };
  }

  // Canonical R9 PDF interface — do not invent a second generator.
  const pdfEndpoint = `/api/hotel-intelligence/dossiers/${encodeURIComponent(reportId)}/pdf`;
  const viewEndpoint = `/api/hotel-intelligence/dossiers/${encodeURIComponent(reportId)}`;

  return {
    contract_version: REPORT_EXPORT_CONTRACT_VERSION,
    report_kind: kind,
    report_id: reportId,
    hotel_id: hotelId,
    parent_report_id: request.parent_report_id || null,
    view_report: { type: "dossier_reader", dossier_id: reportId, api: viewEndpoint },
    download_pdf: { type: "canonical_r9_pdf", endpoint: pdfEndpoint },
    pdf_endpoint: pdfEndpoint,
    note: "PDF generation delegated to Packet 2.6B-R9 canonical engine.",
  };
}

/**
 * Customer-facing report type labels.
 */
export function reportTypeLabel(reportType, templateDisplayName) {
  if (String(reportType).toUpperCase() === REPORT_KINDS.RESEARCH_ADDENDUM) {
    return {
      product_line: "Dealality Intelligence Dossier",
      report_type_label: "Research Addendum",
      title: templateDisplayName || "Research Addendum",
    };
  }
  return {
    product_line: "Dealality Intelligence Dossier",
    report_type_label: "Full Hotel Intelligence Investigation",
    title: templateDisplayName || "Full Hotel Intelligence Investigation",
  };
}
