/**
 * Packet 2.6C publishing hotfix — customer-safe report download filenames.
 * Shared by Full HI + all Research Addenda. No internal IDs in filenames.
 */

export const REPORT_FILENAME_SLUGS = Object.freeze({
  FULL_HOTEL_INTELLIGENCE_INVESTIGATION: "full-hotel-intelligence-investigation",
  FULL_INVESTIGATION: "full-hotel-intelligence-investigation",
  CHANGE_OPPORTUNITY: "change-opportunity-investigation",
  DECISION_AUTHORITY: "decision-authority-investigation",
  BRAND_OPERATOR_AGREEMENT: "brand-operator-agreement-investigation",
  OWNERSHIP_CAPITAL_EVENTS: "ownership-capital-events-investigation",
  REPOSITIONING_DEVELOPMENT: "repositioning-development-investigation",
  OWNER_PORTFOLIO: "owner-portfolio-multi-asset-investigation",
  RESEARCH_ADDENDUM: "research-addendum-investigation",
});

export function slugifyReportToken(input, maxLen = 72) {
  return String(input || "report")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, maxLen) || "report";
}

export function resolveReportFilenameSlug(report = {}) {
  const templateId = String(report.template_id || "").toUpperCase();
  if (templateId && REPORT_FILENAME_SLUGS[templateId]) {
    return REPORT_FILENAME_SLUGS[templateId];
  }
  const type = String(report.dossier_type || report.report_type || "").toUpperCase();
  if (type === "RESEARCH_ADDENDUM" || type === "RESEARCH ADDENDUM") {
    if (templateId && REPORT_FILENAME_SLUGS[templateId]) {
      return REPORT_FILENAME_SLUGS[templateId];
    }
    return REPORT_FILENAME_SLUGS.RESEARCH_ADDENDUM;
  }
  if (
    type === "FULL_HOTEL_INTELLIGENCE_INVESTIGATION" ||
    type === "FULL_INVESTIGATION" ||
    /full.?hotel.?intelligence/i.test(String(report.dossier_id || ""))
  ) {
    return REPORT_FILENAME_SLUGS.FULL_HOTEL_INTELLIGENCE_INVESTIGATION;
  }
  // Cover investigation name fallback (customer-safe)
  const inv =
    report.cover?.investigation_name ||
    report.report_type_label ||
    report.title ||
    "investigation";
  return slugifyReportToken(inv, 64);
}

function formatDateUTC(iso) {
  const d = iso ? new Date(iso) : new Date();
  if (Number.isNaN(d.getTime())) {
    const now = new Date();
    return now.toISOString().slice(0, 10);
  }
  return d.toISOString().slice(0, 10);
}

function formatHHmmUTC(iso) {
  const d = iso ? new Date(iso) : new Date();
  if (Number.isNaN(d.getTime())) return "0000";
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  return `${hh}${mm}`;
}

/**
 * Build customer download filename for a report.
 * @param {object} report — dossier-shaped
 * @param {object} [opts]
 * @param {object[]} [opts.siblings] — other reports for same hotel (collision)
 * @param {string[]} [opts.existingFilenames] — already reserved names
 */
export function buildReportDownloadFilename(report = {}, opts = {}) {
  const hotelSlug = slugifyReportToken(report.hotel_name || "hotel", 48);
  const reportSlug = resolveReportFilenameSlug(report);
  const date = formatDateUTC(report.completed_at || report.updated_at);
  const base = `${hotelSlug}-${reportSlug}-${date}.pdf`;

  const existing = new Set(
    (opts.existingFilenames || []).map((f) => String(f || "").toLowerCase())
  );

  const siblings = Array.isArray(opts.siblings) ? opts.siblings : [];
  const selfId = String(report.dossier_id || report.report_id || "");
  const sameKey = siblings.filter((s) => {
    if (!s) return false;
    const sid = String(s.dossier_id || s.report_id || "");
    if (selfId && sid === selfId) return false;
    return (
      slugifyReportToken(s.hotel_name || "hotel", 48) === hotelSlug &&
      resolveReportFilenameSlug(s) === reportSlug &&
      formatDateUTC(s.completed_at || s.updated_at) === date
    );
  });

  if (!sameKey.length && !existing.has(base.toLowerCase())) {
    return assertNoInternalIds(base);
  }

  // Collision: prefer HHmm from completion time
  const hm = formatHHmmUTC(report.completed_at || report.updated_at);
  const withTime = `${hotelSlug}-${reportSlug}-${date}-${hm}.pdf`;
  if (!existing.has(withTime.toLowerCase())) {
    // Ensure uniqueness vs siblings that share same minute
    const clash = sameKey.some((s) => {
      const other = `${hotelSlug}-${reportSlug}-${date}-${formatHHmmUTC(s.completed_at)}.pdf`;
      return other.toLowerCase() === withTime.toLowerCase() &&
        String(s.dossier_id || "") !== selfId;
    });
    if (!clash) return assertNoInternalIds(withTime);
  }

  for (let i = 2; i < 100; i += 1) {
    const seq = `${hotelSlug}-${reportSlug}-${date}-${String(i).padStart(2, "0")}.pdf`;
    if (!existing.has(seq.toLowerCase())) return assertNoInternalIds(seq);
  }
  return assertNoInternalIds(withTime);
}

function assertNoInternalIds(filename) {
  const s = String(filename || "");
  if (/\brec[A-Za-z0-9]{10,}\b/.test(s)) {
    throw new Error("filename_contains_airtable_id");
  }
  if (/\b(req_|run_|art_|dhl_)[a-z0-9]+\b/i.test(s)) {
    throw new Error("filename_contains_internal_id");
  }
  if (/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i.test(s)) {
    throw new Error("filename_contains_uuid");
  }
  return s;
}

/** Content-Disposition header value (ASCII + UTF-8 filename*). */
export function contentDispositionAttachment(filename) {
  const safe = String(filename || "report.pdf").replace(/"/g, "");
  const encoded = encodeURIComponent(safe).replace(/['()]/g, escape);
  return `attachment; filename="${safe}"; filename*=UTF-8''${encoded}`;
}
