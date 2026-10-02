/**
 * Canonical Dealality PDF report cover — shared by ADP monthly review + GDI.
 * Geometry comes from brand-alignment-snapshot.css + adp-mr-pdf-host rules
 * in adp-monthly-review-report-v1.css. Only slot content differs by report type.
 */

export const DEALALITY_REPORT_COVER_LOGO_URL =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png";

export function escHtml(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function formatCoverShortDate(isoOrLabel) {
  if (!isoOrLabel) return "—";
  const s = String(isoOrLabel);
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return s.toUpperCase();
  const months = [
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
  ];
  const mon = months[parseInt(m[2], 10) - 1] || m[2];
  return `${mon} ${parseInt(m[3], 10)}, ${m[1]}`;
}

export function formatCoverMonthUpper(label) {
  return String(label || "")
    .trim()
    .toUpperCase();
}

export function formatCoverMonthYearFromIso(iso) {
  if (!iso) return "—";
  const m = String(iso).match(/^(\d{4})-(\d{2})/);
  if (!m) return formatCoverMonthUpper(iso);
  const months = [
    "JANUARY",
    "FEBRUARY",
    "MARCH",
    "APRIL",
    "MAY",
    "JUNE",
    "JULY",
    "AUGUST",
    "SEPTEMBER",
    "OCTOBER",
    "NOVEMBER",
    "DECEMBER",
  ];
  const mon = months[parseInt(m[2], 10) - 1] || m[2];
  return `${mon} ${m[1]}`;
}

/**
 * @param {object} opts
 * @param {string} opts.confidentialLine — top metadata line
 * @param {string} opts.reportTitle — bas-cover-doc-type slot (report product title)
 * @param {string} opts.hotelName — h1
 * @param {string} [opts.marketLabel] — location line
 * @param {string} opts.reportClass — bas-cover-sub slot
 * @param {string} opts.periodLine — first bas-cover-date line
 * @param {string} opts.descriptorLine — second bas-cover-date line
 * @param {string} opts.disclaimer — bas-cover-disclaimer
 * @param {string} [opts.logoUrl]
 * @param {string} [opts.sectionAttr] — e.g. data-adp-mr-section="cover" or data-gdi-pdf-section="cover"
 * @param {string} [opts.sectionAttrName] — attribute name without data- prefix
 * @param {string} [opts.sectionAttrValue]
 */
export function renderDealalityReportCover(opts = {}) {
  const logoUrl = opts.logoUrl || DEALALITY_REPORT_COVER_LOGO_URL;
  const market = opts.marketLabel ? String(opts.marketLabel).trim() : "";
  const sectionAttrName = opts.sectionAttrName || "data-adp-mr-section";
  const sectionAttrValue = opts.sectionAttrValue || "cover";
  const sectionAttr = `${sectionAttrName}="${escHtml(sectionAttrValue)}"`;

  return (
    `<section class="bas-cover-page bas-book-page-surface bas-avoid-break hid-cover-page drs-cover-shell" ${sectionAttr} aria-label="Cover">` +
    '<div class="bas-cover-geometric" aria-hidden="true"></div>' +
    `<p class="bas-cover-confidential">${escHtml(opts.confidentialLine || "")}</p>` +
    '<div class="bas-cover-block">' +
    `<p class="bas-cover-doc-type">${escHtml(opts.reportTitle || "")}</p>` +
    `<h1 class="bas-cover-title">${escHtml(opts.hotelName || "Property")}</h1>` +
    (market ? `<p class="bas-cover-location">${escHtml(market)}</p>` : "") +
    '<div class="bas-cover-accent-line" aria-hidden="true"></div>' +
    `<p class="bas-cover-sub">${escHtml(opts.reportClass || "")}</p>` +
    `<p class="bas-cover-date">${escHtml(opts.periodLine || "")}</p>` +
    `<p class="bas-cover-date">${escHtml(opts.descriptorLine || "")}</p>` +
    "</div>" +
    `<p class="bas-cover-disclaimer">${escHtml(opts.disclaimer || "")}</p>` +
    `<div class="bas-cover-hero"><div class="bas-cover-logo-block"><img src="${escHtml(
      logoUrl
    )}" alt="Dealality" class="bas-cover-logo-img" width="140" height="auto"></div></div>` +
    "</section>"
  );
}

/** ADP monthly executive review cover slots (canonical reference). */
export function renderAdpMonthlyReviewCover(review, logoUrl) {
  const prop = review?.property || {};
  const loc = prop.market || prop.city || "";
  const r = review?.reporting || {};
  const periodLine =
    formatCoverMonthUpper(r.reportingMonth) +
    " · CURRENT MONITORING " +
    formatCoverShortDate(r.currentMonitoringDate) +
    " · PRIOR RUN " +
    formatCoverShortDate(r.priorRunDate);

  return renderDealalityReportCover({
    confidentialLine:
      "DEALALITY AI DEMAND INTELLIGENCE · CONFIDENTIAL · FOR RECIPIENT ONLY",
    reportTitle: "AI DEMAND PERFORMANCE REVIEW",
    hotelName: prop.name || "Property",
    marketLabel: loc,
    reportClass: "MONTHLY EXECUTIVE REVIEW",
    periodLine,
    descriptorLine:
      "PERFORMANCE · COMPETITIVE MOVEMENT · EVIDENCE · ACTIONS · ACCOUNTABILITY",
    disclaimer:
      "This AI Demand Performance Review is a Dealality executive monitoring report. Measurement methodology is unchanged by presentation. Confidential — for the named recipient only.",
    logoUrl,
    sectionAttrName: "data-adp-mr-section",
    sectionAttrValue: "cover",
  });
}

/** GDI report cover — same slots as ADP, GDI content only. */
export function renderGdiReportCover(data, logoUrl) {
  const hotel = data?.hotel || {};
  const meta = data?.reportMetadata || {};
  const reportDate = meta.reportDate || "";
  const monthUpper =
    meta.reportMonthLabel ||
    formatCoverMonthYearFromIso(reportDate) ||
    formatCoverMonthUpper(meta.reportDateLabel);
  const periodLine =
    formatCoverMonthUpper(monthUpper) +
    " · REPORT DATE " +
    formatCoverShortDate(reportDate);

  // Prefer human market labels; never surface bare technical codes like "DMV".
  let market =
    hotel.marketLabel ||
    hotel.marketDisplay ||
    hotel.city ||
    hotel.market ||
    "";
  if (/^dmv$/i.test(String(market).trim())) {
    market = "Washington Metropolitan Area";
  }
  if (/washington metropolitan/i.test(String(market))) {
    market = "Washington Metropolitan Area";
  }

  return renderDealalityReportCover({
    confidentialLine:
      "DEALALITY GROUP & DEMAND INTELLIGENCE · CONFIDENTIAL · FOR RECIPIENT ONLY",
    reportTitle: "GROUP & DEMAND INTELLIGENCE",
    hotelName: hotel.name || "Property",
    marketLabel: market,
    reportClass: "COMMERCIAL DEMAND REVIEW",
    periodLine,
    descriptorLine: "OPPORTUNITIES · CONTACTS · PRIORITIES · NEXT ACTIONS",
    // Compact footprint — same ~3-line bounding box as ADP cover disclaimer.
    disclaimer:
      "This Group & Demand Intelligence Review is a Dealality commercial intelligence report. Opportunities reflect evidence available at publication. Confidential — for the named recipient only.",
    logoUrl,
    sectionAttrName: "data-gdi-pdf-section",
    sectionAttrValue: "cover",
  });
}
