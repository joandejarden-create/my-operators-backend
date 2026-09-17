/**
 * Canonical customer/admin filename for AI Demand Performance Review PDFs.
 * Gate: AI_DEMAND_PDF_TEMP_FILE_NAME_NEVER_USER_VISIBLE
 *
 * Convention (founder):
 *   Dealality - <Hotel Name> - AI Demand Performance Review - <MonDDyyyy>.pdf
 * Example:
 *   Dealality - Bethesda Marriott - AI Demand Performance Review - Sep112026.pdf
 */

export const AI_DEMAND_PERFORMANCE_REVIEW_FILENAME_PRODUCT =
  "AI Demand Performance Review";

const MONTHS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];

/** Strip characters illegal on Windows/macOS filesystems; keep spaces and hyphens. */
export function sanitizeAiDemandReviewFilenameToken(input, maxLen = 96) {
  return (
    String(input || "")
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[–—]/g, "-")
      .replace(/[<>:"/\\|?*\u0000-\u001f]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, maxLen) || "Property"
  );
}

/**
 * Deterministic date token: Sep112026 (no spaces, no separators).
 * @param {string|Date|number|null|undefined} isoOrDate
 */
export function formatAiDemandReviewFilenameDate(isoOrDate) {
  const d =
    isoOrDate instanceof Date
      ? isoOrDate
      : isoOrDate
        ? new Date(isoOrDate)
        : new Date();
  const safe = Number.isNaN(d.getTime()) ? new Date() : d;
  const mon = MONTHS[safe.getUTCMonth()] || "Jan";
  const day = String(safe.getUTCDate()).padStart(2, "0");
  const year = String(safe.getUTCFullYear());
  return `${mon}${day}${year}`;
}

/**
 * @param {object} opts
 * @param {string} [opts.hotelName]
 * @param {string} [opts.propertyName]
 * @param {string|Date} [opts.date] — monitoring / generated date
 * @param {string} [opts.reportingMonthKey] — YYYY-MM fallback when date absent
 */
export function buildAiDemandPerformanceReviewFilename(opts = {}) {
  const hotel = sanitizeAiDemandReviewFilenameToken(
    opts.hotelName || opts.propertyName || "Property",
    96
  );
  let dateToken = "";
  if (opts.date) {
    dateToken = formatAiDemandReviewFilenameDate(opts.date);
  } else if (opts.reportingMonthKey) {
    const m = String(opts.reportingMonthKey).match(/^(\d{4})-(\d{2})$/);
    if (m) {
      dateToken = formatAiDemandReviewFilenameDate(
        new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, 1))
      );
    }
  }
  if (!dateToken) dateToken = formatAiDemandReviewFilenameDate(new Date());

  const name = `Dealality - ${hotel} - ${AI_DEMAND_PERFORMANCE_REVIEW_FILENAME_PRODUCT} - ${dateToken}.pdf`;
  // Final safety: no path separators, always .pdf
  return sanitizeAiDemandReviewFilenameToken(name, 200).replace(/\.pdf$/i, "") + ".pdf";
}

export function contentDispositionForAiDemandPerformanceReviewPdf(
  filename,
  { disposition = "inline" } = {}
) {
  const safe = String(filename || "Dealality - AI Demand Performance Review.pdf")
    .replace(/[\r\n"]/g, "")
    .replace(/\.tmp$/i, ".pdf");
  const finalName = /\.pdf$/i.test(safe) ? safe : `${safe}.pdf`;
  const asciiFallback = finalName.replace(/[^\x20-\x7E]/g, "_");
  const encoded = encodeURIComponent(finalName).replace(/['()]/g, escape);
  const mode = disposition === "attachment" ? "attachment" : "inline";
  return `${mode}; filename="${asciiFallback}"; filename*=UTF-8''${encoded}`;
}
