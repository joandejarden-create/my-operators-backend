/**
 * Canonical customer-facing AVA PDF filename.
 *
 * Convention:
 *   Dealality - AI Visibility Audit & Action Plan - [Hotel Name] - [MONTH][YEAR].pdf
 * Example:
 *   Dealality - AI Visibility Audit & Action Plan - Bethesda Marriott - NOVEMBER2026.pdf
 *
 * Never include property IDs, version IDs, or storage paths.
 */
import { AI_VISIBILITY_AUDIT_PRODUCT_TITLE } from "./audit-pdf-contract-v1.js";

export const AVA_PDF_FILENAME_PRODUCT = AI_VISIBILITY_AUDIT_PRODUCT_TITLE;

const MONTH_FULL = [
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

/** Strip filesystem-illegal characters; keep spaces and readable punctuation (& , ' . -). */
export function sanitizeAvaPdfFilenameToken(input, maxLen = 120) {
  return (
    String(input || "")
      .normalize("NFC")
      .replace(/[<>:"/\\|?*\u0000-\u001f]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, maxLen) || "Property"
  );
}

/**
 * Issue period token: NOVEMBER2026 (no spaces).
 * Accepts reportMonthLabel ("NOVEMBER 2026"), YYYY-MM, Date, or ISO string.
 */
export function formatAvaPdfIssueMonthYear(input) {
  if (input == null || input === "") return "";
  if (input instanceof Date && !Number.isNaN(input.getTime())) {
    return `${MONTH_FULL[input.getUTCMonth()]}${input.getUTCFullYear()}`;
  }
  const raw = String(input).trim();
  const labeled = raw.match(
    /^(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s*[-/]?\s*(\d{4})$/i
  );
  if (labeled) {
    return `${labeled[1].toUpperCase()}${labeled[2]}`;
  }
  const compact = raw.match(
    /^(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)(\d{4})$/i
  );
  if (compact) {
    return `${compact[1].toUpperCase()}${compact[2]}`;
  }
  const ym = raw.match(/^(\d{4})-(\d{2})(?:-\d{2})?/);
  if (ym) {
    const idx = Number(ym[2]) - 1;
    if (idx >= 0 && idx < 12) return `${MONTH_FULL[idx]}${ym[1]}`;
  }
  const d = new Date(raw);
  if (!Number.isNaN(d.getTime())) {
    return `${MONTH_FULL[d.getUTCMonth()]}${d.getUTCFullYear()}`;
  }
  return "";
}

/**
 * @param {object} opts
 * @param {string} [opts.hotelName]
 * @param {string} [opts.propertyName]
 * @param {string} [opts.reportMonthLabel] — e.g. "NOVEMBER 2026"
 * @param {string} [opts.issueMonthYear] — e.g. "NOVEMBER2026"
 * @param {string|Date} [opts.date]
 * @param {string} [opts.reportingMonthKey] — YYYY-MM
 */
export function buildAiVisibilityAuditPdfFilename(opts = {}) {
  const hotel = sanitizeAvaPdfFilenameToken(
    opts.hotelName || opts.propertyName || "Property",
    96
  );
  let period =
    opts.issueMonthYear ||
    formatAvaPdfIssueMonthYear(opts.reportMonthLabel) ||
    formatAvaPdfIssueMonthYear(opts.reportingMonthKey) ||
    formatAvaPdfIssueMonthYear(opts.date);
  if (!period) period = formatAvaPdfIssueMonthYear(new Date()) || "NOVEMBER2026";
  period = String(period)
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

  const name = `Dealality - ${AVA_PDF_FILENAME_PRODUCT} - ${hotel} - ${period}.pdf`;
  // Final pass: no path separators; always .pdf
  return (
    sanitizeAvaPdfFilenameToken(name, 220).replace(/\.pdf$/i, "") + ".pdf"
  );
}

export function contentDispositionForAvaPdf(
  filename,
  { disposition = "inline" } = {}
) {
  const safe = String(
    filename || `Dealality - ${AVA_PDF_FILENAME_PRODUCT}.pdf`
  )
    .replace(/[\r\n"]/g, "")
    .replace(/\.tmp$/i, ".pdf");
  const finalName = /\.pdf$/i.test(safe) ? safe : `${safe}.pdf`;
  // Reject anything that looks like a path
  if (/[/\\]/.test(finalName) || /\.\./.test(finalName)) {
    throw new Error("INVALID_PDF_FILENAME");
  }
  const asciiFallback = finalName.replace(/[^\x20-\x7E]/g, "_");
  const encoded = encodeURIComponent(finalName).replace(/['()]/g, escape);
  const mode = disposition === "attachment" ? "attachment" : "inline";
  return `${mode}; filename="${asciiFallback}"; filename*=UTF-8''${encoded}`;
}

/** Stable filename inputs from a stored audit version / payload. */
export function buildAvaPdfFilenameFromVersion(version, meta = {}) {
  const payload = version?.payload || {};
  const hotelName =
    payload.property?.name ||
    meta.name ||
    meta.propertyName ||
    "Property";
  return buildAiVisibilityAuditPdfFilename({
    hotelName,
    reportMonthLabel: payload.reporting?.reportMonthLabel,
    reportingMonthKey: payload.reporting?.reportingMonthKey,
    date: payload.reporting?.generatedAt || version?.savedAt,
  });
}
