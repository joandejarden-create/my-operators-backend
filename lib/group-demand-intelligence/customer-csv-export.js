/**
 * GDI customer-facing CSV export — sales Excel columns only.
 * No DTO wrapper, no hotelId/schemaVersion/_listDto, no nested JSON.
 */

import {
  BOOKING_WINDOW_LABEL,
  DEMAND_TERRITORY_FIT_LABEL,
  OPPORTUNITY_QUALIFICATION_LABEL,
} from "./claim-types.js";

export const GDI_CUSTOMER_CSV_V1 = "gdi_customer_csv_v1";

/** Exact customer column order (binding). */
export const GDI_CUSTOMER_CSV_HEADERS = Object.freeze([
  "Dealality Opportunity ID",
  "Opportunity",
  "Organization",
  "Segment",
  "Priority",
  "Start Date",
  "End Date",
  "Qualification",
  "Booking Window",
  "Demand Territory",
  "Opportunity Summary",
  "Why Now",
  "Recommended Action",
  "Contact Name",
  "Contact Role",
  "Email",
  "Phone",
  "Phone Type",
  "Current Status",
  "Latest Action",
  "Latest Action Date",
  "Latest Outcome",
  "Latest Outcome Date",
]);

const PRIORITY_LABEL = Object.freeze({
  HIGH_PRIORITY: "High Priority",
  MEDIUM_PRIORITY: "Medium Priority",
  WATCHLIST: "Watchlist",
  DISQUALIFIED: "Disqualified",
  HIGH: "High Priority",
  MEDIUM: "Medium Priority",
});

/** Fields that must never appear in customer CSV body or header. */
export const GDI_CSV_FORBIDDEN_TOKENS = Object.freeze([
  "hotelId",
  "runId",
  "schemaVersion",
  "_listDto",
  "hotelFitScore",
  "evidenceConfidence",
  '"ok"',
  "view:",
  "count:",
]);

function humanizeEnum(value) {
  if (value == null || value === "") return "";
  return String(value)
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/** Title-case ALL CAPS labels (e.g. QUALIFY NOW → Qualify Now). */
function toCustomerTitleCase(label) {
  const s = String(label || "").trim();
  if (!s) return "";
  if (/^[A-Z0-9][A-Z0-9\s/-]*$/.test(s) && /[A-Z]/.test(s)) {
    return s
      .toLowerCase()
      .replace(/\b\w/g, (c) => c.toUpperCase());
  }
  return s;
}

function labelFrom(map, value, fallbackHumanize = true) {
  if (value == null || value === "") return "";
  const key = String(value);
  if (map && map[key]) return toCustomerTitleCase(map[key]);
  // Already a mixed-case display label?
  if (/[a-z]/.test(key) && /[A-Z]/.test(key)) return key;
  return fallbackHumanize ? humanizeEnum(key) : key;
}

/** ISO date YYYY-MM-DD for Excel sort; blank when unknown. Never invent. */
export function toCsvIsoDate(value) {
  if (value == null || value === "") return "";
  const s = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const t = Date.parse(s);
  if (Number.isNaN(t)) return "";
  return new Date(t).toISOString().slice(0, 10);
}

/**
 * CSV cell escape + Excel formula-injection guard.
 * Leading = + - @ | tab CR → prefix apostrophe (Excel text).
 */
export function escapeCsvCell(value) {
  let s = value == null ? "" : String(value);
  if (/^[=+\-@|\t\r]/.test(s)) {
    s = `'${s}`;
  }
  if (/[",\n\r]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

export function rowsToCustomerCsv(rows = []) {
  const headers = GDI_CUSTOMER_CSV_HEADERS;
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => escapeCsvCell(row[h])).join(","));
  }
  // CRLF for Excel on Windows
  return lines.join("\r\n") + "\r\n";
}

/**
 * Flatten one opportunity into customer sales columns.
 */
export function toCustomerExportRow(opp = {}) {
  const c = opp.primaryContact || {};
  const cp = opp.commercialProgression || {};
  const priorityRaw = opp.priority || "";
  const bookingRaw = opp.bookingWindowStatus || "";
  const qualRaw =
    opp.opportunityQualification ||
    opp.opportunityQualificationLabel ||
    "";
  const territoryRaw = opp.demandTerritoryFit || "";

  return {
    "Dealality Opportunity ID": opp.id || opp.opportunityId || "",
    Opportunity: opp.title || "",
    Organization: opp.organizationName || "",
    Segment: opp.segment || "",
    Priority:
      opp.priorityLabel ||
      labelFrom(PRIORITY_LABEL, priorityRaw),
    "Start Date": toCsvIsoDate(opp.eventStartDate),
    "End Date": toCsvIsoDate(opp.eventEndDate),
    Qualification:
      opp.opportunityQualificationLabel ||
      labelFrom(OPPORTUNITY_QUALIFICATION_LABEL, qualRaw),
    "Booking Window":
      toCustomerTitleCase(
        opp.bookingWindowLabel || labelFrom(BOOKING_WINDOW_LABEL, bookingRaw)
      ),
    "Demand Territory":
      opp.demandTerritoryFitLabel ||
      labelFrom(DEMAND_TERRITORY_FIT_LABEL, territoryRaw),
    "Opportunity Summary":
      opp.summaryWhat || opp.summaryWhyMatters || "",
    "Why Now": opp.whyNow || "",
    "Recommended Action":
      opp.recommendedAction || opp.recommendedNextStep || "",
    "Contact Name": c.name || "",
    "Contact Role": c.role || c.title || "",
    Email: c.email || "",
    Phone: c.phone || "",
    "Phone Type": c.phoneTypeLabel || c.phoneType || "",
    "Current Status":
      cp.currentStatusLabel ||
      cp.compactStatusLabel ||
      cp.funnelStageLabel ||
      "",
    "Latest Action": cp.latestActionLabel || "",
    "Latest Action Date": toCsvIsoDate(cp.latestActionDate),
    "Latest Outcome": cp.latestOutcomeLabel || "",
    "Latest Outcome Date": toCsvIsoDate(cp.latestOutcomeDate),
  };
}

/**
 * Build UTF-8 BOM + CSV body for Excel.
 * No meta comment block (avoids leaking hotelId / schema / internal filter JSON).
 */
export function buildCustomerExportCsv(opportunities = []) {
  const rows = (opportunities || []).map((o) => toCustomerExportRow(o));
  const body = rowsToCustomerCsv(rows);
  // UTF-8 BOM for Excel accented-character handling
  return `\uFEFF${body}`;
}

/** Readable download filename — no record IDs. */
export function buildCustomerCsvFilename(hotelName = "Hotel") {
  const base = String(hotelName || "Hotel")
    .replace(/[<>:"/\\|?*\x00-\x1f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);
  const safe = base || "Hotel";
  return `${safe} - GDI Opportunities.csv`;
}

/**
 * Content-Disposition attachment header (RFC 5987 filename* for unicode).
 */
export function customerCsvContentDisposition(hotelName) {
  const filename = buildCustomerCsvFilename(hotelName);
  const ascii = filename.replace(/[^\x20-\x7E]/g, "_");
  const encoded = encodeURIComponent(filename);
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encoded}`;
}

export function assertCustomerCsvClean(csvText = "") {
  const text = String(csvText || "");
  const hits = [];
  if (text.trimStart().startsWith("{")) {
    hits.push("json_wrapper");
  }
  for (const tok of GDI_CSV_FORBIDDEN_TOKENS) {
    if (text.includes(tok)) hits.push(tok);
  }
  // Nested JSON blobs
  if (/primaryContact\s*:/.test(text) || /commercialProgression\s*:/.test(text)) {
    hits.push("nested_object");
  }
  return { ok: hits.length === 0, hits };
}
