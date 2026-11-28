/**
 * Customer-facing DATE column for Hotel Explorer Sources & Evidence.
 *
 * Combines publication_date and verified/accessed into one DATE cell.
 * Underlying fields stay separate; display only consolidates them.
 *
 * Sort priority (documented):
 *   1. publication_date (when present)
 *   2. verified_at
 *   3. accessed_at
 * Missing primary sort values sort after dated rows (nulls last on desc).
 */

export const HE_CUSTOMER_SOURCE_DATE_V1 = "he-customer-source-date-v1";

/** Documented DATE-column sort priority. */
export const SOURCE_DATE_SORT_PRIORITY = Object.freeze([
  "publication_date",
  "verified_at",
  "accessed_at",
]);

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

/**
 * Format an existing date value for customer display (e.g. Nov 19, 2026).
 * Does not invent dates — returns null when the value cannot be parsed.
 */
export function formatReadableSourceDate(value) {
  if (value == null || value === "") return null;
  var s = String(value).trim();
  if (!s || s === "—") return null;

  var y;
  var m;
  var d;
  var mIso = /^(\d{4})-(\d{2})-(\d{2})/.exec(s);
  if (mIso) {
    y = Number(mIso[1]);
    m = Number(mIso[2]);
    d = Number(mIso[3]);
  } else if (/^\d{4}$/.test(s)) {
    return s;
  } else {
    var dt = new Date(s);
    if (isNaN(dt.getTime())) return null;
    y = dt.getUTCFullYear();
    m = dt.getUTCMonth() + 1;
    d = dt.getUTCDate();
  }
  if (!y || !m || !d || m < 1 || m > 12) return null;
  return MONTHS[m - 1] + " " + d + ", " + y;
}

/** Access/verification side of the DATE cell — prefer verified_at, else accessed_at. */
export function resolveAccessedDate(dates) {
  var d = dates || {};
  return d.verified_at || d.accessed_at || null;
}

/**
 * Build DATE cell lines from existing publication / access values.
 * @returns {{ lines: string[], text: string, sortValue: string|null, publication_date: *, accessed_date: * }}
 */
export function formatCustomerSourceDateCell(dates) {
  var d = dates || {};
  var publication = d.publication_date || null;
  var accessed = resolveAccessedDate(d);
  var pubLabel = formatReadableSourceDate(publication);
  var accLabel = formatReadableSourceDate(accessed);
  var lines = [];
  if (pubLabel) lines.push("Published: " + pubLabel);
  if (accLabel) lines.push("Accessed: " + accLabel);
  if (!lines.length) {
    return {
      lines: ["—"],
      text: "—",
      sortValue: null,
      publication_date: publication,
      accessed_date: accessed,
    };
  }
  return {
    lines: lines,
    text: lines.join("\n"),
    sortValue: citationDateSortIso(d),
    publication_date: publication,
    accessed_date: accessed,
  };
}

/** ISO-ish sortable string for the primary DATE value (priority order). */
export function citationDateSortIso(dates) {
  var d = dates || {};
  for (var i = 0; i < SOURCE_DATE_SORT_PRIORITY.length; i++) {
    var key = SOURCE_DATE_SORT_PRIORITY[i];
    var raw = d[key];
    if (raw == null || raw === "") continue;
    var s = String(raw).trim();
    if (!s) continue;
    var mIso = /^(\d{4}-\d{2}-\d{2})/.exec(s);
    if (mIso) return mIso[1];
    if (/^\d{4}$/.test(s)) return s + "-01-01";
    var dt = new Date(s);
    if (!isNaN(dt.getTime())) return dt.toISOString().slice(0, 10);
  }
  return null;
}

/**
 * Compare two normalized date objects for DATE-column sorting.
 * Default direction: desc (newest first); nulls last.
 */
export function compareCitationDates(a, b, dir) {
  var direction = dir === "asc" ? "asc" : "desc";
  var sa = citationDateSortIso(a);
  var sb = citationDateSortIso(b);
  if (sa == null && sb == null) return 0;
  if (sa == null) return 1;
  if (sb == null) return -1;
  if (sa === sb) return 0;
  if (direction === "asc") return sa < sb ? -1 : 1;
  return sa < sb ? 1 : -1;
}
