/**
 * Canonical ADP report trends normalizer.
 *
 * Published reports must expose trends as an array of period points.
 * Some promotion/bake paths incorrectly stored an object map
 * (e.g. { "0": {...}, current: {...}, priorOfficialComparable: null, note: "..." }).
 */

/**
 * @param {unknown} trends
 * @returns {object[]}
 */
export function normalizeReportTrends(trends) {
  if (!trends) return [];

  const out = [];
  const seen = new Set();

  function pushPoint(point) {
    if (!point || typeof point !== "object" || Array.isArray(point)) return;
    // Skip null placeholders / metadata strings stored as sibling keys.
    if (point.periodId == null && point.date == null && point.role == null) return;
    const key = `${point.periodId || ""}|${point.role || "current"}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push(point);
  }

  if (Array.isArray(trends)) {
    for (const point of trends) pushPoint(point);
    return out;
  }

  if (typeof trends !== "object") return [];

  // Prefer explicit current / prior_* keys when present (richer metadata first).
  if (trends.current) pushPoint(trends.current);
  if (trends.prior_run) pushPoint(trends.prior_run);
  if (trends.prior) pushPoint(trends.prior);
  if (trends.priorOfficialComparable) pushPoint(trends.priorOfficialComparable);

  // Numeric / array-like keys from `{...array}` spreads.
  for (const [key, value] of Object.entries(trends)) {
    if (key === "note" || key === "meta" || key === "disclosure") continue;
    if (
      key === "current" ||
      key === "prior_run" ||
      key === "prior" ||
      key === "priorOfficialComparable"
    ) {
      continue;
    }
    if (value == null) continue;
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      continue;
    }
    pushPoint(value);
  }

  return out;
}

/**
 * @param {object|null|undefined} report — payload or full report wrapper
 * @returns {object[]}
 */
export function normalizeReportTrendsFromReport(report) {
  if (!report) return [];
  const payload = report.payload && typeof report.payload === "object" ? report.payload : report;
  return normalizeReportTrends(payload?.trends);
}

/**
 * Mutate report/payload in place so downstream consumers always see an array.
 * @param {object|null|undefined} report
 * @returns {object|null|undefined}
 */
export function ensureReportTrendsArray(report) {
  if (!report || typeof report !== "object") return report;
  if (report.payload && typeof report.payload === "object") {
    report.payload.trends = normalizeReportTrends(report.payload.trends);
    return report;
  }
  report.trends = normalizeReportTrends(report.trends);
  return report;
}
