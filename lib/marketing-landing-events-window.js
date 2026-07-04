/**
 * Shared time-window helpers for landing analytics report APIs.
 */

const WINDOW_LABELS = {
  1: "Last 24 Hours",
  7: "Last 7 Days",
  14: "Last 14 Days",
  30: "Last 30 Days",
};

export function parseReportDays(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return 7;
  return Math.min(Math.floor(n), 90);
}

/**
 * @param {number|string} daysInput
 * @returns {{ days: number, since: string, until: string, label: string }}
 */
export function buildReportWindow(daysInput) {
  const days = parseReportDays(daysInput);
  const until = new Date();
  const since = new Date(until.getTime() - days * 24 * 60 * 60 * 1000);
  return {
    days,
    since: since.toISOString(),
    until: until.toISOString(),
    label: WINDOW_LABELS[days] || `Last ${days} Days`,
  };
}

export function loadOptionsForWindow(window) {
  return {
    since: window.since,
    until: window.until,
  };
}
