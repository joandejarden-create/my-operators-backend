/**
 * Leisure need-period integration — optional, fail-open for demand credibility.
 * Never invent occupancy, ADR, or forecast values.
 */

export const GDI_LEISURE_NEED_PERIOD_V1 = "gdi-leisure-need-period-v1";

export const NEED_PERIOD_STATUS = Object.freeze({
  NEED_PERIOD_UNKNOWN: "NEED_PERIOD_UNKNOWN",
  OVERLAP: "OVERLAP",
  PARTIAL_OVERLAP: "PARTIAL_OVERLAP",
  NO_OVERLAP: "NO_OVERLAP",
  UNAUTHORIZED_SENSITIVE: "UNAUTHORIZED_SENSITIVE",
});

function text(v) {
  return String(v == null ? "" : v)
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {object} opportunity
 * @param {object|null} needPeriodContext — authorized hotel need dates / priorities
 */
export function evaluateLeisureNeedPeriod(opportunity = {}, needPeriodContext = null) {
  if (
    needPeriodContext &&
    needPeriodContext.authorized === false
  ) {
    return {
      status: NEED_PERIOD_STATUS.UNAUTHORIZED_SENSITIVE,
      overlaps: [],
      inventedMetrics: false,
      note: "Sensitive commercial need data not authorized for this run.",
    };
  }

  const periods = normalizePeriods(needPeriodContext);
  if (!periods.length) {
    return {
      status: NEED_PERIOD_STATUS.NEED_PERIOD_UNKNOWN,
      overlaps: [],
      inventedMetrics: false,
      note: "Approved need-period information unavailable — leisure demand not rejected.",
    };
  }

  const eventWindow = extractEventWindow(opportunity);
  if (!eventWindow.start && !eventWindow.label) {
    return {
      status: NEED_PERIOD_STATUS.NEED_PERIOD_UNKNOWN,
      overlaps: [],
      inventedMetrics: false,
      note: "Opportunity dates not established; need-period comparison deferred.",
    };
  }

  const overlaps = [];
  for (const p of periods) {
    const hit = windowsOverlap(eventWindow, p);
    if (hit) {
      overlaps.push({
        periodId: p.id || null,
        periodLabel: p.label || null,
        kind: p.kind || null,
        match: hit,
      });
    }
  }

  if (!overlaps.length) {
    return {
      status: NEED_PERIOD_STATUS.NO_OVERLAP,
      overlaps: [],
      inventedMetrics: false,
      note: "No overlap with authorized need periods; demand may still be credible.",
    };
  }

  const partial = overlaps.every((o) => o.match === "partial");
  return {
    status: partial
      ? NEED_PERIOD_STATUS.PARTIAL_OVERLAP
      : NEED_PERIOD_STATUS.OVERLAP,
    overlaps,
    inventedMetrics: false,
    note: null,
  };
}

function normalizePeriods(ctx) {
  if (!ctx || typeof ctx !== "object") return [];
  const raw =
    ctx.needPeriods ||
    ctx.periods ||
    ctx.authorizedNeedDates ||
    ctx.needDates ||
    [];
  if (!Array.isArray(raw)) return [];
  return raw
    .map((p, i) => ({
      id: p.id || `need_${i}`,
      label: text(p.label || p.periodLabel || p.name),
      kind: text(p.kind || p.type) || null,
      start: p.start || p.startDate || null,
      end: p.end || p.endDate || null,
      weekdays: p.weekdays || null,
      weekends: p.weekends === true,
      shoulder: p.shoulder === true || /shoulder|low.?demand/i.test(text(p.kind)),
    }))
    .filter((p) => p.label || p.start);
}

function extractEventWindow(opportunity) {
  const dates = opportunity.eventOrRecurringDates || opportunity.dates || null;
  if (dates && typeof dates === "object") {
    return {
      start: dates.start || dates.startDate || null,
      end: dates.end || dates.endDate || null,
      label: text(dates.label || ""),
    };
  }
  if (typeof dates === "string" && dates.trim()) {
    return { start: null, end: null, label: text(dates) };
  }
  return {
    start: opportunity.startDate || null,
    end: opportunity.endDate || null,
    label: text(opportunity.dateLabel || ""),
  };
}

function windowsOverlap(eventWindow, period) {
  if (eventWindow.start && period.start) {
    const es = Date.parse(eventWindow.start);
    const ee = Date.parse(eventWindow.end || eventWindow.start);
    const ps = Date.parse(period.start);
    const pe = Date.parse(period.end || period.start);
    if (Number.isFinite(es) && Number.isFinite(ps)) {
      const eEnd = Number.isFinite(ee) ? ee : es;
      const pEnd = Number.isFinite(pe) ? pe : ps;
      if (es <= pEnd && eEnd >= ps) return "date_range";
      return null;
    }
  }
  // Label-only soft match — never invent metrics
  if (eventWindow.label && period.label) {
    const a = eventWindow.label.toLowerCase();
    const b = period.label.toLowerCase();
    if (a.includes(b) || b.includes(a)) return "partial";
  }
  return null;
}
