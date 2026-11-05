/**
 * Normalize hotel need periods to validated start/end intervals.
 * Month labels resolve to true calendar bounds (e.g. Feb 2027 → 2027-02-01..2027-02-28).
 * Does not invent occupancy targets.
 */

import {
  NEED_DATE_DEMAND_INTELLIGENCE_V1,
  PERIOD_GRANULARITY,
  NEED_PERIOD_STATUS,
  NEED_REVENUE_TYPE,
} from "./need-date-demand-contract-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isoDay(d) {
  if (!(d instanceof Date) || Number.isNaN(d.getTime())) return null;
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function parseIsoDay(raw) {
  const s = text(raw).slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  const d = new Date(`${s}T00:00:00.000Z`);
  return Number.isNaN(d.getTime()) ? null : d;
}

const MONTH_NAME = Object.freeze({
  january: 1,
  jan: 1,
  february: 2,
  feb: 2,
  march: 3,
  mar: 3,
  april: 4,
  apr: 4,
  may: 5,
  june: 6,
  jun: 6,
  july: 7,
  jul: 7,
  august: 8,
  aug: 8,
  september: 9,
  sep: 9,
  sept: 9,
  october: 10,
  oct: 10,
  november: 11,
  nov: 11,
  december: 12,
  dec: 12,
});

/**
 * Parse "February 2027" / "2027-02" / "Feb 2027" into { year, month }.
 */
export function parseMonthYearLabel(label = "") {
  const s = text(label);
  const ym = s.match(/^(\d{4})-(\d{1,2})$/);
  if (ym) {
    const year = Number(ym[1]);
    const month = Number(ym[2]);
    if (month >= 1 && month <= 12) return { year, month };
  }
  const named = s.match(/^([A-Za-z]+)\s+(\d{4})$/);
  if (named) {
    const month = MONTH_NAME[named[1].toLowerCase()];
    const year = Number(named[2]);
    if (month && year >= 2000 && year <= 2100) return { year, month };
  }
  return null;
}

export function monthBoundsUtc(year, month) {
  const y = Number(year);
  const m = Number(month);
  if (!y || m < 1 || m > 12) return null;
  const start = new Date(Date.UTC(y, m - 1, 1));
  const end = new Date(Date.UTC(y, m, 0)); // last day of month
  return { startDate: isoDay(start), endDate: isoDay(end) };
}

export function daysInclusive(startIso, endIso) {
  const a = parseIsoDay(startIso);
  const b = parseIsoDay(endIso);
  if (!a || !b) return null;
  return Math.floor((b - a) / 86400000) + 1;
}

export function intervalsOverlap(aStart, aEnd, bStart, bEnd) {
  const as = parseIsoDay(aStart);
  const ae = parseIsoDay(aEnd);
  const bs = parseIsoDay(bStart);
  const be = parseIsoDay(bEnd);
  if (!as || !ae || !bs || !be) return false;
  return as <= be && bs <= ae;
}

/**
 * Normalize a raw need-period record (fixture / Airtable-shaped).
 */
export function normalizeNeedPeriod(raw = {}, opts = {}) {
  const hotelId = text(raw.hotelId || raw.hotelRecordId || opts.hotelId);
  const periodId = text(
    raw.needPeriodId || raw.id || raw.recordId || raw.periodId,
  );
  const label = text(raw.periodLabel || raw.label || raw.name);
  let granularity = text(raw.periodGranularity || raw.granularity);
  let startDate = text(raw.startDate).slice(0, 10) || null;
  let endDate = text(raw.endDate).slice(0, 10) || null;

  const monthParsed =
    parseMonthYearLabel(label) ||
    (raw.year && raw.month
      ? { year: Number(raw.year), month: Number(raw.month) }
      : null);

  if ((!startDate || !endDate) && monthParsed) {
    const bounds = monthBoundsUtc(monthParsed.year, monthParsed.month);
    if (bounds) {
      startDate = startDate || bounds.startDate;
      endDate = endDate || bounds.endDate;
      if (!granularity) granularity = PERIOD_GRANULARITY.MONTH;
    }
  }

  if (!granularity) {
    granularity =
      startDate &&
      endDate &&
      startDate.slice(0, 7) === endDate.slice(0, 7) &&
      startDate.endsWith("-01") &&
      daysInclusive(startDate, endDate) >= 28
        ? PERIOD_GRANULARITY.MONTH
        : PERIOD_GRANULARITY.DATE_RANGE;
  }

  const revenueNeed = normalizeRevenueNeedList(
    raw.revenueNeed || raw.revenueNeeds || raw.targetRevenueTypes,
  );

  const startOk = Boolean(parseIsoDay(startDate));
  const endOk = Boolean(parseIsoDay(endDate));
  const orderOk =
    startOk && endOk ? parseIsoDay(startDate) <= parseIsoDay(endDate) : false;

  const source = text(raw.source) || "fixture";
  const hotelReportedUnspecifiedRevenue =
    !revenueNeed.length &&
    /^(HOTEL_REPORTED|HOTEL_SALES_FEEDBACK|hotel_portal|hotel_sales)$/i.test(
      source,
    );

  const errors = [];
  if (!hotelId) errors.push("hotel_id_required");
  if (!periodId) errors.push("need_period_id_required");
  if (!startOk) errors.push("start_date_invalid");
  if (!endOk) errors.push("end_date_invalid");
  if (startOk && endOk && !orderOk) errors.push("end_before_start");
  // Hotel-reported commercial need months may legitimately omit revenue types
  // until sales provides stream detail — do not block inventory matching.
  if (!revenueNeed.length && !hotelReportedUnspecifiedRevenue) {
    errors.push("revenue_need_required");
  }

  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    needPeriodId: periodId || null,
    hotelId: hotelId || null,
    periodLabel:
      label ||
      (monthParsed
        ? `${Object.keys(MONTH_NAME).find((k) => MONTH_NAME[k] === monthParsed.month && k.length > 3) || monthParsed.month} ${monthParsed.year}`
        : `${startDate || "?"} – ${endDate || "?"}`),
    startDate: startOk ? startDate : null,
    endDate: endOk ? endDate : null,
    periodGranularity: granularity,
    revenueNeed,
    priority: text(raw.priority) || "MEDIUM",
    businessObjective: text(raw.businessObjective || raw.objective) || null,
    targetVolume: raw.targetVolume ?? null,
    targetRevenue: raw.targetRevenue ?? null,
    targetDemandSegments: Array.isArray(raw.targetDemandSegments)
      ? raw.targetDemandSegments.map(text).filter(Boolean)
      : [],
    commercialRestrictions: text(raw.commercialRestrictions) || null,
    status: text(raw.status) || NEED_PERIOD_STATUS.ACTIVE,
    // Preserve TEST_SCENARIO and lifecycle statuses from fixtures / Airtable.
    hotelSalesNotes: text(raw.hotelSalesNotes || raw.notes) || null,
    minGroupSize: raw.minGroupSize ?? null,
    maxGroupSize: raw.maxGroupSize ?? null,
    preferredWeekpart: text(raw.preferredWeekpart) || null,
    meetingCateringRequirements:
      text(raw.meetingCateringRequirements) || null,
    preferenceOverrides:
      raw.preferenceOverrides && typeof raw.preferenceOverrides === "object"
        ? raw.preferenceOverrides
        : null,
    source,
    revenueNeedUnspecified: hotelReportedUnspecifiedRevenue,
    valid: errors.length === 0,
    errors,
  };
}

export function normalizeRevenueNeedList(raw) {
  const arr = Array.isArray(raw) ? raw : raw ? [raw] : [];
  const out = [];
  const allowed = new Set(Object.values(NEED_REVENUE_TYPE));
  for (const item of arr) {
    const u = text(item).toUpperCase().replace(/\s+/g, "_");
    if (allowed.has(u)) {
      out.push(u);
      continue;
    }
    // Friendly aliases
    const aliases = {
      ROOMS: NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      GUESTROOMS: NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      GROUP_ROOMS: NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
      MEETINGS: NEED_REVENUE_TYPE.MEETING_SPACE,
      CATERING: NEED_REVENUE_TYPE.LOCAL_CATERING,
      PRIVATE_DINING: NEED_REVENUE_TYPE.PRIVATE_DINING,
      EXTENDED_STAY: NEED_REVENUE_TYPE.EXTENDED_STAY,
      WEDDING: NEED_REVENUE_TYPE.WEDDING_GROUP_GUESTROOMS,
      RESORT: NEED_REVENUE_TYPE.RESORT_GROUP,
    };
    if (aliases[u]) out.push(aliases[u]);
  }
  return [...new Set(out)];
}

export function validateNeedPeriodHotelLink(period, hotelId) {
  const p = period?.valid ? period : normalizeNeedPeriod(period, { hotelId });
  const expected = text(hotelId);
  if (!expected) {
    return { ok: false, reason: "hotel_id_required", period: p };
  }
  if (!p.hotelId) {
    return { ok: false, reason: "period_missing_hotel", period: p };
  }
  if (p.hotelId !== expected) {
    return {
      ok: false,
      reason: "period_hotel_mismatch",
      periodHotelId: p.hotelId,
      expectedHotelId: expected,
      period: p,
    };
  }
  return { ok: p.valid, reason: p.valid ? null : p.errors?.[0] || "invalid", period: p };
}
