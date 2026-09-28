/**
 * GDI active-date eligibility for customer-visible opportunities.
 * Uses event END date (not start) to avoid mid-event and off-by-one defects.
 * Never invents future cycles from past records.
 */

export const ACTIVE_DATE_CLASS = Object.freeze({
  ACTIVE_FUTURE: "ACTIVE_FUTURE",
  ACTIVE_CURRENT: "ACTIVE_CURRENT",
  PAST_CLOSED: "PAST_CLOSED",
  PAST_WITH_VALID_FUTURE_MOTION: "PAST_WITH_VALID_FUTURE_MOTION",
  UNKNOWN_DATE: "UNKNOWN_DATE",
});

/**
 * Local/business calendar day as YYYY-MM-DD (no UTC shift of civil dates).
 */
export function businessDateYmd(now = new Date(), timeZone = null) {
  const tz =
    timeZone ||
    process.env.GDI_BUSINESS_TIMEZONE ||
    process.env.TZ ||
    "America/New_York";
  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(now instanceof Date ? now : new Date(now));
    const y = parts.find((p) => p.type === "year")?.value;
    const m = parts.find((p) => p.type === "month")?.value;
    const d = parts.find((p) => p.type === "day")?.value;
    if (y && m && d) return `${y}-${m}-${d}`;
  } catch {
    /* fall through */
  }
  const dt = now instanceof Date ? now : new Date(now);
  const y = dt.getFullYear();
  const m = String(dt.getMonth() + 1).padStart(2, "0");
  const d = String(dt.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function parseYmd(value) {
  const s = String(value || "").trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  return s;
}

function ymdCompare(a, b) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

/**
 * Explicit future commercial motion after an event end date.
 * Must be evidence-backed flags — never inferred from title alone.
 */
export function hasValidFutureCommercialMotion(opp = {}) {
  if (opp.futureCommercialMotionOpen === true && opp.futureCommercialMotionEvidence) {
    return true;
  }
  if (opp.activeDateClass === ACTIVE_DATE_CLASS.PAST_WITH_VALID_FUTURE_MOTION) {
    return Boolean(opp.futureCommercialMotionEvidence || opp.futureCycleId);
  }
  const cycle = opp.futureCycleState || opp.eventCycleState || null;
  if (
    cycle &&
    /OPEN|CONFIRMED_NEXT|ANNOUNCED_NEXT|ACTIVE_NEXT/i.test(String(cycle)) &&
    (opp.futureCycleId || opp.nextEventCycleId || opp.futureCommercialMotionEvidence)
  ) {
    return true;
  }
  return false;
}

/**
 * Resolve the date used for past/active gating (prefer end, else start).
 */
export function resolveGateEndDate(opp = {}) {
  const end = parseYmd(opp.eventEndDate);
  if (end) return end;
  const start = parseYmd(opp.eventStartDate);
  if (start) return start;
  return null;
}

/**
 * Year-only future (e.g. eventYear 2027 with no day) may stay active.
 * Year-only past must not.
 */
function classifyYearOnly(opp, todayYmd) {
  const yearRaw =
    opp.eventYear ||
    String(opp.eventStartDate || "").match(/^(20\d{2})/)?.[1] ||
    String(opp.title || opp.eventName || "").match(/\b(20\d{2})\b/)?.[1] ||
    null;
  const year = yearRaw ? Number(yearRaw) : null;
  if (!year || !Number.isFinite(year)) return ACTIVE_DATE_CLASS.UNKNOWN_DATE;
  const todayYear = Number(String(todayYmd).slice(0, 4));
  if (year > todayYear) return ACTIVE_DATE_CLASS.ACTIVE_FUTURE;
  if (year < todayYear) return ACTIVE_DATE_CLASS.PAST_CLOSED;
  // Same calendar year, day unknown — not automatically active
  return ACTIVE_DATE_CLASS.UNKNOWN_DATE;
}

/**
 * Classify opportunity timing for the active customer surface.
 */
export function classifyActiveDate(opp = {}, { nowDate = null, timeZone = null } = {}) {
  const todayYmd = parseYmd(nowDate) || businessDateYmd(new Date(), timeZone);
  const start = parseYmd(opp.eventStartDate);
  const end = resolveGateEndDate(opp);

  if (!end && !start) {
    const yearClass = classifyYearOnly(opp, todayYmd);
    if (yearClass === ACTIVE_DATE_CLASS.PAST_CLOSED && hasValidFutureCommercialMotion(opp)) {
      return {
        activeDateClass: ACTIVE_DATE_CLASS.PAST_WITH_VALID_FUTURE_MOTION,
        gateEndDate: null,
        todayYmd,
        activeEligible: true,
      };
    }
    return {
      activeDateClass: yearClass,
      gateEndDate: null,
      todayYmd,
      activeEligible: yearClass === ACTIVE_DATE_CLASS.ACTIVE_FUTURE,
    };
  }

  const gateEnd = end || start;
  const cmpEnd = ymdCompare(gateEnd, todayYmd);

  if (cmpEnd < 0) {
    if (hasValidFutureCommercialMotion(opp)) {
      return {
        activeDateClass: ACTIVE_DATE_CLASS.PAST_WITH_VALID_FUTURE_MOTION,
        gateEndDate: gateEnd,
        todayYmd,
        activeEligible: true,
      };
    }
    return {
      activeDateClass: ACTIVE_DATE_CLASS.PAST_CLOSED,
      gateEndDate: gateEnd,
      todayYmd,
      activeEligible: false,
    };
  }

  if (start && ymdCompare(start, todayYmd) <= 0 && cmpEnd >= 0) {
    return {
      activeDateClass: ACTIVE_DATE_CLASS.ACTIVE_CURRENT,
      gateEndDate: gateEnd,
      todayYmd,
      activeEligible: true,
    };
  }

  return {
    activeDateClass: ACTIVE_DATE_CLASS.ACTIVE_FUTURE,
    gateEndDate: gateEnd,
    todayYmd,
    activeEligible: true,
  };
}

/**
 * True when the opportunity may appear on the default active customer list.
 */
export function isActiveCustomerDateEligible(opp = {}, opts = {}) {
  if (opp.customerActiveEligible === false) return false;
  if (opp.activeDateClass === ACTIVE_DATE_CLASS.PAST_CLOSED) return false;
  const result = classifyActiveDate(opp, opts);
  return result.activeEligible === true;
}

export function applyActiveDateClassification(opp = {}, opts = {}) {
  const result = classifyActiveDate(opp, opts);
  return {
    ...opp,
    activeDateClass: result.activeDateClass,
    activeDateGateEnd: result.gateEndDate,
    activeDateCheckedOn: result.todayYmd,
    customerActiveEligible:
      result.activeEligible && opp.customerActiveEligible !== false,
  };
}
