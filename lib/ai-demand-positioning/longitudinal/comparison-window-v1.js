/**
 * Longitudinal comparison window V1 — Existing Hotel ADP
 *
 * Cadence goal: ~7 days between certified monitoring periods.
 * Prior Run = immediately preceding comparable certified period (not exact date-7).
 *
 * Modes:
 *   PRIOR_RUN | LAST_30_DAYS | MONTH_TO_DATE
 *
 * COMPARISON BASELINE = period used for Δ
 * TREND WINDOW = all qualifying certified periods plotted for the chart
 *
 * Synthetic / production isolation: callers must pass explicit period lists.
 * Never load fixtures into production published history.
 */

export const COMPARISON_WINDOW_VERSION = "adp_comparison_window_v1";

export const COMPARISON_MODES = Object.freeze({
  PRIOR_RUN: "PRIOR_RUN",
  LAST_30_DAYS: "LAST_30_DAYS",
  MONTH_TO_DATE: "MONTH_TO_DATE",
});

export const COMPARISON_MODE_LABELS = Object.freeze({
  [COMPARISON_MODES.PRIOR_RUN]: "Prior Run",
  [COMPARISON_MODES.LAST_30_DAYS]: "Last 30 Days",
  [COMPARISON_MODES.MONTH_TO_DATE]: "Month to Date",
});

export const DELTA_COLUMN_LABELS = Object.freeze({
  [COMPARISON_MODES.PRIOR_RUN]: "Δ VS PRIOR RUN",
  [COMPARISON_MODES.LAST_30_DAYS]: "Δ VS 30 DAYS",
  [COMPARISON_MODES.MONTH_TO_DATE]: "Δ VS MONTH START",
});

/** Prefer later period when two candidates are equidistant from the anchor. */
export const NEAREST_PERIOD_TIE_RULE = "CLOSEST_ABS_DAY_DISTANCE; TIE_BREAK_PREFER_LATER_PERIOD";

export const SAME_COMPARISON_CONTEXT_SAME_PERIOD_SET = "SAME_COMPARISON_CONTEXT_SAME_PERIOD_SET";
export const TREND_WINDOW_HISTORY_INTEGRITY = "TREND_WINDOW_HISTORY_INTEGRITY";
export const COMPARISON_LABEL_SEMANTIC_INTEGRITY = "COMPARISON_LABEL_SEMANTIC_INTEGRITY";

/**
 * Parse executionDate / ISO / YYYY-MM-DD to UTC calendar date (YYYY-MM-DD).
 */
export function toCalendarDate(value) {
  if (!value) return null;
  const s = String(value);
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

export function parseUtcDay(yyyyMmDd) {
  const [y, m, d] = String(yyyyMmDd).split("-").map(Number);
  return Date.UTC(y, m - 1, d);
}

export function dayDistance(a, b) {
  const da = parseUtcDay(a);
  const db = parseUtcDay(b);
  return Math.abs(Math.round((da - db) / 86400000));
}

export function addCalendarDays(yyyyMmDd, deltaDays) {
  const t = parseUtcDay(yyyyMmDd) + deltaDays * 86400000;
  return new Date(t).toISOString().slice(0, 10);
}

function periodDate(p) {
  return toCalendarDate(p?.executionDate || p?.calendarDate || null);
}

/**
 * Default eligibility for longitudinal comparison / trend.
 * Snapshots: finalized + certified-ish. Periods: optional isComparable predicate.
 */
export function defaultPeriodEligible(periodOrSnap) {
  if (!periodOrSnap) return false;
  if (periodOrSnap.syntheticLeakGuard === false) return false;
  if (periodOrSnap.finalized === false) return false;
  if (periodOrSnap.longitudinalComparable === false) return false;
  const status = String(periodOrSnap.certificationStatus || "");
  if (status && !/CERTIFIED/i.test(status) && status !== "SYNTHETIC_CERTIFIED") return false;
  return true;
}

/**
 * Select nearest eligible period to an anchor date among candidates before current.
 * Tie: prefer later (more recent) period.
 */
export function selectNearestEligiblePeriod({
  candidates,
  anchorDate,
  currentPeriodId,
  isEligible = defaultPeriodEligible,
}) {
  const current = (candidates || []).find((p) => p.periodId === currentPeriodId);
  const currentDate = periodDate(current);
  if (!currentDate || !anchorDate) {
    return { period: null, reason: "MISSING_DATES", distances: [] };
  }

  const beforeOrEqual = (candidates || []).filter((p) => {
    if (!p || p.periodId === currentPeriodId || !isEligible(p)) return false;
    const d = periodDate(p);
    return d && d < currentDate;
  });

  if (!beforeOrEqual.length) {
    return { period: null, reason: "NO_ELIGIBLE_PERIOD_BEFORE_CURRENT", distances: [] };
  }

  const scored = beforeOrEqual
    .map((p) => ({
      period: p,
      date: periodDate(p),
      distance: dayDistance(periodDate(p), anchorDate),
    }))
    .sort((a, b) => {
      if (a.distance !== b.distance) return a.distance - b.distance;
      return String(b.date).localeCompare(String(a.date));
    });

  return {
    period: scored[0].period,
    reason: null,
    distances: scored.map((s) => ({
      periodId: s.period.periodId,
      date: s.date,
      distanceDays: s.distance,
    })),
    tieRule: NEAREST_PERIOD_TIE_RULE,
  };
}

/**
 * Immediately prior eligible certified period (by calendar date, then periodId).
 */
export function selectImmediatelyPriorEligible({
  candidates,
  currentPeriodId,
  isEligible = defaultPeriodEligible,
  areComparable = null,
}) {
  const current = (candidates || []).find((p) => p.periodId === currentPeriodId);
  if (!current) return { period: null, reason: "CURRENT_NOT_FOUND", skipped: [] };

  const sorted = [...(candidates || [])]
    .filter((p) => p.periodId !== currentPeriodId && isEligible(p) && periodDate(p))
    .sort((a, b) => {
      const d = String(periodDate(a)).localeCompare(String(periodDate(b)));
      if (d) return d;
      return String(a.periodId).localeCompare(String(b.periodId));
    });

  const currentDate = periodDate(current);
  const before = sorted.filter((p) => periodDate(p) < currentDate);
  const skipped = [];
  for (let i = before.length - 1; i >= 0; i--) {
    const candidate = before[i];
    if (typeof areComparable === "function") {
      const check = areComparable(current, candidate);
      if (!check?.comparable) {
        skipped.push({ periodId: candidate.periodId, reason: check?.reason || "not_comparable" });
        continue;
      }
    }
    return { period: candidate, reason: null, skipped };
  }
  return {
    period: null,
    reason: skipped.length ? "NO_COMPARABLE_PRIOR" : "NO_PRIOR_PERIOD",
    skipped,
  };
}

/**
 * First eligible certified period in the same calendar month as current.
 */
export function selectMonthStartEligible({
  candidates,
  currentPeriodId,
  isEligible = defaultPeriodEligible,
  areComparable = null,
}) {
  const current = (candidates || []).find((p) => p.periodId === currentPeriodId);
  if (!current) return { period: null, reason: "CURRENT_NOT_FOUND" };
  const currentDate = periodDate(current);
  if (!currentDate) return { period: null, reason: "CURRENT_DATE_MISSING" };
  const monthPrefix = currentDate.slice(0, 7); // YYYY-MM

  const inMonth = [...(candidates || [])]
    .filter((p) => {
      const d = periodDate(p);
      return d && d.startsWith(monthPrefix) && d <= currentDate && isEligible(p);
    })
    .sort((a, b) => String(periodDate(a)).localeCompare(String(periodDate(b))));

  if (!inMonth.length) return { period: null, reason: "NO_PERIOD_IN_MONTH" };

  // First in month is baseline; if first === current, no earlier baseline in month
  const first = inMonth[0];
  if (first.periodId === currentPeriodId) {
    return { period: null, reason: "CURRENT_IS_FIRST_IN_MONTH", monthFirstPeriodId: first.periodId };
  }

  if (typeof areComparable === "function") {
    const check = areComparable(current, first);
    if (!check?.comparable) {
      // Policy: do not silently hop to another month-start substitute
      return {
        period: null,
        reason: check.reason || "MONTH_START_NOT_COMPARABLE",
        monthFirstPeriodId: first.periodId,
      };
    }
  }

  return { period: first, reason: null, monthFirstPeriodId: first.periodId };
}

/**
 * Trend window periods (inclusive of current), chronological.
 */
export function resolveTrendWindowPeriods({
  candidates,
  currentPeriodId,
  comparisonMode,
  isEligible = defaultPeriodEligible,
}) {
  const current = (candidates || []).find((p) => p.periodId === currentPeriodId);
  if (!current) return { periods: [], reason: "CURRENT_NOT_FOUND" };
  const currentDate = periodDate(current);
  if (!currentDate) return { periods: [], reason: "CURRENT_DATE_MISSING" };

  let startDate = null;
  if (comparisonMode === COMPARISON_MODES.PRIOR_RUN) {
    // Chart: current + immediately prior only (filled after baseline resolve)
    startDate = null; // special handling below
  } else if (comparisonMode === COMPARISON_MODES.LAST_30_DAYS) {
    startDate = addCalendarDays(currentDate, -30);
  } else if (comparisonMode === COMPARISON_MODES.MONTH_TO_DATE) {
    startDate = `${currentDate.slice(0, 7)}-01`;
  } else {
    return { periods: [], reason: "UNKNOWN_COMPARISON_MODE" };
  }

  const eligible = [...(candidates || [])]
    .filter((p) => isEligible(p) && periodDate(p))
    .sort((a, b) => String(periodDate(a)).localeCompare(String(periodDate(b))));

  if (comparisonMode === COMPARISON_MODES.PRIOR_RUN) {
    // Placeholder — caller should pass priorPeriodId; we return [prior, current] when known
    return {
      periods: eligible.filter((p) => p.periodId === currentPeriodId),
      reason: null,
      mode: comparisonMode,
      windowStartDate: null,
      windowEndDate: currentDate,
      priorRunSpecial: true,
    };
  }

  const inWindow = eligible.filter((p) => {
    const d = periodDate(p);
    return d >= startDate && d <= currentDate;
  });

  // For month-to-date, only include from first certified in month (may be after the 1st)
  let periods = inWindow;
  if (comparisonMode === COMPARISON_MODES.MONTH_TO_DATE) {
    const firstInMonth = inWindow.find((p) => periodDate(p).startsWith(currentDate.slice(0, 7)));
    if (firstInMonth) {
      const firstDate = periodDate(firstInMonth);
      periods = inWindow.filter((p) => periodDate(p) >= firstDate);
    }
  }

  return {
    periods,
    reason: null,
    mode: comparisonMode,
    windowStartDate: startDate,
    windowEndDate: currentDate,
  };
}

/**
 * Canonical resolver — one comparison context for rank Δ, labels, and trend window.
 *
 * @param {object} args
 * @param {string} args.propertyId
 * @param {string} args.currentPeriodId
 * @param {string} args.comparisonMode
 * @param {Array} args.periods — period/snapshot meta list (must include periodId, executionDate, finalized, certificationStatus)
 * @param {string} [args.scope] — informational only at resolve time
 * @param {(cur, cand) => {comparable:boolean, reason?:string}} [args.areComparable]
 * @param {(p) => boolean} [args.isEligible]
 */
export function resolveComparisonWindowV1({
  propertyId,
  currentPeriodId,
  comparisonMode,
  periods,
  scope = "overall",
  areComparable = null,
  isEligible = defaultPeriodEligible,
}) {
  const mode = String(comparisonMode || COMPARISON_MODES.PRIOR_RUN);
  if (!Object.values(COMPARISON_MODES).includes(mode)) {
    return {
      ok: false,
      version: COMPARISON_WINDOW_VERSION,
      propertyId,
      currentPeriodId,
      comparisonMode: mode,
      scope,
      error: "UNKNOWN_COMPARISON_MODE",
    };
  }

  const current = (periods || []).find((p) => p.periodId === currentPeriodId) || null;
  const currentDate = periodDate(current);
  let requestedAnchorDate = null;
  let baselinePick = { period: null, reason: "UNRESOLVED", skipped: [] };

  if (mode === COMPARISON_MODES.PRIOR_RUN) {
    requestedAnchorDate = null; // semantic: immediately prior, not a fixed day offset
    baselinePick = selectImmediatelyPriorEligible({
      candidates: periods,
      currentPeriodId,
      isEligible,
      areComparable,
    });
  } else if (mode === COMPARISON_MODES.LAST_30_DAYS) {
    requestedAnchorDate = currentDate ? addCalendarDays(currentDate, -30) : null;
    baselinePick = selectNearestEligiblePeriod({
      candidates: periods,
      anchorDate: requestedAnchorDate,
      currentPeriodId,
      isEligible,
    });
    if (baselinePick.period && typeof areComparable === "function" && current) {
      const check = areComparable(current, baselinePick.period);
      if (!check?.comparable) {
        baselinePick = {
          period: null,
          reason: check?.reason || "BASELINE_NOT_COMPARABLE",
          skipped: [
            {
              periodId: baselinePick.period.periodId,
              reason: check?.reason || "not_comparable",
            },
          ],
        };
      }
    }
  } else if (mode === COMPARISON_MODES.MONTH_TO_DATE) {
    requestedAnchorDate = currentDate ? `${currentDate.slice(0, 7)}-01` : null;
    baselinePick = selectMonthStartEligible({
      candidates: periods,
      currentPeriodId,
      isEligible,
      areComparable,
    });
  }

  const comparisonPeriod = baselinePick.period || null;
  const actualComparisonDate = periodDate(comparisonPeriod);

  let trend = resolveTrendWindowPeriods({
    candidates: periods,
    currentPeriodId,
    comparisonMode: mode,
    isEligible,
  });

  if (mode === COMPARISON_MODES.PRIOR_RUN) {
    const pts = [];
    if (comparisonPeriod) pts.push(comparisonPeriod);
    if (current) pts.push(current);
    trend = {
      periods: pts,
      reason: null,
      mode,
      windowStartDate: periodDate(comparisonPeriod) || currentDate,
      windowEndDate: currentDate,
      priorRunSpecial: true,
    };
  }

  const comparability =
    current && comparisonPeriod && typeof areComparable === "function"
      ? areComparable(current, comparisonPeriod)
      : current && comparisonPeriod
        ? { comparable: true, reason: null }
        : {
            comparable: false,
            reason: baselinePick.reason || "NO_COMPARISON_PERIOD",
          };

  return {
    ok: true,
    version: COMPARISON_WINDOW_VERSION,
    propertyId,
    scope,
    comparisonMode: mode,
    comparisonModeLabel: COMPARISON_MODE_LABELS[mode],
    deltaColumnLabel: DELTA_COLUMN_LABELS[mode],
    currentPeriod: current,
    currentPeriodId,
    currentDate,
    comparisonPeriod,
    comparisonPeriodId: comparisonPeriod?.periodId || null,
    requestedAnchorDate,
    actualComparisonDate,
    nearestDistances: baselinePick.distances || null,
    tieRule: NEAREST_PERIOD_TIE_RULE,
    periodsInTrendWindow: trend.periods || [],
    trendWindowStartDate: trend.windowStartDate || null,
    trendWindowEndDate: trend.windowEndDate || null,
    comparability,
    unavailableReason: comparability.comparable ? null : comparability.reason || baselinePick.reason,
    skippedIncomparable: baselinePick.skipped || [],
    monthContextLabel:
      mode === COMPARISON_MODES.MONTH_TO_DATE && actualComparisonDate
        ? `Since ${formatDisplayDate(actualComparisonDate)}`
        : null,
    comparisonTooltip: actualComparisonDate
      ? `Compared with ${formatDisplayDate(actualComparisonDate)}`
      : unavailableReasonTooltip(baselinePick.reason || comparability.reason),
    datePairLabel:
      currentDate && actualComparisonDate
        ? `${formatDisplayDate(currentDate)} vs ${formatDisplayDate(actualComparisonDate)}`
        : null,
  };
}

export function formatDisplayDate(yyyyMmDd) {
  if (!yyyyMmDd) return "";
  const [y, m, d] = String(yyyyMmDd).split("-").map(Number);
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return `${months[m - 1]} ${d}, ${y}`;
}

function unavailableReasonTooltip(reason) {
  if (!reason) return "Comparison unavailable";
  if (reason === "CURRENT_IS_FIRST_IN_MONTH") return "No earlier certified run this month";
  if (reason === "NO_PRIOR_PERIOD") return "No prior certified run";
  if (reason === "NO_COMPARABLE_PRIOR") return "Prior run not comparable";
  return `Comparison unavailable (${reason})`;
}

/**
 * Assert one resolution object drives rank + trend + labels.
 */
export function assertSameComparisonContext(resolution, { movementPriorPeriodId, trendPeriodIds, deltaLabel }) {
  const defects = [];
  if (!resolution?.ok) {
    defects.push({ code: SAME_COMPARISON_CONTEXT_SAME_PERIOD_SET, detail: "resolution not ok" });
    return defects;
  }
  if (movementPriorPeriodId !== resolution.comparisonPeriodId) {
    defects.push({
      code: SAME_COMPARISON_CONTEXT_SAME_PERIOD_SET,
      detail: `movement baseline ${movementPriorPeriodId} !== resolver ${resolution.comparisonPeriodId}`,
    });
  }
  const expectedTrend = (resolution.periodsInTrendWindow || []).map((p) => p.periodId).join(",");
  const actualTrend = (trendPeriodIds || []).join(",");
  if (expectedTrend !== actualTrend) {
    defects.push({
      code: SAME_COMPARISON_CONTEXT_SAME_PERIOD_SET,
      detail: `trend set mismatch expected=${expectedTrend} actual=${actualTrend}`,
    });
  }
  if (deltaLabel && deltaLabel !== resolution.deltaColumnLabel) {
    defects.push({
      code: COMPARISON_LABEL_SEMANTIC_INTEGRITY,
      detail: `label ${deltaLabel} !== ${resolution.deltaColumnLabel}`,
    });
  }
  return defects;
}
