/**
 * Need-month visual status + change indicators for Hotel Commercial Priorities V5.
 * Priority and lifecycle status are separate dimensions.
 */

import {
  NEED_PERIOD_STATUS,
  NEED_PERIOD_CHANGE_TYPE,
} from "./need-date-demand-contract-v1.js";
import {
  isNeedPeriodCommerciallyOpen,
  isNeedPeriodInactiveForProspecting,
} from "./need-period-lifecycle-v1.js";

export const NEED_MONTH_VISUAL_STATUS = Object.freeze({
  ACTIVE_NEED: "ACTIVE_NEED",
  CRITICAL_NEED: "CRITICAL_NEED",
  PARTIALLY_FILLED: "PARTIALLY_FILLED",
  FILLED: "FILLED",
  REOPENED: "REOPENED",
  REPLACED: "REPLACED",
  PAUSED: "PAUSED",
  WITHDRAWN: "WITHDRAWN",
  NO_CURRENT_NEED: "NO_CURRENT_NEED",
  TEST_SCENARIO: "TEST_SCENARIO",
});

export const NEED_MONTH_VISUAL_STATUS_LABEL = Object.freeze({
  [NEED_MONTH_VISUAL_STATUS.ACTIVE_NEED]: "Active Need",
  [NEED_MONTH_VISUAL_STATUS.CRITICAL_NEED]: "Critical Need",
  [NEED_MONTH_VISUAL_STATUS.PARTIALLY_FILLED]: "Partially Filled",
  [NEED_MONTH_VISUAL_STATUS.FILLED]: "Filled",
  [NEED_MONTH_VISUAL_STATUS.REOPENED]: "Reopened",
  [NEED_MONTH_VISUAL_STATUS.REPLACED]: "Replaced",
  [NEED_MONTH_VISUAL_STATUS.PAUSED]: "Paused",
  [NEED_MONTH_VISUAL_STATUS.WITHDRAWN]: "Withdrawn",
  [NEED_MONTH_VISUAL_STATUS.NO_CURRENT_NEED]: "No Current Need",
  [NEED_MONTH_VISUAL_STATUS.TEST_SCENARIO]: "Test Scenario",
});

/** Compact icon tokens (not emoji-dependent for a11y — paired with text labels). */
export const NEED_MONTH_VISUAL_STATUS_ICON = Object.freeze({
  [NEED_MONTH_VISUAL_STATUS.ACTIVE_NEED]: "●",
  [NEED_MONTH_VISUAL_STATUS.CRITICAL_NEED]: "◉",
  [NEED_MONTH_VISUAL_STATUS.PARTIALLY_FILLED]: "◐",
  [NEED_MONTH_VISUAL_STATUS.FILLED]: "✓",
  [NEED_MONTH_VISUAL_STATUS.REOPENED]: "↻",
  [NEED_MONTH_VISUAL_STATUS.REPLACED]: "→",
  [NEED_MONTH_VISUAL_STATUS.PAUSED]: "❚❚",
  [NEED_MONTH_VISUAL_STATUS.WITHDRAWN]: "✕",
  [NEED_MONTH_VISUAL_STATUS.NO_CURRENT_NEED]: "○",
  [NEED_MONTH_VISUAL_STATUS.TEST_SCENARIO]: "T",
});

export const NEED_CHANGE_INDICATOR = Object.freeze({
  NEW: "NEW",
  UPDATED: "UPDATED",
  REOPENED: "REOPENED",
  PARTIALLY_FILLED: "PARTIALLY_FILLED",
  FILLED: "FILLED",
  MOVED: "MOVED",
  NONE: "NONE",
});

export const NEED_CHANGE_INDICATOR_LABEL = Object.freeze({
  [NEED_CHANGE_INDICATOR.NEW]: "New",
  [NEED_CHANGE_INDICATOR.UPDATED]: "Updated",
  [NEED_CHANGE_INDICATOR.REOPENED]: "Reopened",
  [NEED_CHANGE_INDICATOR.PARTIALLY_FILLED]: "Partially Filled",
  [NEED_CHANGE_INDICATOR.FILLED]: "Filled",
  [NEED_CHANGE_INDICATOR.MOVED]: "Moved",
  [NEED_CHANGE_INDICATOR.NONE]: "",
});

/** "New" badge expires after this many days (history retained). */
export const NEED_CHANGE_NEW_EXPIRE_DAYS = 14;

const PRIORITY_RANK = { CRITICAL: 4, HIGH: 3, MEDIUM: 2, LOW: 1 };

function text(v) {
  return String(v || "").trim();
}

function daysSince(iso) {
  if (!iso) return Infinity;
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return Infinity;
  return (Date.now() - t) / (86400 * 1000);
}

export function mapPeriodToVisualStatus(period = {}, { lastChangeType = null } = {}) {
  const s = text(period.status);
  const pri = text(period.priority).toUpperCase();

  if (!s) return NEED_MONTH_VISUAL_STATUS.NO_CURRENT_NEED;

  if (s === NEED_PERIOD_STATUS.REPLACED || lastChangeType === NEED_PERIOD_CHANGE_TYPE.REPLACE) {
    return NEED_MONTH_VISUAL_STATUS.REPLACED;
  }
  if (s === NEED_PERIOD_STATUS.FILLED || s === NEED_PERIOD_STATUS.COMPLETED) {
    return NEED_MONTH_VISUAL_STATUS.FILLED;
  }
  if (s === NEED_PERIOD_STATUS.PARTIALLY_FILLED) {
    return NEED_MONTH_VISUAL_STATUS.PARTIALLY_FILLED;
  }
  if (s === NEED_PERIOD_STATUS.CANCELLED || s === NEED_PERIOD_STATUS.CLOSED) {
    return NEED_MONTH_VISUAL_STATUS.WITHDRAWN;
  }
  if (s === NEED_PERIOD_STATUS.REVIEW || period.paused === true) {
    return NEED_MONTH_VISUAL_STATUS.PAUSED;
  }
  if (lastChangeType === NEED_PERIOD_CHANGE_TYPE.REOPEN) {
    return NEED_MONTH_VISUAL_STATUS.REOPENED;
  }
  if (s === NEED_PERIOD_STATUS.TEST_SCENARIO) {
    return pri === "CRITICAL"
      ? NEED_MONTH_VISUAL_STATUS.CRITICAL_NEED
      : NEED_MONTH_VISUAL_STATUS.TEST_SCENARIO;
  }
  if (isNeedPeriodCommerciallyOpen(period)) {
    return pri === "CRITICAL"
      ? NEED_MONTH_VISUAL_STATUS.CRITICAL_NEED
      : NEED_MONTH_VISUAL_STATUS.ACTIVE_NEED;
  }
  if (isNeedPeriodInactiveForProspecting(period)) {
    return NEED_MONTH_VISUAL_STATUS.WITHDRAWN;
  }
  return NEED_MONTH_VISUAL_STATUS.NO_CURRENT_NEED;
}

export function mapChangeToIndicator(change = {}, { now = Date.now() } = {}) {
  const type = text(change.changeType || change.type).toUpperCase();
  const ts = change.timestamp || change.changedAt || change.changed_at;
  if (!type) return { indicator: NEED_CHANGE_INDICATOR.NONE, label: "", timestamp: ts || null };

  if (type === NEED_PERIOD_CHANGE_TYPE.ADD || type === "ADD") {
    const ageDays = daysSince(ts);
    if (ageDays <= NEED_CHANGE_NEW_EXPIRE_DAYS) {
      return {
        indicator: NEED_CHANGE_INDICATOR.NEW,
        label: NEED_CHANGE_INDICATOR_LABEL.NEW,
        timestamp: ts,
        expiresInDays: Math.max(0, Math.ceil(NEED_CHANGE_NEW_EXPIRE_DAYS - ageDays)),
      };
    }
    return {
      indicator: NEED_CHANGE_INDICATOR.NONE,
      label: "",
      timestamp: ts,
      note: "New indicator expired; history retained",
    };
  }
  if (type === NEED_PERIOD_CHANGE_TYPE.REOPEN) {
    return { indicator: NEED_CHANGE_INDICATOR.REOPENED, label: "Reopened", timestamp: ts };
  }
  if (type === NEED_PERIOD_CHANGE_TYPE.PARTIALLY_FILLED) {
    return {
      indicator: NEED_CHANGE_INDICATOR.PARTIALLY_FILLED,
      label: "Partially Filled",
      timestamp: ts,
    };
  }
  if (type === NEED_PERIOD_CHANGE_TYPE.FILLED) {
    return { indicator: NEED_CHANGE_INDICATOR.FILLED, label: "Filled", timestamp: ts };
  }
  if (type === NEED_PERIOD_CHANGE_TYPE.REPLACE) {
    return { indicator: NEED_CHANGE_INDICATOR.MOVED, label: "Moved", timestamp: ts };
  }
  if (
    type === NEED_PERIOD_CHANGE_TYPE.MODIFY ||
    type === NEED_PERIOD_CHANGE_TYPE.PRIORITY_CHANGED ||
    type === NEED_PERIOD_CHANGE_TYPE.REVENUE_OBJECTIVE_CHANGED ||
    type === NEED_PERIOD_CHANGE_TYPE.SPLIT
  ) {
    return { indicator: NEED_CHANGE_INDICATOR.UPDATED, label: "Updated", timestamp: ts };
  }
  return { indicator: NEED_CHANGE_INDICATOR.UPDATED, label: "Updated", timestamp: ts };
}

export function customerFriendlyChangeType(changeType) {
  const map = {
    ADD: "Need added",
    MODIFY: "Need updated",
    SPLIT: "Need split",
    PARTIALLY_FILLED: "Marked partially filled",
    FILLED: "Marked filled",
    CLOSED: "Need closed",
    REOPEN: "Need reopened",
    REPLACE: "Need replaced / moved",
    CANCEL: "Need withdrawn",
    PRIORITY_CHANGED: "Priority changed",
    REVENUE_OBJECTIVE_CHANGED: "Revenue objective changed",
  };
  return map[String(changeType || "").toUpperCase()] || String(changeType || "Changed");
}

/**
 * Summarize all need periods that fall in a calendar month.
 */
export function summarizeMonthVisual(periodsInMonth = [], changesByPeriodId = {}) {
  const open = periodsInMonth.filter((p) => isNeedPeriodCommerciallyOpen(p));
  const inactive = periodsInMonth.filter((p) => !isNeedPeriodCommerciallyOpen(p));

  let headlinePriority = null;
  for (const p of open) {
    const pri = text(p.priority).toUpperCase() || "MEDIUM";
    if (!headlinePriority || (PRIORITY_RANK[pri] || 0) > (PRIORITY_RANK[headlinePriority] || 0)) {
      headlinePriority = pri;
    }
  }

  let primaryPeriod = open[0] || periodsInMonth[0] || null;
  for (const p of open) {
    const pri = text(p.priority).toUpperCase();
    if ((PRIORITY_RANK[pri] || 0) > (PRIORITY_RANK[text(primaryPeriod?.priority).toUpperCase()] || 0)) {
      primaryPeriod = p;
    }
  }

  const lastChange =
    (primaryPeriod &&
      (changesByPeriodId[primaryPeriod.needPeriodId] || [])[0]) ||
    null;
  const visualStatus = primaryPeriod
    ? mapPeriodToVisualStatus(primaryPeriod, {
        lastChangeType: lastChange?.changeType,
      })
    : NEED_MONTH_VISUAL_STATUS.NO_CURRENT_NEED;
  const changeBadge = lastChange
    ? mapChangeToIndicator(lastChange)
    : { indicator: NEED_CHANGE_INDICATOR.NONE, label: "", timestamp: null };

  return {
    hasActiveNeed: open.length > 0,
    activeCount: open.length,
    inactiveCount: inactive.length,
    periodCount: periodsInMonth.length,
    multipleNeeds: periodsInMonth.length > 1,
    headlinePriority,
    visualStatus,
    visualStatusLabel: NEED_MONTH_VISUAL_STATUS_LABEL[visualStatus],
    visualStatusIcon: NEED_MONTH_VISUAL_STATUS_ICON[visualStatus],
    changeIndicator: changeBadge.indicator,
    changeIndicatorLabel: changeBadge.label,
    lastUpdatedAt:
      lastChange?.timestamp ||
      primaryPeriod?.updatedAt ||
      primaryPeriod?.lastConfirmedByHotelAt ||
      null,
    primaryNeedPeriodId: primaryPeriod?.needPeriodId || null,
    periods: periodsInMonth.map((p) => ({
      needPeriodId: p.needPeriodId,
      periodLabel: p.periodLabel,
      status: p.status,
      priority: p.priority,
      commerciallyOpen: isNeedPeriodCommerciallyOpen(p),
      visualStatus: mapPeriodToVisualStatus(p, {
        lastChangeType: (changesByPeriodId[p.needPeriodId] || [])[0]?.changeType,
      }),
    })),
  };
}

export function countActiveNeedMonths(monthlySummaries = []) {
  return monthlySummaries.filter((m) => m.hasActiveNeed).length;
}
