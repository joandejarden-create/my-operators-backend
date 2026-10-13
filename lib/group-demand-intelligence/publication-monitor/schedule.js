/**
 * Publication-window aware check scheduling.
 * Far from window → low frequency; inside window → high; after → grace; then expire.
 */

import {
  CHECK_FREQUENCY,
  FREQUENCY_DAYS,
  MONITOR_STATUS,
} from "./constants.js";

function addDays(isoOrDate, days) {
  const d = new Date(isoOrDate);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function daysUntil(fromIso, toIso) {
  const a = new Date(String(fromIso).slice(0, 10) + "T00:00:00Z");
  const b = new Date(String(toIso).slice(0, 10) + "T00:00:00Z");
  return Math.round((b - a) / 86400000);
}

/**
 * @param {object} monitor
 * @param {Date|string} [now]
 */
export function computeCheckSchedule(monitor = {}, now = new Date()) {
  const today = new Date(now).toISOString().slice(0, 10);
  const windowStart = monitor.expectedPublicationWindowStart || null;
  const windowEnd = monitor.expectedPublicationWindowEnd || null;
  const eventEnd = monitor.eventEndDate || monitor.campaignEventEnd || null;
  const graceDays = Number(monitor.graceDaysAfterWindow ?? 45);

  // Expired: event passed + grace
  if (eventEnd) {
    const expireAfter = addDays(eventEnd, 14);
    if (today > expireAfter) {
      return {
        monitoringStatus: MONITOR_STATUS.EXPIRED,
        frequency: CHECK_FREQUENCY.GRACE,
        nextCheckAt: null,
        intervalDays: null,
        scheduleRationale: `Event ended ${eventEnd}; monitor expired after ${expireAfter}`,
      };
    }
  }

  if (monitor.monitoringStatus === MONITOR_STATUS.COMPLETED) {
    return {
      monitoringStatus: MONITOR_STATUS.COMPLETED,
      frequency: CHECK_FREQUENCY.LOW,
      nextCheckAt: null,
      intervalDays: null,
      scheduleRationale: "Monitor completed — list published and re-decomposition done",
    };
  }

  if (monitor.monitoringStatus === MONITOR_STATUS.PAUSED) {
    return {
      monitoringStatus: MONITOR_STATUS.PAUSED,
      frequency: CHECK_FREQUENCY.LOW,
      nextCheckAt: null,
      intervalDays: FREQUENCY_DAYS.LOW,
      scheduleRationale: "Paused",
    };
  }

  let frequency = CHECK_FREQUENCY.MEDIUM;
  let rationale = "Default medium cadence";

  if (windowStart && windowEnd) {
    if (today < windowStart) {
      const until = daysUntil(today, windowStart);
      if (until > 60) {
        frequency = CHECK_FREQUENCY.LOW;
        rationale = `Far from publication window (starts ${windowStart}; ${until}d)`;
      } else {
        frequency = CHECK_FREQUENCY.MEDIUM;
        rationale = `Approaching publication window (starts ${windowStart}; ${until}d)`;
      }
    } else if (today >= windowStart && today <= windowEnd) {
      frequency = CHECK_FREQUENCY.HIGH;
      rationale = `Inside expected publication window ${windowStart}→${windowEnd}`;
    } else {
      const after = daysUntil(windowEnd, today);
      if (after <= graceDays) {
        frequency = CHECK_FREQUENCY.GRACE;
        rationale = `Past expected window (ended ${windowEnd}); grace ${graceDays}d (${after}d elapsed)`;
      } else {
        return {
          monitoringStatus: MONITOR_STATUS.EXPIRED,
          frequency: CHECK_FREQUENCY.GRACE,
          nextCheckAt: null,
          intervalDays: null,
          scheduleRationale: `Grace exhausted after window end ${windowEnd}`,
        };
      }
    }
  } else if (windowStart && today >= windowStart) {
    frequency = CHECK_FREQUENCY.HIGH;
    rationale = `At/after expected start ${windowStart} (no end bound)`;
  } else if (windowStart) {
    frequency = CHECK_FREQUENCY.LOW;
    rationale = `Waiting for window start ${windowStart}`;
  }

  // Priority boost (e.g. CIELO near-term)
  if (monitor.priorityRank === 1 && frequency !== CHECK_FREQUENCY.HIGH) {
    if (frequency === CHECK_FREQUENCY.LOW) frequency = CHECK_FREQUENCY.MEDIUM;
    else frequency = CHECK_FREQUENCY.HIGH;
    rationale += "; priority campaign boost";
  }

  const intervalDays = FREQUENCY_DAYS[frequency] || FREQUENCY_DAYS.MEDIUM;
  const nextCheckAt = addDays(today, intervalDays);

  return {
    monitoringStatus: MONITOR_STATUS.ACTIVE,
    frequency,
    nextCheckAt,
    intervalDays,
    scheduleRationale: rationale,
  };
}

export function isDueForCheck(monitor = {}, now = new Date()) {
  if (
    [MONITOR_STATUS.PAUSED, MONITOR_STATUS.COMPLETED, MONITOR_STATUS.EXPIRED].includes(
      monitor.monitoringStatus
    )
  ) {
    return false;
  }
  if (!monitor.nextCheckAt) return true;
  const today = new Date(now).toISOString().slice(0, 10);
  return today >= String(monitor.nextCheckAt).slice(0, 10);
}
