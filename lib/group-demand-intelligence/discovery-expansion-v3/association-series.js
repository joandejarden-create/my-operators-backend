/**
 * Recurring association / event-series intelligence.
 * Timing states for watch vs ready — never fabricate exact dates.
 */

export const SERIES_TIMING_STATE = Object.freeze({
  CONFIRMED_EXACT: "CONFIRMED_EXACT",
  CONFIRMED_RANGE: "CONFIRMED_RANGE",
  CONFIRMED_MONTH: "CONFIRMED_MONTH",
  CONFIRMED_YEAR: "CONFIRMED_YEAR",
  RECURRING_EXPECTED: "RECURRING_EXPECTED",
  ROTATION_PREDICTED: "ROTATION_PREDICTED",
  FUTURE_UNCONFIRMED: "FUTURE_UNCONFIRMED",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
});

/**
 * Customer Ready must not use these alone.
 */
export const WATCH_ONLY_TIMING = new Set([
  SERIES_TIMING_STATE.RECURRING_EXPECTED,
  SERIES_TIMING_STATE.ROTATION_PREDICTED,
  SERIES_TIMING_STATE.FUTURE_UNCONFIRMED,
]);

export function classifySeriesTimingState({
  eventStartDate = null,
  eventEndDate = null,
  eventMonth = null,
  eventYear = null,
  historicalCycles = [],
  rotationPattern = null,
  nextPredictedCycle = null,
  predictionConfidence = null,
} = {}) {
  if (eventStartDate && /^\d{4}-\d{2}-\d{2}$/.test(String(eventStartDate).slice(0, 10))) {
    if (eventEndDate && eventEndDate !== eventStartDate) return SERIES_TIMING_STATE.CONFIRMED_RANGE;
    return SERIES_TIMING_STATE.CONFIRMED_EXACT;
  }
  if (eventMonth && eventYear) return SERIES_TIMING_STATE.CONFIRMED_MONTH;
  if (eventYear) return SERIES_TIMING_STATE.CONFIRMED_YEAR;
  if (nextPredictedCycle && rotationPattern && Number(predictionConfidence) >= 0.55) {
    return SERIES_TIMING_STATE.ROTATION_PREDICTED;
  }
  if ((historicalCycles || []).length >= 2) return SERIES_TIMING_STATE.RECURRING_EXPECTED;
  if ((historicalCycles || []).length === 1) return SERIES_TIMING_STATE.HISTORICAL_ONLY;
  return SERIES_TIMING_STATE.FUTURE_UNCONFIRMED;
}

/**
 * Future Watch eligibility for series timing (strict — not customer ready).
 */
export function seriesMayBeValidFutureWatch(series = {}) {
  const state = series.timingState || classifySeriesTimingState(series);
  if (state === SERIES_TIMING_STATE.HISTORICAL_ONLY) return false;
  if (!series.organization && !series.eventSeriesId) return false;
  if (!series.nextValidationTrigger && !series.nextKnownCycle && !series.nextPredictedCycle) {
    return false;
  }
  if (!series.hotelFitExists && series.hotelFitScore == null) return false;
  if (
    state === SERIES_TIMING_STATE.RECURRING_EXPECTED ||
    state === SERIES_TIMING_STATE.ROTATION_PREDICTED
  ) {
    return Boolean(series.historicalRecurrenceReal && series.nextCycleThesis);
  }
  return [
    SERIES_TIMING_STATE.CONFIRMED_EXACT,
    SERIES_TIMING_STATE.CONFIRMED_RANGE,
    SERIES_TIMING_STATE.CONFIRMED_MONTH,
    SERIES_TIMING_STATE.CONFIRMED_YEAR,
    SERIES_TIMING_STATE.FUTURE_UNCONFIRMED,
  ].includes(state);
}

export function buildSeriesRecord(input = {}) {
  const timingState = classifySeriesTimingState(input);
  return {
    organization: input.organization || null,
    eventSeriesId: input.eventSeriesId || null,
    historicalCycles: input.historicalCycles || [],
    historicalHostCities: input.historicalHostCities || [],
    cadence: input.cadence || null,
    rotationPattern: input.rotationPattern || null,
    typicalMonth: input.typicalMonth || null,
    typicalAttendance: input.typicalAttendance ?? null,
    typicalLodgingSignal: input.typicalLodgingSignal || null,
    nextKnownCycle: input.nextKnownCycle || null,
    nextPredictedCycle: input.nextPredictedCycle || null,
    predictionConfidence: input.predictionConfidence ?? null,
    nextValidationTrigger: input.nextValidationTrigger || null,
    nextCycleThesis: input.nextCycleThesis || null,
    historicalRecurrenceReal: Boolean(input.historicalRecurrenceReal),
    hotelFitExists: Boolean(input.hotelFitExists || input.hotelFitScore != null),
    hotelFitScore: input.hotelFitScore ?? null,
    timingState,
    customerReadyEligibleTiming: !WATCH_ONLY_TIMING.has(timingState),
    sourceUrl: input.sourceUrl || null,
  };
}
