/**
 * buildCompSetRepeatPattern() — recurrence from public traces only; never fabricate cycles.
 */

export const REPEAT_CADENCE = Object.freeze({
  ONE_OFF: "ONE_OFF",
  ANNUAL: "ANNUAL",
  BIENNIAL: "BIENNIAL",
  IRREGULAR_RECURRING: "IRREGULAR_RECURRING",
  MULTI_YEAR_REPEAT: "MULTI_YEAR_REPEAT",
  ROTATING_GEOGRAPHY: "ROTATING_GEOGRAPHY",
  REPEAT_SAME_MARKET: "REPEAT_SAME_MARKET",
  REPEAT_SAME_HOTEL: "REPEAT_SAME_HOTEL",
  UNKNOWN: "UNKNOWN",
});

/**
 * Build repeat pattern from one or more traces for the same org/series.
 */
export function buildCompSetRepeatPattern(traces = [], opts = {}) {
  const list = (Array.isArray(traces) ? traces : [traces]).filter(Boolean);
  if (!list.length) return null;

  const primary = list[0];
  const years = [
    ...new Set(
      list
        .flatMap((t) => [t.eventYear, t.eventDate ? String(t.eventDate).slice(0, 4) : null])
        .filter(Boolean)
        .map(String)
    ),
  ].sort();

  const hotels = [...new Set(list.map((t) => t.competitorHotel).filter(Boolean))];
  const markets = [...new Set(list.map((t) => t.market).filter(Boolean))];
  const blob = list
    .map((t) => `${t.eventProgram || ""} ${t.fact || ""} ${t.source || ""}`)
    .join(" ");

  let cadence = REPEAT_CADENCE.UNKNOWN;
  if (/biennial|every two years/i.test(blob)) cadence = REPEAT_CADENCE.BIENNIAL;
  else if (/annual|yearly|édition|each year/i.test(blob) || years.length >= 2) {
    cadence =
      years.length >= 2 && Number(years[years.length - 1]) - Number(years[0]) >= 2
        ? REPEAT_CADENCE.MULTI_YEAR_REPEAT
        : REPEAT_CADENCE.ANNUAL;
  } else if (years.length === 1 && !/annual|series|édition/i.test(blob)) {
    cadence = REPEAT_CADENCE.ONE_OFF;
  } else if (/rotat|host city|site selection/i.test(blob)) {
    cadence = REPEAT_CADENCE.ROTATING_GEOGRAPHY;
  } else if (hotels.length === 1 && years.length >= 1) {
    cadence = REPEAT_CADENCE.REPEAT_SAME_HOTEL;
  } else if (markets.length === 1 && years.length >= 1) {
    cadence = REPEAT_CADENCE.REPEAT_SAME_MARKET;
  } else if (/recurring|series/i.test(blob)) {
    cadence = REPEAT_CADENCE.IRREGULAR_RECURRING;
  }

  const futureYears = years.filter((y) => Number(y) >= 2026);
  const nextKnownCycle = list.find((t) => t.eventDate && String(t.eventDate) >= "2026")?.eventDate
    || futureYears[0]
    || null;

  // Do not invent next expected cycle — only when cadence evidence supports expectation
  let nextExpectedCycle = null;
  if (nextKnownCycle) nextExpectedCycle = nextKnownCycle;
  else if (
    (cadence === REPEAT_CADENCE.ANNUAL || cadence === REPEAT_CADENCE.MULTI_YEAR_REPEAT) &&
    years.length
  ) {
    const last = Number(years[years.length - 1]);
    if (last >= 2024 && last < 2026) {
      nextExpectedCycle = "RECURRING_EXPECTED_NEXT_CYCLE_UNCONFIRMED";
    }
  } else if (cadence === REPEAT_CADENCE.ROTATING_GEOGRAPHY) {
    nextExpectedCycle = "SITE_SELECTION_OR_HOST_WINDOW_UNKNOWN";
  }

  const nextDecisionWindow =
    nextExpectedCycle && String(nextExpectedCycle).startsWith("20")
      ? "PRE_EVENT_HOUSING_WINDOW"
      : nextExpectedCycle
        ? "MONITOR_FOR_NEXT_CYCLE_ANNOUNCEMENT"
        : null;

  return {
    patternId: `rpt_${primary.eventSeriesId || primary.traceId}`.slice(0, 80),
    organization: primary.organization,
    eventSeries: primary.eventSeriesId,
    eventProgram: primary.eventProgram,
    demandEngine: primary.demandEngine,
    historicDates: years.join("|"),
    historicMarkets: markets.join("|"),
    historicHotels: hotels.join("|"),
    cadence,
    organizer: primary.organization,
    housingPartner: null,
    groupSize: null,
    roomBlock: primary.lodgingEvidence?.roomBlockMentioned ? "MENTIONED" : null,
    nextKnownCycle,
    nextExpectedCycle,
    nextDecisionWindow,
    evidenceClasses: [...new Set(list.map((t) => t.evidenceClass))].join("|"),
    traceIds: list.map((t) => t.traceId).join("|"),
    sourceUrls: list.map((t) => t.source).join("|"),
    fabricated: false,
  };
}
