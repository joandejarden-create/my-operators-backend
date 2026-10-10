/**
 * Repeat / rotation intelligence — find NEXT DECISION POINT, do not invent dates.
 */

export const ROTATION_TIMING_STATE = Object.freeze({
  CONFIRMED_FUTURE: "CONFIRMED_FUTURE",
  RECURRING_EXPECTED: "RECURRING_EXPECTED",
  ROTATION_PREDICTED: "ROTATION_PREDICTED",
  FUTURE_UNCONFIRMED: "FUTURE_UNCONFIRMED",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
});

const YEAR_RE = /\b(20(?:2[3-9]|3[0-2]))\b/g;

/**
 * Capture rotation / repeat structure from a signal + optional page evidence.
 */
export function buildRotationRecord(signal = {}, pageEvidence = {}, hotel = {}) {
  const blob = [
    signal.title,
    signal.summaryWhat,
    signal.snippet,
    pageEvidence.updates ? JSON.stringify(pageEvidence.updates) : "",
    pageEvidence.pageKind,
  ]
    .map((x) => String(x || ""))
    .join(" ");

  const years = [...new Set((blob.match(YEAR_RE) || []).map(String))].sort();
  const y2023 = years.includes("2023");
  const y2024 = years.includes("2024");
  const y2025 = years.includes("2025");
  const y2026 = years.includes("2026");
  const y2027plus = years.some((y) => Number(y) >= 2027);

  const confirmedFuture =
    Boolean(signal.eventStartDate || pageEvidence.eventStartDate) &&
    String(signal.eventStartDate || pageEvidence.eventStartDate || "").slice(0, 4) >= "2026";

  const recurring =
    /annual|yearly|édition|edicion|biennial|recurring|series/i.test(blob) &&
    (years.length >= 2 || /next (year|edition|cycle)/i.test(blob));

  const rotation =
    /rotat|site selection|host city|host proposal|bidding|future host/i.test(blob);

  let timingState = ROTATION_TIMING_STATE.FUTURE_UNCONFIRMED;
  if (confirmedFuture) timingState = ROTATION_TIMING_STATE.CONFIRMED_FUTURE;
  else if (rotation && (y2026 || y2027plus || recurring)) {
    timingState = ROTATION_TIMING_STATE.ROTATION_PREDICTED;
  } else if (recurring && (y2026 || y2027plus || !years.length || years.some((y) => Number(y) >= 2025))) {
    timingState = ROTATION_TIMING_STATE.RECURRING_EXPECTED;
  } else if (years.length && years.every((y) => Number(y) < 2026) && !recurring && !rotation) {
    timingState = ROTATION_TIMING_STATE.HISTORICAL_ONLY;
  }

  const nextDecisionPoint =
    timingState === ROTATION_TIMING_STATE.CONFIRMED_FUTURE
      ? signal.eventStartDate || pageEvidence.eventStartDate
      : timingState === ROTATION_TIMING_STATE.ROTATION_PREDICTED
        ? "SITE_SELECTION_OR_HOST_BID_WINDOW"
        : timingState === ROTATION_TIMING_STATE.RECURRING_EXPECTED
          ? "NEXT_CYCLE_ANNOUNCEMENT_WINDOW"
          : null;

  return {
    opportunityId: signal.id,
    hotelKey: hotel.hotelKey || signal.hotelKey,
    organization: signal.organizationName || signal.organization,
    title: signal.title,
    y2023: y2023 ? "2023" : "",
    y2024: y2024 ? "2024" : "",
    y2025: y2025 ? "2025" : "",
    y2026: y2026 ? "2026" : "",
    y2027plus: y2027plus ? years.filter((y) => Number(y) >= 2027).join("|") : "",
    hostMarket: signal.lodgingMarket || hotel.destinationMarket || hotel.market,
    venue: signal.venueStatus || signal.eventLocationSummary || "",
    hotelHousingKnown: signal.lodgingEvidence || pageEvidence.lodgingEvidence ? "YES" : "NO",
    attendance: signal.groupSize || "",
    cadence: recurring ? "ANNUAL_OR_SERIES" : rotation ? "ROTATION" : "UNKNOWN",
    organizer: signal.organizer || signal.organizationName || "",
    agency: signal.agency || "",
    decisionWindow: nextDecisionPoint || "",
    timingState,
    officialSource: signal.officialSource || signal.source,
  };
}
