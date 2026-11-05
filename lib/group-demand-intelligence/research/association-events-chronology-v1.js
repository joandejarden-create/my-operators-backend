/**
 * Association / Event chronology V1.
 *
 * Distinguishes past completed editions, confirmed future dates,
 * announced future editions without dates, historically recurring,
 * discontinued, and ambiguous listings. Never invents future dates.
 */

export const ASSOCIATION_EVENTS_CHRONOLOGY_V1 =
  "gdi-association-events-chronology-v1";

export const EVENT_CHRONOLOGY_STATUS = Object.freeze({
  CONFIRMED_FUTURE: "CONFIRMED_FUTURE",
  ANNOUNCED_FUTURE_UNDATED: "ANNOUNCED_FUTURE_UNDATED",
  HISTORICALLY_RECURRING: "HISTORICALLY_RECURRING",
  PAST_COMPLETED: "PAST_COMPLETED",
  DISCONTINUED: "DISCONTINUED",
  AMBIGUOUS: "AMBIGUOUS",
  INCORRECT_LISTING: "INCORRECT_LISTING",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function parseIsoDate(s) {
  const t = text(s);
  if (!t) return null;
  // YYYY-MM-DD or YYYY/MM/DD
  const m = t.match(/\b(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})\b/);
  if (m) {
    const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
    if (!Number.isNaN(d.getTime())) return d;
  }
  // Month name + year (English / Spanish)
  const months = {
    january: 0, febrero: 1, february: 1, march: 2, marzo: 2, april: 3, abril: 3,
    may: 4, mayo: 4, june: 5, junio: 5, july: 6, julio: 6, august: 7, agosto: 7,
    september: 8, septiembre: 8, october: 9, octubre: 9, november: 10, noviembre: 10,
    december: 11, diciembre: 11, jan: 0, feb: 1, mar: 2, apr: 3, jun: 5, jul: 6,
    aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
  };
  const mm = t.match(
    /\b(january|february|march|april|may|june|july|august|september|october|november|december|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\.?\s+(\d{1,2})?(?:[-–]\d{1,2})?,?\s*(20\d{2})\b/i,
  );
  if (mm) {
    const mo = months[mm[1].toLowerCase()];
    const day = mm[2] ? Number(mm[2]) : 1;
    const year = Number(mm[3]);
    if (mo != null) {
      const d = new Date(Date.UTC(year, mo, day));
      if (!Number.isNaN(d.getTime())) return d;
    }
  }
  // Bare year only — not a confirmed date
  return null;
}

function extractDateHints(opportunity = {}) {
  const blobs = [
    opportunity.eventStartDate,
    opportunity.eventEndDate,
    opportunity.eventDates,
    opportunity.growthEvidenceDate,
    opportunity.activityHeadline,
    opportunity.eventName,
    opportunity.whatIsHappening,
    opportunity.eventHistoryAndActivity,
    ...(opportunity.sources || []).map((s) => s?.excerpt || ""),
  ]
    .map(text)
    .filter(Boolean);
  const joined = blobs.join(" | ");
  const dates = [];
  const re =
    /\b(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})\b|\b((?:january|february|march|april|may|june|july|august|september|october|november|december|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\.?\s+\d{1,2}(?:\s*[-–]\s*\d{1,2})?,?\s*20\d{2})\b/gi;
  let m;
  while ((m = re.exec(joined))) {
    const raw = m[0];
    const d = parseIsoDate(raw);
    if (d) dates.push({ raw, date: d.toISOString().slice(0, 10) });
  }
  // Explicit structured fields
  for (const f of [opportunity.eventStartDate, opportunity.eventEndDate]) {
    const d = parseIsoDate(f);
    if (d) dates.push({ raw: text(f), date: d.toISOString().slice(0, 10) });
  }
  return { joined, dates };
}

/**
 * Classify event chronology relative to asOf (default now).
 */
export function classifyAssociationEventsChronology(opportunity = {}, { asOf = null } = {}) {
  const asOfDate = asOf ? new Date(asOf) : new Date();
  const asOfIso = asOfDate.toISOString().slice(0, 10);
  const { joined, dates } = extractDateHints(opportunity);

  if (opportunity.eventEntityIncorrect === true || opportunity.incorrectListing === true) {
    return {
      version: ASSOCIATION_EVENTS_CHRONOLOGY_V1,
      status: EVENT_CHRONOLOGY_STATUS.INCORRECT_LISTING,
      asOf: asOfIso,
      datesFound: dates,
      commerciallyActionableNow: false,
      futureEditionVerified: false,
      note: "Event entity marked incorrect",
    };
  }

  if (/discontinu|cancelad[oa]|cancelled|no longer held/i.test(joined)) {
    return {
      version: ASSOCIATION_EVENTS_CHRONOLOGY_V1,
      status: EVENT_CHRONOLOGY_STATUS.DISCONTINUED,
      asOf: asOfIso,
      datesFound: dates,
      commerciallyActionableNow: false,
      futureEditionVerified: false,
    };
  }

  const uniqueDates = [...new Set(dates.map((d) => d.date))].sort();
  const futureDates = uniqueDates.filter((d) => d >= asOfIso);
  const pastDates = uniqueDates.filter((d) => d < asOfIso);

  if (futureDates.length) {
    return {
      version: ASSOCIATION_EVENTS_CHRONOLOGY_V1,
      status: EVENT_CHRONOLOGY_STATUS.CONFIRMED_FUTURE,
      asOf: asOfIso,
      datesFound: dates,
      nextVerifiedDate: futureDates[0],
      lastPastDate: pastDates[pastDates.length - 1] || null,
      commerciallyActionableNow: true,
      futureEditionVerified: true,
      note: "At least one verified future calendar date present in evidence",
    };
  }

  // Past structured dates take precedence over incidental future-year language in sources
  if (pastDates.length) {
    if (/anual|annual|edici[oó]n|recurring|cada a[nñ]o|yearly|juntas regionales/i.test(joined)) {
      return {
        version: ASSOCIATION_EVENTS_CHRONOLOGY_V1,
        status: EVENT_CHRONOLOGY_STATUS.HISTORICALLY_RECURRING,
        asOf: asOfIso,
        datesFound: dates,
        lastPastDate: pastDates[pastDates.length - 1],
        commerciallyActionableNow: false,
        futureEditionVerified: false,
        note: "Past edition with recurrence language — investigate next edition; do not invent dates",
      };
    }
    return {
      version: ASSOCIATION_EVENTS_CHRONOLOGY_V1,
      status: EVENT_CHRONOLOGY_STATUS.PAST_COMPLETED,
      asOf: asOfIso,
      datesFound: dates,
      lastPastDate: pastDates[pastDates.length - 1],
      commerciallyActionableNow: false,
      futureEditionVerified: false,
    };
  }

  if (/pr[oó]xim[oa]|next edition|pr[oó]xima edici[oó]n|upcoming|will (?:be|take place)|se celebrar[aá]|announced for 20\d{2}/i.test(joined) &&
      /20(2[7-9]|3\d)/.test(joined)) {
    // Announced future year without exact date — do not invent day/month
    return {
      version: ASSOCIATION_EVENTS_CHRONOLOGY_V1,
      status: EVENT_CHRONOLOGY_STATUS.ANNOUNCED_FUTURE_UNDATED,
      asOf: asOfIso,
      datesFound: dates,
      commerciallyActionableNow: true,
      futureEditionVerified: false,
      note: "Future edition language present; exact start date not confirmed — do not invent dates",
    };
  }

  return {
    version: ASSOCIATION_EVENTS_CHRONOLOGY_V1,
    status: EVENT_CHRONOLOGY_STATUS.AMBIGUOUS,
    asOf: asOfIso,
    datesFound: dates,
    commerciallyActionableNow: false,
    futureEditionVerified: false,
    note: "No reliable event dates extracted from evidence",
  };
}

/**
 * Attach chronology classification onto opportunity.
 */
export function applyAssociationEventsChronology(opportunity = {}, opts = {}) {
  const chronology = classifyAssociationEventsChronology(opportunity, opts);
  return {
    ...opportunity,
    associationEventsChronology: chronology,
    eventChronologyStatus: chronology.status,
    eventCommerciallyActionableNow: chronology.commerciallyActionableNow === true,
    eventFutureEditionVerified: chronology.futureEditionVerified === true,
  };
}
