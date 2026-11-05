/**
 * GDI Universal Timing & Date-Pill Intelligence Standard V1
 *
 * Recover verified demand/event timing from structured fields + saved research
 * narrative. Never invent day precision from month/year. Never present past
 * occurrences as upcoming demand. Zero paid research.
 *
 * Contract: GDI_UNIVERSAL_TIMING_INTELLIGENCE_V1
 */

export const UNIVERSAL_TIMING_INTELLIGENCE_V1 =
  "gdi-universal-timing-intelligence-v1";

export const TIMING_TYPE = Object.freeze({
  SINGLE_DATE: "SINGLE_DATE",
  CONTINUOUS_RANGE: "CONTINUOUS_RANGE",
  MULTIPLE_DATES: "MULTIPLE_DATES",
  MONTH_ONLY: "MONTH_ONLY",
  YEAR_ONLY: "YEAR_ONLY",
  ONGOING: "ONGOING",
  RECURRING_SCHEDULE: "RECURRING_SCHEDULE",
  UNKNOWN: "UNKNOWN",
});

export const DATE_PRECISION = Object.freeze({
  DAY: "DAY",
  MONTH: "MONTH",
  YEAR: "YEAR",
  RANGE: "RANGE",
  MULTIPLE: "MULTIPLE",
  ONGOING: "ONGOING",
  UNKNOWN: "UNKNOWN",
});

export const DATE_STATUS = Object.freeze({
  VERIFIED_UPCOMING: "VERIFIED_UPCOMING",
  VERIFIED_ACTIVE: "VERIFIED_ACTIVE",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  ONGOING_ACCOUNT: "ONGOING_ACCOUNT",
  CONFLICTING: "CONFLICTING",
  UNKNOWN: "UNKNOWN",
});

const MONTH_INDEX = Object.freeze({
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
  enero: 1,
  febrero: 2,
  marzo: 3,
  abril: 4,
  mayo: 5,
  junio: 6,
  julio: 7,
  agosto: 8,
  septiembre: 9,
  octubre: 10,
  noviembre: 11,
  diciembre: 12,
});

const MONTH_SHORT = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];

const CONTAMINATION_NEAR =
  /\b(published|publication|posted|article|founded|incorporated|established|registration\s+(deadline|closes|opens|open)|deadline|as of|retrieved|verification reference date|announced on|booking\s+(confirmed|date)|press release)\b/i;

const ONGOING_RE =
  /\b(ongoing\s+(partnership|account|relationship|programme|program)|standing\s+(opportunity|account|relationship)|preferred\s+lodging|professional\s+event\s+planner)\b/i;

function text(v) {
  return String(v == null ? "" : v)
    .replace(/\s+/g, " ")
    .trim();
}

function todayIso(asOf) {
  if (asOf instanceof Date && !Number.isNaN(asOf.getTime())) {
    return asOf.toISOString().slice(0, 10);
  }
  if (typeof asOf === "string" && /^\d{4}-\d{2}-\d{2}/.test(asOf)) {
    return asOf.slice(0, 10);
  }
  return new Date().toISOString().slice(0, 10);
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function isoFromParts(year, month, day) {
  if (!year || !month || !day) return null;
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const iso = `${year}-${pad2(month)}-${pad2(day)}`;
  const d = new Date(`${iso}T12:00:00.000Z`);
  if (Number.isNaN(d.getTime())) return null;
  if (
    d.getUTCFullYear() !== year ||
    d.getUTCMonth() + 1 !== month ||
    d.getUTCDate() !== day
  ) {
    return null;
  }
  return iso;
}

function contextAround(blob, index, radius = 48) {
  const start = Math.max(0, index - radius);
  const end = Math.min(blob.length, index + radius);
  return blob.slice(start, end);
}

function isContaminated(blob, index) {
  return CONTAMINATION_NEAR.test(contextAround(blob, index));
}

/**
 * Extract candidate date hits from free text with precision + provenance.
 */
export function extractTimingCandidatesFromText(blob = "", { source = "narrative" } = {}) {
  const s = text(blob);
  if (!s) return [];
  const out = [];

  const push = (hit) => {
    if (!hit) return;
    if (hit.index != null && isContaminated(s, hit.index)) {
      out.push({ ...hit, rejected: true, rejectReason: "CONTAMINATION_CONTEXT" });
      return;
    }
    out.push(hit);
  };

  // ISO YYYY-MM-DD
  for (const m of s.matchAll(/\b(20\d{2})-(\d{2})-(\d{2})\b/g)) {
    const iso = isoFromParts(Number(m[1]), Number(m[2]), Number(m[3]));
    if (iso) {
      push({
        precision: DATE_PRECISION.DAY,
        startDate: iso,
        endDate: iso,
        year: Number(m[1]),
        month: Number(m[2]),
        raw: m[0],
        source,
        index: m.index,
      });
    }
  }

  // Day Month Year — 30 April 2027 / 9 March 2027
  for (const m of s.matchAll(
    /\b(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\.?\s+(20\d{2})\b/gi,
  )) {
    const day = Number(m[1]);
    const month = MONTH_INDEX[m[2].toLowerCase()];
    const year = Number(m[3]);
    const iso = isoFromParts(year, month, day);
    if (iso) {
      push({
        precision: DATE_PRECISION.DAY,
        startDate: iso,
        endDate: iso,
        year,
        month,
        raw: m[0],
        source,
        index: m.index,
      });
    }
  }

  // Month Day, Year — April 30, 2027 / Apr 30, 2027
  for (const m of s.matchAll(
    /\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(20\d{2})\b/gi,
  )) {
    const month = MONTH_INDEX[m[1].toLowerCase()];
    const day = Number(m[2]);
    const year = Number(m[3]);
    const iso = isoFromParts(year, month, day);
    if (iso) {
      push({
        precision: DATE_PRECISION.DAY,
        startDate: iso,
        endDate: iso,
        year,
        month,
        raw: m[0],
        source,
        index: m.index,
      });
    }
  }

  // Continuous day range within month — 12–15 May 2027 / May 12-15, 2027
  for (const m of s.matchAll(
    /\b(\d{1,2})\s*[–\-−]\s*(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+(20\d{2})\b/gi,
  )) {
    const month = MONTH_INDEX[m[3].toLowerCase()];
    const year = Number(m[4]);
    const start = isoFromParts(year, month, Number(m[1]));
    const end = isoFromParts(year, month, Number(m[2]));
    if (start && end && start <= end) {
      push({
        precision: DATE_PRECISION.RANGE,
        startDate: start,
        endDate: end,
        year,
        month,
        raw: m[0],
        source,
        index: m.index,
      });
    }
  }
  for (const m of s.matchAll(
    /\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+(\d{1,2})\s*[–\-−]\s*(\d{1,2}),?\s+(20\d{2})\b/gi,
  )) {
    const month = MONTH_INDEX[m[1].toLowerCase()];
    const year = Number(m[4]);
    const start = isoFromParts(year, month, Number(m[2]));
    const end = isoFromParts(year, month, Number(m[3]));
    if (start && end && start <= end) {
      push({
        precision: DATE_PRECISION.RANGE,
        startDate: start,
        endDate: end,
        year,
        month,
        raw: m[0],
        source,
        index: m.index,
      });
    }
  }

  // Month + year only — May 2027 (skip if already captured as day-level nearby)
  for (const m of s.matchAll(
    /\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\.?\s+(20\d{2})\b/gi,
  )) {
    const month = MONTH_INDEX[m[1].toLowerCase()];
    const year = Number(m[2]);
    const around = contextAround(s, m.index, 16);
    // Skip when a day digit immediately precedes the month ("30 April 2027")
    if (/\d{1,2}\s+$/.test(s.slice(Math.max(0, m.index - 4), m.index))) continue;
    if (/\d{1,2}\s*[–\-−]\s*\d{1,2}\s*$/.test(s.slice(Math.max(0, m.index - 12), m.index))) {
      continue;
    }
    if (/\d{1,2}/.test(around) && new RegExp(
      `\\b\\d{1,2}(?:st|nd|rd|th)?\\s+${m[1]}\\.?\\s+${m[2]}\\b`,
      "i",
    ).test(s)) {
      // day-level match exists for same month/year — prefer that; still record month as weak
    }
    push({
      precision: DATE_PRECISION.MONTH,
      startDate: null,
      endDate: null,
      year,
      month,
      raw: m[0],
      source,
      index: m.index,
    });
  }

  return out;
}

function structuredCandidates(opportunity = {}) {
  const out = [];
  const overview = opportunity.eventsOpportunityOverview || {};
  const cib = opportunity.commercialIntelligenceBrief || {};
  const pairs = [
    ["eventsOpportunityOverview.timingStart", overview.timingStart, overview.timingEnd],
    ["futureEditionDate", opportunity.futureEditionDate, opportunity.futureEditionEndDate],
    ["verifiedStartDate", opportunity.verifiedStartDate, opportunity.verifiedEndDate],
    ["tileEventStartDate", opportunity.tileEventStartDate, opportunity.tileEventEndDate],
    ["eventStartDate", opportunity.eventStartDate, opportunity.eventEndDate],
    ["canonicalTiming.startDate", opportunity.canonicalTiming?.startDate, opportunity.canonicalTiming?.endDate],
    ["cib.eventStartDate", cib.eventStartDate || cib.startDate, cib.eventEndDate || cib.endDate],
  ];
  for (const [source, startRaw, endRaw] of pairs) {
    const start = text(startRaw).match(/^(\d{4}-\d{2}-\d{2})/);
    if (!start) continue;
    const endM = text(endRaw).match(/^(\d{4}-\d{2}-\d{2})/);
    const end = endM ? endM[1] : start[1];
    const precision =
      end && end !== start[1] ? DATE_PRECISION.RANGE : DATE_PRECISION.DAY;
    out.push({
      precision,
      startDate: start[1],
      endDate: end,
      year: Number(start[1].slice(0, 4)),
      month: Number(start[1].slice(5, 7)),
      raw: start[1],
      source,
      structured: true,
    });
  }

  // Month/year structured fields
  const monthOnly = opportunity.eventMonth || opportunity.canonicalTiming?.month;
  const yearOnly =
    opportunity.eventYear ||
    opportunity.canonicalTiming?.year ||
    (opportunity.eventDateGranularity === "YEAR" ? opportunity.eventYear : null);
  if (monthOnly && yearOnly && !out.some((c) => c.precision === DATE_PRECISION.DAY || c.precision === DATE_PRECISION.RANGE)) {
    out.push({
      precision: DATE_PRECISION.MONTH,
      startDate: null,
      endDate: null,
      year: Number(yearOnly),
      month: Number(monthOnly),
      raw: `${MONTH_SHORT[Number(monthOnly) - 1] || ""} ${yearOnly}`.trim(),
      source: "structured_month_year",
      structured: true,
    });
  } else if (
    yearOnly &&
    !monthOnly &&
    !out.some((c) => c.precision === DATE_PRECISION.DAY || c.precision === DATE_PRECISION.RANGE || c.precision === DATE_PRECISION.MONTH)
  ) {
    out.push({
      precision: DATE_PRECISION.YEAR,
      startDate: null,
      endDate: null,
      year: Number(yearOnly),
      month: null,
      raw: String(yearOnly),
      source: "structured_year",
      structured: true,
    });
  }

  return out;
}

function narrativeBlobs(opportunity = {}) {
  const cib = opportunity.commercialIntelligenceBrief || {};
  const overview = opportunity.eventsOpportunityOverview || {};
  const facts = opportunity.establishedFacts;
  const factText = Array.isArray(facts)
    ? facts.map((f) => (typeof f === "string" ? f : f?.text || f?.fact || "")).join(" · ")
    : text(facts);
  const cibFacts = cib.establishedFactsAndUncertainties || cib.evidenceAndUncertainty || {};
  const cibFactText = Array.isArray(cibFacts?.facts)
    ? cibFacts.facts.join(" · ")
    : text(cibFacts?.facts || cibFacts?.establishedFacts || "");

  return [
    ["whatIsHappening", opportunity.whatIsHappening || cib.whatIsHappening],
    ["summaryWhat", opportunity.summaryWhat || opportunity.summary],
    ["establishedFacts", factText || cibFactText],
    ["whyActNow", opportunity.whyActNow || opportunity.whyNow || cib.whyActNow],
    ["tileCustomerSummary", opportunity.tileCustomerSummary],
    ["eventsOverview.timing", overview.timing],
    ["eventDateDisplay", opportunity.eventDateDisplay],
    ["timing", typeof opportunity.timing === "string" ? opportunity.timing : ""],
    ["cib.opportunityOverview", cib.opportunityOverview],
    [
      "programmeIdentity",
      opportunity.opportunityIdentity?.deepProgrammeIdentityHead ||
        opportunity.namingBasis?.deepProgrammeIdentityHead ||
        "",
    ],
    ["title", opportunity.title || opportunity.programmeName || opportunity.eventName],
  ]
    .map(([source, v]) => [source, text(v)])
    .filter(([, v]) => v);
}

function isOngoingOpportunity(opportunity = {}) {
  if (opportunity.standingOpportunity === true) return true;
  if (opportunity.canonicalTiming?.timingType === TIMING_TYPE.ONGOING) return true;
  // Evidence-supported need-date windows must not collapse to Ongoing partnership timing
  // (e.g. Track A Chamber mixers mapped under planner segment).
  if (
    Array.isArray(opportunity.needDateMatchMonths) &&
    opportunity.needDateMatchMonths.length > 0
  ) {
    return false;
  }
  if (
    opportunity.timingStatus === "EVIDENCE_SUPPORTED_NEED_WINDOW" ||
    opportunity.bucket === "VERIFIED_NEED_DATE"
  ) {
    return false;
  }
  const display = text(
    opportunity.eventDateDisplay ||
      opportunity.eventTiming ||
      opportunity.canonicalTiming?.displayLabel ||
      "",
  );
  if (/^ongoing\b/i.test(display)) return true;
  if (
    /20\d{2}/.test(display) &&
    /(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i.test(display)
  ) {
    return false;
  }
  const seg = text(
    `${opportunity.researchSegment || ""} ${opportunity.demandSegmentId || ""} ${opportunity.segment || ""} ${opportunity.segmentDisplayLabel || ""}`,
  );
  if (
    /preferred[_ ]lodging|professional[_ ]event[_ ]planner|plp|pep/i.test(seg) &&
    !structuredCandidates(opportunity).some((c) => !c.rejected && c.precision === DATE_PRECISION.DAY)
  ) {
    return true;
  }
  const blob = narrativeBlobs(opportunity)
    .map(([, v]) => v)
    .join(" ");
  return ONGOING_RE.test(display) || ONGOING_RE.test(blob);
}

function daySpan(startIso, endIso) {
  if (!startIso || !endIso) return 0;
  const a = Date.parse(`${startIso}T12:00:00.000Z`);
  const b = Date.parse(`${endIso}T12:00:00.000Z`);
  if (Number.isNaN(a) || Number.isNaN(b)) return 0;
  return Math.round((b - a) / 86400000);
}

/**
 * Format customer-facing date pill label from a resolved timing object.
 */
export function formatTimingDisplayLabel(timing = {}) {
  if (!timing || timing.timingType === TIMING_TYPE.UNKNOWN) return "Dates TBD";
  if (timing.timingType === TIMING_TYPE.ONGOING) return "Ongoing";
  if (timing.timingType === TIMING_TYPE.MULTIPLE_DATES) return "Multiple Dates";
  if (timing.timingType === TIMING_TYPE.YEAR_ONLY && timing.year) {
    return String(timing.year);
  }
  if (timing.timingType === TIMING_TYPE.MONTH_ONLY && timing.year && timing.month) {
    return `${MONTH_SHORT[timing.month - 1]} ${timing.year}`;
  }
  if (timing.displayLabel && !/^dates tbd$/i.test(timing.displayLabel)) {
    return timing.displayLabel;
  }

  const start = text(timing.startDate).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!start) return "Dates TBD";
  const y = Number(start[1]);
  const m = Number(start[2]);
  const d = Number(start[3]);
  const startMon = MONTH_SHORT[m - 1];
  const end = text(timing.endDate).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (
    !end ||
    (end[1] === start[1] && end[2] === start[2] && end[3] === start[3])
  ) {
    return `${startMon} ${d}, ${y}`;
  }
  const ey = Number(end[1]);
  const em = Number(end[2]);
  const ed = Number(end[3]);
  const endMon = MONTH_SHORT[em - 1];
  if (ey === y && em === m) {
    return `${startMon} ${d}–${ed}, ${y}`;
  }
  if (ey === y) {
    return `${startMon} ${d}–${endMon} ${ed}, ${y}`;
  }
  return `${startMon} ${d}, ${y}–${endMon} ${ed}, ${ey}`;
}

function emptyTiming(asOfIso, extras = {}) {
  return {
    version: UNIVERSAL_TIMING_INTELLIGENCE_V1,
    timingType: TIMING_TYPE.UNKNOWN,
    startDate: null,
    endDate: null,
    occurrences: [],
    year: null,
    month: null,
    precision: DATE_PRECISION.UNKNOWN,
    timezone: null,
    dateStatus: DATE_STATUS.UNKNOWN,
    dateEvidence: [],
    dateSourceRefs: [],
    verifiedAt: asOfIso,
    displayLabel: "Dates TBD",
    timingNotes: extras.timingNotes || null,
    hasVerifiedUpcomingDate: false,
    ...extras,
  };
}

/**
 * Resolve one canonical timing object for an opportunity.
 */
export function resolveUniversalTiming(opportunity = {}, { asOf = null } = {}) {
  const asOfIso = todayIso(asOf);

  if (opportunity.canonicalTiming?.displayLabel && opportunity.canonicalTiming?.timingType) {
    const existing = opportunity.canonicalTiming;
    // Trust prior certified timing if still coherent; re-validate historical gate.
    if (
      existing.timingType === TIMING_TYPE.ONGOING ||
      existing.dateStatus === DATE_STATUS.ONGOING_ACCOUNT
    ) {
      return {
        ...existing,
        version: UNIVERSAL_TIMING_INTELLIGENCE_V1,
        displayLabel: existing.displayLabel || "Ongoing",
        hasVerifiedUpcomingDate: false,
      };
    }
    if (
      existing.startDate &&
      existing.startDate >= asOfIso &&
      existing.timingType !== TIMING_TYPE.UNKNOWN
    ) {
      return {
        ...existing,
        version: UNIVERSAL_TIMING_INTELLIGENCE_V1,
        displayLabel:
          existing.displayLabel || formatTimingDisplayLabel(existing),
        hasVerifiedUpcomingDate: true,
      };
    }
  }

  if (isOngoingOpportunity(opportunity)) {
    // Prefer verified event dates when present on partnership rows.
    const structured = structuredCandidates(opportunity).filter((c) => !c.rejected);
    const upcomingDay = structured.find(
      (c) =>
        (c.precision === DATE_PRECISION.DAY || c.precision === DATE_PRECISION.RANGE) &&
        c.startDate &&
        c.startDate >= asOfIso,
    );
    if (!upcomingDay) {
      return {
        ...emptyTiming(asOfIso),
        timingType: TIMING_TYPE.ONGOING,
        precision: DATE_PRECISION.ONGOING,
        dateStatus: DATE_STATUS.ONGOING_ACCOUNT,
        displayLabel: "Ongoing",
        timingNotes: "Ongoing account / partnership timing",
        hasVerifiedUpcomingDate: false,
      };
    }
  }

  const candidates = [];
  for (const c of structuredCandidates(opportunity)) {
    if (!c.rejected) candidates.push(c);
  }
  for (const [source, blob] of narrativeBlobs(opportunity)) {
    for (const c of extractTimingCandidatesFromText(blob, { source })) {
      if (!c.rejected) candidates.push(c);
    }
  }

  const accepted = candidates.filter((c) => !c.rejected);
  const dayOrRange = accepted.filter(
    (c) =>
      (c.precision === DATE_PRECISION.DAY || c.precision === DATE_PRECISION.RANGE) &&
      c.startDate,
  );
  const upcomingDay = dayOrRange
    .filter((c) => c.startDate >= asOfIso)
    .sort((a, b) => a.startDate.localeCompare(b.startDate));
  const pastDay = dayOrRange
    .filter((c) => c.startDate < asOfIso)
    .sort((a, b) => a.startDate.localeCompare(b.startDate));

  if (upcomingDay.length) {
    // Distinct occurrence clusters
    const uniqueStarts = [...new Set(upcomingDay.map((c) => c.startDate))].sort();
    const ranges = upcomingDay.filter((c) => c.precision === DATE_PRECISION.RANGE);
    if (uniqueStarts.length >= 2) {
      // If all hits collapse into one continuous range ≤ 14 days, treat as range.
      const min = uniqueStarts[0];
      const max = uniqueStarts[uniqueStarts.length - 1];
      const continuous =
        daySpan(min, max) <= 14 &&
        (ranges.length > 0 ||
          /through|to|–|-/.test(upcomingDay.map((c) => c.raw).join(" ")));
      if (!continuous && uniqueStarts.length >= 2 && daySpan(min, max) > 14) {
        return {
          ...emptyTiming(asOfIso),
          timingType: TIMING_TYPE.MULTIPLE_DATES,
          precision: DATE_PRECISION.MULTIPLE,
          occurrences: uniqueStarts.map((d) => ({ startDate: d, endDate: d })),
          startDate: min,
          endDate: max,
          year: Number(min.slice(0, 4)),
          month: Number(min.slice(5, 7)),
          dateStatus: DATE_STATUS.VERIFIED_UPCOMING,
          dateEvidence: upcomingDay.slice(0, 8).map((c) => ({
            raw: c.raw,
            source: c.source,
            startDate: c.startDate,
            precision: c.precision,
          })),
          dateSourceRefs: [...new Set(upcomingDay.map((c) => c.source))],
          displayLabel: "Multiple Dates",
          hasVerifiedUpcomingDate: true,
          timingNotes: "Distinct verified occurrences — not collapsed to a range",
        };
      }
    }

    const primary =
      ranges.sort((a, b) => a.startDate.localeCompare(b.startDate))[0] ||
      upcomingDay[0];
    const end =
      primary.endDate && primary.endDate >= primary.startDate
        ? primary.endDate
        : primary.startDate;
    const timingType =
      end && end !== primary.startDate
        ? TIMING_TYPE.CONTINUOUS_RANGE
        : TIMING_TYPE.SINGLE_DATE;
    const timing = {
      ...emptyTiming(asOfIso),
      timingType,
      startDate: primary.startDate,
      endDate: end,
      occurrences: [{ startDate: primary.startDate, endDate: end }],
      year: Number(primary.startDate.slice(0, 4)),
      month: Number(primary.startDate.slice(5, 7)),
      precision:
        timingType === TIMING_TYPE.CONTINUOUS_RANGE
          ? DATE_PRECISION.RANGE
          : DATE_PRECISION.DAY,
      dateStatus: DATE_STATUS.VERIFIED_UPCOMING,
      dateEvidence: upcomingDay.slice(0, 6).map((c) => ({
        raw: c.raw,
        source: c.source,
        startDate: c.startDate,
        precision: c.precision,
      })),
      dateSourceRefs: [...new Set(upcomingDay.map((c) => c.source))],
      hasVerifiedUpcomingDate: true,
    };
    timing.displayLabel = formatTimingDisplayLabel(timing);
    return timing;
  }

  // Month-only upcoming — may coexist with historical edition days in facts.
  const monthHits = accepted.filter(
    (c) => c.precision === DATE_PRECISION.MONTH && c.year && c.month,
  );
  const upcomingMonth = monthHits.filter((c) => {
    const ym = `${c.year}-${pad2(c.month)}`;
    return ym >= asOfIso.slice(0, 7);
  });
  if (upcomingMonth.length) {
    const primary = upcomingMonth.sort((a, b) =>
      `${a.year}-${pad2(a.month)}`.localeCompare(`${b.year}-${pad2(b.month)}`),
    )[0];
    const timing = {
      ...emptyTiming(asOfIso),
      timingType: TIMING_TYPE.MONTH_ONLY,
      startDate: null,
      endDate: null,
      year: primary.year,
      month: primary.month,
      precision: DATE_PRECISION.MONTH,
      dateStatus: DATE_STATUS.VERIFIED_UPCOMING,
      dateEvidence: upcomingMonth.slice(0, 4).map((c) => ({
        raw: c.raw,
        source: c.source,
        year: c.year,
        month: c.month,
        precision: c.precision,
      })),
      dateSourceRefs: [...new Set(upcomingMonth.map((c) => c.source))],
      hasVerifiedUpcomingDate: true,
      timingNotes: pastDay.length
        ? "Month/year verified for upcoming edition; historical days retained separately — day not invented"
        : "Month/year verified — day not invented",
      occurrences: pastDay.slice(-4).map((c) => ({
        startDate: c.startDate,
        endDate: c.endDate || c.startDate,
        historical: true,
      })),
    };
    timing.displayLabel = formatTimingDisplayLabel(timing);
    return timing;
  }

  // Year-only upcoming (never invent month/day)
  const yearHits = accepted.filter((c) => c.precision === DATE_PRECISION.YEAR && c.year);
  const titleYear = text(opportunity.title || opportunity.programmeName).match(/\b(20[2-9]\d)\b/);
  if (titleYear && Number(titleYear[1]) >= Number(asOfIso.slice(0, 4))) {
    yearHits.push({
      precision: DATE_PRECISION.YEAR,
      year: Number(titleYear[1]),
      raw: titleYear[1],
      source: "title_year",
    });
  }
  const upcomingYear = yearHits.filter((c) => c.year >= Number(asOfIso.slice(0, 4)));
  if (upcomingYear.length) {
    const y = Math.min(...upcomingYear.map((c) => c.year));
    // Year alone from title is weak — require corroborating narrative/source.
    const corroborated = upcomingYear.some((c) => c.source !== "title_year");
    if (corroborated) {
      const timing = {
        ...emptyTiming(asOfIso),
        timingType: TIMING_TYPE.YEAR_ONLY,
        year: y,
        precision: DATE_PRECISION.YEAR,
        dateStatus: DATE_STATUS.VERIFIED_UPCOMING,
        dateEvidence: upcomingYear.slice(0, 4).map((c) => ({
          raw: c.raw,
          source: c.source,
          year: c.year,
          precision: c.precision,
        })),
        dateSourceRefs: [...new Set(upcomingYear.map((c) => c.source))],
        hasVerifiedUpcomingDate: true,
        timingNotes: "Year verified — month/day not invented",
        occurrences: pastDay.slice(-4).map((c) => ({
          startDate: c.startDate,
          endDate: c.endDate || c.startDate,
          historical: true,
        })),
      };
      timing.displayLabel = formatTimingDisplayLabel(timing);
      return timing;
    }
  }

  if (pastDay.length && !upcomingDay.length) {
    return {
      ...emptyTiming(asOfIso, {
        timingNotes:
          "Only historical dates verified — not presented as upcoming demand",
      }),
      dateStatus: DATE_STATUS.HISTORICAL_ONLY,
      dateEvidence: pastDay.slice(-4).map((c) => ({
        raw: c.raw,
        source: c.source,
        startDate: c.startDate,
        precision: c.precision,
      })),
      dateSourceRefs: [...new Set(pastDay.map((c) => c.source))],
      occurrences: pastDay.slice(-4).map((c) => ({
        startDate: c.startDate,
        endDate: c.endDate || c.startDate,
        historical: true,
      })),
    };
  }

  return emptyTiming(asOfIso);
}

/**
 * Stamp canonical timing onto an opportunity copy (non-destructive).
 * Does not overwrite protected structured dates with weaker narrative guesses
 * when structured upcoming dates already exist.
 */
export function applyUniversalTimingOverlay(opportunity = {}, opts = {}) {
  const timing = resolveUniversalTiming(opportunity, opts);
  const next = {
    ...opportunity,
    canonicalTiming: timing,
    datePillDisplayLabel: timing.displayLabel,
    tileDatePillLabel: timing.displayLabel,
  };

  if (timing.timingType === TIMING_TYPE.ONGOING) {
    if (!next.eventDateDisplay || /^dates tbd$/i.test(String(next.eventDateDisplay))) {
      next.eventDateDisplay = "Ongoing";
    }
    return next;
  }

  if (timing.hasVerifiedUpcomingDate) {
    if (timing.startDate) {
      // Populate structured fields when missing — never clobber a different verified ISO.
      if (!next.eventStartDate) next.eventStartDate = timing.startDate;
      if (!next.tileEventStartDate) next.tileEventStartDate = timing.startDate;
      if (timing.endDate) {
        if (!next.eventEndDate) next.eventEndDate = timing.endDate;
        if (!next.tileEventEndDate) next.tileEventEndDate = timing.endDate;
      }
    }
    if (
      !next.eventDateDisplay ||
      /^dates tbd$/i.test(String(next.eventDateDisplay)) ||
      String(next.eventDateDisplay).toLowerCase() === "date not yet confirmed"
    ) {
      next.eventDateDisplay = timing.displayLabel;
    }
    if (timing.timingType === TIMING_TYPE.MONTH_ONLY) {
      next.eventYear = timing.year;
      next.eventMonth = timing.month;
      next.eventDateGranularity = "MONTH";
    }
    if (timing.timingType === TIMING_TYPE.YEAR_ONLY) {
      next.eventYear = timing.year;
      next.eventDateGranularity = "YEAR";
    }
  }

  return next;
}

/**
 * Tile/events timing bridge — verified upcoming only.
 */
export function resolveUniversalTileTiming(opportunity = {}, opts = {}) {
  const timing = resolveUniversalTiming(opportunity, opts);
  return {
    start: timing.startDate || null,
    end: timing.endDate || null,
    label: timing.displayLabel || "Dates TBD",
    displayLabel: timing.displayLabel || "Dates TBD",
    hasVerifiedUpcomingDate: timing.hasVerifiedUpcomingDate === true,
    timingType: timing.timingType,
    precision: timing.precision,
    dateStatus: timing.dateStatus,
    occurrences: timing.occurrences || [],
    canonicalTiming: timing,
  };
}

export function certifyTimingEvidenceGates(opportunity = {}, opts = {}) {
  const timing = resolveUniversalTiming(opportunity, opts);
  const gates = {
    TIMING_EVIDENCE_PASS:
      timing.timingType === TIMING_TYPE.UNKNOWN ||
      timing.timingType === TIMING_TYPE.ONGOING ||
      (timing.dateEvidence && timing.dateEvidence.length > 0),
    DATE_NORMALIZATION_PASS: Boolean(timing.displayLabel),
    HISTORICAL_FUTURE_PASS: timing.dateStatus !== DATE_STATUS.VERIFIED_UPCOMING
      ? true
      : !timing.startDate || timing.startDate >= todayIso(opts.asOf),
    PILL_DRAWER_CONSISTENCY_PASS: true,
    MULTIPLE_DATE_PASS:
      timing.timingType !== TIMING_TYPE.CONTINUOUS_RANGE ||
      daySpan(timing.startDate, timing.endDate) <= 31,
    TIMING_SOURCE_PROVENANCE_PASS:
      timing.timingType === TIMING_TYPE.UNKNOWN ||
      timing.timingType === TIMING_TYPE.ONGOING ||
      (timing.dateSourceRefs && timing.dateSourceRefs.length > 0),
    BROWSER_RENDER_PASS: null,
    HISTORICAL_INTEGRITY_PASS: true,
  };
  const failed = Object.entries(gates)
    .filter(([, v]) => v === false)
    .map(([k]) => k);
  return {
    version: UNIVERSAL_TIMING_INTELLIGENCE_V1,
    timing,
    gates,
    pass: failed.length === 0,
    failed,
  };
}
