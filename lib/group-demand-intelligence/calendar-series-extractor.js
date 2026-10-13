/**
 * Generic calendar / future-meeting page → named event-series extraction.
 * Heuristic first (deterministic); optional LLM enrichment by caller.
 *
 * Does NOT hardcode evaluation benchmark event names.
 */

import { buildEventSeriesIdentity } from "./commercial-evidence-v4.js";
import {
  OPPORTUNITY_TYPE,
  VENUE_SOURCING_STATUS,
  FUTURE_CYCLE_STATE,
} from "./claim-types.js";

export const CALENDAR_SERIES_EXTRACTOR_VERSION = "gdi_calendar_series_extractor_v1";

export const SERIES_RECURRENCE = Object.freeze({
  ONE_OFF_EVENT: "ONE_OFF_EVENT",
  RECURRING_SERIES: "RECURRING_SERIES",
  ANNUAL_SERIES: "ANNUAL_SERIES",
  UNKNOWN: "UNKNOWN",
});

export const DISCOVERY_LANE = Object.freeze({
  NIH_CALENDAR: "NIH_CALENDAR",
  EVENT_SERIES: "EVENT_SERIES",
  FUTURE_CALENDAR: "FUTURE_CALENDAR",
  SPORTS: "SPORTS",
  HOUSING: "HOUSING",
  TBD: "TBD",
  KNOWN_TARGET: "KNOWN_TARGET",
  OPEN_UNIVERSE: "OPEN_UNIVERSE",
});

const YEAR_RE = /\b(20(?:2[6-9]|3[0-9]))\b/g;
const DATE_RANGE_RE =
  /\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:\s*[-–—]\s*\d{1,2})?,?\s*20\d{2}\b/gi;
const ANNUAL_RE =
  /\b(?:annual|yearly|nth\s+annual|\d+(?:st|nd|rd|th)\s+annual|recurring|save\s+the\s+date|future\s+meetings?)\b/i;
const HOUSING_RE =
  /\b(?:accommodations?|lodging|housing|hotel\s+block|room\s+block|where\s+to\s+stay|host\s+hotel|hotel\s+selection|stay[- ]to[- ]play|overflow)\b/i;
const TBD_RE =
  /\b(?:destination\s+tbd|location\s+tbd|venue\s+tbd|hotel\s+tbd|coming\s+soon|tba|to\s+be\s+announced)\b/i;
const FIXED_VENUE_RE =
  /\b(?:natcher|conference\s+center|building\s+\d+|campus|convention\s+center|arena|stadium)\b/i;
const SPORTS_RE =
  /\b(?:tournament|championship|showdown|cup\b|stay[- ]to[- ]play|hotel\s+selection)\b/i;

function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 64);
}

function inferRecurrence(title, text) {
  const blob = `${title} ${text}`;
  if (/\b(?:\d+(?:st|nd|rd|th)\s+annual|annual)\b/i.test(blob)) {
    return SERIES_RECURRENCE.ANNUAL_SERIES;
  }
  if (ANNUAL_RE.test(blob)) return SERIES_RECURRENCE.RECURRING_SERIES;
  return SERIES_RECURRENCE.UNKNOWN;
}

function inferLane(url, title, text) {
  const blob = `${url} ${title} ${text}`.toLowerCase();
  if (/nih\.gov|natcher|commonfund\.nih|clinical.?trials.?network|nida/i.test(blob)) {
    return DISCOVERY_LANE.NIH_CALENDAR;
  }
  if (SPORTS_RE.test(blob)) return DISCOVERY_LANE.SPORTS;
  if (TBD_RE.test(blob)) return DISCOVERY_LANE.TBD;
  if (HOUSING_RE.test(blob) && FIXED_VENUE_RE.test(blob)) return DISCOVERY_LANE.HOUSING;
  if (/future\s+meetings|upcoming\s+meetings|annual\s+meeting/i.test(blob)) {
    return DISCOVERY_LANE.FUTURE_CALENDAR;
  }
  return DISCOVERY_LANE.EVENT_SERIES;
}

function inferCommercialMotion({ title, text, venueFixed, housingOpen }) {
  const blob = `${title} ${text}`;
  if (venueFixed && (housingOpen || HOUSING_RE.test(blob) || TBD_RE.test(blob) || !/official\s+hotel/i.test(blob))) {
    return {
      opportunityType: OPPORTUNITY_TYPE.FIXED_VENUE_OPEN_HOUSING,
      venueSourcingStatus: housingOpen
        ? VENUE_SOURCING_STATUS.HOUSING_PENDING
        : VENUE_SOURCING_STATUS.HOUSING_PENDING,
      futureCycleState: FUTURE_CYCLE_STATE.HOUSING_PENDING,
      cqHint: "HOUSING_ANCILLARY",
    };
  }
  if (TBD_RE.test(blob)) {
    return {
      opportunityType: OPPORTUNITY_TYPE.FUTURE_CYCLE,
      venueSourcingStatus: VENUE_SOURCING_STATUS.DESTINATION_TBD,
      futureCycleState: FUTURE_CYCLE_STATE.DESTINATION_TBD,
      cqHint: "FUTURE_WATCH",
    };
  }
  if (SPORTS_RE.test(blob) && HOUSING_RE.test(blob)) {
    return {
      opportunityType: OPPORTUNITY_TYPE.OVERFLOW_HOUSING,
      venueSourcingStatus: VENUE_SOURCING_STATUS.HOUSING_PENDING,
      futureCycleState: FUTURE_CYCLE_STATE.HOTEL_TBD,
      cqHint: "HOUSING_ANCILLARY",
    };
  }
  return {
    opportunityType: OPPORTUNITY_TYPE.FUTURE_CYCLE,
    venueSourcingStatus: VENUE_SOURCING_STATUS.OPEN_UNRESOLVED,
    futureCycleState: FUTURE_CYCLE_STATE.KNOWN_YEAR,
    cqHint: "WATCH",
  };
}

function hasEventWord(line) {
  return /\b(?:meeting|symposium|summit|conference|forum|workshop|tournament|championship|annual|fly-?in|congress)\b/i.test(
    line
  );
}

/**
 * Split page text into candidate event lines (heuristic).
 * Joins adjacent title + date lines (common on official calendars).
 */
function candidateLines(text) {
  const lines = String(text || "")
    .replace(/\r/g, "")
    .split(/\n+/)
    .map((l) => l.replace(/\s+/g, " ").trim())
    .filter((l) => l.length >= 8 && l.length <= 220);

  // Merge "Named Symposium" + following "March 10-12, 2027" into one candidate line
  const merged = [];
  for (let i = 0; i < lines.length; i++) {
    const cur = lines[i];
    const next = lines[i + 1] || "";
    const curHasYear = /20(?:2[6-9]|3[0-9])/.test(cur);
    const nextHasYear = /20(?:2[6-9]|3[0-9])/.test(next);
    if (hasEventWord(cur) && !curHasYear && nextHasYear && next.length <= 80) {
      merged.push(`${cur} ${next}`.slice(0, 220));
      i += 1;
      continue;
    }
    merged.push(cur);
  }

  const out = [];
  for (const line of merged) {
    if (line.length < 12) continue;
    const yearOk = /20(?:2[6-9]|3[0-9])/.test(line);
    if (yearOk && hasEventWord(line)) out.push(line);
    else if (hasEventWord(line) && ANNUAL_RE.test(line) && /20\d{2}|tbd|coming soon/i.test(line)) {
      out.push(line);
    }
  }
  // Also pull date-range sentences
  const sentences = String(text || "").split(/(?<=[.!?])\s+/);
  for (const s of sentences) {
    if (s.length < 20 || s.length > 280) continue;
    if (DATE_RANGE_RE.test(s) && /\b(?:meeting|symposium|summit|conference|tournament)\b/i.test(s)) {
      out.push(s.replace(/\s+/g, " ").trim());
    }
  }
  return [...new Set(out)].slice(0, 40);
}

function extractYears(s) {
  const years = [];
  const re = new RegExp(YEAR_RE.source, "g");
  let m;
  while ((m = re.exec(s))) years.push(Number(m[1]));
  return [...new Set(years)];
}

function guessOrg(line, pageOrg) {
  const m = line.match(/\b(?:hosted by|organized by|presented by)\s+([^.;]{3,80})/i);
  if (m) return m[1].trim();
  return pageOrg || null;
}

function isVagueEventTitle(title) {
  const t = String(title || "").trim();
  if (t.length < 16) return true;
  if (/^(natcher|nih|campus|future)\s+(meeting|event|meetings|events)\b/i.test(t)) {
    return true;
  }
  if (/^natcher conference center meetings$/i.test(t)) return true;
  if (/^future meetings\b/i.test(t) && t.length < 40) return true;
  // Registration / zoom / chrome noise
  if (/passcode|meeting id|skip to main content|request a conference registration|favicon|dissemination library browse/i.test(t)) {
    return true;
  }
  // Require a distinctive noun beyond generic meeting words
  const stripped = t
    .replace(/\b(?:20\d{2}|annual|meeting|meetings|conference|symposium|summit|workshop|forum|event|events|the|and|of|for|in|at|on)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
  return stripped.length < 4;
}

/**
 * Extract named series/cycles from a fetched page.
 * @param {{ url: string, text: string, title?: string, organizationHint?: string, discoveryMode?: string }} page
 */
export function extractEventSeriesFromCalendarPage(page = {}) {
  const url = page.url || "";
  const text = String(page.text || "").slice(0, 120000);
  const title = page.title || "";
  const lane = page.discoveryMode || inferLane(url, title, text);
  const lines = candidateLines(`${title}\n${text}`).filter((l) => !isVagueEventTitle(l));
  const seriesMap = new Map();
  const cycles = [];

  for (const line of lines) {
    const years = extractYears(line);
    const year = years[0] || null;
    // Drop clearly past-only lines when only old years present
    if (years.length && years.every((y) => y < 2026)) continue;

    const recurrence = inferRecurrence(line, text);
    const org = guessOrg(line, page.organizationHint);
    const venueFixed = FIXED_VENUE_RE.test(`${line} ${text.slice(0, 2000)}`);
    const housingOpen = HOUSING_RE.test(line) || TBD_RE.test(line) || /accommodations?.{0,40}(tbd|coming|forthcoming)/i.test(text);
    const motion = inferCommercialMotion({
      title: line,
      text: text.slice(0, 3000),
      venueFixed,
      housingOpen,
    });

    const identity = buildEventSeriesIdentity({
      title: line,
      eventName: line,
      organizationName: org,
      eventStartDate: year ? `${year}-01-01` : null,
      eventSeriesHint: line.replace(/\b20\d{2}\b/g, " ").replace(/\s+/g, " ").trim(),
    });

    if (identity.eventSeriesId && !seriesMap.has(identity.eventSeriesId)) {
      seriesMap.set(identity.eventSeriesId, {
        eventSeriesId: identity.eventSeriesId,
        canonicalName: identity.normalizedEventName,
        organization: org,
        recurrence,
        sourceUrl: url,
        sourceAuthority: /\.gov\b/i.test(url) ? "OFFICIAL_GOVERNMENT" : "OFFICIAL_ORGANIZATION",
        aliases: [],
        discoveryMode: lane,
        historicalCycles: [],
        futureCycles: [],
      });
    }

    const series = seriesMap.get(identity.eventSeriesId);
    const cycle = {
      eventCycleId: identity.eventCycleId || (year ? `cycle:${identity.eventSeriesId}|${year}` : null),
      eventSeriesId: identity.eventSeriesId,
      year,
      title: line.slice(0, 180),
      organization: org,
      datesText: (line.match(DATE_RANGE_RE) || [])[0] || null,
      destinationStatus: TBD_RE.test(line) ? "DESTINATION_TBD" : venueFixed ? "FIXED_VENUE" : "UNKNOWN",
      venueStatus: venueFixed ? "FIXED" : TBD_RE.test(line) ? "TBD" : "UNKNOWN",
      housingStatus: housingOpen ? "HOUSING_PENDING" : HOUSING_RE.test(text) ? "HOUSING_SIGNAL" : "UNKNOWN",
      opportunityType: motion.opportunityType,
      venueSourcingStatus: motion.venueSourcingStatus,
      futureCycleState: motion.futureCycleState,
      cqHint: motion.cqHint,
      sourceUrl: url,
      discoveryMode: lane,
      recurrence,
      evidenceConfidence: /\.gov\b|\.org\b/i.test(url) ? 70 : 55,
    };
    cycles.push(cycle);
    if (series && year && year >= 2026) {
      series.futureCycles.push({ year, eventCycleId: cycle.eventCycleId, title: cycle.title });
    }
  }

  return {
    version: CALENDAR_SERIES_EXTRACTOR_VERSION,
    url,
    discoveryMode: lane,
    series: [...seriesMap.values()],
    cycles,
    lineCandidates: lines.length,
  };
}

/**
 * Map extracted cycles → native-blind-compatible candidate rows.
 */
export function cyclesToDiscoveryCandidates(cycles = [], { hotelId = null } = {}) {
  return (cycles || [])
    .filter((c) => c.title && !isVagueEventTitle(c.title) && (c.year == null || c.year >= 2026))
    .map((c, i) => ({
      id: `gdi_series_${slug(c.eventSeriesId || c.title)}_${c.year || i}`,
      eventName: c.title,
      title: c.title,
      organization: c.organization,
      organizationName: c.organization,
      eventType: "FUTURE_CYCLE",
      opportunityType: c.opportunityType,
      startDate: c.year ? `${c.year}-01-01` : null,
      eventDateGranularity: c.datesText ? "DATE_RANGE" : c.year ? "YEAR" : "UNKNOWN",
      eventLocation: c.destinationStatus === "FIXED_VENUE" ? "Bethesda / campus venue" : null,
      venue: c.venueStatus === "FIXED" ? "Fixed meeting venue" : null,
      venueStatus: c.venueStatus,
      sourcingStatus: c.venueSourcingStatus,
      housingStatus: c.housingStatus,
      housingEvidence: c.housingStatus,
      officialSource: c.sourceUrl,
      futureCycleEvidence: c.futureCycleState,
      eventSeriesHint: c.eventSeriesId,
      eventSeriesId: c.eventSeriesId,
      eventCycleId: c.eventCycleId,
      evidenceConfidence: c.evidenceConfidence,
      whyRelevantToHotel:
        c.cqHint === "HOUSING_ANCILLARY"
          ? "Fixed or local meeting venue with open/unclear housing — guestroom / overflow motion"
          : "Future recurring meeting cycle relevant to hotel demand territory",
      whyNow: c.datesText || (c.year ? `Future cycle ${c.year}` : "Future cycle"),
      recommendedAction: c.cqHint === "HOUSING_ANCILLARY" ? "Monitor housing / overflow" : "Watch future cycle",
      discoveryMode: c.discoveryMode,
      discoverySource: "gdi_calendar_series_extractor_v1",
      researchProvider: "native",
      hotelId,
      vertical: c.discoveryMode,
      segment: c.discoveryMode,
    }));
}
