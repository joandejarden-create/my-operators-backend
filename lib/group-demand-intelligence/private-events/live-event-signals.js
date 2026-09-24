/**
 * Live forward private-event signal discovery for strong venues.
 * Privacy: commercial contact path only — no couple profiles.
 * Portable: venue + hotelCtx inputs; no city hardcodes.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { VENUE_PRIORITY, ROOM_DEMAND_CLAIM, ATTENDANCE_STATUS } from "./constants.js";
import { DEMAND_SIGNAL_TYPE } from "../demand-signal-types.js";
import { isPrivatePersonSourceRejected, modelRoomDemand } from "./lodging-capture.js";
import { computeSignalId } from "./airtable-signal-store.js";

export const LIVE_EVENT_SIGNALS_V1_2 = "gdi_private_events_live_event_signals_v1_2";

const EXTRACT_MODEL =
  process.env.GDI_PE_EXTRACT_MODEL ||
  process.env.GDI_NATIVE_EXTRACT_MODEL ||
  process.env.OPENAI_MODEL ||
  "gpt-4o-mini";

function clean(s) {
  return String(s || "").trim();
}

function hasSerpKey() {
  return Boolean(
    String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim()
  );
}

function hasOpenAiKey() {
  return Boolean(String(process.env.OPENAI_API_KEY || "").trim());
}

async function serpOrganic(query, { num = 6 } = {}) {
  const result = await serpapiSearch({
    engine: "google",
    q: query,
    num,
    hl: "en",
    gl: "us",
  });
  if (!result.ok) {
    return { ok: false, error: result.error, organic: [], charged: 0 };
  }
  return {
    ok: true,
    charged: result.charged ?? 1,
    organic: (result.data?.organic_results || []).map((r) => ({
      title: r.title || null,
      url: r.link || r.url || null,
      snippet: r.snippet || null,
    })),
  };
}

async function callOpenAiJson(system, user, { maxTokens = 2200 } = {}) {
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: EXTRACT_MODEL,
      temperature: 0.1,
      max_tokens: maxTokens,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
    }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(json.error?.message || `openai_http_${res.status}`);
  }
  const content = json.choices?.[0]?.message?.content;
  if (!content) throw new Error("openai_empty_content");
  return JSON.parse(content);
}

const SYSTEM_EVENTS = `Extract FUTURE public private-event / wedding / social event signals for a venue.
Return JSON: { "signals": [ {
  eventName, eventType, eventDate, eventEndDate, eventDateGranularity,
  estimatedAttendance, attendanceStatus, travelLodgingEvidence,
  roomBlockMentioned, destinationLanguage, nonlocalGuestsLikely,
  commercialContactPath, plannerCompany, sourceUrl, isFuture, confidence
} ] }
Rules:
- FUTURE events only (reject clearly past events).
- eventDateGranularity: EXACT_DATE | DATE_RANGE | MONTH | YEAR | UNKNOWN
- Do NOT invent dates. Do not coerce year-only to Jan 1.
- Do NOT collect couple personal emails/phones/addresses/religion/income.
- Prefer venue calendars, official event pages, planner portfolios (commercial).
- eventType: WEDDING | PRIVATE_EVENT | FAMILY_REUNION | RELIGIOUS_CELEBRATION | SOCIAL_EVENT | DESTINATION_WEDDING | OTHER_PRIVATE_EVENT
- If no future public signals, return { "signals": [] }.`;

/**
 * Build second-pass / forward-event queries for one venue (portable).
 */
export function buildForwardEventQueries(venue = {}, { year = null } = {}) {
  const name = clean(venue.venueName);
  const city = clean(venue.city);
  const y = year || new Date().getFullYear() + 1;
  if (!name) return [];
  const geo = city ? ` ${city}` : "";
  return [
    `"${name}"${geo} wedding ${y}`,
    `"${name}"${geo} private event calendar ${y}`,
    `"${name}" accommodations hotel lodging recommended`,
    `"${name}" preferred hotel OR room block`,
  ];
}

/**
 * Discover forward event signals for HIGH_POTENTIAL / EVENT_SIGNAL_TARGET venues.
 */
export async function discoverForwardEventSignals({
  venues = [],
  hotelCtx = null,
  maxVenues = 12,
  maxQueriesPerVenue = 3,
  asOf = new Date(),
} = {}) {
  if (!hasSerpKey() || !hasOpenAiKey()) {
    const err = new Error("SERPAPI_and_OPENAI_required_for_pe_live_event_signals");
    err.code = "keys_required";
    throw err;
  }

  const eligible = venues.filter((v) => {
    const p = v.priority || v.venuePriority;
    return (
      p === VENUE_PRIORITY.HIGH_POTENTIAL_PARTNER ||
      p === VENUE_PRIORITY.EVENT_SIGNAL_TARGET
    );
  });

  const ledger = {
    version: LIVE_EVENT_SIGNALS_V1_2,
    eventSearches: 0,
    serpCharged: 0,
    pagesFetched: 0,
    openaiCalls: 0,
    errors: [],
  };

  const signals = [];
  const year = asOf.getFullYear();
  const nextYear = year + 1;

  for (const venue of eligible.slice(0, maxVenues)) {
    const queries = buildForwardEventQueries(venue, { year: nextYear }).slice(
      0,
      maxQueriesPerVenue
    );
    const organic = [];
    for (const q of queries) {
      try {
        const serp = await serpOrganic(q);
        ledger.eventSearches += 1;
        ledger.serpCharged += serp.charged || 0;
        if (serp.ok) organic.push(...(serp.organic || []));
      } catch (err) {
        ledger.errors.push({
          venue: venue.venueName,
          q,
          error: err?.message || String(err),
        });
      }
    }

    // Fetch up to 2 non-directory pages
    const pages = [];
    for (const row of organic.slice(0, 4)) {
      if (!row.url || !/^https?:\/\//i.test(row.url)) continue;
      if (pages.length >= 2) break;
      try {
        const page = await fetchResearchPage(row.url, { timeoutMs: 18000 });
        if (!page.ok) continue;
        ledger.pagesFetched += 1;
        pages.push({
          url: page.url || row.url,
          title: row.title,
          text: htmlToSearchableText(page.text).replace(/\s+/g, " ").trim().slice(0, 5000),
        });
      } catch {
        /* ignore */
      }
    }

    let extracted = [];
    try {
      const parsed = await callOpenAiJson(
        SYSTEM_EVENTS,
        JSON.stringify({
          venue: {
            venueName: venue.venueName,
            city: venue.city,
            website: venue.website,
          },
          organic: organic.slice(0, 10),
          pages,
          asOf: asOf.toISOString().slice(0, 10),
        })
      );
      ledger.openaiCalls += 1;
      extracted = Array.isArray(parsed?.signals) ? parsed.signals : [];
    } catch (err) {
      ledger.errors.push({
        venue: venue.venueName,
        stage: "extract",
        error: err?.message || String(err),
      });
      continue;
    }

    for (const raw of extracted) {
      if (raw.isFuture === false) continue;
      const signal = {
        signalId: null,
        venueId: venue.venueId,
        venueName: venue.venueName,
        eventName: clean(raw.eventName) || null,
        eventType:
          clean(raw.eventType).toUpperCase() || DEMAND_SIGNAL_TYPE.PRIVATE_EVENT,
        demandSignalType:
          clean(raw.eventType).toUpperCase() || DEMAND_SIGNAL_TYPE.PRIVATE_EVENT,
        eventDate: clean(raw.eventDate) || null,
        eventStartDate: clean(raw.eventDate) || null,
        eventEndDate: clean(raw.eventEndDate) || null,
        eventDateGranularity: clean(raw.eventDateGranularity) || "UNKNOWN",
        estimatedAttendance:
          raw.estimatedAttendance != null ? Number(raw.estimatedAttendance) : null,
        attendanceStatus:
          clean(raw.attendanceStatus).toUpperCase() || ATTENDANCE_STATUS.UNKNOWN,
        travelLodgingEvidence: clean(raw.travelLodgingEvidence) || null,
        roomBlockMentioned: Boolean(raw.roomBlockMentioned),
        destinationLanguage: Boolean(raw.destinationLanguage),
        nonlocalGuestsLikely: Boolean(raw.nonlocalGuestsLikely),
        commercialContactPath: clean(raw.commercialContactPath) || null,
        plannerCompany: clean(raw.plannerCompany) || null,
        sourceUrl: clean(raw.sourceUrl) || pages[0]?.url || organic[0]?.url || null,
        sourceUrls: [
          ...new Set(
            [clean(raw.sourceUrl), ...pages.map((p) => p.url)].filter(Boolean)
          ),
        ],
      };
      signal.signalId = computeSignalId(signal);

      if (isPrivatePersonSourceRejected(signal)) {
        signal._reject = "PRIVATE_PERSON_SOURCE";
        signals.push(signal);
        continue;
      }

      const modeled = modelRoomDemand({
        signal,
        venue,
        hotelCtx,
      });
      signal.potentialRoomsLow = modeled?.potentialRoomsLow ?? null;
      signal.potentialRoomsHigh = modeled?.potentialRoomsHigh ?? null;
      signal.roomDemandStatus =
        modeled?.roomDemandStatus || ROOM_DEMAND_CLAIM.UNKNOWN;
      signal._modeled = modeled;
      signals.push(signal);
    }
  }

  return { version: LIVE_EVENT_SIGNALS_V1_2, signals, ledger };
}
