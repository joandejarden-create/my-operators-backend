/**
 * GDI Discovery Recall V4 — find more plausible demand; Hygiene V3 still qualifies.
 *
 * Does NOT change discovery-hygiene-v3, V11, V12, Surfe, PDL, or weekly UI.
 * Locale + hotel demand archetypes are configuration-driven (no city hardcodes).
 * New Opportunities V1: demand-lane packs expand beyond traditional events.
 */

import {
  ARCHETYPE_DEMAND_LANES,
  LANE_QUERY_HINTS_EN,
  LANE_TO_SIGNAL,
  DEMAND_LANE,
  buildDemandLaneSearchTasks,
  NEW_OPPORTUNITIES_V1,
} from "./demand-signal-types.js";

export const DISCOVERY_RECALL_V4 = "gdi_discovery_recall_v4";

/** Hotel demand archetypes — drive query families, not hotel-specific rules. */
export const DEMAND_ARCHETYPE = Object.freeze({
  URBAN_BUSINESS_MEETINGS: "URBAN_BUSINESS_MEETINGS",
  LUXURY_DESTINATION_SMALL: "LUXURY_DESTINATION_SMALL",
  RESORT_DESTINATION: "RESORT_DESTINATION",
  CORPORATE_SECONDARY: "CORPORATE_SECONDARY",
  MIXED: "MIXED",
});

export const LOCALE_PACKS = Object.freeze({
  en: {
    hl: "en",
    gl: "us",
    eventHints: [
      "conference",
      "congress",
      "summit",
      "forum",
      "symposium",
      "convention",
      "annual meeting",
      "trade show",
      "corporate retreat",
      "incentive travel",
      "executive offsite",
      "board meeting",
    ],
    openHints: [
      "venue TBD",
      "hotel TBD",
      "RFP hotel",
      "room block",
      "housing bureau",
      "official hotel",
      "overflow housing",
    ],
  },
  es: {
    hl: "es",
    gl: "do", // default; callers override gl by country
    eventHints: [
      "congreso",
      "convención",
      "foro",
      "cumbre",
      "simposio",
      "encuentro",
      "asamblea",
      "conferencia",
      "feria",
      "reunión anual",
      "evento corporativo",
      "calendario de eventos",
      "eventos 2026",
      "eventos 2027",
      "incentivos",
      "retiros ejecutivos",
      "junta directiva",
      "buyout hotel",
    ],
    openHints: [
      "sede por definir",
      "hotel por confirmar",
      "bloque de habitaciones",
      "alojamiento oficial",
      "programa de hospedaje",
      "convocatoria hotel",
      "hoteles oficiales",
    ],
  },
});

export const COUNTRY_LOCALE = Object.freeze({
  DO: { languages: ["es", "en"], gl: "do" },
  CO: { languages: ["es", "en"], gl: "co" },
  MX: { languages: ["es", "en"], gl: "mx" },
  US: { languages: ["en"], gl: "us" },
  BM: { languages: ["en"], gl: "bm" },
  DEFAULT: { languages: ["en", "es"], gl: "us" },
});

/**
 * Infer demand archetype from hotel capability / rooms / classification notes.
 * Generic — no hotelId branches.
 */
export function inferDemandArchetype(config = {}, contract = {}) {
  const rooms =
    Number(config?.capabilityProfile?.totalGuestrooms ?? contract.rooms ?? 0) || 0;
  const cls = String(
    config?.capabilityProfile?.classification ||
      config?.capabilityProfile?.serviceLevel ||
      ""
  ).toLowerCase();
  const notes = String(
    config?.commercialPriorities?.notes || config?.capabilityProfile?.notes || ""
  ).toLowerCase();
  const blob = `${cls} ${notes}`;

  if (
    rooms > 0 &&
    rooms <= 40 &&
    /boutique|luxury|vignette|intimate|destination|colonial|buyout/.test(blob)
  ) {
    return DEMAND_ARCHETYPE.LUXURY_DESTINATION_SMALL;
  }
  if (/resort|beach|incentive|destination/.test(blob) && rooms >= 80) {
    return DEMAND_ARCHETYPE.RESORT_DESTINATION;
  }
  if (/corporate|suburban|secondary|norte|valle|mixed/.test(blob)) {
    return DEMAND_ARCHETYPE.CORPORATE_SECONDARY;
  }
  if (/urban|business|meetings|naco|piantini|gateway|upper.?upscale/.test(blob)) {
    return DEMAND_ARCHETYPE.URBAN_BUSINESS_MEETINGS;
  }
  if (rooms > 0 && rooms <= 40) return DEMAND_ARCHETYPE.LUXURY_DESTINATION_SMALL;
  if (rooms >= 120) return DEMAND_ARCHETYPE.URBAN_BUSINESS_MEETINGS;
  return DEMAND_ARCHETYPE.MIXED;
}

const ARCHETYPE_VERTICAL_WEIGHTS = Object.freeze({
  [DEMAND_ARCHETYPE.URBAN_BUSINESS_MEETINGS]: [
    "association_conference",
    "corporate_meeting",
    "medical_scientific",
    "government_ngo",
    "university_academic",
    "housing_overflow",
    "incentive_retreat",
  ],
  [DEMAND_ARCHETYPE.LUXURY_DESTINATION_SMALL]: [
    "incentive_retreat",
    "corporate_meeting",
    "smerf_social",
    "association_conference",
    "housing_overflow",
    "government_ngo",
  ],
  [DEMAND_ARCHETYPE.RESORT_DESTINATION]: [
    "incentive_retreat",
    "smerf_social",
    "association_conference",
    "corporate_meeting",
    "housing_overflow",
  ],
  [DEMAND_ARCHETYPE.CORPORATE_SECONDARY]: [
    "corporate_meeting",
    "association_conference",
    "university_academic",
    "medical_scientific",
    "government_ngo",
    "housing_overflow",
  ],
  [DEMAND_ARCHETYPE.MIXED]: [
    "association_conference",
    "corporate_meeting",
    "incentive_retreat",
    "housing_overflow",
    "medical_scientific",
    "smerf_social",
  ],
});

/**
 * Find ≠ qualify. Emit evidenced events even when venue/sourcing/rooms UNKNOWN.
 */
export const RECALL_EXTRACT_SYSTEM_V4 = `You are Dealality GDI Discovery Recall V4 extraction.
Your job is to FIND real hotel-demand signal candidates from evidence (events AND other public demand signals).
Commercial qualification (open sourcing, room demand, overflow, WHO) happens DOWNSTREAM — do NOT suppress candidates for missing those signals.

EMIT a candidate when evidence supports ALL of:
1) a real named demand signal: event / meeting / congress / forum / training program / project team / relocation / medical program / university program / sports or academic competition / consulting engagement / corporate meeting / board meeting / nonprofit program / incentive or retreat — OR Spanish equivalents
2) a date OR named cycle/year (2026/2027) OR explicit save-the-date OR multi-week mobilization window
3) a location/destination relevant to the hotel market (or feeder)
4) at least one source URL from the evidence

ALLOW and KEEP as UNKNOWN when not evidenced:
- venue UNKNOWN
- sourcingStatus UNKNOWN
- peakRoomEstimate UNKNOWN
- attendance UNKNOWN
- WHO UNKNOWN
- room demand UNKNOWN

DO NOT:
- invent events or demand signals
- invent 2027 cycles from a 2026 recurring event without 2027 evidence (put future possibility in futureCycleEvidence only)
- require TBD/RFP/room-block language to emit a candidate
- equate attendance with rooms
- invent peak rooms
- treat one-hour local seminars / webinars as lodging demand
- emit patient-health personal information
- claim NEW_TO_HOTEL / lapsed account / past customer (hotel CRM data is out of scope)

Spanish and English evidence are equally valid.
One page may contain MULTIPLE signals — emit multiple candidates.
Prefer schema.org/Event or JSON-LD fields when present.

Return JSON: { "candidates": [ {...} ] }
Candidate fields (omit only if unsupported):
eventName, organization, demandSignalType (EVENT|TRAINING_PROGRAM|PROJECT_TEAM|CORPORATE_RELOCATION|MEDICAL_HEALTHCARE_PROGRAM|UNIVERSITY_ACADEMIC_PROGRAM|SPORTS_ACADEMIC_COMPETITION|CONSULTING_ADVISORY_TEAM|CORPORATE_MEETING|BOARD_COMMITTEE_MEETING|NONPROFIT_PROGRAM|INSTITUTIONAL_GATHERING|GOVERNMENT_CONTRACTOR_PROGRAM|PROFESSIONAL_PROJECT_CREW|INCENTIVE_RETREAT|OVERFLOW_HOUSING|OTHER),
opportunityType (PRIMARY_PURSUIT|OVERFLOW_HOUSING|FUTURE_CYCLE|REACTIVATION),
startDate (ISO YYYY-MM-DD when clear), endDate, eventDateStatus (CONFIRMED|INFERRED|UNKNOWN),
location, venue, venueStatus, sourcingStatus,
attendance, attendanceStatus (CONFIRMED|ESTIMATED|INFERRED|UNKNOWN),
peakRoomEstimate, peakRoomsStatus (CONFIRMED|ESTIMATED|INFERRED|UNKNOWN),
roomDemandEvidence, housingEvidence, hotelDemandThesis,
officialSource, supportingSources ([{url,title}]),
geographyEvidence, futureCycleEvidence, eventSeriesHint, subEventHint,
localAttendanceOnly (boolean if clearly local day-program with no lodging),
whoClues ({name,role,organization,email,phone,sourceUrl}),
whyRelevantToHotel, whyNow, evidenceConfidence (0-100), claimKinds, vertical, segment,
jsonLdDetected (boolean).`;

/**
 * Stratified query budget: round-robin across verticals so later families are not truncated.
 */
export function selectQueriesStratified(queryPlan = [], maxQueries = 28) {
  const byVertical = new Map();
  for (const item of queryPlan) {
    const v = item.vertical || item.note || "general";
    if (!byVertical.has(v)) byVertical.set(v, []);
    byVertical.get(v).push(item);
  }
  const keys = [...byVertical.keys()];
  const selected = [];
  let idx = 0;
  while (selected.length < maxQueries) {
    let added = false;
    for (const k of keys) {
      const bucket = byVertical.get(k);
      if (idx < bucket.length) {
        selected.push(bucket[idx]);
        added = true;
        if (selected.length >= maxQueries) break;
      }
    }
    if (!added) break;
    idx += 1;
  }
  return selected;
}

/**
 * Build bilingual recall tasks from geos + archetype + locale packs.
 * When opts.includeDemandLanes !== false, merge New Opportunities V1 lane packs
 * (bounded) so weekly discovery covers training/project/relocation/etc.
 */
export function buildRecallV4SearchTasks({
  geos = [],
  archetype = DEMAND_ARCHETYPE.MIXED,
  countryCode = "DEFAULT",
  yearPriority = ["2026", "2027"],
  yearAllow = ["2028"],
  includeDemandLanes = true,
  maxDemandLaneQueries = 24,
} = {}) {
  const country = COUNTRY_LOCALE[countryCode] || COUNTRY_LOCALE.DEFAULT;
  const yearNear = `(${yearPriority.join(" OR ")})`;
  const yearAll = `(${[...yearPriority, ...yearAllow].join(" OR ")})`;
  const geoList = (geos || []).filter(Boolean).slice(0, 6);
  const verticalOrder =
    ARCHETYPE_VERTICAL_WEIGHTS[archetype] || ARCHETYPE_VERTICAL_WEIGHTS[DEMAND_ARCHETYPE.MIXED];

  const tasks = [];
  const locales = country.languages.map((lang) => {
    const pack = LOCALE_PACKS[lang] || LOCALE_PACKS.en;
    return { lang, ...pack, gl: country.gl };
  });

  for (const vertical of verticalOrder) {
    const queries = [];
    for (const loc of locales) {
      const hints =
        vertical === "housing_overflow" || vertical === "open_sourcing"
          ? loc.openHints
          : loc.eventHints;
      const hintSlice =
        archetype === DEMAND_ARCHETYPE.LUXURY_DESTINATION_SMALL
          ? hints.filter((h) =>
              /retreat|incentiv|board|buyout|ejecutiv|corporat|encuentro|junta|social|wedding|reunión|hotel/i.test(
                h
              )
            ).slice(0, 4)
          : hints.slice(0, 4);
      const useHints = hintSlice.length ? hintSlice : hints.slice(0, 3);
      for (const geo of geoList.slice(0, 3)) {
        for (const hint of useHints.slice(0, 2)) {
          queries.push({
            q: `"${geo}" ${hint} ${yearNear}`,
            lang: loc.lang,
            hl: loc.hl,
            gl: loc.gl,
          });
        }
      }
      // CVB / calendar / association stage hints
      if (geoList[0]) {
        queries.push({
          q: `"${geoList[0]}" (calendario de eventos OR event calendar OR convención OR convention bureau) ${yearNear}`,
          lang: loc.lang,
          hl: loc.hl,
          gl: loc.gl,
        });
      }
    }
    // Dedup by query string
    const seen = new Set();
    const unique = [];
    for (const row of queries) {
      if (seen.has(row.q)) continue;
      seen.add(row.q);
      unique.push(row);
    }
    tasks.push({
      vertical,
      note: vertical,
      queries: unique.slice(0, 8).map((r) => r.q),
      queryMeta: unique.slice(0, 8),
    });
  }

  // Explicit open-sourcing family (always present)
  const openQs = [];
  for (const loc of locales) {
    for (const hint of loc.openHints.slice(0, 3)) {
      if (geoList[0]) {
        openQs.push(`"${geoList[0]}" ${hint} ${yearAll}`);
      }
    }
  }
  tasks.push({
    vertical: "housing_overflow",
    note: "open_sourcing",
    queries: [...new Set(openQs)].slice(0, 6),
  });

  // New Opportunities V1 — expanded demand lanes (bounded budget)
  let demandLanePlan = null;
  if (includeDemandLanes) {
    demandLanePlan = buildDemandLaneSearchTasks({
      geos: geoList,
      archetype,
      yearPriority,
      maxLanes: 14,
      maxQueriesPerLane: 3,
    });
    let laneQueryCount = 0;
    for (const laneTask of demandLanePlan.tasks) {
      // Skip EVENT_ASSOCIATION / OVERFLOW — already covered by vertical packs
      if (
        laneTask.lane === DEMAND_LANE.EVENT_ASSOCIATION ||
        laneTask.lane === DEMAND_LANE.OVERFLOW
      ) {
        continue;
      }
      const remaining = maxDemandLaneQueries - laneQueryCount;
      if (remaining <= 0) break;
      const qs = laneTask.queries.slice(0, remaining);
      if (!qs.length) continue;
      tasks.push({
        vertical: laneTask.lane,
        note: `demand_lane:${laneTask.lane}`,
        demandSignalType: laneTask.demandSignalType,
        lane: laneTask.lane,
        queries: qs,
      });
      laneQueryCount += qs.length;
    }
  }

  return {
    version: DISCOVERY_RECALL_V4,
    newOpportunitiesVersion: includeDemandLanes ? NEW_OPPORTUNITIES_V1 : null,
    archetype,
    countryCode,
    locales: locales.map((l) => ({ lang: l.lang, hl: l.hl, gl: l.gl })),
    tasks,
    demandLanePlan,
  };
}

export function renderRecallV4Brief({
  hotelName,
  hotelId,
  market,
  rooms,
  archetype,
  peakRoomsMin,
  peakRoomsMax,
} = {}) {
  return [
    "DEALALITY GDI DISCOVERY RECALL V4",
    `Hotel: ${hotelName || hotelId} (${hotelId})`,
    `Market: ${market || "n/a"}`,
    `Rooms: ${rooms ?? "n/a"} | Peak band: ${peakRoomsMin ?? "?"}-${peakRoomsMax ?? "?"}`,
    `Demand archetype: ${archetype}`,
    "FIND real evidenced events/group demand. Do NOT suppress for missing open-sourcing or rooms.",
    "UNKNOWN venue/sourcing/rooms/WHO is allowed. Hygiene V3 will qualify commercially.",
    "Spanish and English evidence are equal. Empty is only for no real evidenced event.",
    "Do not invent future cycles without evidence.",
  ].join("\n");
}

/**
 * Resolve SERP locale from country (config-driven).
 */
export function resolveSerpLocale(countryCode = "DEFAULT", language = "en") {
  const country = COUNTRY_LOCALE[countryCode] || COUNTRY_LOCALE.DEFAULT;
  const pack = LOCALE_PACKS[language] || LOCALE_PACKS.en;
  return { hl: pack.hl, gl: country.gl };
}

/**
 * Classify query-family yield for telemetry reports.
 */
export function classifyQueryYield({ urls = 0, usablePages = 0, candidates = 0, trueOrWatch = 0 } = {}) {
  if (candidates >= 3 || trueOrWatch >= 1) return "HIGH_YIELD";
  if (candidates >= 1) return "MEDIUM_YIELD";
  if (usablePages >= 2 || urls >= 4) return "LOW_YIELD";
  return "ZERO_YIELD";
}
