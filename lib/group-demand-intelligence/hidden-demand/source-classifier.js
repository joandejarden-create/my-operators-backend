/**
 * Classify URLs / titles into structured source types.
 * Generic SERP is a routing layer, not primary entity source.
 */

import { SOURCE_TYPE } from "./v2-constants.js";

/**
 * @param {{ url?: string, title?: string, snippet?: string }} hit
 * @returns {string} SOURCE_TYPE
 */
export function classifyStructuredSource(hit = {}) {
  const url = String(hit.url || hit.link || "").toLowerCase();
  const title = String(hit.title || "").toLowerCase();
  const snippet = String(hit.snippet || "").toLowerCase();
  const blob = `${url} ${title} ${snippet}`;

  const isPdf = /\.pdf($|\?)/i.test(url) || /\bpdf\b/.test(title);

  if (isPdf) {
    if (/hous(ing|e)|hotel.?block|room.?block|accommodation|lodging/.test(blob)) {
      return SOURCE_TYPE.HOUSING_PDF;
    }
    if (/registrat|attendee.?guide|delegate.?guide/.test(blob)) {
      return SOURCE_TYPE.REGISTRATION_PDF;
    }
    if (/program|agenda|schedule|prospectus|exhibitor.?kit|sponsor.?kit/.test(blob)) {
      return SOURCE_TYPE.PROGRAM_PDF;
    }
    return SOURCE_TYPE.PROGRAM_PDF;
  }

  if (/exhibitor.?list|exhibitor.?director|exhibitor.?directory|booth.?list|who.?s.?exhibiting|exhibitors?\//.test(blob)) {
    return SOURCE_TYPE.EXHIBITOR_DIRECTORY;
  }
  if (/sponsor.?list|sponsor.?director|partners?\/|sponsorship/.test(blob) && /list|directory|partners/.test(blob)) {
    return SOURCE_TYPE.SPONSOR_DIRECTORY;
  }
  if (/participant.?list|attendee.?list|delegat/.test(blob)) {
    return SOURCE_TYPE.PARTICIPANT_LIST;
  }
  if (/staff.?director|committee.?roster|board.?roster|leadership.?team/.test(blob)) {
    return SOURCE_TYPE.STAFF_DIRECTORY;
  }
  if (/tour.?schedule|departure.?calendar|itinerary.?series|escorted.?tour/.test(blob)) {
    return SOURCE_TYPE.TOUR_SCHEDULE;
  }
  if (/team.?schedule|tournament.?bracket|roster|fixture.?list/.test(blob)) {
    return SOURCE_TYPE.TEAM_SCHEDULE;
  }
  if (/press.?release|newsroom|\/news\/|prnewswire|businesswire/.test(blob)) {
    return SOURCE_TYPE.NEWS_RELEASE;
  }
  if (/program|agenda|sessions|speakers/.test(blob) && !/exhibitor/.test(blob)) {
    return SOURCE_TYPE.PROGRAM_PAGE;
  }

  return SOURCE_TYPE.GENERIC_SERP;
}

/**
 * Score source for fetch priority (higher = fetch sooner).
 */
export function structuredSourceScore(sourceType) {
  const order = {
    [SOURCE_TYPE.EXHIBITOR_DIRECTORY]: 100,
    [SOURCE_TYPE.SPONSOR_DIRECTORY]: 95,
    [SOURCE_TYPE.PROGRAM_PDF]: 90,
    [SOURCE_TYPE.HOUSING_PDF]: 88,
    [SOURCE_TYPE.REGISTRATION_PDF]: 85,
    [SOURCE_TYPE.PARTICIPANT_LIST]: 80,
    [SOURCE_TYPE.STAFF_DIRECTORY]: 75,
    [SOURCE_TYPE.TOUR_SCHEDULE]: 70,
    [SOURCE_TYPE.TEAM_SCHEDULE]: 65,
    [SOURCE_TYPE.PROGRAM_PAGE]: 55,
    [SOURCE_TYPE.NEWS_RELEASE]: 45,
    [SOURCE_TYPE.PROFESSIONAL_PROFILE]: 30,
    [SOURCE_TYPE.GENERIC_SERP]: 10,
  };
  return order[sourceType] || 0;
}

/**
 * Routing queries that seek structured participant sources — not "events in NYC".
 */
export function buildStructuredRoutingQueries({ marketLabel = "New York", year = 2027, max = 25 } = {}) {
  const m = marketLabel;
  const y = year;
  const rows = [
    { family: "EXHIBITOR_VENDOR", q: `NRF ${y} exhibitor directory OR exhibitor list site:nrf.com OR site:maps.nrf.com` },
    { family: "EXHIBITOR_VENDOR", q: `NY NOW ${y} exhibitor list OR exhibitor directory` },
    { family: "EXHIBITOR_VENDOR", q: `Javits ${y} exhibitor directory OR "who's exhibiting" filetype:pdf` },
    { family: "EXHIBITOR_VENDOR", q: `"exhibitor list" OR "exhibitor directory" ${m} ${y} filetype:pdf` },
    { family: "EXHIBITOR_VENDOR", q: `${m} trade show sponsor list OR sponsor directory ${y}` },
    { family: "ASSOCIATION_SUBGROUP", q: `${m} association annual meeting program PDF committee board ${y}` },
    { family: "ASSOCIATION_SUBGROUP", q: `"board meeting" OR "committee meeting" program ${m} ${y} filetype:pdf` },
    { family: "EXHIBITOR_VENDOR", q: `${m} ${y} housing guide OR hotel block OR "official housing" filetype:pdf` },
    { family: "EXHIBITOR_VENDOR", q: `"exhibitor prospectus" OR "sponsor prospectus" ${m} ${y} filetype:pdf` },
    { family: "TOUR_SERIES", q: `escorted tour ${m} departure schedule OR itinerary series hotel ${y}` },
    { family: "TOUR_SERIES", q: `student tour operator ${m} group itinerary hotel ${y}` },
    { family: "EDUCATION", q: `university ${m} MBA residency OR performing arts tour schedule ${y}` },
    { family: "SPORTS_ADJACENT", q: `${m} college tournament participant list OR officials lodging ${y}` },
    { family: "AGENCY", q: `experiential agency brand activation ${m} case study ${y}` },
    { family: "PRODUCTION_CREW", q: `${m} event production AV staging crew assignment ${y}` },
    { family: "MEDICAL_PHARMA", q: `${m} advisory board OR investigator meeting program ${y} filetype:pdf` },
    { family: "DELEGATION", q: `trade mission ${m} delegation itinerary hotel ${y}` },
    { family: "FASHION", q: `NY market week exhibitor OR showroom directory ${y}` },
    { family: "CORPORATE_PROJECT", q: `${m} customer conference OR sales kickoff participant list ${y}` },
    { family: "TRAINING", q: `${m} corporate academy cohort schedule hotel ${y}` },
    { family: "EXHIBITOR_VENDOR", q: `Comic Con New York exhibitor list ${y}` },
    { family: "EXHIBITOR_VENDOR", q: `International Franchise Expo OR IFE exhibitor directory ${y}` },
    { family: "ASSOCIATION_SUBGROUP", q: `Independent Lodging Congress advisory board program ${y}` },
    { family: "TOUR_SERIES", q: `Broadway tour package series Midtown hotel operator calendar ${y}` },
    { family: "EXHIBITOR_VENDOR", q: `site:expocad.com OR site:a2zinc.net ${m} exhibitor ${y}` },
  ];
  return rows.slice(0, max).map((r, i) => ({
    queryId: `hdv2_q_${i + 1}`,
    query: r.q,
    family: r.family,
    marketLabel: m,
    year: y,
  }));
}
