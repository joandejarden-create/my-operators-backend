/**
 * V4 NYC-first routing queries — seek structured participant sources.
 * No hotel brand hardcodes. No forced benchmark event names as sole targets.
 */

import { SOURCE_FAMILY } from "./v4-constants.js";

/**
 * @returns {{ queryId, query, family, marketLabel, year }[]}
 */
export function buildNycSourceAcquisitionQueries({
  year = 2027,
  max = 40,
} = {}) {
  const y = year;
  const yPrev = year - 1;
  const rows = [
    // E1 Javits exhibitor
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `Javits Center ${y} exhibitor directory OR "who's exhibiting" OR exhibitor list` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `site:javitscenter.com events ${y} OR ${yPrev}` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `"exhibitor directory" OR "exhibitor list" "New York" OR Javits ${y} -istanbul -las -vegas` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `site:a2zinc.net New York OR Javits exhibitor ${y}` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `site:expocad.com New York OR Javits exhibitor ${y}` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `site:mapyourshow.com New York exhibitor ${y}` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `NRF ${y} exhibitor list OR exhibitor directory site:nrf.com OR site:maps.nrf.com` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `"NY NOW" ${y} exhibitor directory OR exhibitor list` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `International Franchise Expo New York OR IFE Javits exhibitor directory ${y}` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `"New York" trade show ${y} sponsor directory OR sponsor list exhibitors` },
    // E2 Housing
    { family: SOURCE_FAMILY.EXHIBITOR_HOUSING, q: `Javits OR "New York" ${y} "official housing" OR "exhibitor housing" OR "hotel block"` },
    { family: SOURCE_FAMILY.EXHIBITOR_HOUSING, q: `"official housing" exhibitors "New York" OR Midtown ${y} filetype:pdf` },
    { family: SOURCE_FAMILY.EXHIBITOR_HOUSING, q: `"room block" OR "group lodging" exhibitor "New York" ${y}` },
    { family: SOURCE_FAMILY.EXHIBITOR_HOUSING, q: `onPeak OR Connections Housing "New York" OR Javits ${y}` },
    // E3 Event service
    { family: SOURCE_FAMILY.EVENT_SERVICE, q: `Javits ${y} "official contractor" OR "general service contractor" OR "exhibitor appointed contractor"` },
    { family: SOURCE_FAMILY.EVENT_SERVICE, q: `"exhibitor manual" Javits OR "New York" ${y} freight OR installation OR AV` },
    { family: SOURCE_FAMILY.EVENT_SERVICE, q: `"booth builder" OR "exhibit house" "New York" trade show ${y}` },
    // E4 Entertainment / production
    { family: SOURCE_FAMILY.ENTERTAINMENT_PRODUCTION, q: `experiential agency "New York" brand activation ${y} case study OR project` },
    { family: SOURCE_FAMILY.ENTERTAINMENT_PRODUCTION, q: `"event production" OR "staging company" Midtown OR "Times Square" ${y}` },
    { family: SOURCE_FAMILY.ENTERTAINMENT_PRODUCTION, q: `Broadway tour OR production crew lodging "New York" ${y}` },
    // E5 Tour series
    { family: SOURCE_FAMILY.TOUR_SERIES, q: `escorted tour "New York" ${y} departure schedule OR itinerary hotel` },
    { family: SOURCE_FAMILY.TOUR_SERIES, q: `student tour "New York" ${y} group itinerary OR hotel package` },
    { family: SOURCE_FAMILY.TOUR_SERIES, q: `"theater tour" OR "Broadway package" series Midtown hotel ${y}` },
    { family: SOURCE_FAMILY.TOUR_SERIES, q: `affinity travel OR alumni tour "New York" ${y} departure calendar` },
    // E6 Education
    { family: SOURCE_FAMILY.EDUCATION, q: `university "New York" MBA residency OR performing arts tour schedule ${y}` },
    { family: SOURCE_FAMILY.EDUCATION, q: `student competition travel "New York" ${y} hotel OR lodging cohort` },
    { family: SOURCE_FAMILY.EDUCATION, q: `academic delegation "New York" ${y} itinerary` },
    // E7 Delegations
    { family: SOURCE_FAMILY.DELEGATION, q: `trade mission "New York" ${y} delegation itinerary OR hotel` },
    { family: SOURCE_FAMILY.DELEGATION, q: `"buying group" OR "business delegation" "New York" ${y} visit` },
    { family: SOURCE_FAMILY.DELEGATION, q: `international delegation "New York" Midtown ${y} lodging` },
    // E8 Sports-adjacent
    { family: SOURCE_FAMILY.SPORTS_ADJACENT, q: `"New York" college tournament ${y} participant list OR officials lodging -yankees -mets` },
    { family: SOURCE_FAMILY.SPORTS_ADJACENT, q: `sports media OR production crew "New York" ${y} hotel block` },
    { family: SOURCE_FAMILY.SPORTS_ADJACENT, q: `sponsor activation "New York" sports event ${y} crew lodging` },
    // Extra structured paths
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `"exhibitor portal" OR "floorplan" exhibitors "New York" ${y}` },
    { family: SOURCE_FAMILY.EXHIBITOR_HOUSING, q: `"housing registration" trade show "New York" ${y}` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `site:informamarkets.com New York exhibitor ${y}` },
    { family: SOURCE_FAMILY.JAVITS_EXHIBITOR, q: `site:rxglobal.com OR site:reedexpo.com New York exhibitor ${y}` },
    { family: SOURCE_FAMILY.TOUR_SERIES, q: `"group travel" "New York City" ${y} hotel series schedule` },
    { family: SOURCE_FAMILY.EVENT_SERVICE, q: `"production schedule" OR "load-in" Javits ${y} crew` },
    { family: SOURCE_FAMILY.ENTERTAINMENT_PRODUCTION, q: `"Times Square" activation OR takeover ${y} agency OR production` },
  ];

  return rows.slice(0, max).map((r, i) => ({
    queryId: `hdv4_q_${i + 1}`,
    query: r.q,
    family: r.family,
    marketLabel: "New York Midtown",
    year: y,
  }));
}
