/**
 * Shared market hidden-demand query plan — territory-driven, not hotel-hardcoded.
 */

import { DEMAND_FAMILY } from "./constants.js";

function clean(s) {
  return String(s || "").trim();
}

/**
 * @param {{ marketLabel: string, year?: number, maxQueries?: number }} opts
 */
export function buildHiddenDemandMarketQueries(opts = {}) {
  const market = clean(opts.marketLabel) || "New York Midtown";
  const year = opts.year || new Date().getUTCFullYear() + 1;
  const max = Number(opts.maxQueries) > 0 ? Number(opts.maxQueries) : 50;

  const lanes = [
    {
      family: DEMAND_FAMILY.EXHIBITOR_VENDOR,
      q: `${market} trade show exhibitor list OR sponsor list ${year} hotel staff`,
    },
    {
      family: DEMAND_FAMILY.EXHIBITOR_VENDOR,
      q: `Javits ${year} exhibitor directory OR participating companies lodging`,
    },
    {
      family: DEMAND_FAMILY.PRODUCTION_CREW,
      q: `${market} event production company crew lodging OR show management hotel block ${year}`,
    },
    {
      family: DEMAND_FAMILY.PRODUCTION_CREW,
      q: `${market} AV staging crew hotel OR production travel ${year}`,
    },
    {
      family: DEMAND_FAMILY.PRODUCTION_CREW,
      q: `Broadway ${year} touring company hotel OR cast crew lodging Times Square`,
    },
    {
      family: DEMAND_FAMILY.CORPORATE_PROJECT,
      q: `${market} consulting implementation team hotel OR temporary assignment ${year}`,
    },
    {
      family: DEMAND_FAMILY.CORPORATE_PROJECT,
      q: `"office opening" OR "product launch" team New York hotel block ${year}`,
    },
    {
      family: DEMAND_FAMILY.TRAINING,
      q: `${market} corporate training academy cohort hotel ${year}`,
    },
    {
      family: DEMAND_FAMILY.TRAINING,
      q: `leadership program New York residential cohort lodging ${year}`,
    },
    {
      family: DEMAND_FAMILY.TOUR_SERIES,
      q: `${market} escorted tour operator hotel series schedule ${year}`,
    },
    {
      family: DEMAND_FAMILY.TOUR_SERIES,
      q: `student tour New York group hotel block operator ${year}`,
    },
    {
      family: DEMAND_FAMILY.DELEGATION,
      q: `${market} trade mission delegation hotel OR chamber buying mission ${year}`,
    },
    {
      family: DEMAND_FAMILY.EDUCATION,
      q: `university MBA cohort New York hotel OR alumni group lodging ${year}`,
    },
    {
      family: DEMAND_FAMILY.EDUCATION,
      q: `performing arts school New York competition hotel block ${year}`,
    },
    {
      family: DEMAND_FAMILY.SPORTS_ADJACENT,
      q: `${market} college team hotel block OR tournament officials lodging ${year}`,
    },
    {
      family: DEMAND_FAMILY.SPORTS_ADJACENT,
      q: `youth tournament New York stay-to-play hotel operator ${year}`,
    },
    {
      family: DEMAND_FAMILY.AGENCY,
      q: `${market} experiential agency brand activation hotel team ${year}`,
    },
    {
      family: DEMAND_FAMILY.FASHION,
      q: `Fashion Week New York showroom team OR buyer hotel block ${year}`,
    },
    {
      family: DEMAND_FAMILY.FASHION,
      q: `NY market week brand team lodging OR production crew hotel ${year}`,
    },
    {
      family: DEMAND_FAMILY.MEDICAL_PHARMA,
      q: `${market} pharma advisory board hotel OR investigator meeting lodging ${year}`,
    },
    {
      family: DEMAND_FAMILY.MEDICAL_PHARMA,
      q: `medical education cohort New York hotel block ${year}`,
    },
    {
      family: DEMAND_FAMILY.SOCIAL,
      q: `${market} wedding venue preferred hotel block OR reunion group lodging ${year}`,
    },
    {
      family: DEMAND_FAMILY.ASSOCIATION_SUBGROUP,
      q: `${market} association board meeting OR committee meeting hotel ${year}`,
    },
    {
      family: DEMAND_FAMILY.ASSOCIATION_SUBGROUP,
      q: `leadership summit New York hotel retreat association ${year}`,
    },
    {
      family: DEMAND_FAMILY.EXHIBITOR_VENDOR,
      q: `NRF OR NY NOW OR Comic Con exhibitor hotel staff travel ${year}`,
    },
    {
      family: DEMAND_FAMILY.CORPORATE_PROJECT,
      q: `site:com "hotel block" "New York" "project team" OR "field team" ${year}`,
    },
    {
      family: DEMAND_FAMILY.DELEGATION,
      q: `international trade delegation New York itinerary hotel ${year}`,
    },
    {
      family: DEMAND_FAMILY.TOUR_SERIES,
      q: `Broadway package tour series hotel Midtown operator schedule ${year}`,
    },
    {
      family: DEMAND_FAMILY.PRODUCTION_CREW,
      q: `film OR TV production New York crew lodging hotel Midtown ${year}`,
    },
    {
      family: DEMAND_FAMILY.AGENCY,
      q: `PR firm New York client event hotel room block ${year}`,
    },
  ];

  return lanes.slice(0, max).map((row, i) => ({
    queryId: `hdq_${i + 1}`,
    query: row.q,
    family: row.family,
    marketLabel: market,
    year,
  }));
}
