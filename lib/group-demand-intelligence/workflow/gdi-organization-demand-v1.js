/**
 * Organization demand is separate from an event listing.
 * An event disposition is preserved. It does not create a hotel opportunity.
 */

import { canonicalEntityKey } from "./gdi-global-weekly-feedback-v2.js";
import { planBuyerQueries } from "./gdi-buyer-discovery-v1.js";
import { hostOf } from "./gdi-organization-investigation-v1.js";

export const DEMAND_CLAIM = Object.freeze({
  VERIFIED_HISTORICAL_DEMAND: "VERIFIED_HISTORICAL_DEMAND",
  VERIFIED_RECURRING_PROGRAM: "VERIFIED_RECURRING_PROGRAM",
  CREDIBLE_BUT_UNCONFIRMED_FUTURE_DEMAND: "CREDIBLE_BUT_UNCONFIRMED_FUTURE_DEMAND",
  VERIFIED_FUTURE_DEMAND: "VERIFIED_FUTURE_DEMAND",
  EXISTING_COMMITTED_BOOKING: "EXISTING_COMMITTED_BOOKING",
  COMMERCIALLY_ACTIONABLE_PROSPECT: "COMMERCIALLY_ACTIONABLE_PROSPECT",
  RESEARCH_MONITORING_CANDIDATE: "RESEARCH_MONITORING_CANDIDATE",
});

export const PORTFOLIO = Object.freeze({
  ACTIONABLE: "actionable_opportunity_portfolio",
  PIPELINE: "future_account_development_pipeline",
  MONITORING: "demand_monitoring_inventory",
  REJECTED: "rejected_or_unsupported",
});

const WEEKLY_TRIGGERS = new Set([
  "new_event_announcement",
  "new_edition",
  "venue_decision",
  "organizer_contact_change",
  "hotel_sales_feedback",
  "timing_window",
]);

export function organizationKey(event = {}) {
  const host = hostOf(event.seedUrl || "");
  if (!host || /cvent\.com|eventbrite\./i.test(host)) return canonicalEntityKey(event.organizationName);
  const parts = host.split(".");
  return parts.slice(-2).join(".");
}

export function shouldCreateOpportunity(record = {}) {
  return record.portfolio === PORTFOLIO.ACTIONABLE
    && record.verifiedBuyerRoute === true
    && record.hotelFacing === true;
}

export function organizationWeeklyDecision({ fingerprint, completedFingerprints = [], signals = [] } = {}) {
  const trigger = signals.find((signal) => WEEKLY_TRIGGERS.has(signal));
  if (trigger) return { revisit: true, reason: trigger };
  if (fingerprint && completedFingerprints.includes(fingerprint)) {
    return { revisit: false, reason: "same_historical_event" };
  }
  return { revisit: fingerprint ? false : true, reason: fingerprint ? "no_new_signal" : "unseen_organization" };
}

export function planHotelBuyerResearch({ language = "en", territoryLabels = [], market = "" } = {}) {
  const labels = territoryLabels.length ? territoryLabels : [""];
  const rows = [];
  for (const label of labels) {
    rows.push(...planBuyerQueries({ language, territoryLabel: label }));
    if (market === "mexico_city") {
      rows.push({
        segment: "meeting_industry",
        query: `asociación de la industria de reuniones ${label}`.replace(/\s+/g, " ").trim(),
        language,
        legislatureRisk: false,
      });
    }
    if (market === "northern_virginia") {
      rows.push({
        segment: "meeting_industry",
        query: `${label} meeting planner association`.replace(/\s+/g, " ").trim(),
        language: "en",
        legislatureRisk: false,
      });
    }
  }
  return rows;
}

function recurring(event) {
  return /annual|anual|session|spring conference/i.test(`${event.organizationName || ""} ${event.demandVerified || ""}`)
    || /RECURRING/.test(event.demandVerified || "");
}

function inMarket(event, territoryTerms) {
  const blob = `${event.organizationName || ""} ${event.venue || ""}`.toLowerCase();
  return territoryTerms.some((term) => blob.includes(String(term).toLowerCase()));
}

export function classifyOrganization(events = [], territoryTerms = []) {
  const keptDispositions = events.map((event) => event.disposition);
  const committed = events.some((event) => event.disposition === "NOT_OPEN_BOOKING");
  const outside = events.every((event) => event.disposition === "OUT_OF_MARKET");
  const repeats = events.some(recurring);
  const local = events.some((event) => inMarket(event, territoryTerms) || event.venueRelevance === "FUTURE_LOCAL_RELEVANT" || event.venueRelevance === "NOT_OPEN_BOOKING");
  const verifiedBuyerRoute = events.some((event) => event.contactRoute === "verified_organizer_route");
  const functionalRoute = events.some((event) => event.contactRoute === "relevant_organizational_route");
  const listingOnly = events.every((event) => event.contactRoute === "event_listing_contact");
  const claims = [];
  if (repeats) claims.push(DEMAND_CLAIM.VERIFIED_RECURRING_PROGRAM);
  else claims.push(DEMAND_CLAIM.VERIFIED_HISTORICAL_DEMAND);
  if (committed) claims.push(DEMAND_CLAIM.EXISTING_COMMITTED_BOOKING);
  if (repeats && !outside) claims.push(DEMAND_CLAIM.CREDIBLE_BUT_UNCONFIRMED_FUTURE_DEMAND);
  let portfolio = PORTFOLIO.MONITORING;
  let additionalDemand = "No demand mechanism beyond the identified event is supported.";
  if (outside) {
    portfolio = PORTFOLIO.REJECTED;
    additionalDemand = "The documented program is outside the hotel market. A later edition in this market is not supported.";
  } else if (!local && !committed) {
    portfolio = PORTFOLIO.REJECTED;
    additionalDemand = "The program is real and its destination is not in this hotel market.";
  } else if (verifiedBuyerRoute && events.some((event) => event.disposition === "QUALIFIED_ACTIONABLE")) {
    portfolio = PORTFOLIO.ACTIONABLE;
    claims.push(DEMAND_CLAIM.COMMERCIALLY_ACTIONABLE_PROSPECT);
    additionalDemand = "A verified organizer route and an open local program are both on the official record.";
  } else if (repeats && local && !committed) {
    portfolio = PORTFOLIO.MONITORING;
    additionalDemand = "A recurring program is named. The next venue and lodging need are not published.";
  } else if (committed && local) {
    portfolio = PORTFOLIO.MONITORING;
    additionalDemand = "This edition names a venue. No later open cycle or lodging bid is published.";
  }
  if (!outside && listingOnly) {
    additionalDemand = "The only source is an event listing. The organizer's own program page was not in the authorized set.";
  }
  const lodgingEvidence = events.some((event) => /room block|lodging|housing/i.test(`${event.venue || ""} ${event.dateEvidence || ""}`));
  return {
    organizationKey: organizationKey(events[0]),
    organizationName: events.length === 1 ? events[0].organizationName : events.map((event) => event.organizationName).join(" | "),
    events: events.map((event) => ({
      name: event.organizationName,
      seedUrl: event.seedUrl,
      disposition: event.disposition,
    })),
    eventDispositionsPreserved: keptDispositions,
    claims,
    portfolio,
    additionalDemand,
    verifiedBuyerRoute,
    functionalOrganizingRoute: functionalRoute && !verifiedBuyerRoute,
    hotelFacing: false,
    opportunityCreated: false,
    lodgingEvidence,
  };
}

export function reviewOrganizations({ events = [], territoryTerms = [], market = "", language = "en", territoryLabels = [] } = {}) {
  const groups = new Map();
  for (const event of events) {
    const key = organizationKey(event);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(event);
  }
  const organizations = [...groups.values()].map((group) => {
    const record = classifyOrganization(group, territoryTerms);
    record.opportunityCreated = shouldCreateOpportunity(record);
    record.weekly = organizationWeeklyDecision({
      fingerprint: `org:${record.organizationKey}:${group.map((event) => event.disposition).join("|")}`,
      completedFingerprints: [`org:${record.organizationKey}:${group.map((event) => event.disposition).join("|")}`],
      signals: [],
    });
    return record;
  });
  return {
    organizations,
    opportunitiesCreated: organizations.filter((record) => record.opportunityCreated).length,
    plan: planHotelBuyerResearch({ language, territoryLabels, market }),
    serpApiCredits: 0,
  };
}
