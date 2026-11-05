/**
 * Demand-mechanism discovery for the existing GDI supervisor.
 * An announced event is not the default buyer. Recurring demand is not an open booking.
 */

import { canonicalEntityKey } from "./gdi-global-weekly-feedback-v2.js";
import { queryInvitesLegislature } from "./gdi-buyer-discovery-v1.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";

export const BUYING_MECHANISM = Object.freeze({
  ACTIVE_SOURCING: "ACTIVE_SOURCING",
  FUTURE_ACCOUNT_DEVELOPMENT: "FUTURE_ACCOUNT_DEVELOPMENT",
  MONITORING: "MONITORING",
  NOT_ACTIONABLE: "NOT_ACTIONABLE",
});

export const PROPOSED_ACCOUNT_DEVELOPMENT_WORDING = "Account development lead. This organization has a documented group program in the hotel market. No open room block, RFP, or venue availability is claimed. It is not shown to the customer until a verified organizer route and the existing publication gate both pass.";

const WORKSTREAM_PATHWAYS = Object.freeze({
  associations_government: ["recurring_group_buyers", "rotational_meetings", "active_venue_lodging_sourcing"],
  sports: ["group_accommodation_travel", "recurring_group_buyers"],
  corporate_meetings_training: ["corporate_institutional_programs", "recurring_group_buyers"],
  entertainment_production: ["group_accommodation_travel", "emerging_group_demand"],
  medical_academic: ["recurring_group_buyers", "rotational_meetings", "active_venue_lodging_sourcing"],
  tours_educational_specialist: ["group_accommodation_travel"],
  weddings_social_private_events: ["competitive_hotel_use", "emerging_group_demand"],
  professional_planners_dmc: ["corporate_institutional_programs", "group_accommodation_travel"],
  scaling_companies: ["emerging_group_demand", "corporate_institutional_programs"],
  local_meetings_catering: ["corporate_institutional_programs"],
  exhibitor_sponsor_teams: ["active_venue_lodging_sourcing", "competitive_hotel_use"],
});

export function pathwaysForWorkstream(segmentId) {
  return WORKSTREAM_PATHWAYS[segmentId] || [];
}

export function weeklyWorkstreamDecision({ prior = [], next = [] } = {}) {
  const priorIds = new Set(prior.map((row) => row.segmentId));
  const added = next.filter((row) => !priorIds.has(row.segmentId));
  return {
    mode: "WEEKLY_INCREMENTAL",
    added: added.length,
    unchanged: next.length - added.length,
    rerunAll: false,
    release: false,
  };
}

const PATHWAY_IDS = [
  "active_venue_lodging_sourcing",
  "recurring_group_buyers",
  "group_accommodation_travel",
  "rotational_meetings",
  "competitive_hotel_use",
  "corporate_institutional_programs",
  "emerging_group_demand",
];

const COPY = {
  active_venue_lodging_sourcing: {
    hypothesis: "A buyer is currently asking for a venue, room block, or group hotel.",
    en: (place) => `${place} association seeking hotel room block`,
    es: (place) => `asociación profesional ${place} busca hotel sede`,
    sources: ["official association RFP or housing page", "procurement notice"],
  },
  recurring_group_buyers: {
    hypothesis: "An organization repeats group meetings even when this edition is not open.",
    en: (place) => `${place} professional association meetings department`,
    es: (place) => `directorio de asociaciones empresariales ${place} reuniones`,
    sources: ["official association site", "relevant organization directory"],
  },
  group_accommodation_travel: {
    hypothesis: "A program moves people who need group lodging, not only a meeting room.",
    en: (place) => `${place} destination management company group hotel room block`,
    es: (place) => `programa de viajes de grupo ${place} hospedaje`,
    sources: ["official travel or housing program page"],
  },
  rotational_meetings: {
    hypothesis: "A convention rotates among destinations and a future cycle is not yet committed.",
    en: (place) => `${place} rotating association convention site selection`,
    es: (place) => `convención rotativa asociación ${place} selección de sede`,
    sources: ["official convention history or site-selection page"],
  },
  competitive_hotel_use: {
    hypothesis: "A buyer has used hotels in this market and may need another edition.",
    en: (place) => `${place} association official hotel past meeting`,
    es: (place) => `asociación ${place} hotel oficial reunión anterior`,
    sources: ["official past-meeting or housing page"],
  },
  corporate_institutional_programs: {
    hypothesis: "A company or institution runs training or offsites that need group rooms.",
    en: (place) => `${place} company meeting planner offsite group hotel accommodations`,
    es: (place) => `capacitación empresarial ${place} programa de grupos`,
    sources: ["official corporate or institutional program page"],
  },
  emerging_group_demand: {
    hypothesis: "A new program has group demand before a venue is announced.",
    en: (place) => `${place} professional association new meeting group lodging`,
    es: (place) => `nuevo programa profesional ${place} reunión de grupos`,
    sources: ["official program announcement"],
  },
};

export function demandPathways() {
  return PATHWAY_IDS.map((id) => ({
    id,
    hypothesis: COPY[id].hypothesis,
    applicableCapabilities: ["group lodging", "meeting facilities"],
    capabilityLimit: "Unverified guestroom and meeting-space counts do not establish hotel fit.",
    sourceTypes: COPY[id].sources,
    minimumEvidence: ["official organization identity", "a demand mechanism stated on the source"],
    organizationResolution: "Same official host is one organization. A registration host is not the organizer.",
    buyerRoute: "A verified organizer route is required for publication. An event listing or venue contact is not a route.",
    commercialQualification: id === "active_venue_lodging_sourcing"
      ? "ACTIVE_SOURCING only when the source states a current venue, lodging, or procurement request."
      : "FUTURE_ACCOUNT_DEVELOPMENT only with history, in-market destination, and an official outreach path, and without an open-booking claim.",
    continuation: "Continue while an official same-host page can still answer the missing venue, lodging, or contact question.",
    rejection: "Reject legislature, news, generic directory spam, out-of-market destinations, and committed events with no separate future mechanism.",
    monitoring: "Monitor a verified organization when the mechanism is real and the commercial evidence is not current.",
  }));
}

export function planDemandSearches({ language = "en", territoryLabel = "", pathways = PATHWAY_IDS } = {}) {
  const place = String(territoryLabel || "").trim();
  const voice = language === "es" ? "es" : "en";
  return pathways.filter((id) => COPY[id]).map((id) => {
    const query = COPY[id][voice](place).replace(/\s+/g, " ").trim();
    return {
      pathway: id,
      query,
      language: voice,
      legislatureRisk: queryInvitesLegislature(query, voice),
      primarySource: "buyer_mechanism",
    };
  });
}

export function auditExecutedQueries(queries = []) {
  const rows = queries.map((query) => {
    const text = String(query).toLowerCase();
    let focus = "announced_event";
    if (/group travel|viajes de grupo|destination management/.test(text)) focus = "group_travel";
    if (/seeking hotel|busca hotel|room block|bloque de habitaciones|hospedaje|hotel sede/.test(text)) focus = "lodging_or_sourcing";
    if (/corporate|empresarial|training|capacitación|offsite/.test(text)) focus = "corporate_or_institutional";
    if (/rotat|site selection|selección de sede/.test(text)) focus = "future_site_selection";
    if (/directory|directorio|meetings department/.test(text)) focus = "recurring_organization";
    return { query, focus };
  });
  const counts = rows.reduce((acc, row) => {
    acc[row.focus] = (acc[row.focus] || 0) + 1;
    return acc;
  }, {});
  return {
    rows,
    counts,
    announcedEventShare: rows.length ? (counts.announced_event || 0) / rows.length : 0,
    underResearched: PATHWAY_IDS.filter((id) => {
      if (id === "active_venue_lodging_sourcing") return !counts.lodging_or_sourcing;
      if (id === "recurring_group_buyers") return !counts.recurring_organization;
      if (id === "group_accommodation_travel") return !counts.group_travel;
      if (id === "rotational_meetings") return !counts.future_site_selection;
      if (id === "corporate_institutional_programs") return !counts.corporate_or_institutional;
      return true;
    }),
    eventTiedLodging: rows.filter((row) => row.focus === "lodging_or_sourcing" && /conference|congreso|convenc|meeting|asamblea/i.test(row.query)).length,
  };
}

const SOURCING = /\b(seeking hotel|soliciting venues|room block request|busca hotel|buscan sede|solicitud de propuesta)\b/i;

function sourcingLanguage(blob) {
  if (SOURCING.test(blob)) return true;
  return /\brfp\b/i.test(blob) && /\b(hotel|lodging|venue|room block|sede|hospedaje)\b/i.test(blob);
}
const DIRECTORY_SPAM = /eventbrite\.|10times\.com|clocate\.com|peerspace\.com|cvent\.com/i;
const NEWS_OR_LEGISLATURE = /congresocdmx|excelsior\.com|eluniversal\.com|bizjournals\.com|wtop\.com|\/news\//i;

export function classifyDemandEvidence(row = {}, territoryTerms = []) {
  const blob = `${row.title || ""} ${row.organizationName || ""} ${row.venue || ""} ${row.snippet || ""}`;
  const url = row.url || row.seedUrl || "";
  const local = territoryTerms.some((term) => blob.toLowerCase().includes(String(term).toLowerCase()));
  if (row.disposition === "OUT_OF_MARKET" || row.disposition === "NOT_OPEN_BOOKING") {
    return { mechanism: BUYING_MECHANISM.NOT_ACTIONABLE, falsePositive: false, reason: "committed_or_out_of_market_event" };
  }
  if (NEWS_OR_LEGISLATURE.test(`${url} ${blob}`) || DIRECTORY_SPAM.test(url)) {
    return { mechanism: BUYING_MECHANISM.NOT_ACTIONABLE, falsePositive: true, reason: "news_legislature_or_generic_directory" };
  }
  if (sourcingLanguage(blob) && local) {
    return { mechanism: BUYING_MECHANISM.ACTIVE_SOURCING, falsePositive: false, pathway: "active_venue_lodging_sourcing", reason: "source_states_a_current_procurement_request" };
  }
  if (sourcingLanguage(blob) && !local) {
    return { mechanism: BUYING_MECHANISM.NOT_ACTIONABLE, falsePositive: false, reason: "sourcing_language_without_market_destination" };
  }
  const recurring = /annual|anual|recurring|rotat/i.test(blob);
  const route = row.contactRoute === "verified_organizer_route";
  if (recurring && local && route) {
    return {
      mechanism: BUYING_MECHANISM.FUTURE_ACCOUNT_DEVELOPMENT,
      falsePositive: false,
      openBookingClaimed: false,
      reason: "history_destination_and_official_route_without_an_open_booking",
    };
  }
  if (row.organizationName || row.title) {
    return { mechanism: BUYING_MECHANISM.MONITORING, falsePositive: false, reason: "verified_or_named_program_without_current_commercial_evidence" };
  }
  return { mechanism: BUYING_MECHANISM.NOT_ACTIONABLE, falsePositive: false, reason: "unsupported" };
}

function evidenceBlob(record = {}) {
  return [
    record.title,
    record.organizationName,
    record.eventLocationSummary,
    record.venueSourcingRationale,
    record.roomDemandRationale,
  ].filter(Boolean).join(" ");
}

export function marketTermsFromConfig(config = {}) {
  const keywords = config?.demandTerritory?.classificationKeywords || {};
  return Object.entries(keywords).flatMap(([key, list]) => (
    key === "OUTSIDE_REALISTIC_TERRITORY" || !Array.isArray(list) ? [] : list
  ));
}

export function destinationRelevance(record = {}, config = {}) {
  const blob = evidenceBlob(record).toLowerCase();
  const keywords = config.demandTerritory?.classificationKeywords || {};
  const outside = (keywords.OUTSIDE_REALISTIC_TERRITORY || []).some((term) => blob.includes(String(term).toLowerCase()));
  const inside = marketTermsFromConfig(config).some((term) => blob.includes(String(term).toLowerCase()));
  if (inside) return "in_market";
  if (outside || /OUT_OF_MARKET|DESTINATION_MISMATCH/.test(String(record.eventLocationStatus || ""))) return "outside";
  return "unresolved";
}

const LEGITIMATE_ROUTES = new Set([
  "verified_individual_buyer",
  "verified_organizing_department",
  "legitimate_functional_organizer_route",
]);

export function commercialRouteKind(record = {}, gate = {}) {
  const url = [
    gate.route?.surface?.contactUrl,
    record.publicContactPath,
    record.organizationContactUrl,
    record.officialContactPath,
  ].filter(Boolean).join(" ");
  if (/cvent\.com|eventbrite\./i.test(url)) return "event_registration";
  if (/marriott\.com|wyndhamhotels\.com|fiestamericana/i.test(url)) return "venue_contact";
  const kind = gate.readyContactClass;
  if (kind === "NAMED_BUYER_PERSON" && gate.route?.ok) return "verified_individual_buyer";
  if (kind === "RELEVANT_FUNCTION_CONTACT" && gate.route?.ok) return "verified_organizing_department";
  if (kind === "NAMED_BUYER_ROLE_PATH" && gate.route?.ok) return "legitimate_functional_organizer_route";
  if (kind === "GENERAL_ORG_CONTACT" || gate.route?.reason === "homepage_or_vague_only") return "generic_corporate_inquiry";
  if (kind === "SOURCE_PAGE") return "unrelated_page";
  if (gate.route?.ok) return "usable_contact_without_organizing_role";
  return "no_route";
}

function isStoredOpportunity(record = {}) {
  return Boolean(
    record.id ||
    record.opportunityId ||
    record.demandStatus ||
    record.priority ||
    record.venueSourcingStatus ||
    record.customerVisible != null
  );
}

export function classifyStoredRecordLegacy(record = {}) {
  const blob = evidenceBlob(record);
  const sourcing = String(record.venueSourcingStatus || "").toUpperCase();
  if (/^rfp\b|bids, rfp|temporary staffing|government contracts/i.test(record.organizationName || "")) {
    return { mechanism: BUYING_MECHANISM.NOT_ACTIONABLE, falsePositive: true, reason: "generic_procurement_listing" };
  }
  if (/OUT_OF_MARKET|DESTINATION_MISMATCH/.test(String(record.eventLocationStatus || ""))) {
    return { mechanism: BUYING_MECHANISM.NOT_ACTIONABLE, reason: "stored_destination_mismatch" };
  }
  if (sourcing === "OPEN" || sourcing === "RFP_ACTIVE_SOURCING") {
    return { mechanism: BUYING_MECHANISM.ACTIVE_SOURCING, reason: "stored_status_without_reading_the_publication_gate" };
  }
  if (sourcingLanguage(blob)) {
    return { mechanism: BUYING_MECHANISM.ACTIVE_SOURCING, reason: "stored_sourcing_language" };
  }
  return classifyDemandEvidence({
    organizationName: record.organizationName,
    title: record.title,
    venue: record.eventLocationSummary,
    contactRoute: record.contactRoute,
  }, record.territoryTerms || []);
}

export function classifyCommercialRecord(record = {}, context = {}) {
  const prior = classifyStoredRecordLegacy(record);
  const gate = evaluateProspectPublicationEligibility(record);
  const routeKind = commercialRouteKind(record, gate);
  const destination = destinationRelevance(record, context.config || {});
  const corroborated = sourcingLanguage(evidenceBlob(record));
  const generic = /^rfp\b|bids, rfp|temporary staffing|government contracts/i.test(record.organizationName || "");
  const disqualified = gate.eligibility === "NOT_A_PROSPECT";
  const failed = gate.suitability?.failed || [];
  const onlyTravelGap = failed.length === 1 && failed[0] === "travel_need_evidence";
  const legitimateRoute = LEGITIMATE_ROUTES.has(routeKind);
  const accountHypothesis = !generic && !disqualified && destination === "in_market" && legitimateRoute && (gate.suitability?.ok || onlyTravelGap) && !corroborated;

  let mechanism = BUYING_MECHANISM.NOT_ACTIONABLE;
  let blocker = "legitimate_rejection";
  let reason = "unsupported";
  if (generic) {
    reason = "generic_procurement_listing";
  } else if (disqualified || destination === "outside" || record.disposition === "OUT_OF_MARKET" || record.disposition === "NOT_OPEN_BOOKING") {
    reason = disqualified ? "disqualified_or_research_layer" : "destination_or_committed_event";
  } else if (corroborated && destination === "in_market") {
    mechanism = BUYING_MECHANISM.ACTIVE_SOURCING;
    blocker = gate.hotelFacing ? "none" : "missing_evidence_or_route";
    reason = "sourcing_language_corroborates_a_current_request";
  } else if (gate.hotelFacing === true) {
    mechanism = BUYING_MECHANISM.FUTURE_ACCOUNT_DEVELOPMENT;
    blocker = "none";
    reason = "publication_gate_passes_and_no_open_booking_is_claimed";
  } else if (accountHypothesis) {
    mechanism = BUYING_MECHANISM.FUTURE_ACCOUNT_DEVELOPMENT;
    blocker = onlyTravelGap ? "missing_lodging_evidence" : "none";
    reason = "group_demand_market_and_organizing_route_without_an_open_booking";
  } else if (record.organizationName || record.title) {
    mechanism = BUYING_MECHANISM.MONITORING;
    blocker = gate.route?.ok ? "missing_evidence" : "missing_buyer_route";
    if (failed.length) blocker = "missing_evidence";
    reason = "organization_without_enough_commercial_evidence";
  }

  const mappingDefect = prior.mechanism === BUYING_MECHANISM.MONITORING
    && (mechanism === BUYING_MECHANISM.FUTURE_ACCOUNT_DEVELOPMENT || mechanism === BUYING_MECHANISM.ACTIVE_SOURCING);
  return {
    mechanism,
    reason,
    priorMechanism: prior.mechanism,
    hotelFacing: gate.hotelFacing === true,
    publicationEligibility: gate.eligibility,
    customerVisible: record.customerVisible === true,
    routeKind,
    destination,
    suitabilityFailed: failed,
    blocker,
    openBookingClaimed: mechanism === BUYING_MECHANISM.ACTIVE_SOURCING,
    accountDevelopmentCandidate: mechanism === BUYING_MECHANISM.FUTURE_ACCOUNT_DEVELOPMENT,
    differenceCause: prior.mechanism === mechanism
      ? "unchanged"
      : mappingDefect
        ? "classifier_did_not_read_the_publication_gate"
        : disqualified
          ? "unread_disqualification"
          : "evidence_rule",
  };
}

export function classifyStoredRecord(record = {}, context = {}) {
  if (!isStoredOpportunity(record)) {
    return classifyDemandEvidence({
      organizationName: record.organizationName,
      title: record.title,
      url: record.url || record.seedUrl,
      venue: record.venue || record.eventLocationSummary,
      disposition: record.disposition,
      contactRoute: record.contactRoute,
    }, context.territoryTerms || record.territoryTerms || []);
  }
  return classifyCommercialRecord(record, context);
}

export function sameOrganization(left = "", right = "") {
  const a = canonicalEntityKey(left);
  const b = canonicalEntityKey(right);
  return Boolean(a) && a === b;
}

export function weeklyPathwayForSignal(signal) {
  const map = {
    new_sourcing_signal: "active_venue_lodging_sourcing",
    buyer_decision_window: "active_venue_lodging_sourcing",
    new_edition: "recurring_group_buyers",
    venue_decision: "rotational_meetings",
    organizer_contact_change: "recurring_group_buyers",
    hotel_sales_feedback: "competitive_hotel_use",
    unresolved_gap: "emerging_group_demand",
  };
  return map[signal] || null;
}
