/**
 * Equivalent Evidence Policy — fact standard fixed; evidence form may vary.
 * Does NOT lower Ready/Watch thresholds.
 */

import { EVIDENCE_ROUTE } from "./constants.js";

/** Canonical commercial facts that multiple routes may prove. */
export const CANONICAL_FACTS = Object.freeze({
  LODGING_SELECTION_ACTIVE: "LODGING_SELECTION_ACTIVE",
  NAMED_TRAVELING_ENTITY: "NAMED_TRAVELING_ENTITY",
  BUYER_OR_CONTROLLER_PATH: "BUYER_OR_CONTROLLER_PATH",
  FUTURE_DECISION_POINT: "FUTURE_DECISION_POINT",
  GROUP_TRAVEL_MOTION: "GROUP_TRAVEL_MOTION",
  HOTEL_FIT: "HOTEL_FIT",
});

/**
 * Routes that may establish each fact (must still meet quality bars).
 */
export const EQUIVALENT_ROUTES = Object.freeze({
  [CANONICAL_FACTS.LODGING_SELECTION_ACTIVE]: [
    {
      route: EVIDENCE_ROUTE.OFFICIAL_PUBLIC,
      forms: ["official_housing_page", "official_hotel_list", "preferred_hotel_page"],
    },
    {
      route: EVIDENCE_ROUTE.PCO_DMC,
      forms: ["pco_accommodation_instructions", "dmc_hotel_program"],
    },
    {
      route: EVIDENCE_ROUTE.HOUSING_PROVIDER,
      forms: ["housing_bureau_portal", "passkey_onpeak_block"],
    },
    {
      route: EVIDENCE_ROUTE.PROCUREMENT_RFP,
      forms: ["public_rfp", "procurement_notice_lodging"],
    },
    {
      route: EVIDENCE_ROUTE.HOTEL_SUPPLIED,
      forms: ["organizer_rate_request", "hotel_list_invitation", "room_block_request"],
    },
    {
      route: EVIDENCE_ROUTE.EVENT_MANUAL,
      forms: ["exhibitor_manual_housing", "delegate_guide_housing"],
    },
    {
      route: EVIDENCE_ROUTE.REGISTRATION_PORTAL,
      forms: ["registration_housing_step"],
    },
  ],
  [CANONICAL_FACTS.BUYER_OR_CONTROLLER_PATH]: [
    {
      route: EVIDENCE_ROUTE.OFFICIAL_PUBLIC,
      forms: ["named_buyer_person", "functional_secretariat_email"],
    },
    {
      route: EVIDENCE_ROUTE.PCO_DMC,
      forms: ["pco_contact", "dmc_contact", "housing_contact"],
    },
    {
      route: EVIDENCE_ROUTE.PROCUREMENT_RFP,
      forms: ["procurement_contact"],
    },
    {
      route: EVIDENCE_ROUTE.HOTEL_SUPPLIED,
      forms: ["organizer_response_contact", "pco_response"],
    },
  ],
  [CANONICAL_FACTS.NAMED_TRAVELING_ENTITY]: [
    {
      route: EVIDENCE_ROUTE.OFFICIAL_PUBLIC,
      forms: ["exhibitor_list", "participant_list", "speaker_list"],
    },
    {
      route: EVIDENCE_ROUTE.EVENT_MANUAL,
      forms: ["program_pdf_named_org"],
    },
    {
      route: EVIDENCE_ROUTE.HOTEL_SUPPLIED,
      forms: ["past_group_lookalike_with_future_trigger"],
    },
    {
      route: EVIDENCE_ROUTE.ORGANIZER_RESPONSE,
      forms: ["confirmed_account_name"],
    },
  ],
  [CANONICAL_FACTS.FUTURE_DECISION_POINT]: [
    {
      route: EVIDENCE_ROUTE.OFFICIAL_PUBLIC,
      forms: ["housing_open_date", "event_future_date", "list_publish_date"],
    },
    {
      route: EVIDENCE_ROUTE.HOTEL_SUPPLIED,
      forms: ["selecting_hotels_next_month", "list_publishes_january"],
    },
    {
      route: EVIDENCE_ROUTE.HISTORICAL_PROCESS,
      forms: ["typical_cycle_timing_needs_current_confirm"],
      note: "Historical timing is process intelligence only until current evidence confirms",
    },
  ],
});

/** Quality bars that every route must still clear. */
export const EVIDENCE_QUALITY_BARS = Object.freeze({
  mustBePublicOrHotelSupplied: true,
  mustLinkToNamedCampaignOrAccount: true,
  historicalCannotProveCurrentPlacement: true,
  genericOrganizerInsufficientForLodgingAuthority: true,
  genericDmcRequiresCampaignLink: true,
  noSpeculativeLodging: true,
  provenanceRequired: true,
});

/**
 * Evaluate whether a piece of evidence can establish a canonical fact.
 * Returns eligibility — does not auto-promote maturity.
 */
export function evaluateEquivalentEvidence({
  fact,
  route,
  form,
  evidence = {},
  isHistorical = false,
} = {}) {
  const factKey = String(fact || "").toUpperCase();
  const routes = EQUIVALENT_ROUTES[factKey];
  if (!routes) {
    return { ok: false, reason: "unknown_fact", qualityBars: EVIDENCE_QUALITY_BARS };
  }

  const routeKey = String(route || "").toUpperCase();
  const match = routes.find((r) => r.route === routeKey);
  if (!match) {
    return { ok: false, reason: "route_not_equivalent_for_fact", allowedRoutes: routes.map((r) => r.route) };
  }

  const formOk =
    !form || match.forms.some((f) => f === form || String(form).toLowerCase().includes(f.split("_")[0]));
  if (!formOk) {
    return { ok: false, reason: "form_not_listed", allowedForms: match.forms };
  }

  if (isHistorical && factKey !== CANONICAL_FACTS.FUTURE_DECISION_POINT) {
    if (
      factKey === CANONICAL_FACTS.LODGING_SELECTION_ACTIVE ||
      factKey === CANONICAL_FACTS.NAMED_TRAVELING_ENTITY
    ) {
      return {
        ok: false,
        reason: "historical_cannot_establish_current_fact",
        note: "Use historical evidence for process pattern only",
      };
    }
  }

  if (
    factKey === CANONICAL_FACTS.LODGING_SELECTION_ACTIVE &&
    !evidence.sourceUrl &&
    evidence.provenance !== "HOTEL_SUPPLIED_EVIDENCE"
  ) {
    return { ok: false, reason: "missing_provenance" };
  }

  if (
    routeKey === EVIDENCE_ROUTE.PCO_DMC &&
    !evidence.campaignId &&
    !evidence.campaignName &&
    !evidence.eventName
  ) {
    return { ok: false, reason: "generic_dmc_without_campaign_link" };
  }

  return {
    ok: true,
    reason: "equivalent_route_qualifies",
    fact: factKey,
    route: routeKey,
    form: form || null,
    qualityBars: EVIDENCE_QUALITY_BARS,
    promotesMaturity: false,
    note: match.note || null,
  };
}

export function summarizeEquivalentEvidencePolicy() {
  return {
    principle: "THE_FACT_STANDARD_IS_FIXED_THE_EVIDENCE_FORM_MAY_VARY",
    facts: Object.keys(CANONICAL_FACTS),
    routesByFact: EQUIVALENT_ROUTES,
    qualityBars: EVIDENCE_QUALITY_BARS,
    readyThresholdChanged: false,
    watchThresholdChanged: false,
  };
}
