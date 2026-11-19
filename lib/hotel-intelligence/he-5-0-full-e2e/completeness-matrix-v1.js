/**
 * Eight-hotel Minimum HPC vs Full Hotel Explorer completeness matrix.
 * Domains are never conflated: min onboarding ≠ full HE completion.
 */

export const DOMAIN_STATUS = Object.freeze({
  VERIFIED_PERSISTED: "VERIFIED_PERSISTED",
  VERIFIED_STAGED: "VERIFIED_STAGED",
  RESEARCH_INCOMPLETE: "RESEARCH_INCOMPLETE",
  INTEGRATION_BLOCKED: "INTEGRATION_BLOCKED",
  PROVIDER_RESTRICTED: "PROVIDER_RESTRICTED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

const D = DOMAIN_STATUS;

/** Domain keys for Full HE Intelligence */
export const FULL_HE_DOMAINS = Object.freeze([
  "ownership",
  "stakeholders",
  "public_contacts",
  "asset_history",
  "meeting_facilities",
  "amenities",
  "news_market_alerts",
  "reputation",
  "demand_generators",
  "connectivity",
  "sources_evidence",
]);

/** Minimum HPC Onboarding domains */
export const MIN_HPC_DOMAINS = Object.freeze([
  "identity",
  "location",
  "rooms",
  "status",
  "brand",
  "operator",
  "basic_attributes",
]);

export const EIGHT_HOTEL_COMPLETENESS = Object.freeze([
  {
    hotel: "Bethesda Marriott",
    pilotKey: "bethesda_marriott",
    hpcId: "recLuxvwwxID7U2B8",
    minHpc: {
      identity: D.VERIFIED_PERSISTED,
      location: D.VERIFIED_PERSISTED,
      rooms: D.VERIFIED_PERSISTED,
      status: D.VERIFIED_PERSISTED,
      brand: D.VERIFIED_PERSISTED,
      operator: D.VERIFIED_PERSISTED,
      basic_attributes: D.VERIFIED_STAGED, // year built staged; renovation dual events staged
    },
    fullHe: {
      ownership: D.VERIFIED_STAGED,
      stakeholders: D.VERIFIED_STAGED,
      public_contacts: D.VERIFIED_STAGED,
      asset_history: D.VERIFIED_STAGED,
      meeting_facilities: D.VERIFIED_STAGED,
      amenities: D.VERIFIED_PERSISTED,
      news_market_alerts: D.RESEARCH_INCOMPLETE,
      reputation: D.PROVIDER_RESTRICTED,
      demand_generators: D.RESEARCH_INCOMPLETE,
      connectivity: D.INTEGRATION_BLOCKED, // US TI sparse / honest empty
      sources_evidence: D.VERIFIED_STAGED,
    },
    readinessNote: "Deepest research hotel — still not FULL_E2E until apply + re-read + browser",
  },
  {
    hotel: "AC Hotel Newtown Square",
    pilotKey: "ac_hotel_newtown_square",
    hpcId: null,
    minHpc: {
      identity: D.VERIFIED_STAGED,
      location: D.VERIFIED_STAGED,
      rooms: D.VERIFIED_STAGED, // 140 HIGH resolved
      status: D.VERIFIED_STAGED,
      brand: D.VERIFIED_STAGED,
      operator: D.RESEARCH_INCOMPLETE, // Gulph Creek press — not HI edge
      basic_attributes: D.RESEARCH_INCOMPLETE,
    },
    fullHe: blankFull(D.RESEARCH_INCOMPLETE),
    readinessNote: "Min HPC CREATE READY — not Full HE",
  },
  {
    hotel: "Carton House, a Fairmont Hotel",
    pilotKey: "carton_house_fairmont",
    hpcId: null,
    minHpc: {
      identity: D.VERIFIED_STAGED,
      location: D.VERIFIED_STAGED,
      rooms: D.VERIFIED_STAGED,
      status: D.VERIFIED_STAGED,
      brand: D.VERIFIED_STAGED,
      operator: D.VERIFIED_STAGED, // Fairmont managed (brand ops)
      basic_attributes: D.RESEARCH_INCOMPLETE,
    },
    fullHe: blankFull(D.RESEARCH_INCOMPLETE),
    readinessNote: "Min HPC CREATE READY — not Full HE",
  },
  {
    hotel: "Hilton Garden Inn Newtown Square Radnor",
    pilotKey: "hgi_newtown_square_radnor",
    hpcId: null,
    minHpc: {
      identity: D.VERIFIED_STAGED,
      location: D.VERIFIED_STAGED,
      rooms: D.VERIFIED_STAGED, // 138 HIGH autonomous
      status: D.VERIFIED_STAGED,
      brand: D.VERIFIED_STAGED,
      operator: D.RESEARCH_INCOMPLETE,
      basic_attributes: D.RESEARCH_INCOMPLETE,
    },
    fullHe: blankFull(D.RESEARCH_INCOMPLETE),
    readinessNote: "Min HPC CREATE READY — rooms 138 HIGH",
  },
  {
    hotel: "JW Marriott Costa Elena Resort All-Inclusive",
    pilotKey: "jw_marriott_costa_elena",
    hpcId: "recq9KWr8UE7z2549",
    minHpc: {
      identity: D.VERIFIED_PERSISTED,
      location: D.VERIFIED_PERSISTED,
      rooms: D.VERIFIED_STAGED, // 415 staged null-fill
      status: D.VERIFIED_PERSISTED,
      brand: D.VERIFIED_PERSISTED,
      operator: D.RESEARCH_INCOMPLETE, // MHM trade press held
      basic_attributes: D.RESEARCH_INCOMPLETE,
    },
    fullHe: {
      ...blankFull(D.RESEARCH_INCOMPLETE),
      ownership: D.RESEARCH_INCOMPLETE,
      asset_history: D.VERIFIED_STAGED, // Dreams Las Mareas alias
    },
    readinessNote: "Matched HPC + rooms/alias READY — Full HE incomplete",
  },
  {
    hotel: "Secrets Cap Cana Resort & Spa",
    pilotKey: "secrets_cap_cana",
    hpcId: "recd7XAM6tLjzQhaI",
    minHpc: {
      identity: D.VERIFIED_PERSISTED, // confirmed ≠ Dreams
      location: D.VERIFIED_PERSISTED,
      rooms: D.VERIFIED_STAGED, // 457 autonomous free pass
      status: D.VERIFIED_PERSISTED,
      brand: D.VERIFIED_PERSISTED,
      operator: D.RESEARCH_INCOMPLETE,
      basic_attributes: D.RESEARCH_INCOMPLETE,
    },
    fullHe: blankFull(D.RESEARCH_INCOMPLETE),
    readinessNote: "Identity verified ≠ Dreams; rooms 457 READY null-fill; Full HE incomplete",
  },
  {
    hotel: "Secrets Papagayo Costa Rica",
    pilotKey: "secrets_papagayo",
    hpcId: null,
    hotelCensusId: null,
    legacyHotelCensusProvenanceId: "recyX1wLsHBJYRPw7",
    minHpc: {
      identity: D.VERIFIED_STAGED, // HPC-native CREATE after collision absent
      location: D.VERIFIED_STAGED,
      rooms: D.VERIFIED_STAGED, // 202
      status: D.VERIFIED_STAGED,
      brand: D.VERIFIED_STAGED,
      operator: D.RESEARCH_INCOMPLETE,
      basic_attributes: D.RESEARCH_INCOMPLETE,
    },
    fullHe: {
      ...blankFull(D.RESEARCH_INCOMPLETE),
      ownership: D.VERIFIED_STAGED, // legacy mullencap Owner Website = signal only
    },
    readinessNote: "HPC CREATE READY (legacy census provenance only) — not Full HE",
  },
  {
    hotel: "Secrets St. Lucia",
    pilotKey: "secrets_st_lucia",
    hpcId: null,
    minHpc: {
      identity: D.VERIFIED_STAGED,
      location: D.VERIFIED_STAGED,
      rooms: D.VERIFIED_STAGED,
      status: D.VERIFIED_STAGED,
      brand: D.VERIFIED_STAGED,
      operator: D.RESEARCH_INCOMPLETE,
      basic_attributes: D.RESEARCH_INCOMPLETE,
    },
    fullHe: blankFull(D.RESEARCH_INCOMPLETE),
    readinessNote: "Min HPC CREATE READY — not Full HE",
  },
]);

function blankFull(status) {
  const o = {};
  for (const k of FULL_HE_DOMAINS) o[k] = status;
  return o;
}

export function summarizeCompleteness(rows = EIGHT_HOTEL_COMPLETENESS) {
  return {
    hotels: rows.length,
    note: "Minimum HPC Onboarding ≠ Full Hotel Explorer Intelligence",
    rows,
    legend: DOMAIN_STATUS,
  };
}
