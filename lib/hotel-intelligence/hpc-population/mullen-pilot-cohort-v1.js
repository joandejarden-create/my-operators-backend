/**
 * Mullen Real Estate Capital — eight-hotel HPC population pilot cohort (v1).
 * Portfolio candidates from mullencap.com — not verified current holdings.
 * Identity resolution only; no Airtable writes from this module.
 */

export const MULLEN_PILOT_COHORT_VERSION = "he-mullen-hpc-population-pilot-v1";
export const MULLEN_PILOT_BASE_ID_CANONICAL = "appCCUsuGsE1ifoLk";
export const MULLEN_PILOT_HPC_TABLE_ID = "tbl9aY5ijiuIzzWam";
export const MULLEN_PILOT_HOTEL_CENSUS_TABLE_ID = "tblgj2qEwxjTcg6q0";

/** Founder identity outcomes (Phase 1). */
export const IDENTITY_OUTCOME = Object.freeze({
  MATCHED_HPC: "MATCHED_HPC",
  MATCHED_HOTEL_CENSUS: "MATCHED_HOTEL_CENSUS",
  MATCHED_EXISTING_ALIAS: "MATCHED_EXISTING_ALIAS",
  NEW_HPC_CANDIDATE: "NEW_HPC_CANDIDATE",
  AMBIGUOUS_MATCH: "AMBIGUOUS_MATCH",
  IDENTITY_UNRESOLVED: "IDENTITY_UNRESOLVED",
});

/**
 * Eight pilot hotels. knownHpcId only when already canonical.
 * Aliases include prior brand/development names for collision search.
 */
export const MULLEN_PILOT_HOTELS = Object.freeze([
  Object.freeze({
    pilotKey: "bethesda_marriott",
    hotelName: "Bethesda Marriott",
    knownHpcId: "recLuxvwwxID7U2B8",
    brand: "Marriott Hotels",
    city: "Bethesda",
    country: "United States",
    marketHint: "Bethesda / Montgomery County",
    aliases: ["Bethesda Marriott Hotel", "Marriott Bethesda"],
    notes: "Canonical HPC already known from Bethesda Phase 3/4.",
  }),
  Object.freeze({
    pilotKey: "ac_hotel_newtown_square",
    hotelName: "AC Hotel Newtown Square",
    knownHpcId: null,
    brand: "AC Hotels",
    city: "Newtown Square",
    country: "United States",
    marketHint: "Philadelphia / Newtown Square",
    aliases: ["AC Hotel by Marriott Newtown Square", "AC Newtown Square"],
    notes: "US current portfolio listing — pending HPC match.",
  }),
  Object.freeze({
    pilotKey: "carton_house_fairmont",
    hotelName: "Carton House, a Fairmont Hotel",
    knownHpcId: null,
    brand: "Fairmont",
    city: "Maynooth",
    country: "Ireland",
    marketHint: "Ireland",
    aliases: [
      "Carton House",
      "Carton House Hotel",
      "Fairmont Carton House",
      "Carton House Fairmont",
    ],
    notes: "Ireland current portfolio listing.",
  }),
  Object.freeze({
    pilotKey: "hgi_newtown_square_radnor",
    hotelName: "Hilton Garden Inn Newtown Square Radnor",
    knownHpcId: null,
    brand: "Hilton Garden Inn",
    city: "Newtown Square",
    country: "United States",
    marketHint: "Philadelphia / Newtown Square",
    aliases: [
      "Hilton Garden Inn Newtown Square",
      "Hilton Garden Inn Radnor",
      "HGI Newtown Square",
    ],
    notes: "US current portfolio listing.",
  }),
  Object.freeze({
    pilotKey: "jw_marriott_costa_elena",
    hotelName: "JW Marriott Costa Elena Resort All-Inclusive",
    knownHpcId: null,
    brand: "JW Marriott",
    city: null,
    country: "Costa Rica",
    marketHint: "Costa Rica",
    aliases: [
      "JW Marriott Guanacaste Resort & Spa",
      "Dreams Las Mareas",
      "Dreams Las Mareas Costa Rica",
      "Costa Elena Resort",
      "JW Marriott Costa Elena",
    ],
    notes: "MREC notes formerly Dreams Las Mareas — alias search required.",
  }),
  Object.freeze({
    pilotKey: "secrets_cap_cana",
    hotelName: "Secrets Cap Cana Resort & Spa",
    knownHpcId: null,
    brand: "Secrets",
    city: "Cap Cana",
    country: "Dominican Republic",
    marketHint: "Dominican Republic",
    aliases: ["Secrets Cap Cana", "Secrets Cap Cana Resort"],
    notes: "DR current portfolio listing.",
  }),
  Object.freeze({
    pilotKey: "secrets_papagayo",
    hotelName: "Secrets Papagayo Costa Rica",
    knownHpcId: null,
    brand: "Secrets",
    city: null,
    country: "Costa Rica",
    marketHint: "Costa Rica",
    aliases: ["Secrets Papagayo", "Secrets Resort Papagayo", "Papagayo Secrets"],
    notes: "Costa Rica current portfolio listing.",
  }),
  Object.freeze({
    pilotKey: "secrets_st_lucia",
    hotelName: "Secrets St. Lucia",
    knownHpcId: null,
    brand: "Secrets",
    city: null,
    country: "Saint Lucia",
    marketHint: "St. Lucia",
    aliases: [
      "Secrets St Lucia",
      "Secrets St. Lucia Resort",
      "Secrets Halcyon",
      "Secrets St Lucia Resort & Spa",
    ],
    notes: "St. Lucia listing — confirm operating vs announced name.",
  }),
]);

/**
 * Map live HPC collision outcomes → founder Phase 1 identity vocabulary.
 */
export function mapHpcCollisionToIdentityOutcome(collision, censusHits = []) {
  const outcome = collision?.outcome || null;
  if (outcome === "HPC_EXISTING_RECORD_VERIFIED" && collision?.hpcRecordId) {
    const method = String(collision.matchMethod || "");
    if (method.includes("alias") || (collision.query?.aliases || []).length) {
      // Prefer MATCHED_HPC; alias evidence is recorded separately.
      return IDENTITY_OUTCOME.MATCHED_HPC;
    }
    return IDENTITY_OUTCOME.MATCHED_HPC;
  }
  if (outcome === "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW") {
    return IDENTITY_OUTCOME.AMBIGUOUS_MATCH;
  }
  if (outcome === "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT") {
    if (censusHits.length === 1) return IDENTITY_OUTCOME.MATCHED_HOTEL_CENSUS;
    if (censusHits.length > 1) return IDENTITY_OUTCOME.AMBIGUOUS_MATCH;
    return IDENTITY_OUTCOME.NEW_HPC_CANDIDATE;
  }
  if (outcome === "HPC_LIVE_LOOKUP_UNAVAILABLE") {
    return IDENTITY_OUTCOME.IDENTITY_UNRESOLVED;
  }
  if (censusHits.length === 1) return IDENTITY_OUTCOME.MATCHED_HOTEL_CENSUS;
  if (censusHits.length > 1) return IDENTITY_OUTCOME.AMBIGUOUS_MATCH;
  return IDENTITY_OUTCOME.IDENTITY_UNRESOLVED;
}

export function founderPreviewUrl(hpcId, port = 8091) {
  const id = String(hpcId || "").trim();
  if (!/^rec[a-zA-Z0-9]{14}$/.test(id)) return null;
  return `http://127.0.0.1:${port}/hotel-explorer-preview.html?hotelExplorer=${encodeURIComponent(
    id
  )}&previewMode=proposed`;
}
