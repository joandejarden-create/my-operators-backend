/**
 * Cohort A evaluation ceilings. Code is the authority.
 * A ledger file cannot raise these amounts. Cohort B cannot spend them.
 */

export const COHORT_A_BUDGET_VERSION = "gdi-cohort-a-benchmark-budget-v1";

export const COHORT_A_AGGREGATE_USD = 18;

export const COHORT_A_HOTELS = Object.freeze({
  recLuxvwwxID7U2B8: Object.freeze({
    censusRecordId: "recLuxvwwxID7U2B8",
    displayName: "Bethesda Marriott",
    adpPropertyId: "adp_bethesda_marriott",
    identityKey: null,
    ceilingUsd: 3,
  }),
  recnPB3Q02NO8Ma1o: Object.freeze({
    censusRecordId: "recnPB3Q02NO8Ma1o",
    displayName: "Wyndham San Diego Bayside",
    adpPropertyId: null,
    identityKey: null,
    ceilingUsd: 4,
  }),
  recrPQcZg7SFARRb2: Object.freeze({
    censusRecordId: "recrPQcZg7SFARRb2",
    displayName: "YOTEL Geneva Lake",
    adpPropertyId: "adp_yotel_geneva_lake",
    identityKey: "yotel_ch_geneva_lake_founex",
    ceilingUsd: 6,
  }),
  recC2kbCaS48jFjFF: Object.freeze({
    censusRecordId: "recC2kbCaS48jFjFF",
    displayName: "Nobu Hotel London Portman Square",
    adpPropertyId: "adp_nobu_london_portman",
    identityKey: null,
    ceilingUsd: 5,
  }),
});

/** Reserved identities. Spend against these ids is refused. */
export const COHORT_B_HOTEL_IDS = Object.freeze([
  "rec0eTEnCqlsGJJt9",
  "recRkGGNKslJptFIL",
  "recterKmNPdntCm0v",
  "recIEcSWuhSCoNLFN",
]);

export const COHORT_B_RESERVED_USD = 14;

export function cohortAHotel(hotelId) {
  return COHORT_A_HOTELS[String(hotelId || "").trim()] || null;
}

export function isCohortBHotel(hotelId) {
  return COHORT_B_HOTEL_IDS.includes(String(hotelId || "").trim());
}
