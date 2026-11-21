/**
 * HIF Track A — Continuous foundation waves (current-backward commercial discovery).
 * CUSTOMER_BLOCKED. Reuses foundation persistence v1 destinations only.
 */

export const HIF_TRACK_A_CONTINUOUS_FOUNDATION_WAVES = "hif-track-a-continuous-foundation-waves-v1";

/** IDs reserved for this Track A wave — Track B must not overwrite. */
export const TRACK_A_WAVE_IDS = Object.freeze([
  "disc_cb1_lakeville_inn_mn_2026",
  "find_cb1_lakeville_bct_brand_search_2026",
  "disc_cfa_hilton_trinidad_pos_2026",
  "find_cfa_hilton_trinidad_mct_hma_sale_2026",
  "disc_cfa_magdalena_grand_tobago_2026",
  "find_cfa_magdalena_grand_sale_eoi_2026",
  "disc_cfa_george_hotel_stranraer_2026",
  "find_cfa_george_hotel_stranraer_operator_2026",
  "disc_cfa_gideon_putnam_saratoga_2026",
  "find_cfa_gideon_putnam_concession_2026",
  "disc_cfa_pueblo_downtown_hotel_2026",
  "find_cfa_pueblo_downtown_dev_2026",
  // CB2 resume wave
  "disc_cb2_legacy_hotel_green_bay_2026",
  "find_cb2_legacy_hotel_green_bay_bct_2026",
  "disc_cb2_hotel_do_mel_gois_2026",
  "find_cb2_hotel_do_mel_gois_operator_2026",
  "disc_cb2_jericho_beach_hostel_2026",
  "find_cb2_jericho_beach_hostel_operator_2026",
  "disc_cb2_torre_uriz_navarra_2026",
  "find_cb2_torre_uriz_concession_2026",
  "disc_cb2_white_sands_grand_turk_2026",
  "find_cb2_white_sands_lease_ops_2026",
]);

export const HIF_TRACK_A_CB2 = "hif-track-a-cb2-commercial-discovery-v1";

export function isTrackAContinuousWaveId(id) {
  return TRACK_A_WAVE_IDS.includes(String(id || ""));
}

/**
 * Distinguish agreement instrument from commercial opportunity type.
 * Never force an unresolved instrument into a principal BCT/MCT bucket.
 */
export function classifyAgreementVsOpportunity(row = {}) {
  const agreementType = String(row.agreementType || "UNRESOLVED_INSTRUMENT").toUpperCase();
  const opportunityType = String(row.opportunityType || "UNCLASSIFIED").toUpperCase();
  const principal =
    opportunityType === "BCT" ||
    opportunityType === "MCT" ||
    opportunityType === "BRAND_CONVERSION_TARGET" ||
    opportunityType === "MANAGEMENT_CONTRACT_TARGET";

  return {
    agreementType,
    opportunityType,
    principalCommercialOpportunity: principal,
    note:
      agreementType === "UNRESOLVED_INSTRUMENT"
        ? "Do not force unresolved contractual instrument into a specific category"
        : "Agreement type preserved separately from opportunity type",
  };
}

/**
 * Sale / expiration alone is insufficient without prospective commercial decision evidence.
 */
export function qualifiesPrincipalFirst100(row = {}) {
  if (row.countsTowardFirst100 !== true) return false;
  if (row.priority !== "PRIORITY_A") return false;
  if (!row.hotelSpecificEvidence) return false;
  if (row.saleOrExpirationOnly === true && !row.prospectiveCommercialDecisionEvidence) return false;
  const rel = String(row.commercialRelevance || "");
  return rel === "VERIFIED_RELEVANT" || rel === "EVIDENCE_SUPPORTED_POTENTIAL";
}
