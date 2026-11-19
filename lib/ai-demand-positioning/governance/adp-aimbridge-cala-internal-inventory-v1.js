/**
 * ADP_AIMBRIDGE_CALA_INTERNAL_INVENTORY_V1
 *
 * Founder/admin internal inventory for Aimbridge CALA ADP subjects that are
 * outside the frozen canonical 31 Full E2E execution cohort.
 *
 * Identity is linked to existing HPC census records — do not invent duplicates.
 * customerDropdownVisible remains false until certified customer publication.
 */

export const ADP_AIMBRIDGE_CALA_INTERNAL_INVENTORY_V1 =
  "ADP_AIMBRIDGE_CALA_INTERNAL_INVENTORY_V1";

/**
 * @typedef {{
 *   propertyId: string,
 *   censusRecordId: string,
 *   name: string,
 *   city: string,
 *   country: string,
 *   affiliation: string,
 *   measurementPeriodId: string,
 *   outstandingWork: readonly string[],
 * }} AimbridgeCalaInternalHotel
 */

/** @type {readonly AimbridgeCalaInternalHotel[]} */
export const ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS = Object.freeze([
  Object.freeze({
    propertyId: "adp_sheraton_guadalajara_expo",
    censusRecordId: "recCAGqkERSXgiku4",
    name: "Sheraton Guadalajara Expo",
    city: "Guadalajara",
    country: "Mexico",
    affiliation: "Sheraton",
    measurementPeriodId:
      "adp_period_adp_sheraton_guadalajara_expo_20261103133203_79f540",
    outstandingWork: Object.freeze([
      "STAGED_STORE_POINTER_MISSING_LOCALLY",
      "EVIDENCE_PACKAGE_EMPTY_ITEMS",
    ]),
  }),
  Object.freeze({
    propertyId: "adp_wyndham_alltra_punta_cana",
    censusRecordId: "recXh4NgF8sDbi9cQ",
    name: "Wyndham Alltra Punta Cana",
    city: "Punta Cana",
    country: "Dominican Republic",
    affiliation: "Wyndham Alltra",
    measurementPeriodId:
      "adp_period_adp_wyndham_alltra_punta_cana_20261103145403_091760",
    outstandingWork: Object.freeze([
      "MEASUREMENT_DENOMINATOR_RECONCILIATION",
      "STAGED_STORE_POINTER_MISSING_LOCALLY",
      "EVIDENCE_PACKAGE_EMPTY_ITEMS",
    ]),
  }),
  Object.freeze({
    propertyId: "adp_voco_cancun_zona_hotelera",
    censusRecordId: "recTk5I8VOzsPhxDz",
    name: "voco Cancún Zona Hotelera",
    city: "Cancún",
    country: "Mexico",
    affiliation: "voco",
    measurementPeriodId:
      "adp_period_adp_voco_cancun_zona_hotelera_20261103141133_bb45de",
    outstandingWork: Object.freeze([
      "STAGED_STORE_POINTER_MISSING_LOCALLY",
      "EVIDENCE_PACKAGE_EMPTY_ITEMS",
    ]),
  }),
]);

export const ADP_AIMBRIDGE_CALA_INTERNAL_PROPERTY_IDS = Object.freeze(
  ADP_AIMBRIDGE_CALA_INTERNAL_HOTELS.map((h) => h.propertyId)
);

export function isAimbridgeCalaInternalPropertyId(propertyId) {
  return ADP_AIMBRIDGE_CALA_INTERNAL_PROPERTY_IDS.includes(
    String(propertyId || "").trim()
  );
}
