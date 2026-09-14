/**
 * Product-safe census field quarantine (P8.1).
 *
 * Technical governance — not a legal opinion.
 * High-risk legacy / license-uncertain fields must not silently flow to product DTOs
 * unless independently replaced or contract rights are confirmed.
 *
 * Disable quarantine only for internal debugging:
 *   PRODUCT_CENSUS_QUARANTINE=0
 */

/** Fields omitted from customer/product census payloads by default. */
export const PRODUCT_CENSUS_QUARANTINED_FIELDS = Object.freeze([
  "strNumber",
  "STR Number",
]);

/** Fields still product-required but flagged CONTRACT_REVIEW_REQUIRED. */
export const PRODUCT_CENSUS_CONTRACT_REVIEW_FIELDS = Object.freeze([
  "chainScale",
  "Chain Scale",
  "market",
  "Market",
  "submarket",
  "Submarket",
]);

export function productCensusQuarantineEnabled() {
  return process.env.PRODUCT_CENSUS_QUARANTINE !== "0";
}

/**
 * Strip quarantined keys from a hotel DTO (mutates a shallow copy).
 * @param {Record<string, unknown>} hotel
 */
export function applyProductCensusQuarantine(hotel) {
  if (!hotel || typeof hotel !== "object") return hotel;
  if (!productCensusQuarantineEnabled()) return hotel;

  const out = { ...hotel };
  for (const key of PRODUCT_CENSUS_QUARANTINED_FIELDS) {
    if (key in out) {
      out[key] = null;
    }
  }
  out._provenanceFlags = {
    ...(out._provenanceFlags || {}),
    strNumber_quarantined: true,
    chainScale_contract_review_required: true,
    quarantine_policy: "p81_product_safe_census_fields_v1",
  };
  return out;
}
