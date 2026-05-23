/**
 * Operator in scope + UI visibility helpers for Deal Setup and snapshots.
 */

export { isOperatorInScopeFromFields } from "./operator-capability-inputs.js";

import { isOperatorInScopeFromFields, SI_FIELDS, strVal } from "./operator-capability-inputs.js";

/**
 * Show third-party operator preference block (legacy UI helper).
 * @param {Record<string, unknown>} fields
 * @returns {boolean}
 */
export function shouldShowThirdPartyOperatorBlock(fields) {
  const plan = strVal(fields[SI_FIELDS.planSelfManage]);
  const bids = strVal(
    fields[SI_FIELDS.whoReceivesBids] || fields[SI_FIELDS.whoReceivesBidsAirtable]
  );
  if (plan === "Owner-Operated") return false;
  if (bids === "Hotel Brands Only (Franchise/License)") return false;
  return isOperatorInScopeFromFields(fields) || /third.party|brand-managed/i.test(plan);
}

/**
 * Show P0 operator capability fields in intake.
 * @param {Record<string, unknown>} fields
 * @returns {boolean}
 */
export function shouldShowOperatorCapabilityFields(fields) {
  return isOperatorInScopeFromFields(fields);
}
