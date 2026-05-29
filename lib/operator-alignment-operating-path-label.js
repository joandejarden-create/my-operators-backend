/**
 * Display label for Operating Path summary (My Deals Operator Strategy, OAS context).
 * Neutral language; no scoring impact.
 */

const NOT_PROVIDED = "Not provided";

/**
 * @param {object} normalized — from normalizeOperatorAlignmentDealInputs
 * @returns {string}
 */
export function buildOperatingPathDisplayLabel(normalized) {
  const deal = normalized || {};
  const parts = [];

  const brand = String(deal.brandAgreementStructure || "").trim();
  const op = String(deal.operatingModel || "").trim();
  const mgmt = Array.isArray(deal.preferredManagementStructure)
    ? deal.preferredManagementStructure.filter(Boolean)
    : [];

  if (brand) parts.push(brand);
  if (op) {
    parts.push(op);
  } else if (mgmt.length) {
    parts.push(mgmt.join(", "));
  } else if (deal.legacyDealStructure) {
    parts.push(String(deal.legacyDealStructure).trim());
  }

  if (parts.length) return parts.join(" + ");

  return NOT_PROVIDED;
}
