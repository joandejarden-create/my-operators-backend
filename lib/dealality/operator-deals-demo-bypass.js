/**
 * Temporary demo bypass for My Operator Deals UI walkthroughs.
 * Remove or set OPERATOR_DEALS_DEMO_BYPASS_ROLE=false before production hardening.
 */

function isTruthyEnv(value) {
  if (value == null || value === "") return false;
  const v = String(value).trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

function isExplicitlyDisabled(value) {
  if (value == null || value === "") return false;
  const v = String(value).trim().toLowerCase();
  return v === "0" || v === "false" || v === "no" || v === "off";
}

/** @returns {boolean} */
export function isOperatorDealsDemoBypassEnabled() {
  const explicit = process.env.OPERATOR_DEALS_DEMO_BYPASS_ROLE;
  if (isExplicitlyDisabled(explicit)) return false;
  if (isTruthyEnv(explicit)) return true;
  return process.env.NODE_ENV !== "production";
}

/** Scoped demo company when bypassing role for non-operator accounts. */
export function getOperatorDealsDemoCompanyName() {
  return (
    process.env.OPERATOR_DEALS_DEMO_COMPANY ||
    "GHL Hoteles (GHL Holding)"
  ).trim();
}

export const DEMO_BYPASS_WARNING = "operator_deals_demo_bypass_role";
