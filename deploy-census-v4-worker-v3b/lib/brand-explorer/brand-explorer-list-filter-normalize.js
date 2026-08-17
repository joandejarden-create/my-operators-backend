/**
 * Canonical Explorer filter values for Brand Setup — Project Fit multi-selects.
 * Maps legacy Airtable option strings to form / Explorer labels.
 */
export const BRAND_EXPLORER_PROJECT_TYPE_ALIASES = {
  Conversion: "Conversion / Reflag",
  "Repositioning / Rebrand": "Renovation / Repositioning",
};

export const BRAND_EXPLORER_AGREEMENT_TYPE_ALIASES = {
  "Brand + Third-Party Mgmt": "Brand + Third-Party Mgmt. (Combined)",
  "Brand-Managed": "Brand-Managed Only",
};

/**
 * @param {string} value
 * @param {Record<string, string>} aliasMap
 * @returns {string}
 */
export function canonicalExplorerFilterValue(value, aliasMap) {
  const s = String(value || "").trim();
  if (!s) return "";
  return aliasMap[s] || s;
}

/**
 * @param {unknown[]} values
 * @param {Record<string, string>} aliasMap
 * @returns {string[]}
 */
export function normalizeExplorerFilterValueList(values, aliasMap) {
  const out = new Set();
  for (const v of values || []) {
    const canonical = canonicalExplorerFilterValue(v, aliasMap);
    if (canonical) out.add(canonical);
  }
  return [...out];
}

export function normalizeBrandExplorerProjectTypes(values) {
  return normalizeExplorerFilterValueList(values, BRAND_EXPLORER_PROJECT_TYPE_ALIASES);
}

export function normalizeBrandExplorerAgreementTypes(values) {
  return normalizeExplorerFilterValueList(values, BRAND_EXPLORER_AGREEMENT_TYPE_ALIASES);
}
