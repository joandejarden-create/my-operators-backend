/**
 * Customer-facing GDI opportunity visibility.
 * Excludes synthetic test/fixture/sample rows from list/share/CSV.
 */

const TEST_ID_RE =
  /_test_|fixture|sample|link_only|canary_promo|schema_repair|gdi_pe_test|v13_test|temp_validation/i;

const SYNTHETIC_TITLE_RE =
  /\bpe link test\b|\[test\]|\[schema repair\]|(?:^|:\s*)\S+\s+(?:GARDEN|WEDDING(?:\s+VENUE)?|COUNTRY(?:\s+CLUB)?|BANQUET(?:\s+HALL)?|EVENT(?:\s+VENUE)?|MUSEUM|RELIGIOUS|HISTORIC|PRIVATE|WINERY)\s+\d+/i;

/**
 * True when the opportunity must never appear on customer auth/share surfaces.
 */
export function isGdiTestOrFixtureOpportunity(opp = {}) {
  if (opp == null || typeof opp !== "object") return false;
  if (opp.isTestData === true) return true;
  if (opp.customerVisible === false) return true;
  const id = String(opp.id || opp.opportunityId || "");
  if (TEST_ID_RE.test(id)) return true;
  const title = String(opp.title || opp.opportunityName || "");
  const org = String(opp.organizationName || "");
  if (SYNTHETIC_TITLE_RE.test(title) || SYNTHETIC_TITLE_RE.test(org)) return true;
  if (opp.peVenueId === "pev_test" || /^pev_test/i.test(String(opp.peVenueId || ""))) {
    return true;
  }
  const sources = Array.isArray(opp.sources)
    ? opp.sources.map((s) => s?.url || "").join(" ")
    : "";
  if (/\.example\b/i.test(sources)) return true;
  return false;
}

export function filterCustomerFacingOpportunities(opportunities = []) {
  return (opportunities || []).filter((o) => !isGdiTestOrFixtureOpportunity(o));
}

/**
 * Mark synthetic validation writes so they cannot leak to customers.
 */
export function markAsTestOpportunity(opp = {}) {
  return {
    ...opp,
    isTestData: true,
    customerVisible: false,
  };
}
