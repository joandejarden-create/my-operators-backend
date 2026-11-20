/**
 * Separate SEC entity roles — registrant ≠ owner ≠ manager ≠ franchisor.
 */

export const ENTITY_ATTRIBUTION_V2 = "sec-edgar-entity-attribution-v2";

export const ENTITY_ROLE = Object.freeze({
  FILING_REGISTRANT: "FILING_REGISTRANT",
  LEGAL_OWNER: "LEGAL_OWNER",
  PARENT_COMPANY: "PARENT_COMPANY",
  MANAGEMENT_COMPANY: "MANAGEMENT_COMPANY",
  OPERATOR: "OPERATOR",
  FRANCHISOR: "FRANCHISOR",
  FRANCHISEE: "FRANCHISEE",
  BRAND: "BRAND",
  UNKNOWN: "UNKNOWN",
});

/**
 * Build attribution object from a discovery row without inferring ownership
 * from registrant alone.
 */
export function buildEntityAttribution(row = {}, opts = {}) {
  const registrant = row.registrant || opts.registrant || null;
  const brand = row.historicalBrand || row.brandHint || row.brandFamily || null;

  const relationships = [];
  if (registrant) {
    relationships.push({
      role: ENTITY_ROLE.FILING_REGISTRANT,
      name: registrant,
      cik: row.cik || null,
      confidence: "HIGH",
      note: "Registrant of the SEC filing that disclosed the hotel — not automatically the legal owner or manager",
    });
  }
  if (brand) {
    relationships.push({
      role: ENTITY_ROLE.BRAND,
      name: brand,
      confidence: "MEDIUM",
      note: "Brand affiliation as disclosed/detected — may be historical",
    });
  }
  if (row.owner) {
    relationships.push({
      role: ENTITY_ROLE.LEGAL_OWNER,
      name: row.owner,
      confidence: row.ownerVerified ? "HIGH" : "LOW",
    });
  }
  if (row.operator || row.managementCompany) {
    relationships.push({
      role: ENTITY_ROLE.MANAGEMENT_COMPANY,
      name: row.operator || row.managementCompany,
      confidence: row.operatorVerified ? "HIGH" : "LOW",
    });
  }
  if (row.franchisor) {
    relationships.push({
      role: ENTITY_ROLE.FRANCHISOR,
      name: row.franchisor,
      confidence: "MEDIUM",
    });
  }

  return {
    version: ENTITY_ATTRIBUTION_V2,
    propertyName: row.hotelName || null,
    relationships,
    warnings: [
      ...(registrant && !row.owner
        ? ["Do not treat filing registrant as legal owner without explicit ownership language"]
        : []),
      ...(registrant && !row.operator && !row.managementCompany
        ? ["Do not treat filing registrant as management company without explicit management language"]
        : []),
    ],
  };
}

/**
 * Detect city/state carry-over risks: city equals previous hotel fragment, etc.
 */
export function locationAttributionLooksSuspicious(row = {}, prevRow = null) {
  const issues = [];
  const city = String(row.city || "");
  const name = String(row.hotelName || "");
  if (/\b(Owner|LLC|Inc|Corp|LP|Trust|Company)\b/i.test(city)) {
    issues.push("city_looks_like_legal_entity");
  }
  if (prevRow?.city && city && city === prevRow.city && name === prevRow.hotelName) {
    issues.push("identical_to_previous_row");
  }
  // "Diego" alone after "San Diego" split failure
  if (/^(Diego|York|Angeles|Jose|Antonio|Francisco|Louis|Paul)$/i.test(city)) {
    issues.push("city_fragment_possible_table_split");
  }
  if (/^(AZ|CA|TX|NV|MD|WA|FL|NY|IL|CO)\s+/i.test(name) && !row._statePrefixNormalized) {
    issues.push("state_column_bleed_into_name");
  }
  return { suspicious: issues.length > 0, issues };
}
