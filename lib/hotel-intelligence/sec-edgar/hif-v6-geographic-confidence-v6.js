/**
 * HIF V6 — Physical hotel geography confidence (founder 2026-11-21).
 * Physical location ≠ filing jurisdiction / issuer domicile / brand HQ.
 * Worldwide eligible; unknown country must not be guessed.
 */

export const HIF_V6_GEOGRAPHIC_CONFIDENCE = "hif-v6-geographic-confidence-v6";

export const GEO_CLASS = Object.freeze({
  VERIFIED: "VERIFIED",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  UNRESOLVED: "UNRESOLVED",
});

/** Prefer explicit locality cues — avoid Inn/OR/ME syllable false positives. */
const US_STATE_EXPLICIT =
  /(?:,|\()\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b/;

const US_STATE_SAFE_TRAILING =
  /\b(TX|CA|FL|NY|PA|IL|OH|GA|NC|VA|WA|CO|AZ|NV|TN|MO|WI|MN|MI|MD|MA|NJ|CT|SC|AL|LA|KY|OK|OR|UT|IA|AR|MS|KS|NM|NE|WV|ID|HI|NH|ME|MT|RI|DE|SD|ND|AK|VT|WY|DC)\s*$/i;

const CITY_HINT =
  /\b(New York|Los Angeles|Chicago|Houston|Phoenix|Philadelphia|San Antonio|San Diego|Dallas|Austin|Jacksonville|Fort Worth|Columbus|Charlotte|San Francisco|Indianapolis|Seattle|Denver|Boston|El Paso|Nashville|Detroit|Oklahoma City|Portland|Las Vegas|Memphis|Louisville|Baltimore|Milwaukee|Albuquerque|Tucson|Fresno|Sacramento|Mesa|Kansas City|Atlanta|Colorado Springs|Raleigh|Omaha|Miami|Oakland|Minneapolis|Tulsa|Wichita|Arlington|Tampa|New Orleans|Cleveland|Honolulu|Anaheim|Pearland|Marysville|Ballantyne|Fredericksburg|Pensacola|Bloomington|Neptune|Grovetown|Wadsworth|Savannah|Exton|London|Manchester|Birmingham|Toronto|Vancouver|Montreal|Mexico City|Cancun|Guadalajara|Sao Paulo|Rio de Janeiro|Bras[ií]lia)\b/i;

/**
 * Infer coarse location tokens from hotel name / address fields only.
 * Never invent a country code from filing jurisdiction alone.
 */
export function extractLocationHints(row = {}) {
  const blob = [
    row.hotelName,
    row.propertyName,
    row.address,
    row.propertyAddress,
    row.city,
    row.state,
    row.market,
    row.submarket,
    ...(row.notes || []),
  ]
    .filter(Boolean)
    .join(" ");

  const stateMatch = blob.match(US_STATE_EXPLICIT) || blob.match(US_STATE_SAFE_TRAILING);
  const cityMatch = blob.match(CITY_HINT);
  return {
    stateProvince: row.state || row.stateProvince || (stateMatch ? String(stateMatch[1]).toUpperCase() : null),
    city: row.city || row.hotelMarket || (cityMatch ? cityMatch[1] : null),
    market: row.market || row.submarket || null,
    address: row.address || row.propertyAddress || null,
    hasUsStateToken: Boolean(stateMatch),
    hasCityToken: Boolean(cityMatch),
  };
}

/**
 * Classify physical geography confidence.
 * Source jurisdiction alone → not VERIFIED.
 */
export function classifyPhysicalGeography(row = {}) {
  const declared = String(row.hotelCountry || row.physicalCountry || row.country || "")
    .trim()
    .toUpperCase();
  const sourceJ =
    String(row.sourceJurisdiction || row.filingJurisdiction || "")
      .trim()
      .toUpperCase() || null;
  const hints = extractLocationHints(row);
  const evidence = [];

  if (!declared || declared === "UNATTRIBUTED" || declared === "UNKNOWN") {
    return {
      physicalCountry: null,
      stateProvince: hints.stateProvince,
      city: hints.city,
      market: hints.market,
      verifiedPropertyAddress: hints.address,
      geographicClass: GEO_CLASS.UNRESOLVED,
      geographicConfidence: "UNRESOLVED",
      geographicEvidence: ["NO_PHYSICAL_COUNTRY_DECLARED"],
      sourceJurisdiction: sourceJ,
      note: "Unknown physical country must not be guessed from filing jurisdiction",
    };
  }

  // Explicit verified address / geo evidence on the record
  if (hints.address && /[0-9]/.test(hints.address)) {
    evidence.push("PROPERTY_ADDRESS_PRESENT");
  }
  if (hints.hasUsStateToken && declared === "US") evidence.push("US_STATE_TOKEN_IN_PROPERTY_IDENTITY");
  if (hints.hasCityToken) evidence.push("CITY_TOKEN_IN_PROPERTY_IDENTITY");
  if (row.geoVerified === true || row.geographicClass === "VERIFIED") {
    evidence.push("PRIOR_GEO_VERIFIED_FLAG");
  }
  if (row.sourceFamily === "SEC_CMBS" && declared === "US" && (hints.hasUsStateToken || hints.hasCityToken)) {
    evidence.push("CMBS_US_PROPERTY_IDENTITY_CUES");
  }
  if (row.sourceFamily === "SEC_CMBS" && declared === "US" && !hints.hasUsStateToken && !hints.hasCityToken) {
    evidence.push("CMBS_COUNTRY_ATTRIBUTED_WITHOUT_LOCALITY_CUES");
  }

  let geographicClass = GEO_CLASS.REVIEW_REQUIRED;
  if (
    evidence.includes("PRIOR_GEO_VERIFIED_FLAG") ||
    evidence.includes("PROPERTY_ADDRESS_PRESENT") ||
    (declared === "US" && evidence.includes("US_STATE_TOKEN_IN_PROPERTY_IDENTITY")) ||
    (declared === "US" && evidence.includes("CITY_TOKEN_IN_PROPERTY_IDENTITY") && evidence.includes("CMBS_US_PROPERTY_IDENTITY_CUES"))
  ) {
    geographicClass = GEO_CLASS.VERIFIED;
  } else if (declared && evidence.includes("CMBS_COUNTRY_ATTRIBUTED_WITHOUT_LOCALITY_CUES")) {
    geographicClass = GEO_CLASS.REVIEW_REQUIRED;
  } else if (declared && sourceJ && declared !== sourceJ && !hints.hasCityToken && !hints.hasUsStateToken) {
    geographicClass = GEO_CLASS.REVIEW_REQUIRED;
    evidence.push("PHYSICAL_COUNTRY_DIFFERS_FROM_SOURCE_JURISDICTION_WITHOUT_LOCALITY");
  } else if (declared) {
    geographicClass = GEO_CLASS.REVIEW_REQUIRED;
    evidence.push("COUNTRY_DECLARED_LOCALITY_WEAK");
  }

  return {
    physicalCountry: declared,
    stateProvince: hints.stateProvince,
    city: hints.city,
    market: hints.market,
    verifiedPropertyAddress: hints.address,
    geographicClass,
    geographicConfidence: geographicClass,
    geographicEvidence: evidence,
    sourceJurisdiction: sourceJ,
    note: "Physical hotel location used for coverage; source jurisdiction preserved separately",
  };
}
