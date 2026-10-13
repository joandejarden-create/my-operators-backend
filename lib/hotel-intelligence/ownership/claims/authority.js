/**
 * Source authority is claim-type-specific — not one universal ranking.
 * Scores are 0–1 evidence quality weights for reconciliation / promotion.
 */

export const CLAIM_AUTHORITY_VERSION = "claim-source-authority-v1";

/** @type {Record<string, Record<string, number>>} */
const AUTHORITY_BY_CLAIM = Object.freeze({
  ENTITY_IS_OWNER_OF_PROPERTY: {
    securities_filing: 0.95,
    issuer_annual_report: 0.95,
    issuer_relevant_event: 0.9,
    corporate_registry: 0.85,
    first_party_hotel_page: 0.55,
    brand_press_release: 0.35,
    operator_website: 0.3,
    trade_press: 0.45,
    executive_biography: 0.2,
    professional_profile: 0.15,
    secondary_listing: 0.2,
    other: 0.25,
  },
  ENTITY_OWNS_PERCENT_OF_ENTITY: {
    securities_filing: 0.95,
    issuer_annual_report: 0.95,
    issuer_relevant_event: 0.9,
    brand_press_release: 0.7,
    trade_press: 0.55,
    other: 0.3,
  },
  ENTITY_PARTICIPATES_IN_JV: {
    securities_filing: 0.9,
    issuer_relevant_event: 0.9,
    brand_press_release: 0.75,
    trade_press: 0.55,
    other: 0.3,
  },
  ENTITY_OPERATES_HOTEL: {
    operator_website: 0.9,
    first_party_hotel_page: 0.85,
    issuer_annual_report: 0.85,
    brand_press_release: 0.6,
    dealality_census: 0.7,
    trade_press: 0.5,
    other: 0.35,
  },
  ENTITY_MANAGES_HOTEL: {
    operator_website: 0.9,
    issuer_annual_report: 0.85,
    dealality_census: 0.7,
    other: 0.35,
  },
  HOTEL_CURRENT_BRAND: {
    first_party_hotel_site: 0.9,
    brand_press_release: 0.7,
    first_party_hotel_page: 0.85,
    dealality_census: 0.65,
    other: 0.4,
  },
  HOTEL_ANNOUNCED_BRAND: {
    brand_press_release: 0.95,
    trade_press: 0.7,
    issuer_relevant_event: 0.8,
    other: 0.4,
  },
  HOTEL_FORMER_BRAND: {
    first_party_hotel_site: 0.75,
    trade_press: 0.65,
    brand_press_release: 0.55,
    other: 0.4,
  },
  PERSON_HAS_TITLE: {
    executive_biography: 0.85,
    professional_profile: 0.7,
    issuer_annual_report: 0.8,
    secondary_listing: 0.45,
    other: 0.35,
  },
  PERSON_HAS_SIGNING_AUTHORITY: {
    securities_filing: 0.9,
    corporate_registry: 0.85,
    executive_biography: 0.25,
    professional_profile: 0.15,
    secondary_listing: 0.1,
    other: 0.2,
  },
  ENTITY_DEVELOPED_PROPERTY: {
    trade_press: 0.7,
    issuer_relevant_event: 0.8,
    brand_press_release: 0.65,
    other: 0.4,
  },
  ENTITY_SPONSORED_PROJECT: {
    brand_press_release: 0.75,
    trade_press: 0.65,
    securities_filing: 0.8,
    other: 0.35,
  },
});

const DEFAULT_BY_SOURCE = Object.freeze({
  securities_filing: 0.8,
  issuer_annual_report: 0.8,
  issuer_relevant_event: 0.75,
  corporate_registry: 0.7,
  brand_press_release: 0.55,
  first_party_hotel_page: 0.55,
  first_party_hotel_site: 0.55,
  operator_website: 0.55,
  dealality_census: 0.5,
  trade_press: 0.45,
  exchange_issuer_profile: 0.7,
  executive_biography: 0.4,
  professional_profile: 0.35,
  secondary_listing: 0.25,
  other: 0.3,
});

/**
 * @param {string} claimType
 * @param {string} sourceAuthorityKey — normalized source class / authority label
 * @returns {number}
 */
export function authorityForClaimType(claimType, sourceAuthorityKey) {
  const key = normalizeAuthorityKey(sourceAuthorityKey);
  const byClaim = AUTHORITY_BY_CLAIM[claimType];
  if (byClaim && byClaim[key] != null) return byClaim[key];
  if (DEFAULT_BY_SOURCE[key] != null) return DEFAULT_BY_SOURCE[key];
  return DEFAULT_BY_SOURCE.other;
}

/**
 * Normalize heterogeneous authority labels from corpora.
 * @param {string} raw
 */
export function normalizeAuthorityKey(raw) {
  const s = String(raw || "")
    .toLowerCase()
    .trim();
  if (!s) return "other";
  if (/securities|bmv|annual.?report|reporte.?anual|issuer_annual/.test(s)) {
    return /relevant.?event|evento.?relevante/.test(s)
      ? "issuer_relevant_event"
      : /annual|reporte.?anual/.test(s)
        ? "issuer_annual_report"
        : "securities_filing";
  }
  if (/relevant.?event|evento.?relevante/.test(s)) return "issuer_relevant_event";
  if (/brand.?press|newsroom|hyatt.?news/.test(s)) return "brand_press_release";
  if (/first.?party.?hotel.?site|hotel.?official|hotel_first_party/.test(s)) {
    return "first_party_hotel_site";
  }
  if (/first.?party.?hotel.?page|gsf.?first.?party/.test(s)) {
    return "first_party_hotel_page";
  }
  if (/operator|management.?company/.test(s)) return "operator_website";
  if (/census/.test(s)) return "dealality_census";
  if (/trade.?press|press|maritur|centro.?urbano/.test(s)) return "trade_press";
  if (/linkedin|professional/.test(s)) return "professional_profile";
  if (/biography|bio|executive/.test(s)) return "executive_biography";
  if (/tripadvisor|listing|ota/.test(s)) return "secondary_listing";
  if (/exchange.?issuer|bmv.?issuer/.test(s)) return "exchange_issuer_profile";
  if (/registry|gleif|rfc|cnpj/.test(s)) return "corporate_registry";
  return "other";
}
