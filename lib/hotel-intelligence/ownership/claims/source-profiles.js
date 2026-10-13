/**
 * Packet 2.5A — Source-class extraction profiles (bounded; existing corpora only).
 */

export const SOURCE_PROFILE_VERSION = "claim-source-profile-v1";

export const SOURCE_PROFILES = Object.freeze({
  ANNUAL_REPORT: {
    prioritize: [
      "ENTITY_IS_OWNER_OF_PROPERTY",
      "ENTITY_HOLDS_TITLE",
      "ENTITY_OPERATES_HOTEL",
      "ENTITY_OWNS_PERCENT_OF_ENTITY",
      "ENTITY_IS_SUBSIDIARY_OF_ENTITY",
      "PROPERTY_IS_ADJACENT_TO",
    ],
    demote: ["PERSON_HAS_TITLE"],
  },
  SECURITIES_FILING: {
    prioritize: [
      "ENTITY_IS_OWNER_OF_PROPERTY",
      "ENTITY_CONTROLS_ENTITY",
      "ENTITY_ACQUIRED_ENTITY_OR_ASSET",
    ],
    demote: ["HOTEL_CURRENT_BRAND"],
  },
  BRAND_PRESS_RELEASE: {
    prioritize: [
      "HOTEL_ANNOUNCED_BRAND",
      "ENTITY_DEVELOPED_PROPERTY",
      "ENTITY_SPONSORED_PROJECT",
    ],
    demote: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  COMPANY_PORTFOLIO: {
    prioritize: ["ENTITY_OPERATES_HOTEL", "ENTITY_IS_OWNER_OF_PROPERTY"],
    demote: [],
    note: "portfolio_listing_weaker_than_explicit_ownership",
  },
  TRANSACTION_RELEASE: {
    prioritize: [
      "ENTITY_ACQUIRED_ENTITY_OR_ASSET",
      "ENTITY_OWNS_PERCENT_OF_ENTITY",
      "ENTITY_SOLD_ENTITY_OR_ASSET",
    ],
    demote: [],
  },
  DEVELOPER_SITE: {
    prioritize: ["ENTITY_DEVELOPED_PROPERTY", "ENTITY_SPONSORED_PROJECT"],
    demote: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  CORPORATE_LEADERSHIP_PAGE: {
    prioritize: ["PERSON_HAS_TITLE", "PERSON_REPRESENTS_ORGANIZATION"],
    demote: ["PERSON_HAS_SIGNING_AUTHORITY", "ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  NEWS_ARTICLE: {
    prioritize: [
      "HOTEL_ANNOUNCED_BRAND",
      "ENTITY_ACQUIRED_ENTITY_OR_ASSET",
      "PROPERTY_ALIAS",
    ],
    demote: [],
  },
  FIRST_PARTY_HOTEL: {
    prioritize: [
      "HOTEL_CURRENT_BRAND",
      "HOTEL_FORMER_BRAND",
      "PROPERTY_WAS_REBRANDED",
      "PROPERTY_HAS_ROOM_COUNT",
    ],
    demote: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  CENSUS: {
    prioritize: ["ENTITY_OPERATES_HOTEL"],
    demote: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  DEFAULT: {
    prioritize: [],
    demote: [],
  },
});

/**
 * @param {object} doc
 */
export function profileForDocument(doc) {
  const auth = String(doc.authority || doc.source_type || "").toLowerCase();
  if (/annual|reporte.?anual/.test(auth)) return "ANNUAL_REPORT";
  if (/securities|exchange.?issuer|bmv/.test(auth)) return "SECURITIES_FILING";
  if (/brand.?press|newsroom/.test(auth)) return "BRAND_PRESS_RELEASE";
  if (/relevant.?event|transaction/.test(auth)) return "TRANSACTION_RELEASE";
  if (/first.?party.?hotel|hotel.?official|hotel_first_party/.test(auth)) {
    return "FIRST_PARTY_HOTEL";
  }
  if (/census/.test(auth)) return "CENSUS";
  if (/professional|leadership|linkedin|biography/.test(auth)) {
    return "CORPORATE_LEADERSHIP_PAGE";
  }
  if (/trade.?press|news|secondary/.test(auth)) return "NEWS_ARTICLE";
  if (/portfolio|first.?party.?hotel.?page|gsf.?first/.test(auth)) {
    return "COMPANY_PORTFOLIO";
  }
  return "DEFAULT";
}

export function getSourceProfile(doc) {
  return SOURCE_PROFILES[profileForDocument(doc)] || SOURCE_PROFILES.DEFAULT;
}
