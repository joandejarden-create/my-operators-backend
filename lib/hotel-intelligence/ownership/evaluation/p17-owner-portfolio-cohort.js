/**
 * P1.7 Test B — five HIGH/VERIFIED economic-owner organizations from public evidence.
 * Selection is first-party / listed-issuer websites — not CoStar portfolio size.
 */

export const OWNERSHIP_P17_OWNER_PORTFOLIO_VERSION = "ownership-p17-owner-portfolio-v1";

/**
 * Public CALA-relevant owners. URLs are independently fetchable first-party pages.
 * GTM seed URLs reused as methodology only — no CoStar True Owner values.
 */
export const P17_CONFIRMED_PUBLIC_OWNERS = Object.freeze([
  {
    slug: "fibrahotel",
    legal_name: "Concentradora Fibra Hotelera Mexicana, S.A. de C.V.",
    display_name: "FibraHotel",
    entity_type: "reit",
    country: "Mexico",
    jurisdiction: "MX",
    verification_status: "high",
    evidence_basis: "BMV-listed hotel REIT; first-party IR / corporate site",
    website: "https://www.fibrahotel.com",
    portfolio_urls: ["https://www.fibrahotel.com", "https://fibrahotel.mx"],
    relationship_prior: "OWNED_BY",
    cala_expected: true,
  },
  {
    slug: "fibrainn",
    legal_name: "Fibra Inn",
    display_name: "Fibra Inn",
    entity_type: "reit",
    country: "Mexico",
    jurisdiction: "MX",
    verification_status: "high",
    evidence_basis: "BMV-listed hotel REIT (FINN13); first-party IR",
    website: "https://fibrainn.mx",
    portfolio_urls: [
      "https://fibrainn.mx",
      "https://fibrainn.mx/en/investors/home",
    ],
    relationship_prior: "OWNED_BY",
    cala_expected: true,
  },
  {
    slug: "grupo-hotelero-santa-fe",
    legal_name: "Grupo Hotelero Santa Fe, S.A.B. de C.V.",
    display_name: "Grupo Hotelero Santa Fe",
    entity_type: "company",
    country: "Mexico",
    jurisdiction: "MX",
    verification_status: "high",
    evidence_basis: "BMV ticker HOTEL; first-party corporate site",
    website: "https://gsf-hotels.com",
    portfolio_urls: ["https://gsf-hotels.com"],
    relationship_prior: "OWNED_BY",
    cala_expected: true,
  },
  {
    slug: "jhsf",
    legal_name: "JHSF Participações S.A.",
    display_name: "JHSF",
    entity_type: "company",
    country: "Brazil",
    jurisdiction: "BR",
    verification_status: "high",
    evidence_basis: "B3-listed; first-party site / IR; Fasano hotel assets",
    website: "https://www.jhsf.com.br",
    portfolio_urls: ["https://www.jhsf.com.br", "https://ri.jhsf.com.br"],
    relationship_prior: "OWNED_BY",
    cala_expected: true,
  },
  {
    slug: "essendi",
    legal_name: "Essendi",
    display_name: "Essendi (formerly AccorInvest)",
    entity_type: "institution",
    country: "France",
    jurisdiction: "FR",
    verification_status: "high",
    evidence_basis: "Public hospitality owner (ex-AccorInvest); first-party site; CALA divestiture risk",
    website: "https://www.essendi.com",
    portfolio_urls: ["https://www.essendi.com"],
    relationship_prior: "OWNED_BY",
    cala_expected: true,
  },
]);
