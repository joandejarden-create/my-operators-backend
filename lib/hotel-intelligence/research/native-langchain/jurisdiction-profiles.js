/**
 * Packet 2.8C-1 — JurisdictionResearchProfile (provider-neutral).
 * Same quality standard; different source paths.
 */

export const JURISDICTION_RESEARCH_PROFILE_VERSION = "jurisdiction-research-profile-v1";

/** @type {Record<string, object>} */
export const JURISDICTION_PROFILES = Object.freeze({
  MX: {
    country_code: "MX",
    country: "Mexico",
    languages: ["es", "en"],
    primary_query_language: "es",
    legal_entity_forms: ["S.A.B. de C.V.", "S.A. de C.V.", "S. de R.L. de C.V.", "S. de R.L."],
    corporate_sources: ["BMV", "CNBV", "investor relations", "reporte anual", "hechos relevantes"],
    government_sources: ["PROFECO", "Registro Público de la Propiedad (paid/local)", "SE"],
    media_sources: ["local business press", "hospitality trade MX"],
    disclosure_limitations: [
      "deed/folio often not public online",
      "UBO often private",
      "PropCo may appear only in related-party notes",
    ],
    source_ladder: [
      "organization_evidence_corpus",
      "issuer_filings_ir",
      "official_company_site",
      "brand_operator_first_party",
      "government_consumer_filings",
      "reputable_trade_press",
      "local_press",
    ],
    query_families: {
      ownership: ["{hotel} propietario", "{company} reporte anual {year}", "{hotel} Inmobiliaria"],
      brand_history: ["{hotel} marca anterior", "{hotel} rebrand", "{hotel} Breathless OR Hilton OR Sheraton"],
      transactions: ["{hotel} adquisición", "{hotel} venta hotel", "{company} hecho relevante"],
      development: ["{hotel} apertura", "{hotel} construcción", "{hotel} inversión"],
      people: ["{person} {company} LinkedIn", "director {company} hotel"],
      operator: ["{hotel} operado por", "{hotel} Aimbridge OR GSF OR CAPITALI"],
    },
  },
  BM: {
    country_code: "BM",
    country: "Bermuda",
    languages: ["en"],
    primary_query_language: "en",
    legal_entity_forms: ["Limited", "Ltd.", "Holdings Limited"],
    corporate_sources: ["company registry (limited public)", "first-party resort", "lender disclosures"],
    government_sources: ["Bermuda Tourism Authority / Tourism Order", "Companies Registry (access-limited)"],
    media_sources: ["Royal Gazette", "Bernews", "Caribbean hospitality press"],
    disclosure_limitations: [
      "beneficial ownership often opaque",
      "deed-level PropCo may require paid registry",
      "operator transitions may appear only in press",
    ],
    source_ladder: [
      "organization_evidence_corpus",
      "first_party_resort",
      "tourism_authority_orders",
      "local_reputable_press",
      "lender_or_financing_mentions",
      "trade_press",
    ],
    query_families: {
      ownership: ["{hotel} owner OR acquired OR Dovetail", "{propco} Bermuda", "Cambridge Beaches Holdings"],
      brand_history: ["Cambridge Beaches independent brand"],
      transactions: ["Cambridge Beaches acquisition", "Butterfield Cambridge Beaches"],
      development: ["Cambridge Beaches renovation Tourism Order"],
      people: ["{person} Dovetail Hospitality Bermuda"],
      operator: ["Cambridge Beaches Benchmark OR Pyramid OR Dovetail operator"],
    },
  },
  DEFAULT: {
    country_code: "XX",
    country: "Unknown",
    languages: ["en"],
    primary_query_language: "en",
    legal_entity_forms: [],
    corporate_sources: ["official company site", "trade press"],
    government_sources: [],
    media_sources: ["reputable press"],
    disclosure_limitations: ["jurisdiction profile incomplete"],
    source_ladder: ["organization_evidence_corpus", "first_party", "press"],
    query_families: {
      ownership: ["{hotel} owner"],
      brand_history: ["{hotel} rebrand"],
      transactions: ["{hotel} sale OR acquisition"],
      development: ["{hotel} renovation OR opening"],
      people: ["{hotel} general manager OR owner"],
      operator: ["{hotel} managed by OR operated by"],
    },
  },
});

export function resolveJurisdictionProfile(countryOrCode) {
  const raw = String(countryOrCode || "").trim().toUpperCase();
  if (raw === "MX" || raw === "MEXICO" || raw.includes("MEXICO")) return JURISDICTION_PROFILES.MX;
  if (raw === "BM" || raw === "BERMUDA" || raw.includes("BERMUDA")) return JURISDICTION_PROFILES.BM;
  return JURISDICTION_PROFILES.DEFAULT;
}

export function buildJurisdictionAwareQueries({ domain, hotel_name, company, propco, year, profile }) {
  const p = profile || JURISDICTION_PROFILES.DEFAULT;
  const key = String(domain || "ownership")
    .toLowerCase()
    .replace(/_gap$/, "")
    .replace(/brand_history/, "brand_history")
    .replace(/property_fundamentals/, "ownership");
  const familyKey =
    key.includes("brand")
      ? "brand_history"
      : key.includes("transact")
        ? "transactions"
        : key.includes("develop")
          ? "development"
          : key.includes("people") || key.includes("person")
            ? "people"
            : key.includes("operator")
              ? "operator"
              : "ownership";
  const templates = p.query_families[familyKey] || p.query_families.ownership;
  const y = year || new Date().getFullYear();
  return templates.map((t) =>
    t
      .replaceAll("{hotel}", hotel_name || "")
      .replaceAll("{company}", company || hotel_name || "")
      .replaceAll("{propco}", propco || "")
      .replaceAll("{person}", company || "")
      .replaceAll("{year}", String(y))
      .replace(/\s+/g, " ")
      .trim()
  );
}
