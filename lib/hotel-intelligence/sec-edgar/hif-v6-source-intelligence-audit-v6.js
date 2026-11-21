/**
 * HIF V6 Source Intelligence Audit — discovery patterns derived from evidence.
 * Name-free discovery preference: document-type / contractual-phrase / issuer-first.
 * CUSTOMER_BLOCKED. No ownership enrichment campaigns.
 */

export const HIF_V6_SOURCE_INTELLIGENCE_AUDIT_V6 = "hif-v6-source-intelligence-audit-v6";

/** Strategies compared in the audit (founder §4). */
export const DISCOVERY_STRATEGIES = Object.freeze({
  A_HOTEL_NAME_FIRST: "A_HOTEL_NAME_FIRST",
  B_CONTRACTUAL_PHRASE_FIRST: "B_CONTRACTUAL_PHRASE_FIRST",
  C_DOCUMENT_TYPE_SECTION_FIRST: "C_DOCUMENT_TYPE_SECTION_FIRST",
  D_ISSUER_PORTFOLIO_FIRST: "D_ISSUER_PORTFOLIO_FIRST",
  E_METADATA_RELATED_EXHIBIT: "E_METADATA_RELATED_EXHIBIT",
  F_HYBRID: "F_HYBRID",
});

/**
 * Evidence-derived taxonomy: what actually worked in prior HIF waves.
 * Distinguishes successful CMBS schedule docs from prose/narrative false positives.
 */
export const SOURCE_FAMILY_TAXONOMY = Object.freeze({
  US_SEC_CMBS_424B: {
    sourceFamily: "SEC_CMBS",
    filingForms: ["424B2", "424B5", "424H", "ABS-15G"],
    highYieldFileNamePatterns: [
      /n\d+_x\d+-424b2\.htm/i,
      /n\d+_424b2-x\d+\.htm/i,
      /n\d+-x\d+_424b2\.htm/i,
      /n\d+_x\d+-424h\.htm/i,
    ],
    structuralSignals: {
      sectionHeadings: [
        "Annex A",
        "Mortgage Pool Characteristics",
        "Hotel Properties",
        "Significant Loan Summary",
        "Description of the Mortgage Pool",
        "Property Characteristics",
      ],
      tableHeaders: [
        // Highest-yield observed header (Wave 2 verified annexes):
        "Expiration/Termination of Related License/ Franchise Agreement, Operating Agreement or Management Agreement",
        "Franchise Expiration Date",
        "Franchise Expiration",
        "Management Agreement Expiration",
        "License Agreement Expiration",
        "Property Name",
        "Mortgage Loan Cut-off Date Balance",
        "Property Type",
        "Brand",
        "Flag",
        "Number of Rooms",
        "Cut-off Date Balance",
        "Maturity Date of the Related Mortgage Loan",
      ],
      narrativePhrases: [
        "franchise agreement expires",
        "franchise expiration",
        "management agreement expires",
        "hotel management agreement",
        "brand affiliation",
        "franchise fee",
      ],
      falsePositiveHeaders: [
        "Loan Maturity Date",
        "Anticipated Repayment Date",
        "ARD",
        "Ground Lease Expiration",
      ],
    },
    discoveryWithoutHotelName: {
      preferredStrategies: [
        DISCOVERY_STRATEGIES.C_DOCUMENT_TYPE_SECTION_FIRST,
        DISCOVERY_STRATEGIES.B_CONTRACTUAL_PHRASE_FIRST,
        DISCOVERY_STRATEGIES.E_METADATA_RELATED_EXHIBIT,
      ],
      eftsQueries: [
        {
          id: "cmbs_expiration_termination_related_license",
          q: '"Expiration/Termination of Related License"',
          forms: "424B2",
          strategy: DISCOVERY_STRATEGIES.B_CONTRACTUAL_PHRASE_FIRST,
          notes:
            "Primary high-yield column header observed in verified Wave 2 annexes (license/franchise/operating/management)",
        },
        {
          id: "cmbs_related_license_franchise_agreement",
          q: '"Related License/ Franchise Agreement"',
          forms: "424B2",
          strategy: DISCOVERY_STRATEGIES.C_DOCUMENT_TYPE_SECTION_FIRST,
        },
        {
          id: "cmbs_franchise_expiration_date_col",
          q: '"Franchise Expiration Date"',
          forms: "424B2",
          strategy: DISCOVERY_STRATEGIES.B_CONTRACTUAL_PHRASE_FIRST,
          notes:
            "Secondary; many hits are narrative key-money/cash-sweep prose — require HTML table header cells",
        },
        {
          id: "cmbs_mgmt_expiration_date_col",
          q: '"Management Agreement Expiration Date"',
          forms: "424B2",
          strategy: DISCOVERY_STRATEGIES.B_CONTRACTUAL_PHRASE_FIRST,
        },
        {
          id: "cmbs_related_exhibit_424h_seed",
          q: '"Expiration/Termination of Related License"',
          forms: "424H",
          strategy: DISCOVERY_STRATEGIES.E_METADATA_RELATED_EXHIBIT,
        },
      ],
    },
    extractionLimitations: [
      "Many 424B2 hits are narrative-only (Hotel Properties prose) without property-level franchise columns",
      "Bare phrase 'Franchise Expiration' frequently denotes Franchise Expiration Reserve/Trigger — not a date column",
      "'Franchise Expiration Date' often appears in key-money / cash-sweep narrative without a schedule table",
      "EFTS accession CIK (1539497 filing agent) is NOT the archive path CIK — use depositor/issuer ciks[] and retry alternates on 404",
      "Nested HTML tables often lose column alignment after stripHtml",
      "Loan maturity columns must never be classified as franchise/management expirations",
      "PDF annexes linked from HTML TOC are frequently not followed by current extractors",
      "Current HTML extractor requires HOTEL_NAME_RE brand-like tokens — independent hotels may be under-extracted",
    ],
    falsePositiveSources: [
      "Franchise Expiration Reserve / Franchise Expiration Trigger Event (cash management)",
      "Narrative 'franchise expiration date' in key-money / FF&E sweep clauses",
      "EFTS hits with wrong CIK path (NoSuchKey / stub XML)",
      "Country-keyword SEC search mislabeling US hotels as MX/BR",
      "Brand-only names (Embassy Suites, Hilton)",
      "Same-Property / system-wide aggregates",
      "EX-10 boilerplate without property identity",
    ],
  },

  US_SEC_EX10: {
    sourceFamily: "SEC_EX10",
    filingForms: ["8-K", "10-K", "10-Q"],
    fileTypeFilter: "EX-10.*",
    structuralSignals: {
      sectionHeadings: [
        "ARTICLE",
        "Term",
        "Expiration",
        "Franchise Agreement",
        "Hotel Management Agreement",
        "License Agreement",
      ],
      tableHeaders: ["Property", "Hotel", "Expiration Date", "Initial Term"],
      narrativePhrases: [
        "initial term of",
        "shall expire on",
        "franchise agreement dated",
        "management agreement dated",
      ],
    },
    discoveryWithoutHotelName: {
      preferredStrategies: [
        DISCOVERY_STRATEGIES.E_METADATA_RELATED_EXHIBIT,
        DISCOVERY_STRATEGIES.B_CONTRACTUAL_PHRASE_FIRST,
      ],
      eftsQueries: [
        {
          id: "ex10_franchise_agreement",
          q: '"Franchise Agreement" hotel (expire OR expiration OR term)',
          forms: "8-K",
          strategy: DISCOVERY_STRATEGIES.B_CONTRACTUAL_PHRASE_FIRST,
        },
        {
          id: "ex10_hma",
          q: '"Hotel Management Agreement" OR "Management Agreement" hotel (expire OR term)',
          forms: "8-K",
          strategy: DISCOVERY_STRATEGIES.B_CONTRACTUAL_PHRASE_FIRST,
        },
      ],
    },
    extractionLimitations: [
      "EFTS forms=EX-10 alone often returns 0; search parent 8-K/10-K then file_type EX-10.*",
      "Name NER on exhibits produces high quarantine rates without schedule structure",
    ],
  },

  US_SEC_8K_COMMERCIAL_EVENTS: {
    sourceFamily: "SEC_8K_OR_DISCLOSURE",
    filingForms: ["8-K"],
    structuralSignals: {
      narrativePhrases: [
        "reflag",
        "brand conversion",
        "franchise termination",
        "non-renewal",
        "operator replacement",
        "appointed as manager",
      ],
    },
    discoveryWithoutHotelName: {
      preferredStrategies: [DISCOVERY_STRATEGIES.B_CONTRACTUAL_PHRASE_FIRST],
      notes: "Yield wave validated reflag phrase-first without hotel names; property NER still required after retrieval",
    },
  },

  MX_BMV_EMISNET: {
    sourceFamily: "BMV_EMISNET",
    filingClasses: [
      "Evento Relevante",
      "Reporte Anual (411000-AR)",
      "Reporte Trimestral",
      "Información Anual",
      "Prospecto / suplemento",
    ],
    spanishTerminology: {
      franchise: ["franquicia", "contrato de franquicia", "vencimiento de franquicia", "cancelación de franquicia"],
      management: [
        "contrato de administración",
        "contrato de operación hotelera",
        "Hotel Management Agreement",
        "HMA",
        "operador hotelero",
        "contratos celebrados con los operadores",
      ],
      conversion: [
        "cambio de marca",
        "reasignación de marca",
        "terminación del contrato de operación",
        "rescisión",
        "no renovación",
      ],
      portfolio: ["portafolio hotelero", "activos hoteleros", "propiedades hoteleras"],
    },
    structuralSignals: {
      sectionHeadings: [
        "Contratos con operadores",
        "Patentes, Licencias, Marcas y Otros Contratos",
        "Factores de Riesgo",
        "Eventos Relevantes",
      ],
    },
    discoveryWithoutHotelName: {
      preferredStrategies: [
        DISCOVERY_STRATEGIES.D_ISSUER_PORTFOLIO_FIRST,
        DISCOVERY_STRATEGIES.C_DOCUMENT_TYPE_SECTION_FIRST,
      ],
      notes:
        "Start from hospitality emisoras (FibraHotel, Grupo Hotelero Santa Fe, RLH, etc.) → Reporte Anual / Evento Relevante; do not keyword-translate 'franchise expiration' literally as sole query",
    },
    falsePositiveSources: [
      "SEC filings mentioning Mexico as demand market for US hotels",
      "Condo / residential associated assets without hotel commercial event",
    ],
  },

  BR_CVM: {
    sourceFamily: "CVM",
    documentCategories: [
      "Fato Relevante",
      "Comunicado ao Mercado",
      "Formulário de Referência",
      "Prospecto",
      "Demonstrações Financeiras",
      "IPE (Informações Periódicas e Eventuais)",
    ],
    portugueseTerminology: {
      management: [
        "contrato de gestão hoteleira",
        "contrato de administração hoteleira",
        "operador hoteleiro",
        "contrato de operação",
      ],
      franchise: ["contrato de franquia", "franquia hoteleira", "marca hoteleira"],
      condoHotel: [
        "condo-hotel",
        "condohotel",
        "multipropriedade",
        "contrato de administração condominial",
        "promessa de compra e venda",
      ],
      lease: ["contrato de locação", "arrendamento"],
      conversion: ["troca de bandeira", "rebranding", "desfiliação", "rescisão contratual"],
    },
    classificationGate: {
      doNotMisclassifyAsHMA: ["administração condominial", "locação", "arrendamento", "multipropriedade"],
      requireForHMA: ["gestão hoteleira", "operação hoteleira", "operador hoteleiro"],
    },
    discoveryWithoutHotelName: {
      preferredStrategies: [
        DISCOVERY_STRATEGIES.D_ISSUER_PORTFOLIO_FIRST,
        DISCOVERY_STRATEGIES.C_DOCUMENT_TYPE_SECTION_FIRST,
      ],
    },
  },

  GB_COMPANIES_HOUSE: {
    sourceFamily: "COMPANIES_HOUSE",
    documentTypes: [
      "accounts",
      "confirmation-statement",
      "mortgage",
      "insolvency",
      "officers",
      "persons-with-significant-control",
    ],
    discoveryWithoutHotelName: {
      preferredStrategies: [DISCOVERY_STRATEGIES.D_ISSUER_PORTFOLIO_FIRST],
      notes:
        "CH is supporting identity/source — not primary opportunity discovery. Prefer RNS / 8-K / prospectus for commercial events. Do not run ownership enrichment campaigns.",
    },
    falsePositiveSources: [
      "SIC hotel classification without property-specific commercial event",
      "Registered office address ≠ physical hotel",
    ],
  },

  CA_SEDAR_PLUS: {
    sourceFamily: "SEDAR_PLUS",
    documentTypes: [
      "Annual Information Form",
      "MD&A",
      "Material Change Report",
      "Prospectus",
      "Material Contract",
      "Business Acquisition Report",
    ],
    discoveryWithoutHotelName: {
      preferredStrategies: [
        DISCOVERY_STRATEGIES.D_ISSUER_PORTFOLIO_FIRST,
        DISCOVERY_STRATEGIES.C_DOCUMENT_TYPE_SECTION_FIRST,
      ],
      englishPhrases: [
        "franchise agreement",
        "management agreement",
        "hotel portfolio",
        "reflag",
        "operator",
      ],
      notes: "Hospitality REIT issuer-first (e.g. American Hotel Income Properties, InnVest legacy patterns) then Material Contract / AIF property schedules",
    },
  },
});

/**
 * Score whether a retrieved document looks like a high-yield CMBS schedule
 * (vs narrative-only Hotel Properties section).
 */
export function scoreCmbsScheduleYield(text = "", url = "") {
  const t = String(text || "");
  const fileHit = SOURCE_FAMILY_TAXONOMY.US_SEC_CMBS_424B.highYieldFileNamePatterns.some((re) =>
    re.test(url),
  );
  // True schedule column headers — exclude Reserve/Trigger cash-management language
  const reserveTrap = /franchise expiration\s+(reserve|trigger)/i.test(t);
  const headerHits = [
    /expiration\/termination of related license/i,
    /related license\/\s*franchise agreement/i,
    /franchise expiration date/i,
    /franchise agreement expiration/i,
    /expiration date of franchise/i,
    /management agreement expiration(?:\s+date)?/i,
    /license agreement expiration(?:\s+date)?/i,
  ].filter((re) => re.test(t)).length;
  const bareFranchiseExpirationOnly =
    /franchise expiration/i.test(t) && headerHits === 0 && reserveTrap;
  const hasPropertyTypeHotel = /property\s*type[\s\S]{0,80}hotel/i.test(t) || /\bhotel\b/i.test(t);
  const tableish = (t.match(/\|/g) || []).length > 20 || /<\/td>/i.test(t);
  const loanOnlyTrap =
    /loan maturity/i.test(t) && headerHits === 0 && !/franchise expiration date/i.test(t);

  let score = 0;
  if (fileHit) score += 2;
  score += headerHits * 4;
  if (hasPropertyTypeHotel) score += 1;
  if (tableish) score += 1;
  if (loanOnlyTrap) score -= 4;
  if (bareFranchiseExpirationOnly) score -= 6;
  if (reserveTrap && headerHits === 0) score -= 5;

  return {
    score,
    fileHit,
    headerHits,
    tableish,
    loanOnlyTrap,
    reserveTrap,
    bareFranchiseExpirationOnly,
    likelyExtractableSchedule: score >= 6 && headerHits >= 1,
  };
}

/**
 * Classify a document into discovery strategy buckets based on how it was found.
 */
export function classifyDiscoveryMethod(meta = {}) {
  if (meta.hotelNameQuery) return DISCOVERY_STRATEGIES.A_HOTEL_NAME_FIRST;
  if (meta.issuerFirst || meta.portfolioFirst) return DISCOVERY_STRATEGIES.D_ISSUER_PORTFOLIO_FIRST;
  if (meta.relatedExhibit || meta.fileTypeFilter) return DISCOVERY_STRATEGIES.E_METADATA_RELATED_EXHIBIT;
  if (meta.formType && meta.sectionPhrase) return DISCOVERY_STRATEGIES.C_DOCUMENT_TYPE_SECTION_FIRST;
  if (meta.contractPhrase) return DISCOVERY_STRATEGIES.B_CONTRACTUAL_PHRASE_FIRST;
  if (meta.hybrid) return DISCOVERY_STRATEGIES.F_HYBRID;
  return DISCOVERY_STRATEGIES.F_HYBRID;
}

export function listNameFreeQueries() {
  const out = [];
  for (const fam of Object.values(SOURCE_FAMILY_TAXONOMY)) {
    for (const q of fam.discoveryWithoutHotelName?.eftsQueries || []) {
      out.push({ sourceFamily: fam.sourceFamily, ...q });
    }
  }
  return out;
}
